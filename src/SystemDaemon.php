<?php
namespace pgq;
declare(ticks=1);

/**
 * Classes implementing pgq\SystemDaemon must implement following methods:
 *  - php_error_hook()
 *  - kill_hook($pid)
 *  - config()
 *  - process()
 *
 * None take argument.
 *
 * php_error_hook() allows you to define what to in case of PHP error
 * of E_ERROR or E_USER_ERROR severity. pgq\SystemDaemon will not consider
 * them as fatal by default, it'll just log them and continue. You're
 * free to disagree by defining a custom php_error_hook().
 *
 * config() will get called at init stage and upon receiving a SIGHUP,
 * which can be made with the reload command. The config() call is
 * made between two process() run.
 *
 * config() should provide a way to set at least those parameters:
 *  $this->delay, the sleeping time between two process() call
 *  $this->logfile, where logs are sent to
 *  $this->loglevel, the verbosity.
 *
 * process() will get run form an infinite loop, which you still can
 * break out of by sendig SIGTERM or SIGINT to the daemon, which can
 * be made with the kill and stop command.
 *
 * You can implement a kill_hook($pid) function which will get called before
 * signaling the running daemon.
 *
 * The stop command (or kill -INT <pid>) won't have immediate effect
 * but will rather get the daemon to quit at next loop beginning:
 * current processing is not canceled. The kill command (or kill -TERM
 * <pid>) will have immediate effect.
 *
 * Supported command line commands are:
 *  start, stop, kill, reload, restart, status, logless, logmore
 *
 * The logmore and logless commands resp. send SIGUSR1 and SIGUSR2
 * signals to the running daemon and have immediate effect.
 *
 * Classic usage is to define a daemon class implementing both
 * config() and process() methods, then
 *   MyDaemonInstance = MyDaemon($argc, $argv);
 *
 * Here the daemon starts, forking and all, and exit() the father:
 * you're done. If you save your daemon PHP code into foo.php, you now
 * control the running daemon instance with thos commands:
 *  php foo.php start
 *  php foo.php status
 *  php foo.php reload
 *  php foo.php logmore
 *  php foo.php logless
 *  php foo.php stop
 */
abstract class SystemDaemon
{
	protected $loglevel = DEBUG;
	protected $logfile;
	protected $delay = 15;
	protected $log;
	protected $commands = array("start", "stop", "kill", "restart",
		"status", "reload",
		"logmore", "logless");

	protected $name;
	protected $fullname;

	protected $debug = false; // if true, stdin/out/err won't be closed

	protected $pidfile;
	protected $sid;
	protected $killed = False;
	protected $huped = False;

	public function __construct($argc, &$argv)
	{
		$this->fullname = $argv[0];
		$this->name = basename($this->fullname);
		$this->pidfile = sprintf("%s/%s.pid", PIDFILE_PREFIX, $this->name);

		$this->log = new SimpleLogger(WARNING, STDOUT);
		$this->main($argc, $argv);
	}

	/**
	 * Implement those functions when inheriting from this class.
	 */
	protected function config()
	{
	}

	protected function process()
	{
	}

	protected function php_error_hook()
	{
	}

	protected function kill_hook($pid)
	{
	}

	/**
	 * main is responsible of command line parsing and daemon interactions
	 */
	public function main($argc, $argv)
	{
		if ($argc != 2) {
			fprintf(STDERR, $this->usage($this->name));
			exit(1);
		}

		switch ($argv[1]) {
			case "start":
				$pid = $this->getpid();
				if ($pid !== false) {
					printf("Trying to start already running daemon '%s' [%s] \n",
						$this->name, $pid);
					exit(4);
				} else
					$this->start();
				break;

			case "stop":
				$pid = $this->checkpid(4);
				posix_kill($pid, SIGINT);
				break;

			case "kill":
				$pid = $this->checkpid(4);
				$this->kill_hook($pid);
				posix_kill($pid, SIGTERM);
				break;

			case "restart":
				$pid = $this->checkpid(4);
				posix_kill($pid, SIGINT);

				while (file_exists($this->pidfile)) {
					sleep(1);
				}
				$this->start();
				break;

			case "status":
				$this->status();
				break;

			case "reload":
				$pid = $this->checkpid(4);
				posix_kill($pid, SIGHUP);
				break;

			case "logmore":
				$pid = $this->checkpid(4);
				posix_kill($pid, SIGUSR1);
				break;

			case "logless":
				$pid = $this->checkpid(4);
				posix_kill($pid, SIGUSR2);
				break;

			default:
				printf($this->usage($this->name));
				exit(1);
				break;
		}
	}

	/**
	 * start the daemon, fork(), exit from the father, start a new
	 * session group from the child, and close STDIN, STDOUT and STDERR.
	 *
	 * The first $this->config() call is made here.
	 */
	public function start()
	{
		/**
		 * UNIX daemon startup.
		 */

		$pid = pcntl_fork();
		if ($pid < 0) {
			fprintf(STDERR, "fork failure: %s",
				posix_strerror(posix_get_last_error()));
			exit;
		} else if ($pid) {
			/**
			 * Father exit, and let is daemon child work.
			 */
			exit;
		} else {
			/**
			 * The child works.
			 */
			$this->sid = posix_setsid();

			if ($this->sid < 0) {
				fprintf(STDERR, "setsid() failure: %s",
					posix_strerror(posix_get_last_error()));
				exit;
			}

			if (!$this->debug) {
				// don't forget a daemon gets to close those
				fclose(STDIN);
				fclose(STDOUT);
				fclose(STDERR);

				// reopen ids 0, 1 and 2 so that hard coded libs have no problem
				fopen("/dev/null", "a+"); // STDIN
				fopen("/dev/null", "a+"); // STDOUT
				fopen("/dev/null", "a+"); // STDERR
			}

			/**
			 * config() provides log filename and loglevel
			 */
			$this->config();
			unset($this->log);
			$this->log = new SimpleLogger($this->loglevel, $this->logfile);

			if (!$this->log->check()) {
				fprintf(STDERR, "FATAL: could not open logfile '%s': %s",
					$this->logfile,
					posix_strerror(posix_get_last_error()));
				exit;
			}
			$this->log->notice("Init done (config & logger)");

			// Redefine PHP language error handlers
			set_error_handler(array($this, "phpFault"));
			set_exception_handler(array($this, "exceptFault"));

			$this->createpidfile();

			/**
			 * Handle following signals
			 */
			pcntl_signal(SIGTERM, array(&$this, "handleSignals"));
			pcntl_signal(SIGINT, array(&$this, "handleSignals"));
			pcntl_signal(SIGHUP, array(&$this, "handleSignals"));
			pcntl_signal(SIGUSR1, array(&$this, "handleSignals"));
			pcntl_signal(SIGUSR2, array(&$this, "handleSignals"));

			/**
			 * Now we're ready to run.
			 */
			$this->run();
		}
	}

	/**
	 * At quitting time, drop the pidfile and write to the logs we're done.
	 */
	public function stop()
	{
		$this->droppidfile();
		$this->log->debug("Quitting...");
		exit(0);
	}

	/**
	 * status will simply print out if daemon is running, and under which pid.
	 */
	public function status()
	{
		$pid = $this->getpid();
		if ($pid === False)
			printf("pgq\SystemDaemon %s is not running.\n", $this->name);
		else {
			printf("pgq\SystemDaemon %s is running with pid %d\n", $this->name, $pid);
		}
	}

	/**
	 * Print out the supported commands.
	 */
	public function usage($progname)
	{
		return sprintf("%s: %s\n", $progname, implode("|", $this->commands));
	}

	/**
	 * checkpid() will call getpid() and exit with the given error code
	 * when the daemon is not running.
	 */
	public function checkpid($errcode)
	{
		$pid = $this->getpid();

		if ($pid === false) {
			fprintf(STDERR, "No daemon '%s' running \n", $this->name);
			exit($errcode);
		}
		return $pid;
	}

	/**
	 * getpid() ensure that the daemon is running, returning its pid
	 * when it's the case and False when it's no more running.
	 *
	 * Current implementation assumes a Linux environment and abuse
	 * /proc facility to ensure that pidfile content matches our daemon.
	 */
	public function getpid()
	{
		if (file_exists($this->pidfile)) {
			$pid = file_get_contents($this->pidfile);

			if (!file_exists(sprintf("/proc/%s", $pid))) {
				$this->droppidfile();
				return false;
			}

			/**
			 * Both file_get_contents() and fopen()/fgetc() methods are
			 * unable to get the /proc/... file content
			 */
			$cmdline = sprintf("/proc/%s/cmdline", $pid);
			unset($cmd);
			exec(sprintf("cat %s", $cmdline), $cmd);

			$cmd = explode("\0", $cmd[0]);
			$cmd = $cmd[1];

			if (basename($cmd) == basename($this->fullname))
				return $pid;

			else {
				printf("pidfile: /proc/%s/cmdline does not match '%s' \n",
					$pid, $this->fullname);
				$this->droppidfile();
				return false;
			}
		} else
			return false;
	}

	/**
	 * startup time utility to write our pid to pidfile.
	 *
	 * We check for stale pidfile and remove it if necessary.
	 */
	public function createpidfile()
	{
		if (file_exists($this->pidfile)) {
			$this->error("Pidfile '%s' already exists", $this->pidfile);
			$this->droppidfile();
		}
		$fd = fopen($this->pidfile, "w+");

		if ($fd !== false) {
			if (fwrite($fd, getmypid()) === False) {
				$this->log->fatal("Pidfile fwrite('%s') failed: %s",
					$this->pidfile,
					posix_strerror(posix_get_last_error()));
				$this->droppidfile();
				exit(2);
			}

			if (fclose($fd) === False) {
				$this->log->fatal("Pidfile fclose('%s') failed: %s",
					$this->pidfile,
					posix_strerror(posix_get_last_error()));
				$this->droppidfile();
				exit(2);
			}
		} else {
			$this->log->fatal("Pidfile fopen('%s') failed: %s",
				$this->pidfile,
				posix_strerror(posix_get_last_error()));
			exit(2);
		}
		$this->log->notice("Pidfile '%s' created with '%s'",
			$this->pidfile, getmypid());
	}

	/**
	 * drop our pidfile
	 */
	public function droppidfile()
	{
		$this->log->notice("rm %s", $this->pidfile);

		if (file_exists($this->pidfile)) {
			if (!unlink($this->pidfile))
				$this->log->error("Cound not unlink '%s'", $this->pidfile);
		} else
			$this->log->error("Pidfile '%s' does not exist", $this->pidfile);
	}

	/**
	 * The run() function leads the daemon work, by calling user
	 * function process() and sleeping $this->delay, as long as we
	 * didn't get INT or TERM signal.
	 */
	public function run()
	{
		$this->log->notice("run");

		while (!$this->killed) {
			if ($this->huped) {
				$this->config();

				// Don't forget to forward the loglevel change if any
				if ($this->loglevel)
					$this->log->loglevel = $this->loglevel;

				// And to force logfile reopening (be nice to log rotating)
				$this->log->reopen();

				$this->huped = False;
			}

			$this->process();

			if (!$this->killed) {
				$this->log->debug("sleeping %d seconds", $this->delay);
				usleep(1000000 * $this->delay);
			}
		}
		$this->stop();
	}

	/**
	 * React to supported user signals
	 */
	public function handleSignals($sig)
	{
		switch ($sig) {
			case SIGTERM:
				$this->log->warning("Received TERM signal.");
				$this->stop();
				break;

			case SIGINT:
				$this->log->warning("Received INT signal.");
				$this->killed = True;
				break;

			case SIGHUP:
				$this->log->warning("Received HUP signal");
				$this->huped = True;
				break;

			case SIGUSR1:
				$this->log->warning("Received USR1 signal, logging more");
				$this->log->logmore();
				break;

			case SIGUSR2:
				$this->log->warning("Received USR2 signal, logging less");
				$this->log->logless();
				break;
		}
	}

	/**
	 * Register our own PHP language error handlers
	 */
	function phpFault($errno, $errstr, $errfile, $errline)
	{
		$message = "PHP: " . strip_tags($errstr) . " in {$errfile}:{$errline}";

		switch ((int)$errno) {
			case E_PARSE:
			case E_CORE_ERROR:
			case E_CORE_WARNING:
			case E_COMPILE_ERROR:
			case E_COMPILE_WARNING:
				$this->log->fatal($message);
				$this->stop();
				break;

			case E_USER_ERROR:
			case E_ERROR:
			case E_RECOVERABLE_ERROR:
				$this->log->error($message);
				$this->php_error_hook();
				break;

			case E_WARNING:
			case E_USER_WARNING:
				$this->log->warning($message);
				return true;
				break;

			case E_STRICT:
			case E_DEPRECATED:
			case E_USER_DEPRECATED:
			case E_NOTICE:
			case E_USER_NOTICE:
				$this->log->notice($message);
				return true;
				break;
		}
		return false;
	}

	/**
	 * We also support non-catched exceptions and consider them as fatal
	 * errors.
	 */
	function exceptFault($exception)
	{
		$trace = $exception->getTrace();
		$message = $exception->getMessage();

		if (is_array($trace) && count($trace) > 0) {
			$message .= "; source: " . $trace[0]["file"] . ":" . $trace[0]["line"];
		}

		$this->log->fatal($message);
		$this->stop();
	}
}

?>
