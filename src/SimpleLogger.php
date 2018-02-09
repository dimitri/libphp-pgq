<?php

namespace pgq;

class SimpleLogger
{
	private $logfile_fd = False;
	public $loglevel = WARNING;
	public $logfile;

	public function __construct($loglevel, $logfile)
	{
		$this->loglevel = $loglevel;

		if (empty($logfile)) {
			$this->logfile = tempnam("/tmp", "pgq\SimpleLogger-");
			if ($this->logfile !== False)
				rename($this->logfile, sprintf("%s.log", $this->logfile));
		} else
			$this->logfile = $logfile;

		date_default_timezone_set(DEFAULT_TZ);
		$this->open();
	}

	function __destruct()
	{
		/* Only close the logfile when we opened it ourselves */
		if (!is_resource($this->logfile)) {
			$this->notice("Closing log file " . $this->logfile);
			fclose($this->logfile_fd);
		}
	}

	/**
	 * Opens the given filename, or use the given stream resource (STDOUT)
	 */
	private function open()
	{
		if (is_resource($this->logfile)) {
			$this->logfile_fd = $this->logfile;
		} else {
			$this->logfile_fd = fopen($this->logfile, "a+");
		}

		if ($this->logfile_fd === false) {
			fprintf(STDERR, "FATAL: couldn't open '%s' \n", $this->logfile);
		} else
			$this->notice("Logging to file '%s'", $this->logfile);
	}

	/**
	 * At reload time, don't forget to reopen $this->logfile
	 * This allows for log rotating.
	 */
	public function reopen()
	{
		$this->warning("Closing log file " . $this->logfile);
		fclose($this->logfile_fd);
		$this->logfile_fd = False;
		$this->open();
	}

	/**
	 * Check that the logfile has been opened with success.
	 * @return bool
	 */
	public function check()
	{
		return $this->logfile_fd !== false;
	}

	function debug()
	{
		$args = func_get_args();
		$this->_log(DEBUG, $args);
	}

	function verbose()
	{
		$args = func_get_args();
		$this->_log(VERBOSE, $args);
	}

	function notice()
	{
		$args = func_get_args();
		$this->_log(NOTICE, $args);
	}

	function warning()
	{
		$args = func_get_args();
		$this->_log(WARNING, $args);
	}

	function error()
	{
		$args = func_get_args();
		$this->_log(ERROR, $args);
	}

	function fatal()
	{
		$args = func_get_args();
		$this->_log(FATAL, $args);
	}

	function _log($level, $args)
	{
		if ($level >= $this->loglevel) {
			$format = array_shift($args);
			$date = date("Y-m-d H:i:s");
			$vargs = array_merge(array($date, $this->strlevel($level)), $args);
			$mesg = vsprintf("%s\t%s\t" . $format . "\n", $vargs);

			fwrite($this->logfile_fd, $mesg);
		}
	}

	function strlevel($level)
	{
		switch ($level) {
			case DEBUG:
				return "DEBUG";
				break;

			case VERBOSE:
				return "VERBOSE";
				break;

			case NOTICE:
				return "NOTICE";
				break;

			case WARNING:
				return "WARNING";
				break;

			case ERROR:
				return "ERROR";
				break;

			case FATAL:
				return "FATAL";
				break;

			default:
				return $level;
		}
	}

	/**
	 * On the fly log level control utility functions
	 */
	public function logless()
	{
		$this->_log($this->loglevel, array("Incrementing loglevel"));

		if ($this->loglevel < FATAL)
			$this->loglevel += 10;

		$this->_log($this->loglevel,
			array("loglevel is now %s", $this->strlevel($this->loglevel)));
	}

	public function logmore()
	{
		$this->_log($this->loglevel, array("Decrementing loglevel"));

		if ($this->loglevel > DEBUG)
			$this->loglevel -= 10;

		$this->_log($this->loglevel,
			array("loglevel is now %s", $this->strlevel($this->loglevel)));
	}
}

?>