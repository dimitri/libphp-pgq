<?php
require_once("pgq/PGQ.php");
require_once("pgq/SystemDaemon.php");

/**
 * PGQConsumer is a SystemDaemon providing the PGQ SQL API for PHP
 * applications, and implementing a simple Consumer model atop of it.
 *
 * Depending on your needs, please check more specialized classes
 * PGQRemoteConsumer and PGQEventRemoteConsumer.
 *
 * Online references about PGQ are to be found here:
 *  http://skytools.projects.postgresql.org/doc/pgq-admin.html
 *  http://skytools.projects.postgresql.org/doc/pgq-sql.html
 *  http://skytools.projects.postgresql.org/doc/pgq-nodupes.html
 * 
 *  http://kaiv.wordpress.com/2007/10/19/skytools-database-scripting-framework-pgq/
 *
 *  http://www.pgcon.org/2008/schedule/events/79.en.html
 *
 * When implementing a PGQConsumer, you should define following methods:
 *  - php_error_hook()
 *  - config()
 *  - process_event(&$event)  --- $event is a PGQEvent
 *
 * See SystemDaemon header documentation for details about first two
 * functions.
 *
 * The consumer model provided by PGQConsumer works by letting the
 * inheriting worker define how to process a single event. The
 * function process_event() will get called while in a transaction, so
 * don't issue COMMIT or ROLLBACK on $this->pg_src_con.
 *
 * You should return one of those constants from process_event():
 * PGQ_EVENT_OK, PGQ_EVENT_FAILED, PGQ_EVENT_RETRY or PGQ_ABORT_BATCH.
 *
 * The last will force ROLLBACK, use it when you encounter PostgreSQL
 * level errors: the transactions are no more valid, you return
 * PGQ_ABORT_BATCH and PGQConsumer will handle: batch is not finished,
 * the error is considered not fatal.
 *
 * When you return PGQ_EVENT_RETRY or PGQ_EVENT_FAILED, PGQConsumer
 * will call the matching pgq SQL function to tag the Event. In the
 * retry case, PGQConsumer will get $event->retry_delay attribute to
 * give to the SQL level API, and in the failed case,
 * $event->failed_reason.
 *
 * PGQConsumer supports same commands as SystemDaemon, and some more:
 *   install, uninstall, create_queue, drop_queue, register, unregister
 *   failed, delete <event_id|all>, retry <event_id|all>
 *
 * See SystemDaemon documentation for using this class at PHP and
 * command line levels.
 */

abstract class PGQConsumer extends SystemDaemon
{
  protected $connected  = False;
  protected $src_constr;
  protected $pg_src_con;
		
  protected $cname;  // consumer name (pgq consumer id)
  protected $qname; // queue name
	
  public function __construct($cname, $qname, $argc, $argv, $src_constr) 
  {
    $this->cname = $cname;
    $this->qname = $qname;
    
    $this->commands[] = "install";
    $this->commands[] = "uninstall";
    $this->commands[] = "check";
    $this->commands[] = "create_queue";
    $this->commands[] = "drop_queue";
    $this->commands[] = "register";
    $this->commands[] = "unregister";
    $this->commands[] = "failed";
    $this->commands[] = "delete";
    $this->commands[] = "retry";
    
    $this->src_constr  = $src_constr;
		
    parent::__construct($argc, $argv);
  }

  /**
   * Implement those functions when inheriting from this class.
   */
  public function config() {}

  /**
   * Optionnal hooks
   */
  public function preprocess_event($event) {}
  public function postprocess_event($event) {}
	
  /**
   * We overload SystemDaemon::main() in order to support our specific
   * commands too.
   */
  public function main($argc, $argv) {
    if( $argc < 2 ) {
      fprintf(STDERR, $this->usage($this->name));
      exit(1);
    }
		
    switch( $argv[1] )
      {
      case "install":
	$pid = $this->getpid();
	if( $pid !== False )
	  $this->log->fatal("Can not install an already running daemon.");
	else {
	  $ret = $this->install();
	  exit($ret ? 0 : 1);
	}
	break;
	
      case "uninstall":
	$pid = $this->getpid();
	if( $pid !== False )
	  $this->log->fatal("Can not uninstall a running daemon.");
	else {
	  $ret = $this->uninstall();
	  exit($ret ? 0 : 1);
	}
	break;
	
      case "check":
	$pid = $this->getpid();
	if( $pid !== False ) {
	  $this->log->fatal("Daemon already running.");
	  $this->status();
	}
	else {
	  $ret = $this->check();
	  exit($ret ? 0 : 1);
	}
	break;

      case "create_queue":
	$pid = $this->getpid();
	if( $pid !== False )
	  $this->log->fatal("Can not create queue for an already running daemon.");
	else {
	  $this->connect();
	  $ret = $this->create_queue();
	  $this->deconnect();
	  
	  exit($ret ? 0 : 1);
	}
	break;

      case "drop_queue":
	$pid = $this->getpid();
	if( $pid !== False )
	  $this->log->fatal("Can not drop queue for a running daemon.");
	else {
	  $this->connect();
	  $ret = $this->drop_queue();
	  $this->deconnect();
	  
	  exit($ret ? 0 : 1);
	}
	break;

      case "register":
	$pid = $this->getpid();
	if( $pid !== False )
	  $this->log->fatal("Can not register already running daemon.");
	else {
	  $this->connect();
	  $ret = $this->register();
	  $this->deconnect();
	  
	  exit($ret ? 0 : 1);
	}
	break;
	
      case "unregister":
	$pid = $this->getpid();
	if( $pid !== False )
	  $this->log->fatal("Can not unregister already running daemon.");
	else {
	  $this->connect();
	  $ret = $this->unregister();
	  $this->deconnect();
	  
	  exit($ret ? 0 : 1);
	}
	break;

      case "failed":
      	$this->connect();
      	$events = $this->failed_event_list();
      	if( $events !== False )
			foreach( $events  as $event ) {
	  			echo $event."\n";
			}
		else
			$this->log->warning("Failed event list is empty.");
	$this->deconnect();
	exit(0);
	break;

      case "delete":
	if( $argc < 3 ) {
	  $this->log->fatal("delete <event_id> [<event_id> ...]");
	  exit(1);
	}
	$this->connect();
	if( $argv[2] == "all" )
	  $this->failed_event_delete_all();
	else {
	  for($i = 2; $i < $argc; $i++) {
	    $this->failed_event_delete($argv[$i]);
	  }
	}
	$this->deconnect();
	exit(0);
	break;

      case "retry":
	if( $argc < 3 ) {
	  $this->log->fatal("delete <event_id> [<event_id> ...]");
	  exit(1);
	}
	$this->connect();
	if( $argv[2] == "all" )
	  $this->failed_event_retry_all();
	else {
	  for($i = 2; $i < $argc; $i++) {
	    $this->failed_event_retry($argv[$i]);
	  }
	}
	$this->deconnect();
	exit(0);
	break;
	
      default:
	/**
	 * Support daemon commands: start, stop, reload, restart, ...
	 */
	parent::main($argc, $argv);
	break;
      }
  }

  /**
   * Check installation is ok before to run.
   */
  public function run() {
    if( $this->check() )
      parent::run();
    else
      $this->stop();
  }
	
  /**
   * Stop is called either at normal exit or when receiving an error,
   * PHP errors included.
   */
  public function stop() {
    if( $this->connected ) {
      $this->rollback();
      $this->deconnect();
    }
    parent::stop();
  }
	
  /**
   * Process available batches, sleeping only when next_batch() returns null.
   *
   * We want to consume empty batches as fast as possible to avoid
   * lagging behind.
   *
   * Sleep only when next_batch() is null.
   *  null:  no more batches awaiting us to process, we sleep
   *  True:  successfully processed a batch, have it processed
   *  False: failed to get next_batch, sleep (temp. failure?)
   *
   * The sleeping is cared about at SystemDaemon level, in run()
   * method, between process() calls.
   */
  public function process()
  {
    $sleep = False;
    
    $this->connect();
    while( ! $sleep && ! $this->killed ) {
      $batch_id = $this->next_batch();

      switch( $batch_id ) {
      case null:
	$this->log->verbose("PGQConsumer.process: next_batch is null, sleep.");
	$sleep = True;
	break;

      case False:
	$this->log->verbose("PGQConsumer.process: failed to get batch.");
	break;

      case True:
	$this->log->verbose("PGQConsumer.process: \$this->process_batch(%d).",
			    $batch_id);

	$this->process_batch($batch_id);
	break;
      }
    }
    $this->deconnect();
  }

  /**
   * We want that all events of a same batch to have been either
   * processed or postponed by the end of this loop, so we only
   * COMMIT; when no error arise.
   */
  public function process_batch($batch_id) {
    $events = $this->preprocess_batch($batch_id);

    if( $events === False) {
	$this->log->verbose("PGQConsumer.preprocess_batch got not events (False).");
      return False;
    }

    /**
     * Event processing loop!
     */
    $abort_batch = False;

    foreach( $events as $event ) {
      if( $abort_batch )
	break;

      if( $this->preprocess_event($event) === PGQ_ABORT_BATCH ) {
	$abort_batch = True;
      }
      else {
	$tag = $event->tag( $this->process_event($event) );
	$this->log->verbose("PGQConsumer.process_batch processed event %d of batch %d, tag %d",
	                    $event->id, $batch_id, $tag);

	switch( $tag  ) {
	case PGQ_ABORT_BATCH:
	  $this->log->verbose("PGQConsumer.process_batch got PGQ_ABORT_BATCH");
	  $abort_batch = True;
	  break;

	case PGQ_EVENT_FAILED:
	  $this->event_failed($batch_id, $event);
	  break;

	case PGQ_EVENT_RETRY:
	  $this->event_retry($batch_id, $event);
	  break;

	case PGQ_EVENT_OK:
	  break;
	}
	
	if( ! $abort_batch ) {
	  if( $this->postprocess_event($event) === PGQ_ABORT_BATCH) {
	    $this->log->verbose("PGQConsumer.postprocess_event(%d) abort batches", $event->id);
	    $abort_batch = True;
	  }
	}
      }
    }
    return $this->postprocess_batch($batch_id, $abort_batch);
  }

  /**
   * Prepare batch processing, returns PGQEvent list or False if
   * empty.
   *
   * Exists as a separate function for implementers to accomodate
   * easily, see PGQRemoteConsumer.
   */
  public function preprocess_batch($batch_id) {
    pg_query($this->pg_src_con, "BEGIN;");

    $events = $this->get_batch_events($batch_id);
		
    if( $events === False ) {
      // batch with no event
      $this->log->debug("PGQConsumer.preprocess_batch got no events");
      $this->finish_batch($batch_id);
      pg_query($this->pg_src_con, "COMMIT;");
      return False;
    }
    $this->log->debug("PGQConsumer.preprocess_batch got %d events", count($events));
    return $events;
  }

  /**
   * Conclude batch processing, COMMIT or ROLLBACK if $abort_batch,
   * and call finish_batch().
   */
  public function postprocess_batch($batch_id, $abort_batch = False) {
    if( $abort_batch) {
    	$this->log->warning("PGQConsumer.postprocess_batch aborts: ROLLBACK");
      $this->rollback();
      return False;
    }

    if( $this->finish_batch($batch_id) === False )
      $this->log->warning("Could not mark batch id %d as finished", $batch_id);
    
    pg_query($this->pg_src_con, "COMMIT;");
    return True;
  }

  /**
   * print out deamon status, and detailed queue informations
   */
  public function status() {
    $pid = $this->getpid();
    if( $pid === False )
      printf("PGQConsumer %s is not running.\n", $this->name);
    else {
      printf("PGQConsumer %s is running with pid %d\n", $this->name, $pid);

      $this->connect();
      $status = $this->get_consumer_info();
      foreach( $status as $k => $v)
	printf("%s: %s\n", $k, $v);
      
      $this->deconnect();
    }
  }
  
  /**
   * Consumer installation: create the queue and register consumer.
   */
  protected function install() {
    $this->connect();
    $ret = $this->create_queue();
    
    if( $ret ) {
      $ret = $this->register();
    }
    $this->deconnect();

    return $ret;
  }

  /**
   * Consumer uninstall: unregister the consumer and drop the queue
   */
  protected function uninstall() {
    $this->connect();
    $ret = $this->unregister();
    
    if( $ret ) {
      $ret = $this->drop_queue();
    }
    $this->deconnect();

    return $ret;
  }

  /**
   * Consumer check: check the queue exists and the consumer is
   * registered.
   */
  protected function check() {
    $this->connect();
    $ret = $this->queue_exists();

    if( $ret ) {
      $ret = $this->is_registered();
    }
    $this->deconnect();

    return $ret;
  }

  /**
   * Connects to the conw & conp connection strings.
   */
  public function connect($force = False) { 
    if( $this->connected && ! $force ) {
      $this->log->notice("connect called when connected is True");
      return;
    }

    if( $this->src_constr != "" ) {
      $this->log->verbose("Opening pg_src connexion '%s'.", $this->src_constr);
      $this->pg_src_con = pg_connect($this->src_constr);
      
      if( $this->pg_src_con === False ) {
	$this->log->fatal("Could not open pg_src connection '%s'.",
			  $this->src_constr);
	$this->stop();
      }
    }
    $this->connected = True;
  }
	
  /**
   * Deconnect from databases
   */
  public function deconnect() {
    if( ! $this->connected ) {
      $this->log->notice("deconnect called when $this->connected is False");
      return;
    }
    
    if( $this->pg_src_con != null && $this->pg_src_con !== False ) {
      $this->log->verbose("Closing pg_src connection '%s'.",
			  $this->src_constr);
      pg_close($this->pg_src_con);
      $this->pg_src_con = null;
    }
    $this->connected = False;
  }
	
  /**
   * ROLLBACK ongoing transactions on src & dst connections
   */
  protected function rollback() {
    if( $this->pg_src_con != null && $this->pg_src_con !== False ) {
      $this->log->notice("ROLLBACK pg_src connection '%s'.",
			 $this->src_constr);
      pg_query($this->pg_src_con, "ROLLBACK;");
    }
  }
	
  /**
   * This hook is called when a PHP error at level
   * E_USER_ERROR or E_ERROR is raised.
   * 
   * Those errors are not considered fatal: abort current batch
   * processing, but don't have the PGQConsumer daemon quit.
   */
  protected function php_error_hook() {
    $this->rollback();
  }
    

  protected function create_queue() {
    return PGQ::create_queue($this->log, $this->pg_src_con, $this->qname);
  }		

  protected function drop_queue() {
    return PGQ::drop_queue($this->log, $this->pg_src_con, $this->qname);
  }

  protected function queue_exists() {
    return PGQ::queue_exists($this->log, $this->pg_src_con, $this->qname);
  }
	
  protected function register() {			
    return PGQ::register($this->log, $this->pg_src_con, 
			 $this->qname, $this->cname);
  }
	
  protected function unregister() {
    return PGQ::unregister($this->log, $this->pg_src_con, 
			   $this->qname, $this->cname);
  }
	
  protected function is_registered() {
    return PGQ::is_registered($this->log, $this->pg_src_con, 
			      $this->qname, $this->cname);
  }

  protected function get_consumer_info() {
    return PGQ::get_consumer_info($this->log, $this->pg_src_con, 
				  $this->qname, $this->cname);
  }
	
  protected function next_batch() {
    return PGQ::next_batch($this->log, $this->pg_src_con, 
			   $this->qname, $this->cname);
  }

  protected function finish_batch($batch_id) {
    return PGQ::finish_batch($this->log, $this->pg_src_con, $batch_id);
  }

  protected function get_batch_events($batch_id) {
    return PGQ::get_batch_events($this->log, $this->pg_src_con, $batch_id);
  }
	
  protected function event_failed($batch_id, $event) {
    return PGQ::event_failed($this->log, $this->pg_src_con, $batch_id, $event);
  }

  protected function event_retry($batch_id, $event) {
    return PGQ::event_retry($this->log, $this->pg_src_con, $batch_id, $event);
  }

  protected function failed_event_list() {
    return PGQ::failed_event_list($this->log, $this->pg_src_con,
				  $this->qname, $this->cname);
  }

  protected function failed_event_delete_all() {
    return PGQ::failed_event_delete_all($this->log, $this->pg_src_con,
					$this->qname, $this->cname);
  }

  protected function failed_event_delete($event_id) {
    return PGQ::failed_event_delete($this->log, $this->pg_src_con,
				    $this->qname, $this->cname, $event_id);
  }

  protected function failed_event_retry_all() {
    return PGQ::failed_event_retry_all($this->log, $this->pg_src_con,
				       $this->qname, $this->cname);
  }

  protected function failed_event_retry($event_id) {
    return PGQ::failed_event_retry($this->log, $this->pg_src_con,
				   $this->qname, $this->cname, $event_id);
  }
}
