<?php
require_once("SystemDaemon.php");
require_once("PGQEvent.php");

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
   * Queue creation
   */
  protected function create_queue() {
    $sql_cq = sprintf("SELECT pgq.create_queue('%s');", 
		      pg_escape_string($this->qname));
    $this->log->verbose("create_queue: %s", $sql_cq);

    $r = pg_query($this->pg_src_con, $sql_cq);
    if( $r === False ) {
      $this->log->fatal("Could not create queue '%s'", $this->qname);
      return False;
    }
    $result = (pg_fetch_result($r, 0, 0) == 1);
    
    if( ! $result ) {
    	$this->log->error("PGQConsumer: could not create queue.");
    }
    
    return $result;
  }		

  /**
   * Queue drop
   */
  protected function drop_queue() {
    $sql = sprintf("SELECT pgq.drop_queue('%s');", 
		   pg_escape_string($this->qname));
    $this->log->verbose("drop_queue: %s", $sql);

    $r = pg_query($this->pg_src_con, $sql);
    if( $r === False ) {
      $this->log->fatal("Could not drop queue '%s'", $this->qname);
      return False;
    }
    return pg_fetch_result($r, 0, 0) == 1;
  }

  /**
   * Queue exists?
   */
  protected function queue_exists() {
    $sql = sprintf("SELECT * FROM pgq.get_queue_info()");
		                  
    $this->log->verbose("%s", $sql);
    if( ($r = pg_query($this->pg_src_con, $sql)) === False ) {
      $this->log->error("Could not get queue info");
      return False;
    }		
    $queues    = array();
    $resultset = pg_fetch_all($r);
    
    if( $resultset === False ) {
      $this->log->notice("PGQConsumer.queue_exists() got no queue.");
      return False;
    }
    
    foreach( $resultset as $row ) {
      if( $row["queue_name"] == $this->qname )
	return True;
    }
    return False;
  }
	
  /**
   * Register PGQ Consumer.
   *
   * @return: boolean
   */
  protected function register() {			
    $sql_rg = sprintf("SELECT pgq.register_consumer('%s', '%s');",
		      pg_escape_string($this->qname),
		      pg_escape_string($this->cname));
    
    $this->log->verbose("%s", $sql_rg);
    if( ($r = pg_query($this->pg_src_con, $sql_rg)) === False ) {
      $this->log->warning("Could not register consumer '%s' ".
			  "to queue '%s'", $this->cname, $this->qname);
      return False;
    }
    
    $registered = pg_fetch_result($r, 0, 0);
    if( $registered == "1" ) {
      return True;
    }
    else {
      $this->log->warning("Register Consumer failed (%d).", $registered);
      return False;
    }
  }
	
  /**
   * Unregister PGQ Consumer. Called from stop().
   */
  protected function unregister() {
    if( ! $this->connected )
      $this->connect();
    
    $sql_ur = sprintf("SELECT pgq.unregister_consumer('%s', '%s');",
		      pg_escape_string($this->qname),
		      pg_escape_string($this->cname));
    
    $this->log->verbose("%s", $sql_ur);
    if( pg_query($this->pg_src_con, $sql_ur) === False ) {
      $this->log->fatal("Could not unregister consumer '%s' ".
			"to queue '%s'", $this->cname, $this->qname);
    }
  }
	
  /**
   * are we registered already?
   */
  protected function is_registered() {
    $infos = $this->get_consumer_info();
    
    if( $infos !== False ) {
      $this->log->debug("is_registered %s",
			( $infos["queue_name"] == $this->qname
		       && $infos["consumer_name"] == $this->cname
			  ?
			  "True" : "False" ));
      
      return $infos["queue_name"] == $this->qname 
	&& $infos["consumer_name"] == $this->cname;
    }
    $this->log->warning("is_registered: count not get consumer infos.");
    return False;
  }

  /**
   * get_consumer_info
   */
  protected function get_consumer_info() {
    $sql_ci = sprintf("SELECT * FROM pgq.get_consumer_info('%s', '%s')",
		      pg_escape_string($this->qname),
		      pg_escape_string($this->cname));
    
    $this->log->debug("%s", $sql_ci);
    $result = pg_query($this->pg_src_con, $sql_ci);
    
    if(  $result === False ) {
      $this->log->warning("Could not get consumer info for '%s'", $this->cname);
      return False;
    }
    if( pg_num_rows($result) == 1 )
      return pg_fetch_assoc($result, 0);
    else {
      $this->log->warning("get_consumer_info('%s', '%s') ".
			  "did not get 1 row.", $this->qname, $this->cname);
      return False;
    }
  }
	
  /**
   * Get next batch id
   *
   * Returns null when pgq.next_batch() returns null or failed.
   */
  protected function next_batch() {
    $sql_nb = sprintf("SELECT pgq.next_batch('%s', '%s')",
		      pg_escape_string($this->qname),
		      pg_escape_string($this->cname));
    
    $this->log->verbose("%s", $sql_nb);
    if( ($r = pg_query($this->pg_src_con, $sql_nb)) === False ) {
      $this->log->error("Could not get next batch");
      return False;
    }
    
    $batch_id = pg_fetch_result($r, 0, 0);	
    $this->log->debug("Get batch_id %s (isnull=%s)", 
		      $batch_id, 
		      ($batch_id === null ? "True" : "False"));
    return $batch_id;
  }

  /**
   * Get batch events
   * 
   * @return array(PGQEvents);
   */
  protected function get_batch_events($batch_id) {
    $sql_ge = sprintf("SELECT * FROM pgq.get_batch_events(%d)", (int)$batch_id);
		                  
    $this->log->verbose("%s", $sql_ge);
    if( ($r = pg_query($this->pg_src_con, $sql_ge)) === False ) {
      $this->log->error("Could not get next batch events from batch %d",
			$batch_id);
      return False;
    }		
    $events    = array();
    $resultset = pg_fetch_all($r);
    
    if( $resultset === False ) {
      $this->log->notice("get_batch_events(%d) got 'False' ".
			  "(empty list or error)", $batch_id);
      return False;
    }
    
    foreach( $resultset as $row ) {
      $events[] = new PGQEvent($this->log, $row);
    }
    return $events;
  }
	
  /**
   * Mark event as failed
   */
  protected function event_failed($batch_id, $event) {
    $sql_ef = sprintf("SELECT pgq.event_failed(%d, %d, '%s');",
		      (int)$batch_id,
		      (int)$event->id,
		      pg_escape_string($event->failed_reason));
    
    $this->log->verbose("%s", $sql_ef);
    if( pg_query($this->pg_src_con, $sql_ef) === False ) {
      $this->log->error("Could not mark failed event %d from batch %d",
			(int)$event->id, (int)$batch_id);
      return False;
    }
    return True;
  }
	
  /**
   * Mark event for retry
   */
  protected function event_retry($batch_id, $event) {
    $sql_er = sprintf("SELECT pgq.event_retry(%d, %d, %d);",
		      (int)$batch_id,
		      (int)$event->id,
		      (int)$event->retry_delay);
    
    $this->log->verbose("%s", $sql_er);
    if( pg_query($this->pg_src_con, $sql_er) === False ) {
      $this->log->error("Could not retry event %d from batch %d",
			(int)$event->id, (int)$batch_id);
      return False;
    }
    return True;
  }
	
  /**
   * Finish Batch
   */
  protected function finish_batch($batch_id) {
    $sql_fb = sprintf("SELECT pgq.finish_batch(%d);", (int)$batch_id);
    
    $this->log->verbose("%s", $sql_fb);
    if( pg_query($this->pg_src_con, $sql_fb) === False ) {
      $this->log->error("Could not finish batch %d", (int)$batch_id);
      return False;
    }	
    return True;
  }

  /**
   * failed_event_list
   * returns array(PGQEvent)
   */
  protected function failed_event_list() {
    $sql = sprintf("SELECT * FROM pgq.failed_event_list('%s', '%s')",
                   pg_escape_string($this->qname),
                   pg_escape_string($this->cname));
		                  
    $this->log->verbose("%s", $sql);
    if( ($r = pg_query($this->pg_src_con, $sql)) === False ) {
      $this->log->error("Could not get next failed event list");
      return False;
    }		
    $events    = array();
    $resultset = pg_fetch_all($r);
    
    if( $resultset === False ) {
      $this->log->notice("failed_event_list(%d) got 'False' ".
			  "(empty list or error)", $batch_id);
      return False;
    }
    
    foreach( $resultset as $row ) {
      $events[] = new PGQEvent($this->log, $row);
    }
    return $events;
  }

  /**
   * Helper function failed_event_delete_all
   */
  protected function failed_event_delete_all() {
    $allok = True;
    foreach( $this->failed_event_list() as $event_id => $event ) {
      $allok = $allok && $this->failed_event_delete($event_id);

      if( ! $allok )
	return False;
    }
    return True;
  }

  /**
   * failed_event_delete
   */
  protected function failed_event_delete($event_id) {
    $sql = sprintf("SELECT pgq.failed_event_delete('%s', '%s', %d)",
		   pg_escape_string($this->qname),
		   pg_escape_string($this->cname),
		   $event_id);
    
    $this->log->debug("%s", $sql);
    $result = pg_query($this->pg_src_con, $sql);
    
    if( $result === False ) {
      $this->log->warning("Could not delete failed event %d", $event_id);
      return False;
    }
    if( pg_num_rows($result) == 1 ) {
      print pg_fetch_assoc($result, 0);
      return True;
    }
    else {
      $this->log->warning("failed_event_delete('%s', '%s', %d) ".
			  "did not get 1 row.", 
			  $this->qname, $this->cname, $event_id);
      return False;
    }
    return True;
  }

  /**
   * Helper function failed_event_retry_all
   */
  protected function failed_event_retry_all() {
    $allok = True;

    foreach( $this->failed_event_list() as $event_id => $event ) {
      $allok = $allok && $this->failed_event_retry($event_id);

      if( ! $allok )
	return False;
    }
    return True;
  }

  /**
   * failed_event_delete
   */
  protected function failed_event_retry($event_id) {
    $sql = sprintf("SELECT pgq.failed_event_retry('%s', '%s', %d)",
		   pg_escape_string($this->qname),
		   pg_escape_string($this->cname),
		   $event_id);
    
    $this->log->debug("%s", $sql);
    $result = pg_query($this->pg_src_con, $sql);
    
    if( $result === False ) {
      $this->log->warning("Could not retry failed delete event %d", $event_id);
      return False;
    }
    if( pg_num_rows($result) == 1 ) {
      print pg_fetch_assoc($result, 0);
      return True;
    }
    else {
      $this->log->warning("failed_event_retry('%s', '%s', %d) ".
			  "did not get 1 row.", 
			  $this->qname, $this->cname, $event_id);
      return False;
    }
    return True;
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
}