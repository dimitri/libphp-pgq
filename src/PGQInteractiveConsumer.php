<?php

namespace pgq;

/**
 * This pgq\PGQ consumer class allows to have interactive consuming. That
 * means the queue consuming is done on demand, the
 * pgq\PGQInteractiveConsumer will consume all events of all batches
 * available up until it receives a null batch from pgq\PGQ::next_batch().
 *
 * This is useful when you propose an HTTP interface for clients to
 * get their list of events to process, and the clients have to care
 * about the never-ending consuming loop.
 */
abstract class PGQInteractiveConsumer
{

	protected $qname;
	protected $cname;
	protected $src_constr;
	protected $pgcon;
	protected $log;

	public function __construct($qname, $cname,
								$src_constr, $loglevel, $logfile)
	{
		$this->qname = $qname;
		$this->cname = $cname;
		$this->src_constr = $src_constr;

		$this->pgcon = pg_connect($this->src_constr);
		$this->log = new SimpleLogger($loglevel, $logfile);
	}

	/**
	 * Implementers need to define process_event.
	 */
	public function process_event(&$event)
	{
	}

	/**
	 * Will consume all existing events & batches. Implementer should
	 * call it at one point in order for process_event() to get fired.
	 */
	public function process()
	{
		if ($this->connect() === False)
			return False;

		do {
			$batch_id = $this->next_batch();
			if ($batch_id !== False) {
				$this->log->notice("Processing batch %d", $batch_id);

				pg_query($this->pgcon, "BEGIN;");

				$events = $this->get_batch_events($batch_id);

				foreach ($events as $event) {
					$this->log->verbose("Processing event %d", $event->id);
					$tag = $event->tag($this->process_event($event));

					switch ($tag) {
						case PGQ_EVENT_OK:
						case PGQ_EVENT_FAILED:
						case PGQ_EVENT_RETRY:
							// ignore
							break;

						case PGQ_ABORT_BATCH:
							$this->rollback();
							return PGQ_ABORT_BATCH;
							break;
					}
				}

				$this->finish_batch($batch_id);
				pg_query($this->pgcon, "COMMIT;");
			}
		} while ($batch_id !== null);

		$this->disconnect();

		$this->log->notice("pgq\PGQInteractiveConsumer.process: next_batch is null");
		return True;
	}

	/**
	 * Connects to the conw & conp connection strings.
	 */
	public function connect($force = False)
	{
		if ($this->connected && !$force) {
			$this->log->notice("connect called when connected is True");
			return;
		}

		if ($this->src_constr != "") {
			$this->log->verbose("Opening pg_src connexion '%s'.", $this->src_constr);
			$this->pg_src_con = pg_connect($this->src_constr);

			if ($this->pg_src_con === False) {
				$this->log->fatal("Could not open pg_src connection '%s'.",
					$this->src_constr);
				$this->stop();
			}
		}
		$this->connected = True;
	}

	/**
	 * Disconnect from databases
	 */
	public function disconnect()
	{
		if (!$this->connected) {
			$this->log->notice("disconnect called when $this->connected is False");
			return;
		}

		if ($this->pg_src_con != null && $this->pg_src_con !== False) {
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
	protected function rollback()
	{
		if ($this->pg_src_con != null && $this->pg_src_con !== False) {
			$this->log->notice("ROLLBACK pg_src connection '%s'.",
				$this->src_constr);
			pg_query($this->pg_src_con, "ROLLBACK;");
		}
	}

	/**
	 * Separate reimplementation of same methods from pgq\PGQConsumer,
	 * because we can't have pgq\PGQConsumer extends more that one class and
	 * it needs to be a pgq\SystemDaemon.
	 */
	protected function queue_exists()
	{
		return PGQ::queue_exists($this->log, $this->pg_src_con, $this->qname);
	}

	protected function is_registered()
	{
		return PGQ::is_registered($this->log, $this->pg_src_con,
			$this->qname, $this->cname);
	}

	protected function get_consumer_info()
	{
		return PGQ::get_consumer_info($this->log, $this->pg_src_con,
			$this->qname, $this->cname);
	}

	protected function next_batch()
	{
		return PGQ::next_batch($this->log, $this->pg_src_con,
			$this->qname, $this->cname);
	}

	protected function finish_batch($batch_id)
	{
		return PGQ::finish_batch($this->log, $this->pg_src_con, $batch_id);
	}

	protected function get_batch_events($batch_id)
	{
		return PGQ::get_batch_events($this->log, $this->pg_src_con, $batch_id);
	}

	protected function event_failed($batch_id, $event)
	{
		return PGQ::event_failed($this->log, $this->pg_src_con, $batch_id, $event);
	}

	protected function event_retry($batch_id, $event)
	{
		return PGQ::event_retry($this->log, $this->pg_src_con, $batch_id, $event);
	}

	protected function failed_event_list()
	{
		return PGQ::failed_event_list($this->log, $this->pg_src_con,
			$qname, $cname);
	}

	protected function failed_event_delete_all()
	{
		return PGQ::failed_event_delete_all($this->log, $this->pg_src_con,
			$qname, $cname);
	}

	protected function failed_event_delete($event_id)
	{
		return PGQ::failed_event_delete($this->log, $this->pg_src_con,
			$qname, $cname, $event_id);
	}

	protected function failed_event_retry_all()
	{
		return PGQ::failed_event_retry_all($this->log, $this->pg_src_con,
			$qname, $cname);
	}

	protected function failed_event_retry($event_id)
	{
		return PGQ::failed_event_retry($this->log, $this->pg_src_con,
			$qname, $cname, $event_id);
	}
}

?>
