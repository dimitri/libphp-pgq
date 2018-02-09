<?php

namespace pgq;

/**
 * pgq\PGQEventRemoteConsumer is a pgq\PGQRemoteConsumer which handles nested
 * transactions for event management, allowing the remote processing
 * to be commited or rollbacked at event level.
 */

abstract class PGQEventRemoteConsumer extends PGQRemoteConsumer
{
	public function get_savepoint($event)
	{
		return sprintf("pgq_event_%d", $event->id);
	}

	public function preprocess_event($event)
	{
		$sql_savepoint = sprintf("SAVEPOINT %s", $this->get_savepoint($event));
		$this->log->debug($sql_savepoint);

		if (pg_query($this->pg_dst_con, $sql_savepoint) === False) {
			$this->log->warning("pgq\PGQEventRemoteConsumer.preprocess_event " .
				"could not place SAVEPOINT for event %d", $event->id);
			return PGQ_ABORT_BATCH;
		}
		return True;
	}

	public function postprocess_event($event)
	{
		$savepoint = $this->get_savepoint($event);

		switch ($event->tag) {
			case PGQ_EVENT_OK:
				$sql_release = sprintf("RELEASE SAVEPOINT %s", $savepoint);
				$this->log->debug($sql_release);
				$result = pg_query($this->pg_dst_con, $sql_release);

				if ($result === False) {
					$this->log->notice("Could not release savepoint %s", $savepoint);
					return PGQ_ABORT_BATCH;
				}
				break;

			case PGQ_EVENT_FAILED:
				$sql_rollback = sprintf("ROLLBACK TO SAVEPOINT %s", $savepoint);
				$this->log->debug($sql_rollback);
				$result = pg_query($this->pg_dst_con, $sql_rollback);

				if ($result === False) {
					$this->log->notice("Could not rollback to savepoint %s",
						$savepoint);
					return PGQ_ABORT_BATCH;
				}
				break;

			case PGQ_EVENT_RETRY:
				$sql_rollback = sprintf("ROLLBACK TO SAVEPOINT %s", $savepoint);
				$this->log->debug($sql_rollback);
				$result = pg_query($this->pg_dst_con, $sql_rollback);

				if ($result === False) {
					$this->log->notice("Could not tollback to savepoint %s",
						$savepoint);
					return PGQ_ABORT_BATCH;
				}
				break;
		}
		return True;
	}
}

?>