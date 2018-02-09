<?php
namespace pgq;

/**
 * If PHP new about modules or namespaces, this would be a pgq\PGQ module.
 *
 * It's an abstract pgq\PGQ class containing only static methods which you
 * simple use like modules and functions in other languages:
 *
 *  $batch_id = pgq\PGQ::next_batch();
 */
abstract class PGQ
{
	/**
	 * Queue creation
	 */
	public static function create_queue($log, $pgcon, $qname)
	{
		$sql = sprintf("SELECT pgq.create_queue('%s');", pg_escape_string($qname));
		$log->verbose("create_queue: %s", $sql);

		$r = pg_query($pgcon, $sql);
		if ($r === False) {
			$log->fatal("Could not create queue '%s'", $qname);
			return False;
		}
		$result = (pg_fetch_result($r, 0, 0) == 1);

		if (!$result) {
			$log->fatal("pgq\PGQConsumer: could not create queue.");
		}
		return $result;
	}

	/**
	 * Queue drop
	 */
	public static function drop_queue($log, $pgcon, $qname)
	{
		$sql = sprintf("SELECT pgq.drop_queue('%s');", pg_escape_string($qname));
		$log->verbose("drop_queue: %s", $sql);

		$r = pg_query($pgcon, $sql);
		if ($r === False) {
			$log->fatal("Could not drop queue '%s'", $qname);
			return False;
		}
		return pg_fetch_result($r, 0, 0) == 1;
	}

	/**
	 * Queue exists?
	 */
	public static function queue_exists($log, $pgcon, $qname)
	{
		$sql = sprintf("SELECT * FROM pgq.get_queue_info()");

		$log->verbose("%s", $sql);
		if (($r = pg_query($pgcon, $sql)) === False) {
			$log->error("Could not get queue info");
			return False;
		}
		$queues = array();
		$resultset = pg_fetch_all($r);

		if ($resultset === False) {
			$log->notice("pgq\PGQConsumer.queue_exists() got no queue.");
			return False;
		}

		foreach ($resultset as $row) {
			if ($row["queue_name"] == $qname)
				return True;
		}
		return False;
	}

	/**
	 * Register pgq\PGQ Consumer.
	 *
	 * @return: boolean
	 */
	public static function register($log, $pgcon, $qname, $cname)
	{
		$sql = sprintf("SELECT pgq.register_consumer('%s', '%s');",
			pg_escape_string($qname),
			pg_escape_string($cname));

		$log->verbose("%s", $sql);
		$r = pg_query($pgcon, $sql);
		if ($r === False) {
			$log->warning("Could not register consumer '%s' to queue '%s'",
				$cname, $qname);
			return False;
		}

		$registered = pg_fetch_result($r, 0, 0);
		if ($registered == "1") {
			return True;
		} else {
			$log->fatal("Register Consumer failed (%d).", $registered);
			return False;
		}
	}

	/**
	 * Unregister pgq\PGQ Consumer. Called from stop().
	 */
	public static function unregister($log, $pgcon, $qname, $cname)
	{
		$sql = sprintf("SELECT pgq.unregister_consumer('%s', '%s');",
			pg_escape_string($qname),
			pg_escape_string($cname));

		$log->verbose("%s", $sql);
		$r = pg_query($pgcon, $sql);
		if ($r === False) {
			$log->fatal("Could not unregister consumer '%s' to queue '%s'",
				$cname, $qname);
			return False;
		}

		$unregistered = pg_fetch_result($r, 0, 0);
		if ($unregistered == "1") {
			return True;
		} else {
			$log->fatal("Unregister Consumer failed (%d).", $unregistered);
			return False;
		}
	}

	/**
	 * are we registered already?
	 */
	public static function is_registered($log, $pgcon, $qname, $cname)
	{
		$infos = PGQ::get_consumer_info($log, $pgcon, $qname, $cname);

		if ($infos !== False) {
			$log->debug("is_registered %s",
				($infos["queue_name"] == $qname
				&& $infos["consumer_name"] == $cname
					?
					"True" : "False"));

			return $infos["queue_name"] == $qname
				&& $infos["consumer_name"] == $cname;
		}
		$log->warning("is_registered: count not get consumer infos.");
		return False;
	}

	/**
	 * get_consumer_info
	 */
	public static function get_consumer_info($log, $pgcon, $qname, $cname)
	{
		$sq = sprintf("SELECT * FROM pgq.get_consumer_info('%s', '%s')",
			pg_escape_string($qname),
			pg_escape_string($cname));

		$log->debug("%s", $sq);
		$result = pg_query($pgcon, $sq);

		if ($result === False) {
			$log->warning("Could not get consumer info for '%s'", $cname);
			return False;
		}

		if (pg_num_rows($result) == 1)
			return pg_fetch_assoc($result, 0);
		else {
			$log->warning("get_consumer_info('%s', '%s') did not get 1 row.",
				$qname, $cname);
			return False;
		}
	}

	/**
	 * get_consumers returns a list of consumers attached to the queue
	 */
	public static function get_consumers($log, $pgcon, $qname)
	{
		$sq = sprintf("SELECT * FROM pgq.get_consumer_info('%s')",
			pg_escape_string($qname));

		$log->debug("%s", $sq);
		$result = pg_query($pgcon, $sq);
		$resultset = $result !== False ? pg_fetch_all($result) : False;

		if ($result === False or $resultset === False) {
			$log->warning("Could not get consumers list for '%s'", $qname);
			return False;
		}
		$clist = array();

		foreach ($resultset as $row) {
			$clist[] = $row;
		}
		return $clist;
	}

	/**
	 * Get next batch id
	 *
	 * Returns null when pgq.next_batch() returns null or failed.
	 */
	public static function next_batch($log, $pgcon, $qname, $cname)
	{
		$sql = sprintf("SELECT pgq.next_batch('%s', '%s')",
			pg_escape_string($qname),
			pg_escape_string($cname));

		$log->verbose("%s", $sql);
		if (($r = pg_query($pgcon, $sql)) === False) {
			$log->error("Could not get next batch");
			return False;
		}

		$batch_id = pg_fetch_result($r, 0, 0);
		$log->debug("Get batch_id %s (isnull=%s)",
			$batch_id,
			($batch_id === null ? "True" : "False"));
		return $batch_id;
	}

	/**
	 * Finish Batch
	 */
	public static function finish_batch($log, $pgcon, $batch_id)
	{
		$sql = sprintf("SELECT pgq.finish_batch(%d);", (int)$batch_id);

		$log->verbose("%s", $sql);
		if (pg_query($pgcon, $sql) === False) {
			$log->error("Could not finish batch %d", (int)$batch_id);
			return False;
		}
		return True;
	}

	/**
	 * Get batch events
	 *
	 * @return array(PGQEvents);
	 */
	public static function get_batch_events($log, $pgcon, $batch_id)
	{
		$sql = sprintf("SELECT * FROM pgq.get_batch_events(%d)", (int)$batch_id);

		$log->verbose("%s", $sql);
		if (($r = pg_query($pgcon, $sql)) === False) {
			$log->error("Could not get next batch events from batch %d",
				$batch_id);
			return False;
		}
		$events = array();
		$resultset = pg_fetch_all($r);

		if ($resultset === False) {
			$log->notice("get_batch_events(%d) got 'False' " .
				"(empty list or error)", $batch_id);
			return False;
		}

		foreach ($resultset as $row) {
			$events[] = new PGQEvent($log, $row);
		}
		return $events;
	}


	/**
	 * Mark event as failed
	 */
	public static function event_failed($log, $pgcon, $batch_id, $event)
	{
		$sql = sprintf("SELECT pgq.event_failed(%d, %d, '%s');",
			(int)$batch_id,
			(int)$event->id,
			pg_escape_string($event->failed_reason));

		$log->verbose("%s", $sql);
		if (pg_query($pgcon, $sql) === False) {
			$log->error("Could not mark failed event %d from batch %d",
				(int)$event->id, (int)$batch_id);
			return False;
		}
		return True;
	}

	/**
	 * Mark event for retry
	 */
	public static function event_retry($log, $pgcon, $batch_id, $event)
	{
		$sql = sprintf("SELECT pgq.event_retry(%d, %d, %d);",
			(int)$batch_id,
			(int)$event->id,
			(int)$event->retry_delay);

		$log->verbose("%s", $sql);
		if (pg_query($pgcon, $sql) === False) {
			$log->error("Could not retry event %d from batch %d",
				(int)$event->id, (int)$batch_id);
			return False;
		}
		return True;
	}

	/**
	 * Call the retry_queue maintenance function, which is responsible of
	 * pushing the events there back into main queue when the ev_retry_after
	 * is in the past.
	 */
	public static function maint_retry_events($log, $pgcon)
	{
		$sql = sprintf("SELECT pgq.maint_retry_events();");

		$log->verbose("%s", $sql);
		if (($r = pg_query($pgcon, $sql)) === False) {
			$log->error("Failed to process retry queue");
			return False;
		}
		/* the SQL function signature is: returns integer */
		$count = pg_fetch_result($r, 0, 0);

		if ($count === False) {
			$log->warning("maint_retry_events got no result");
			return False;
		}

		return $count;
	}

	/**
	 * failed_event_list
	 * returns array(pgq\PGQEvent)
	 */
	public static function failed_event_list($log, $pgcon, $qname, $cname)
	{
		$sql = sprintf("SELECT * FROM pgq.failed_event_list('%s', '%s')",
			pg_escape_string($qname),
			pg_escape_string($cname));

		$log->verbose("%s", $sql);
		if (($r = pg_query($pgcon, $sql)) === False) {
			$log->error("Could not get next failed event list");
			return False;
		}
		$events = array();
		$resultset = pg_fetch_all($r);

		if ($resultset === False) {
			$log->notice("failed_event_list(%d) got 'False' " .
				"(empty list or error)", $batch_id);
			return False;
		}

		foreach ($resultset as $row) {
			$event = new PGQEvent($log, $row);
			$events[$event->id] = $event;
		}
		return $events;
	}

	/**
	 * Helper function failed_event_delete_all
	 */
	public static function failed_event_delete_all($log, $pgcon,
												   $qname, $cname)
	{
		$allok = True;
		foreach (PGQ::failed_event_list($log, $pgcon, $qname, $cname)
				 as $event_id => $event) {
			$allok = $allok && PGQ::failed_event_delete($log, $pgcon,
					$qname, $cname, $event_id);
			if (!$allok)
				return False;
		}
		return True;
	}

	/**
	 * failed_event_delete
	 */
	public static function failed_event_delete($log, $pgcon,
											   $qname, $cname, $event_id)
	{
		$sql = sprintf("SELECT pgq.failed_event_delete('%s', '%s', %d)",
			pg_escape_string($qname),
			pg_escape_string($cname),
			$event_id);

		$log->debug("%s", $sql);
		$result = pg_query($pgcon, $sql);

		if ($result === False) {
			$log->error("Could not delete failed event %d", $event_id);
			return False;
		}
		if (pg_num_rows($result) == 1) {
			$event = new PGQEvent($log, pg_fetch_assoc($result, 0));
			echo $event . "\n";
			return True;
		} else {
			$log->warning("failed_event_delete('%s', '%s', %d) did not get 1 row.",
				$qname, $cname, $event_id);
			return False;
		}
		return True;
	}

	/**
	 * Helper function failed_event_retry_all
	 */
	public static function failed_event_retry_all($log, $pgcon, $qname, $cname)
	{
		$allok = True;

		foreach (PGQ::failed_event_list($log, $pgcon, $qname, $cname)
				 as $event_id => $event) {
			$allok = $allok && PGQ::failed_event_retry($log, $pgcon,
					$qname, $cname, $event_id);
			if (!$allok)
				return False;
		}
		return True;
	}

	/**
	 * failed_event_retry
	 */
	public static function failed_event_retry($log, $pgcon,
											  $qname, $cname, $event_id)
	{
		$sql = sprintf("SELECT pgq.failed_event_retry('%s', '%s', %d)",
			pg_escape_string($qname),
			pg_escape_string($cname),
			$event_id);

		$log->debug("%s", $sql);
		$result = pg_query($pgcon, $sql);

		if ($result === False) {
			$log->error("Could not retry failed delete event %d", $event_id);
			return False;
		}
		if (pg_num_rows($result) == 1) {
			$event = new PGQEvent($log, pg_fetch_assoc($result, 0));
			echo $event . "\n";
			return True;
		} else {
			$log->error("failed_event_retry('%s', '%s', %d) did not get 1 row.",
				$qname, $cname, $event_id);
			return False;
		}
		return True;
	}
}

?>
