<?php
// No sense is given nor necessary for those constants, as soon as
// there's no colision.
define("PGQ_EVENT_OK",     1);
define("PGQ_EVENT_FAILED", 2);
define("PGQ_EVENT_RETRY",  5);
define("PGQ_ABORT_BATCH", 11);

/**
 * log levels
 */
define("FATAL",   60);
define("ERROR",   50);
define("WARNING", 40);
define("NOTICE",  30);
define("VERBOSE", 20);
define("DEBUG",   10);

defined("DEFAULT_TZ") || define("DEFAULT_TZ", "Europe/Paris");

define("PGQ_TRIGGER_NAME", "ins_to_queue");
define("PGQ_LAST_BATCH", "pgq_last_batch");

defined("PIDFILE_PREFIX") || define("PIDFILE_PREFIX", "/tmp");

if (!defined('E_RECOVERABLE_ERROR')) {
  define('E_RECOVERABLE_ERROR', 4096);
}
if (!defined('E_DEPRECATED')) {
  define('E_DEPRECATED', 8192);
}
if (!defined('E_USER_DEPRECATED')) {
  define('E_USER_DEPRECATED', 16384);
}
