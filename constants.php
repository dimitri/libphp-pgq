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
