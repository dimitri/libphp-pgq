#! /usr/bin/php5
<?php
// adjust if needs
require __DIR__.'/vendor/autoload.php';

use pgq\SystemDaemon;

define("CONFIGURATION", "testd.conf");

class Testd extends SystemDaemon
{
	public function config( )
	{
		unset($Config);
		if( $this->log !== null )
			$this->log->notice("Reloading configuration (HUP) from '%s'", CONFIGURATION);

		global $Config;
		require(CONFIGURATION);

		$this->loglevel = $Config["LOGLEVEL"];
		$this->logfile  = $Config["LOGFILE"];
		$this->delay    = $Config["DELAY"];
	}

	public function process( )
	{
		$this->log->notice("Starting process loop");

		for( $j=0; $j < 5; $j++) {
			for( $i = 0; $i < 1000000; $i++ ) {
				if( $i % 200000 == 0 )
					$this->log->debug("Inner loop %d", $i);
			}
			$this->log->verbose("Let's loop again! ". $j);
		}
		$this->log->notice("Ending process loop");
	}
}

$myTestd = new Testd($argc, $argv);
?>
