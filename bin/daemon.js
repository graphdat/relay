#!/usr/bin/env node
;(function ()
{
	var _path = require('path');
	var RELAY_PATH = _path.resolve(__dirname, '..', 'lib', 'relay.js');

	// test for windows, do not run as a daemon in windows
	var isWindows = !!process.platform.match(/^win/);
	if (isWindows)
	{
		// wrapper in case we're in module_context mode
		// windows: running "graphdat-relay blah" in this folder will invoke WSH, not node.
		if (typeof WScript !== "undefined")
		{
			WScript.echo("graphdat-relay does not work when run\n" + "with the Windows Scripting Host\n\n" + "'cd' to a different directory,\n" + "or type 'graphdat-relay.cmd <args>',\n" + "or type 'node graphdat-relay <args>'.");
			WScript.quit(1);
			return;
		}
		// do not daemonize on windows
		require(RELAY_PATH);
		return;
	}

	var daemon = require("daemonize.redux").setup({
		cwd: process.cwd(),
		main: RELAY_PATH,
		args: "--expose-gc",
		name: "graphdat-relay",
		pidfile: _path.join(process.cwd(), "graphdat-relay.pid")
	});

	daemon.on("error", function(err)
	{
		console.log("Daemon failed to start:  " + err.message);
	});

	switch (process.argv[2]) {
		case "start":
			daemon.start();
			break;
		case "stop":
			daemon.stop();
			break;
		case "kill":
			daemon.kill();
			break;
		case "restart":
			daemon.stop(function(err) { daemon.start(); });
			break;
		case "reload":
			console.log("Reload signal sent");
			daemon.sendSignal("SIGUSR1");
			break;
		case "status":
			var pid = daemon.status();
			if (pid)
				console.log("Daemon running. PID: " + pid);
			else
				console.log("Daemon is not running.");
			break;
		default:
			console.log("Usage: [start|stop|restart|status]");
	}
})();
