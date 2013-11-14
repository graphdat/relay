#!/usr/bin/env node
;(function ()
{ // wrapper in case we're in module_context mode

	// windows: running "graphdat-relay blah" in this folder will invoke WSH, not node.
	if (typeof WScript !== "undefined")
	{
		WScript.echo("graphdat-relay does not work when run\n" + "with the Windows Scripting Host\n\n" + "'cd' to a different directory,\n" + "or type 'graphdat-relay.cmd <args>',\n" + "or type 'node graphdat-relay <args>'.");
		WScript.quit(1);
		return;
	}

	require(require('path').resolve(__dirname, '..', 'lib', 'relay.js'));
})();
