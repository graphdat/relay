var _fs = require('fs');
var _path = require('path');
var _util = require('util');
var _os = require('os');
var _url = require('url');
var _childProcess = require('child_process');
var _zlib = require('zlib');
var _crypto = require('crypto');
var _request = require('request');
var _ = require('underscore');
var _optimist = require('optimist');
var _async = require('async');
var _cbuff = require('CBuffer');
var _walk = require('walk');
var _zip = require('adm-zip');
var _http;

var _defaultConfig =
{
	name : _os.hostname(),
	sendInterval : 4000,
	pollInterval : 1000,
	sendCompress : true,
	sendHost : 'localhost',
	sendPort : 8005,
	sendBacklog : 200,
	sendClear : true
};


var _plugins = {};
var _outboundQueue;
var _outboundMsg = new _cbuff(1000);
var _ejectedCount = 0;
var _sendFailed;
var _confPath = _path.join(__dirname, 'config.json');
var _invalidPluginPath = _path.join(__dirname, 'plugins-archive');
var _pluginsHaveRefreshed;
var _pluginsToDisable = [];

var MSG_TYPE_ERROR = 'error';
var MSG_TYPE_INFO = 'info';

var _opt = _optimist.usage('Graphdat Relay')
	.options('l', {
		describe : 'Run local.  Do not use remote configuration.',
		alias : 'local'
	})
	.options('v', {
		describe : 'Verbose output',
		alias : 'verbose'
	})
	.options('h', {
		describe : 'Displays this message',
		alias : 'help'
	})
	.options('q', {
		describe : 'Suppress output',
		alias : 'quiet'
	})
	.options('e', {
		describe : 'Set email address used for Graphdat API',
		alias : 'email'
	})
	.options('t', {
		describe : 'Set Graphdat API token used for Graphdat API',
		alias : 'token'
	})
	.wrap(80);

var _argv = _opt.argv;

if (_argv.help)
{
	_opt.showHelp();
	return;
}

function addMsg(type, plugin, msg)
{
	_outboundMsg.push({
		type : type,
		plugin : _.isObject(plugin) ? plugin.name : plugin,
		msg : msg,
		ts : Math.round(Date.now() / 1000)
	});

	if (!_argv.quiet)
	{
		if (type === MSG_TYPE_ERROR)
			console.error('!! ' + msg);
		else
			console.info(msg);
	}
}

var _log =
{
	error : function(msg)
	{
		var str = _util.format.apply(_util, Array.prototype.slice.call(arguments, 0));

		addMsg(MSG_TYPE_ERROR, null, str);
	},
	info : function(msg)
	{
		var str = _util.format.apply(_util, Array.prototype.slice.call(arguments, 0));

		addMsg(MSG_TYPE_INFO, null, str);
	}
};

process.on('uncaughtException', function(err)
{
	_log.error(err);
	if (err.stack)
		_log.error(err.stack);

	process.exit(1);
});

/***
 * Parse JSON text without throwing an exception.
 * Pass in a path for error reporting purposes only.
 * Returns undefined if parse failed
 *
 * @param json
 * @param path
 * @returns {*}
 */
function safeParseJSON(json, path)
{
	var obj;
	try
	{
		obj = JSON.parse(json);
	}
	catch(ex)
	{
		_log.error('Unexpected error trying to parse %s: %s', path || json, ex);
		return undefined;
	}

	return obj;
}

/***
 * Load a text file, parse it for JSON
 *
 * @param path
 * @returns {*}
 */
function safeParseJSONFile(path)
{
	if (!_fs.existsSync(path))
		return null;

	var json = _fs.readFileSync(path, 'utf8');

	return safeParseJSON(json, path);
}

/***
 * Configuration overview -
 *
 * There is a file in the same directory as relay called config.json which is loaded at startup
 * Among other settings there is a property called 'config.plugins' which contains the
 * set of running plugins.
 * If running local, the plugins set is used to start/configure plugins immediately
 * If not running local, we wait until the first send() to retrieve any update to the config before starting plugins
 * When running remote, any changes to config made remotely will trigger a file overwrite
 * When launching plugins the parameters in config.plugins[<pluginname>] are written into param.json in plugin dir
 * as well as made available to macro substitution during plugin launch
 */
var _conf = safeParseJSONFile(_confPath);

function writeConfig()
{
	_fs.writeFileSync(_confPath, JSON.stringify(_conf, null, 3), 'utf8');
}

/***
 * Check that our config has the correct values
 *
 * @returns {boolean}
 */
function validateConfig()
{
	if (_argv.email || _argv.token)
	{
		if (!_conf)
			_conf = {};

		if (_argv.email)
			_conf.email = _argv.email;
		if (_argv.token)
			_conf.apiToken = _argv.token;

		writeConfig();

		_log.info('Configuration written, exiting');

		return false;
	}

	if (_conf === undefined)
	{
		_log.error('Could not parse config');
		return false;
	}

    if (!_conf.email)
	{
		_log.error('Missing config parameter email');
		return false;
	}

	if (!_conf.apiToken)
	{
		_log.error('Missing config parameter apiToken');
		return false;
	}

	for(var def in _defaultConfig)
	{
		if (!_conf[def])
			_conf[def] = _defaultConfig[def];
	}

	if (_conf.lastModified)
		_log.info('Using cached config, modified ' + new Date(_conf.lastModified * 1000));

	return true;
}

/***
 * Write a message to log only if in verbose mode
 *
 * @param msg
 */
function verbose(msg)
{
	if (!_argv.verbose)
		return;

	var str = _util.format.apply(_util, Array.prototype.slice.call(arguments, 0));

	_log.info(str);
}

/***
 * Break up command into argv
 *
 * @param cmd
 * @returns {Array}
 */
function parseCommand(cmd)
{
	var args = [];
	var readingPart = false;
	var part = '';
	for(var i=0; i < cmd.length; i++)
	{
		if(cmd.charAt(i) === ' ' && !readingPart)
		{
			args.push(part);
			part = '';
		}
		else
		{
			if(cmd.charAt(i) === '\"')
			{
				readingPart = !readingPart;
			}
			else
			{
				part += cmd.charAt(i);
			}
		}
	}

	args.push(part);

	return args;
}

/***
 * Report an error for the plugin by placing a message in the error circular buff
 *
 * @param p
 * @param msg
 */
function pluginErr(p, msg)
{
	var str = _util.format.apply(_util, Array.prototype.slice.call(arguments, 1));

	addMsg(MSG_TYPE_ERROR, p, str);
}

function pluginInfo(p, msg)
{
	var str = _util.format.apply(_util, Array.prototype.slice.call(arguments, 1));

	addMsg(MSG_TYPE_INFO, p, str);
}


/***
 * Remove a single line from the plugin stdio buffer and return it
 *
 * @param p
 * @returns {string}
 */
function getLine(p)
{
	var i = p.data.indexOf('\n');

	if (i == -1)
		return null;

	var line = p.data.substr(0, i);

	p.data = p.data.substr(i+1);

	return line;
}

/***
 * Alias underscore func
 */
var isString = _.isString;

/***
 * Check for a parsable float
 *
 * @param s
 * @returns {boolean}
 */
function isNumber(s)
{
	return !isNaN(parseFloat(s));
}

/***
 * Assumes the format:
 *
 * [<metric name>] <measure> [<source>] [<unix timestamp>]
 *
 * If source is omitted the host name is assumed
 * If timestamp is omitted current time is used
 * If metric name is omitted the first metric in the plugin metrics will be used
 *
 * @param p
 * @param line
 */
function parseLine(p, line)
{
	var parts = line.split(' ');

	if (parts.length < 2)
	{
		pluginErr(p, 'bad data encountered, not enough arguments: %s, ignoring', line);
		return;
	}

	var metric;
	var measure;
	var source;
	var ts;

	if (isNumber(parts[0]))
		parts.unshift(p.metrics[0]);

	if (!isString(parts[0]))
	{
		pluginErr(p, 'bad data encountered, expecting metric name as first argument: %s, ignoring', line);
		return;
	}
	metric = parts[0].toUpperCase();

	if (!isNumber(parts[1]))
	{
		pluginErr(p, 'bad data encountered, expecting measurement as second argument: %s, ignoring', line);
		return;
	}
	measure = parseFloat(parts[1]);

	if (parts.length > 2)
	{
		if (isString(parts[2]))
		{
			source = parts[2];
			if (parts.length > 3)
			{
				if (!isNumber(parts[3]))
				{
					pluginErr(p, 'bad data encountered, expecting timestamp as fourth argument: %s, ignoring', line);
					return;
				}
				ts = parseInt(parts[3]);
			}
		}
		else if (isNumber(parts[2]))
		{
			ts = parseInt(parts[2]);
		}
		else
		{
			pluginErr(p, 'bad data encountered, expecting either a string or number for third argument: %s, ignoring', line);
			return;
		}
	}
	if (!source)
		source = _conf.name;
	if (!ts)
		ts = Math.round(Date.now() / 1000);

	_outboundQueue.push([source, metric, measure, ts]);
}

/***
 * Called when circular before for outbound queue fills, should never happen in normal operation
 *
 * @param data
 */
function handleEject(data)
{
	_ejectedCount++;
}

/***
 * Check if we've had to eject any outbound data due to send failures, give error
 *
 */
function checkEject()
{
	if (_ejectedCount > 0)
	{
		_log.error('%d reading(s) ejected', _ejectedCount);
		_ejectedCount = 0;
	}
}

/***
 * Parse each line of data received, add metrics as avaialable
 *
 * @param p
 * @param data
 */
function addData(p, data)
{
	p.data += data;

	var line;
	while((line = getLine(p)))
	{
		parseLine(p, line);
	}
}

function lineBuffer(cb)
{
	var buff = '';

	return function(data)
	{
		buff += data;
		var idx;
		while((idx = buff.indexOf('\n')) != -1)
		{
			var line = buff.substr(0, idx);

			cb(line);

			buff = buff.substr(idx+1);
		}
	};
}

/***
 * Spawns a plugin process
 *
 * @param p					Plugin info
 * @param command			Command to spawn
 * @param cbClose			Callback when command exits
 * @param cbOutput			Callback for stdout output
 * @returns {*}				Process object
 */
function spawn(p, command, cbClose, cbOutput)
{
	verbose('spawning plugin %s, command "%s"', p.name, command);

	var parts = parseCommand(command);

	var prc = _childProcess.spawn(parts[0], parts.slice(1), {cwd : p.dir, env : process.env});

	prc.on('exit', function(code)
	{
		cbClose(code);
	});

	prc.stderr.setEncoding('utf8');
	prc.stderr.on('data', lineBuffer(function(line)
	{
		var m = line.match(/execvp\(\): (.*)/);

		if (m)
		{
			pluginErr(p, 'unable to execute %s : %s', command, m[1]);
		}
		else
		{
			pluginErr(p, line);
		}
	}));

	prc.stdout.setEncoding('utf8');
	prc.stdout.on('data', lineBuffer(function(line)
	{
		verbose('Received "%s" from plugin %s', line, p.name);

		return cbOutput && cbOutput(line);
	}));

	return prc;
}


/***
 * Make sure the command for the plugin is running
 *
 * @param p
 */
function ensureProcess(p)
{
	if (p.prc)
		return;

	p.prc = spawn(p, p.command, function()
	{
		p.prc = null;
	},
	function(data)
	{
		addData(p, data);
	});
}

/***
 * Kill the process for the plugin
 *
 * @param p
 */
function killProcess(p)
{
	if (!p.prc)
		return;

	p.prc.kill('SIGTERM');
}

/***
 * Strobe the set of loaded plugins, ensure that a process is running for each.
 * This check is made on the time interval specified in the config for the plugin.
 *
 * Note that for some plugins it is preferred to launch the process once and let it
 * poll itself, sending data as it is gathered.  In this case it is recommended
 * that the macro $(pollInterval) be passed as an argument to the plugin so the
 * plugin has an opportunity to obey its configuration.
 */
function poll()
{
	verbose('polling');

	var nextTime = 10000;

	for(var pname in _plugins)
	{
		var plugin = _plugins[pname];

		if (plugin.nextPoll <= Date.now())
		{
			ensureProcess(plugin);

			nextTime = Math.min(nextTime, plugin.pollInterval);
		}
	}

	setTimeout(poll, nextTime);
}

function getPluginParam(pluginName, name)
{
	return _conf && _conf.config && _conf.config.plugins && _conf.config.plugins[pluginName.toLowerCase] && _conf.config.plugins[pluginName.toLowerCase][name];
}

function getPluginDef(pluginName, name, def)
{
	return (_plugins && _plugins[pluginName.toLowerCase()] && _plugins[pluginName.toLowerCase()].def[name]) || def;
}

function pluginDisabled(pluginName)
{
	return _plugins[pluginName.toLowerCase()].disabled || getPluginParam(pluginName, 'disabled');
}

/***
 * Case insensitive property lookup
 *
 * @param o
 * @param name
 * @returns {*}
 */
function propVal(o, name)
{
	if (!o)
		return undefined;

	name = name.toLowerCase();

	for(var prop in o)
	{
		if (prop.toLowerCase() === name)
		{
			return o[prop];
		}
	}

	return undefined;
}

/***
 * Fills in macros within the text given using the parameters from the plugin param.json
 * Also checks for reserved parameters.  These include:
 *
 *	pollInterval
 *
 * @param p
 * @param txt
 * @param param
 * @returns {*}
 */
function replaceParams(p, txt, param)
{
	while(true)
	{
		var res = /\$\(([\w#?]*)\)/.exec(txt);
		if (!res)
			break;

		var val = null;
		var fReq = true;
		if (res[1].substr(res[1].length-1,1) == '?')
		{
			res[1] = res[1].substr(0,res[1].length-1);
			fReq = false;
		}
		// Check for reserved
		switch(res[1])
		{
			case 'pollInterval':
				val = p.pollInterval;
				break;
		}

		if (!val)
		{
			val = propVal(param, res[1]);

			if (fReq && val === undefined)
				throw 'Missing required param ' + res[1] + ' while formatting "' + txt + '"';
		}

		// Replace all
		txt = txt.replace(new RegExp('\\$\\(' + res[1] + '\\)', 'g'), val);
	}

	return txt;
}

function disablePlugin(pluginName, cb)
{
	_log.info('Disabling plugin %s', pluginName);

	var p = _plugins[pluginName.toLowerCase()];

	p.disabled = true;

	killProcess(p);

	return cb && cb();
}

function enablePlugin(pluginName, cb)
{
	_log.info('Enabling plugin %s', pluginName);

	var p = _plugins[pluginName.toLowerCase()];

	p.disabled = false;

	return cb && cb();
}

function getFileSHA(path)
{
	var buff = _fs.readFileSync(path);

	var generator = _crypto.createHash('sha1');
	generator.update('blob ' + buff.length + '\0');
	generator.update( buff );
	return generator.digest('hex');
}

function getPluginLocalTree(name, cb)
{
	var pi = _plugins[name.toLowerCase()];

	var files = [];

	var ignore = getPluginDef(name, 'ignore', []);

	if (_.isString(ignore))
		ignore = [ignore];

	// Always ignore params
	ignore.push('param.json');

	var opt = { followLinks : false };

	var walker = _walk.walk(pi.dir, opt);

	walker.on('file', function(root, fstat, next)
	{
		var path = _path.join(root.substr(pi.dir.length + 1), fstat.name);

		if (ignore.some(function(e)	{ return path.substr(0, e.length).toLowerCase() === e.toLowerCase(); }))
			return next();

		files.push(
			{
				path : path,
				sha : getFileSHA(_path.join(root, fstat.name))
			});
		next();
	});

	walker.on('end', function()
	{
		cb(null, files);
	});
}

function pluginFileDiff(name, cb)
{
	// Get repo tree (remove dir entries)
	var rtree = getPluginDef(name, 'tree').filter(function(e) { return e.type === 'blob'; });

	getPluginLocalTree(name, function(err, ltree)
	{
		if (err)
			return cb(err);

		if (rtree.length != ltree.length)
			return cb(null, 'file count differs (see ' + ltree.length + ' files, need ' + rtree.length + ' files)');

		var reason = null;
		var fNameSame;
		var fSHAMatch;
		for(var il = 0; il < ltree.length; il++)
		{
			fSHAMatch = false;
			for(var ir = 0; ir < rtree.length && !fSHAMatch; ir++)
			{
				fNameSame = ltree[il].path.toLowerCase() === rtree[ir].path.toLowerCase();

				if (fNameSame)
				{
					fSHAMatch = ltree[il].sha === rtree[ir].sha;

					if (!fSHAMatch)
						break;
				}
			}

			if (!fSHAMatch)
			{
				if (fNameSame)
					reason = 'local file ' + ltree[il].path + ' appears to be different';
				else
					reason = 'extra local extra ' + ltree[il].path;

				return cb(null, reason);
			}
		}

		return cb(null, null);
	});
}

function moveInvalidPlugin(name, cb)
{
	var pi = _plugins[name.toLowerCase()];

	try
	{
		if (!_fs.existsSync(pi.dir))
			return cb();

		// Make sure invalid dir exists
		if (!_fs.existsSync(_invalidPluginPath))
			_fs.mkdirSync(_invalidPluginPath);

		var target;
		while(_fs.existsSync(target = _path.join(_invalidPluginPath, pi.name + '-' + Date.now())));

		_fs.renameSync(pi.dir, target);

		_log.info('Moved local plugin %s to %s', name, target);

		return cb();
	}
	catch(ex)
	{
		return cb(ex);
	}
}

function findPluginEntry(zip)
{
	var entries = zip.getEntries();
	var rootPath;
	entries.every(function(e)
	{
		var parts = e.entryName.split('/');
		if (parts[parts.length-1] === 'plugin.json')
		{
			rootPath = e.entryName.substr(0, e.entryName.lastIndexOf('/') + 1);
			return false;
		}
		return true;
	});

	var entry = zip.getEntry(rootPath);

	return entry;
}

function recv(url, fAuth, fBuffer, cb)
{
	var opt = {};
	if (fAuth)
		opt.auth = { user : _conf.email, password : _conf.apiToken };
	if (fBuffer)
		opt.encoding = null;

	_request.get(url, opt, function(err, res, body)
	{
		if (err)
			return cb(err);

		if (res.statusCode != 200)
		{
			err = safeParseJSON(body, null);
			var msg;
			if (err && err.message)
				msg = err.message + ' trying to get ' + url;
			else
				msg = 'Unexpected error code ' + res.statusCode + ' attempting to get ' + url;

			return cb(new Error(msg));
		}

		if (res.headers['content-type'])
		{
			var type = res.headers['content-type'].split(';')[0];
			if (type === 'application/json')
				body = safeParseJSON(body, null);
		}

		cb(null, body);
	});
}

/***
 * Verify all local files, download/extract if needed
 * If an existing plugin folder has invalid files, move the plugin to ./plugins-invalid
 *
 * @param name
 * @param cbVerify
 */
function verifyPlugin(name, cbVerify)
{
	var pi = _plugins[name.toLowerCase()];
	var zip;

	_async.waterfall([
		function(cb)
		{
			pi.def = _conf.config.plugins[name.toLowerCase()]._metadata;

			pluginFileDiff(name, cb);
		},
		function(diff, cb)
		{
			if (!diff)
			{
				_log.info('Plugin %s validated', name);
				return cbVerify();
			}
			_log.info('Plugin %s appears to be different from repository: %s.\nDownloading current %s plugin from repository.', name, diff, name);

			var url = getPluginDef(name, 'download');

			recv(url, false, true, cb);
		},
		function(zipbuff, cb)
		{
			zip = new _zip(zipbuff);

			disablePlugin(name, cb);
		},
		function(cb)
		{
			moveInvalidPlugin(name, cb);
		},
		function(cb)
		{
			try
			{
				// Find the plugin.json
				var entry = findPluginEntry(zip);

				zip.extractEntryTo(entry, pi.dir, false, true);

				_log.info('Extracted plugin %s', name);
			}
			catch(ex)
			{
				return cb(ex);
			}
			return cb();
		},
		function(cb)
		{
			// Do post extract if specified
			var post = getPluginDef(name, 'postExtract');

			if (!post)
				return cb();

			pluginInfo(pi, 'Executing postExtract: %s', post);

			var prc = spawn(pi, post, function(code)
			{
				// If we fail the postExtract we keep plugin disabled
				if (code)
				{
					pluginErr(pi, 'Error executing postExtract: %s, disabling plugin', post);

					_pluginsToDisable.push(pi.name);
				}
				else
				{
					pluginInfo(pi, 'Successful postExtract');
					return cb();
				}
			},
			function(data)
			{
				pluginInfo(pi, data);
			});
		},
		function(cb)
		{
			enablePlugin(name, cb);
		}

	], cbVerify);
}

/***
 * Writes parameters from in-memory configuration to plugin param.json file
 *
 * @param pname
 */
function writeParams(pname)
{
	var pi = _plugins[pname.toLowerCase()];

	var path = _path.join(pi.dir, 'param.json');

	// Remove internal properties
	var params = _.clone((_conf && _conf.config && _conf.config.plugins && _conf.config.plugins[pname.toLowerCase()]) || {});

	delete params._metadata;

	pluginInfo(pi, 'Writing param.json');

	_fs.writeFileSync(path, JSON.stringify(params, null, 3), 'utf8');
}

/***
 * Compares given params to params stored in memory, returns true if equal
 *
 * @param pname
 * @param params
 */
function paramsEqual(pname, p1)
{
	var p2 = (_conf && _conf.config && _conf.config.plugins && _conf.config.plugins[pname.toLowerCase()]) || {};

	for(var prop1 in p1)
	{
		var fFound = false;

		for(var prop2 in p2)
		{
			if (prop1.toLowerCase() === prop2.toLowerCase())
			{
				fFound = _.isEqual(p1[prop1], p2[prop2]);
				break;
			}
		}

		if (!fFound)
			return false;
	}

	return true;
}

/***
 * Load plugin from the plugins folder by folder name.  Reads + validates configuration, adds to global plugin set
 *
 * @param pname
 * @param [cb]
 * @returns {boolean}
 */
function loadPlugin(pname, cb)
{
	var plugin = _plugins[pname];

	if (!plugin)
	{
		plugin = {};

		_plugins[pname] = plugin;

		var dir = _path.join(__dirname, 'plugins', pname);

		plugin.name = pname;
		plugin.dir = dir;
		plugin.data = '';
	}

	var params  = _conf && _conf.config && _conf.config.plugins && _conf.config.plugins[pname.toLowerCase()];

	function finish()
	{
		var path = _path.join(plugin.dir, 'plugin.json');

		if (!_fs.existsSync(path))
		{
			pluginErr(plugin, 'Unable to find %s', path);
			disablePlugin(pname);
			return false;
		}

		var config = safeParseJSONFile(path);
		if (!config)
		{
			_log.error('Cannot parse $s', path);
			disablePlugin(pname);
			return false;
		}

		// If we've got params, load them

		var pathParam = _path.join(plugin.dir, 'param.json');
		var param;

		if (_argv.local)
			param = safeParseJSONFile(pathParam);
		else
		{
			// We are running remote, use the remote params, write them local
			param = params;

			// If we have existing params, check if they have changed
			if (_fs.existsSync(pathParam))
			{
				var paramCheck = safeParseJSONFile(pathParam);

				if (!paramsEqual(pname, paramCheck))
				{
					pluginInfo(plugin, 'Config changed' + (plugin.prc ? ', restarting' : ''));

					killProcess(plugin);

					writeParams(pname);
				}
			}
			else
			{
				writeParams(pname);
			}
		}


		function checkParam(name, def)
		{
			var val;

			// First check params
			val = propVal(param, name);

			// If no param, try config
			if (!val)
				val = propVal(config, name);

			// If no config, try default
			if (!val)
				val = def;

			// If still nothing and there was no default we fail
			if (!val && arguments.length == 1)
			{
				pluginErr(plugin, 'Missing required config parameter %s, skipping plugin %s', name, pname);
				return false;
			}

			// Do replacement
			try
			{
				plugin[name] = replaceParams(plugin, val, param);
			}
			catch(errmsg)
			{
				pluginErr(plugin, errmsg);
				return false;
			}

			return true;
		}

		checkParam('pollInterval', _conf.pollInterval);
		checkParam('disabled', false);
		checkParam('metrics');

		if (!checkParam('command'))
		{
			disablePlugin(pname);
			return false;
		}

		if (plugin.disabled)
		{
			_log.info('Plugin %s disabled by config, skipping', pname);
			return true;
		}

		plugin.nextPoll = Date.now();

		_log.info('Loaded plugin %s', pname);
	}

	if (_argv.local)
		finish();
	else
	{
		verifyPlugin(pname, function(err)
		{
			if (err)
				return cb(err);

			finish();
		});
	}
}


function refreshPlugins(cb)
{
	_pluginsHaveRefreshed = true;

	// Make sure all plugins not in configuration are stopped
	var plugins = Object.keys[_plugins];

	if (plugins)
	{
		plugins.forEach(function(name)
		{
			var fExists = _conf && _conf.config && _conf.config.plugins && _conf.config.plugins[name.toLowerCase()];

			if (!fExists || pluginDisabled(name))
			{
				_log.info('Plugin %s has been %s, stopping', name, !fExists ? 'removed' : 'disabled');

				killProcess(_plugins[name]);

				if (!fExists)
					delete _plugins[name];
			}
		});
	}

	if (!_conf.config || !_conf.config.plugins)
		return;

	// Now scan configured plugins
	var funcs = [];

	for(var name in _conf.config.plugins)
	{
		funcs.push((function(name)
		{
			return function(cb)
			{
				loadPlugin(name, cb);
			};
		})(name));
	}

	_async.series(funcs, cb);
}


function handleConfig(cfg, cb)
{
	if (!cfg)
		return cb && cb();

	_log.info('New config found, modified ' + new Date(cfg.lastModified * 1000));

	_conf.lastModified = cfg.lastModified;

	_conf.config = cfg.config;

	// Write it out
	writeConfig();

	refreshPlugins(cb);
}


/***
 * Send all current outbound queued metrics.
 *
 * If there is an error we keep retrying.  Metrics are put in a circular buffer such that if the backlog
 * exceeds a limit we begin to toss out the oldest metrics.
 *
 * @param cb
 */
function send(cb)
{
	var measures;

	verbose('sending %d measurement(s)', _outboundQueue.size);

	var opt =
	{
		hostname : _conf.sendHost,
		port : _conf.sendPort,
		method : 'POST',
		auth :  _conf.email + ':' + _conf.apiToken,
		path : '/v1/batch',
		headers :
		{
			'Content-Type' : 'application/json',
			'Connection' : 'keep-alive'
		}
	};

	var batch = [];
	var handlers = [];

	// First do a heartbeat
	batch.push({
		method : 'POST',
		path : '/v1/relays/heartbeat',
		body : { hostname : _conf.name }
	});
	handlers.push(null);

	// Next we send measurements
	if (_outboundQueue.size)
	{
		measures = _outboundQueue.toArray();
		_outboundQueue.empty();

		batch.push({
			method : 'POST',
			path : '/v1/measurements',
			body : measures
		});

		handlers.push(null);
	}

	// Disable plugins if needed
	if (_pluginsToDisable.length)
	{
		var list = _pluginsToDisable;
		_pluginsToDisable = [];

		list.forEach(function(pname)
		{
			batch.push({
				method : 'POST',
				path : '/v1/relays/' + _conf.name + '/togglePlugin',
				body : { plugin : pname, disabled : true }
			});

			handlers.push(null);
		});
	}

	// Add output if exists
	if (_outboundMsg.size)
	{
		var msgs = _outboundMsg.toArray();
		_outboundMsg.empty();

		batch.push({
			method : 'POST',
			path : '/v1/relays/' + _conf.name + '/output',
			body : msgs
		});

		handlers.push(null);
	}

	// Now grab configuration if it has changed
	if (!_argv.local)
	{
		batch.push({
			method : 'GET',
			path : '/v1/relays/' + _conf.name + '/config',
			body : { since : _conf.lastModified || 0 }
		});

		handlers.push(handleConfig);
	}

	var body = JSON.stringify(batch);

	function handleFail(msg)
	{
		if (!_sendFailed)
		{
			_log.error('Failed to send to %s:%d%s: %s, will retry sending data every %d ms', opt.hostname, opt.port, opt.path, msg, _conf.sendInterval);
			_sendFailed = true;
		}

		// Put data back into the outbound queue to try again
		if (measures)
			_outboundQueue.unshift.apply(_outboundQueue, measures);

		setTimeout(send, _conf.sendInterval);

		return cb && cb();
	}

	function handleSucceed()
	{
		if (_sendFailed)
		{
			_log.info('Send succeeded after failure to %s:%d%s', opt.hostname, opt.port, opt.path);
			_sendFailed = false;
		}

		verbose('Successfully sent to %s:%d%s', opt.hostname, opt.port, opt.path);

		// Make sure we've done an initial refresh of plugins
		// We do this here so that we know have a current config before first refresh
		if (!_pluginsHaveRefreshed)
			refreshPlugins();

		setTimeout(send, _conf.sendInterval);

		return cb && cb();
	}

	function makeRequest()
	{
		opt.headers['Content-length'] = body.length;

		var req = _http.request(opt, function(res)
		{
			var output = '';
			res.on('data', function(data)
			{
				output += data.toString();
			});

			res.on('error', function(err)
			{
				return handleFail('unexpected ' + err);
			});

			res.on('end', function()
			{
				var obj;
				if (res.statusCode == 200)
				{
					obj = safeParseJSON(output, null);

					if (obj && obj.result)
					{
						// Call handlers for each response
						for(var i = 0; i < obj.result.length; i++)
						{
							if (handlers[i])
								handlers[i](obj.result[i]);
						}
					}

					handleSucceed();
				}
				else
				{
					obj = safeParseJSON(output, null);

					// Special case: if we receive an unknown metrics error then remove every other type from the batch
					// so we only try to resend the unknown metrics
					if (obj && obj.code === 'ERR_UNKNOWN_METRICS')
						measures = measures.filter(function(m) { return obj.metrics.indexOf(m[1]) != -1; });

					handleFail(obj && obj.message);
				}
			});
		});

		req.on('error', function(err)
		{
			handleFail(err.message);
		});

		req.end(body);
	}

	if (_conf.sendCompress)
	{
		_zlib.gzip(new Buffer(body,'utf8'), function(err, buff)
		{
			opt.headers['Content-Encoding'] = 'gzip';
			body = buff;
			makeRequest();
		});
	}
	else
		makeRequest();
}

/**
 * Scans local plugins
 *
 * @returns {boolean}
 */
function startLocal()
{
	var pdir = _path.join(__dirname, 'plugins');

	if (!_fs.existsSync(pdir))
	{
		_log.error('No plugins directory found');
		return false;
	}
	if (!_fs.lstatSync(pdir).isDirectory())
	{
		_log.error('plugins is not a directory');
		return false;
	}

	// We are running without remote config, scan for existing plugins
	var dirs = _fs.readdirSync(pdir);

	var good = 0;
	dirs.forEach(function(e)
	{
		var name = e.toLowerCase();

//		if (_conf.local && _conf.local.plugin && _conf.local.plugin[name] && !_conf.local.plugin[name].disabled)
//			good += loadPlugin(e) ? 1 :0;
	});

	if (!good)
	{
		_log.error('No usable or enabled plugins found');

		return false;
	}

	return true;
}

if (!validateConfig())
{
	process.exit(1);
}

_http = require(_conf.sendClear ? 'http' : 'https');

if (_argv.local)
{
	_log.info('Remote configuration disabled');

	if (!startLocal())
		_log.info('No plugins running');
}
else
	_log.info('Remote configuration enabled');

function closeAndExit()
{
	process.removeAllListeners('SIGINT');
	process.removeAllListeners('SIGKILL');
	process.removeAllListeners('SIGTERM');

	process.exit(0);
}

process.on('exit', function()
{
	_log.info('exiting');
});

process.on('SIGINT', closeAndExit);
process.on('SIGKILL', closeAndExit);
process.on('SIGTERM', closeAndExit);

_outboundQueue = new _cbuff(_conf.sendBacklog);
_outboundQueue.overflow = handleEject;

_log.info('graphdat-relay running');

setInterval(checkEject, 5000);
setTimeout(send, _argv.local ? _conf.sendInterval : 0);
poll();


