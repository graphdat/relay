var _defaultConfig =
{
	sendInterval : 4000,
	pollInterval : 1000,
	sendCompress : true,
	sendHost : 'localhost',
	sendPort : 8005,
	sendBacklog : 200,
	sendClear : true
};

var _fs = require('fs');
var _path = require('path');
var _util = require('util');
var _os = require('os');
var _childProcess = require('child_process');
var _zlib = require('zlib');
var _ = require('underscore');
var _optimist = require('optimist');
var _cbuff = require('CBuffer');
var _http;

var _log = console;
var _plugins = {};
var _outboundQueue;
var _ejectedCount = 0;

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
	.wrap(80);

var _argv = _opt.argv;

if (_argv.help)
{
	_opt.showHelp();
	return;
}


process.on('uncaughtException', function(err)
{
	_log.error(err);
	if (err.stack)
		_log.error(err.stack);

	process.exit(1);
});

var _conf = require('./config.json');

/***
 * Check that our config has the correct values
 *
 * @returns {boolean}
 */
function validateConfig()
{
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

	_log.info.apply(_log, Array.prototype.slice.call(arguments, 0));
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

	p.err.push(str);

	verbose('plugin %s had an error: %s', p.name, str);
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
 * <metric name> <measure> [<source>] [<unix timestamp>]
 *
 * If source is omitted the host name is assumed
 * If timestamp is omitted current time is used
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
		source = _os.hostname();
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

/***
 * Make sure the command for the plugin is running
 *
 * @param p
 */
function ensureProcess(p)
{
	if (p.prc)
		return;

	verbose('spawning plugin %s, command "%s"', p.name, p.command);

	var parts = parseCommand(p.command);

	p.prc = _childProcess.spawn(parts[0], parts.slice(1), {cwd : p.dir, env : process.env});

	p.prc.stderr.setEncoding('utf8');
	p.prc.stderr.on('data', function(data)
	{
		var m = data.match(/execvp\(\): (.*)/);

		if (m)
		{
			pluginErr(p, 'unable to execute %s : %s', p.command, m[1]);
		}
		else
		{
			pluginErr(p, data);
		}

		p.prc = null;
	});

	p.prc.stdout.setEncoding('utf8');
	p.prc.stdout.on('data', function(data)
	{
		verbose('Received "%s" from plugin %s', data, p.name);
		addData(p, data);
	});
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
	if (!_outboundQueue.size)
		return;

	verbose('sending %d measurement(s)', _outboundQueue.size);

	var tosend = _outboundQueue.toArray();
	_outboundQueue.empty();

	var opt =
	{
		hostname : _conf.sendHost,
		port : _conf.sendPort,
		method : 'POST',
		auth :  _conf.email + ':' + _conf.apiToken,
		path : '/v1/measurements',
		headers :
		{
			'Content-Type' : 'application/json',
			'Connection' : 'keep-alive'
		}
	};

	var body = JSON.stringify(tosend);;

	function handleFail(msg)
	{
		_log.error('Failed to send to %s:%d%s: %s, will retry sending data', opt.hostname, opt.port, opt.path, msg);

		// Put data back into the outbound queue to try again
		_outboundQueue.unshift.apply(_outboundQueue, tosend);

		setTimeout(send, _conf.sendInterval);

		return cb && cb();
	}

	function handleSucceed()
	{
		verbose('Successfully sent to %s:%d%s', opt.hostname, opt.port, opt.path);

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
				if (res.statusCode == 200)
					handleSucceed();
				else
				{
					var obj = safeParseJSON(output);

					// Special case: if we receive an unknown metrics error then remove every other type from the batch
					// so we only try to resend the unknown metrics
					if (obj && obj.code === 'ERR_UNKNOWN_METRICS')
						tosend = tosend.filter(function(m) { return obj.metrics.indexOf(m[1]) != -1; });

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
 * Parse JSON text without throwing an exception.
 * Pass in a path for error reporting purposes only.
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
	}

	return obj;
}

/***
 * Fills in macros within the text given using the parameters from the plugin param.json
 * Also checks for reserved parameters.  These include:
 *
 * 		pollInterval
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
			if (fReq && !param)
			{
				pluginErr(p, 'Missing required param %s while formatting "%s"', res[1], txt);
				val = '';
			}
			else
			{
				val = param[res[1]];

				if (fReq && val === undefined)
					pluginErr(p, 'Missing required param %s while formatting "%s"', res[1], txt);
			}
		}

		// Replace all
		txt = txt.replace(new RegExp('\\$\\(' + res[1] + '\\)', 'g'), val);
	}

	return txt;
}

/***
 * Load plugin from the plugins folder by folder name.  Reads + validates configuration, adds to global plugin set
 *
 * @param pname
 * @returns {boolean}
 */
function loadPlugin(pname)
{
	var plugin = {};

	_plugins[pname] = plugin;

	var dir = _path.join(__dirname, 'plugins', pname);

	plugin.name = pname;
	plugin.err = new _cbuff(50);
	plugin.dir = dir;
	plugin.data = '';

	var path = _path.join(dir, 'plugin.json');

	if (!_fs.existsSync(path))
	{
		pluginErr(plugin, 'Unable to find %s, skipping plugin', path);
		plugin.disabled = true;
		return false;
	}

	var config = safeParseJSONFile(path);
	if (!config)
	{
		_log.error('Skipping plugin %s', pname);
		plugin.disabled = true;
		return false;
	}

	// If we've got params, load them
	var pathParam = _path.join(dir, 'param.json');
	var param = safeParseJSONFile(pathParam);


	function checkParam(name, def)
	{
		var val;

		// First check params
		val = param && param[name];

		// If no param, try config
		if (!val)
			val = config[name];

		// If no config, try default
		if (!val)
			val = def;

		// If still nothing and there was no default we fail
		if (!val && arguments.length == 1)
		{
			pluginErr(plugin, 'Missing required config parameter %s, skipping plugin %s', name, pname);
			plugin.disabled = true;
			return false;
		}

		// Do replacement
        plugin[name] = replaceParams(plugin, val, param);

		return true;
	}

	checkParam('pollInterval', _conf.pollInterval);
	checkParam('disabled', false);

	if (!checkParam('command'))
	{
		plugin.disabled = true;
		return false;
	}

	if (plugin.disabled)
	{
		_log.info('Plugin %s disabled by config, skipping', pname);
		return true;
	}

	plugin.nextPoll = Date.now();

	_log.info('Loaded plugin %s', pname);

	return true;
}

if (!validateConfig())
{
	process.exit(1);
}

_http = require(_conf.sendClear ? 'http' : 'https');

if (_argv.local)
{
	_log.info('Running local');

	var pdir = _path.join(__dirname, 'plugins');

	if (!_fs.existsSync(pdir))
	{
		_log.error('No plugins directory found, exiting');
		process.exit(1);
	}
	if (!_fs.lstatSync(pdir).isDirectory())
	{
		_log.error('plugins is not a directory, exiting');
		process.exit(1);
	}

	// We are running without remote config, scan for existing plugins
	var dirs = _fs.readdirSync(pdir);

	var good = 0;
	dirs.forEach(function(e) { good += loadPlugin(e) ? 1 :0; });

	if (!good)
	{
		_log.error('No usable plugins found, exiting');

		process.exit(1);
	}
}
else
{
	_log.info('Running with remote configuration');
}

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
setTimeout(send, _conf.sendInterval);
poll();


