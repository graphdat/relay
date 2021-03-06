// Insures a single instance within the directory
var _fs = require('fs');
var _path = require('path');

var _filename = _path.join(__dirname, '_single');
var _package = require(_path.join(__dirname, '..', 'package.json'));

var _fd;

function check(cb)
{
	try
	{
		_fd = _fs.openSync(_filename, 'wx');
	}
	catch(ex)
	{
		return cb('Only one instance of ' + _package.name + ' is allowed at one time.  If you think this error is incorrect try removing ' + _filename + ' and re-start.');
	}

	cb(null);
}

process.on('exit', function()
{
	if (!_fd)
		return;

	_fs.closeSync(_fd);
	_fs.unlinkSync(_filename);
});

module.exports = {
	check : check
};
