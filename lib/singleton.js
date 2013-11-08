// Insures a single instance within the directory
var _path = require('path');
var _package = _path.join(process.cwd(), 'package.json');
var _fs = require('fs');

var _filename = _path.join(__dirname, '_single');
var _fd;

function check(cb)
{
	try
	{
		_fd = _fs.openSync(_filename, 'wx');
		cb(null);
	}
	catch(ex)
	{
		cb('Only one instance of ' + _package.name + ' is allowed at one time.  If you think this error is incorrect try removing ' + _filename + ' and re-start.');
	}
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