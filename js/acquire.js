var page = require('webpage').create(),
    fs = require('fs'),
    system = require('system');

page.onConsoleMessage = function(msg) {
    console.log('log: ' + msg);
};

if (system.args.length !== 3) {
    console.log('Usage: acquire.js <source URL> <target file>');
    phantom.exit();
}

page.onLoadFinished = function() {
    console.log("page load finished");
    fs.write(system.args[2], page.content, 'w');
    phantom.exit();
};

page.open(system.args[1], function() {
});
