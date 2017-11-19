var page_factory = require('webpage'),
    fs = require('fs'),
    system = require('system'),
    server = require('webserver').create();

if (system.args.length !== 2) {
    console.log('Usage: standalone.js <port>');
    phantom.exit();
}

var port = parseInt(system.args[1]),
    service = server.listen(port, function (request, response) {
	var src = request.post['src'],
	    dst = request.post['dst'];
	if (!src || !dst) {
	    console.log('>>>' + request.postRaw + '<<<')
	    phantom.exit();
	} else {
	    var page = page_factory.create();
	    page.open(src, function(status) {
		if (status !== 'success') {
		    console.log('Unable to post: ' + status);
		    response.write(status);
		    response.close();
		} else {
		    var old_len = 0,
			try_count = 0,
			interval = setInterval(function () {
			    var len = page.content.length;
			    console.log(old_len + ' -> ' + len);
			    if ((len == old_len) && ((old_len > 0) || (try_count >= 20))) {
				clearInterval(interval);
				fs.write(dst, page.content, 'w');			
				response.statusCode = 200;
				response.headers = {
				    'Cache': 'no-cache',
				    'Content-Type': 'text/plain;charset=utf-8'
				};

				response.write(request.postRaw);
				response.close();
				page.close();
			    } else {
				old_len = len;
				++try_count;
			    }
			}, 100);
		}
	    });
	}
    });


