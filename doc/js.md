drive.py is an alternative downloader using phantomjs, for pages that
need to run JavaScript to be interesting. It can either start
phantomjs for every downloaded page, or use a standalone phantomjs
process as a kind of proxy (it isn't quite a proxy, in that it doesn't
send the downloaded page back to drive.py but writes it directly
to disk, but from the user's point of view that doesn't make much
difference). To start the standalone script, run

<pre>
phantomjs js/standalone.js 9000
</pre>

where the argument to standalone.js is a port number, which must also
be configured in ampelopsis.ini:

<pre>
[root]
...
phantomjs_port = 9000
</pre>

When drive.py is run without configured phantomjs_port, it'll run
js/acquire.js under a new instance of phantomjs for each downloaded
page.