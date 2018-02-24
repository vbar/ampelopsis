ampelopsis is a simple crawler, recursively downloading pages from
specified hosts. It doesn't do much with the downloaded pages beyond
storing, optionally compressing and indexing them (in a PostgreSQL
database) - it's designed to be a step in a larger, more useful
pipeline, modular and easily extensible. Its code is in scripts in the
python source directory. ampelopsis is written in Python 3, and in
addition to psycopg2, it requires PycURL (for parallelized download)
and lxml (for HTML parsing).

Before running ampelopsis, it's necessary to create its database, also
named ampelopsis, by running the scripts in sql source directory.
Database access must be specified in an ampelopsis.ini configuration
file, placed in the top directory (above the python and sql
directories):

<pre>
[root]
dbuser = name
dbpass = password
</pre>

Other configuration settings are optional; some are mentioned
below. After configuration, the crawler is ready to run, except it
doesn't yet know what to crawl. That is specified in the ampelopsis
database tables, and the easiest way to initialize them is by running
the seed.py script with the top-level URLs (or hostnames) on its
command line. The crawler follows links only to hostnames passed to
seed.py (or, less strictly, to their domains, when the match_domain
option is set).

Default downloader is in the download.py script - when run, it
downloads the top-level URLs and waits for more work. More work is
normally provided by the parse.py script, which parses the downloaded
pages and stores links in them (that have the whitelisted domains) to
the database. Downloader and parser can be run at the same time, and
multiple times in parallel, which isn't particularly useful for the
default setup (download.py multiplexes connections internally, up to
the value of max_num_conn setting, and parse.py is generally fast
enough to keep up with download), but becomes handy when using
drive.py (see doc/js.md).

This package contains just one parser, but it's expected that specific
scraping projects will modify or replace it, primarily to be more
selective in which URLs it feeds back to downloader. Using the
unmodified parse.py, found links can be filtered by specifying
url_whitelist_rx or url_blacklist_rx setting. Downloader and parser
communicate through the database, using PostgreSQL notifications.

Some other notes are available in the doc directory. For more detail,
use the source.
