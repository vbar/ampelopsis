ampelopsis is a simple crawler, recursively downloading pages from
specified hosts. It doesn't do much with the downloaded pages beyond
storing them (in ZIP archives, indexed from a PostgreSQL database) -
it's designed to be a single step in a larger, more useful
pipeline. It also isn't particularly configurable - rather, it's meant
to be extended programmatically. ampelopsis is written in Python 3,
and requires PycURL (for parallelized download) and lxml (for HTML
parsing).

Before running ampelopsis, it's necessary to create its database, also
named ampelopsis, by running the scripts in sql source directory.
Database access must be specified in an ampelopsis.ini configuration
file, placed in the top directory (one level above code directory):

<pre>
[root]
dbuser = name
dbpass = password
</pre>

After that, the crawler is ready to run, except it doesn't yet know
what to crawl. That is specified in the ampelopsis database tables,
and the easiest way to initialize them is by running the seed.py
script with the top-level URLs (or hostnames) on its command line.
The crawler follows links only to hostnames passed to seed.py .

Crawler is in the download.py script - when run, it downloads the
top-level URLs and waits for more work. More work is normally provided
by the parse.py script, which parses the downloaded pages and stores
links in them (that have the whitelisted domains) to the
database. download.py and parse.py can be run at the same time, and
multiple times in parallel (which is more practical for parsing than
download - parallel downloads can easily overload the target host);
they communicate through the database, using PostgreSQL notifications.
For further details, use the source.
