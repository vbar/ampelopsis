ampelopsis downloaders (download.py and drive.py) store downloaded
pages under the tmp subdirectory of the top project directory. Name of
each file (which also determines its parent directory) is simply the
ID of its source URL stored in the ampelopsis database. In addition to
HTML contents, download.py also stores HTTP headers of the page into a
separate file (same name, with appended 'h').

This format is simple and almost readable without special tools
(nonetheless, the dump.py script, which converts downloaded URLs to
their page contents, is probably more convenient), but not
particularly space-efficient. When downloading more than a few GB,
it's therefore recommended to also run the compress.py script, which
packs the pages into zip archives. compress.py can be run in parallel
with download and parsing (but no more than once). compress.py
produces standard ZIP archives under the data subdirectory of the top
project directory. Note that compress.py doesn't successfully end -
when it has nothing more to do it just pauses, and creating the last
volume after download has ended must be invoked by interrupting
compress.py by Ctrl-C.

The default setup described above requires a separate top project
directory for every crawl and also a separate database, which may be
inconvenient when switching between multiple crawls on the same
desktop. Fortunately, PostgreSQL has a schema namespace mechanism,
which allows multiple tables with the same name to exist independently
in the same database, which is leveraged by ampelopsis to allow
multiple crawls using the same database. Use of (non-default) schema
is enabled by naming it in ampelopsis.ini:

<pre>
[root]
...
schema = testcrawl
</pre>

Schema names are also used as subdirectories of both tmp and data
directories, so they must be 1) valid PostgreSQL names, 2) valid
directory names and 3) not clashing with file and directory names
mentioned above (e.g. not composed of just numbers).

Setting a new schema in ampelopsis.ini config makes existing database
tables and file storage inaccessible to ampelopsis scripts using that
config; the database must be re-created by running the scripts in sql
source directory in the configured schema, then refilled (normally by
running download.py and parse.py, but copying existing crawler results
is also possible and occasionally useful). The easiest way to create
the tables in the correct schema is by running the genschema.py script
(after the config change), which adds a file into the sql directory
with PostgreSQL commands creating the schema and setting up search
path to it. The command

<pre>
cat sql/*.sql | psql
</psq>

then creates the tables in the correct schema (correct until the next
config change).
