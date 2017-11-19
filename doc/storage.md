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
with download and parsing (but no more than once).
