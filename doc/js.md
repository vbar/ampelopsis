drive.py is an alternative downloader using Selenium and Chrome, for
pages that need to run JavaScript to be interesting. The Chrome
process is started (and, if everything goes well, ended) by the script
using chromedriver, which must be on path.

Unlike download.py, drive.py normally requires code changes for every
specific project. In the first place, the default condition for
considering the page loaded is simply the presence of &lt;a&gt;
element in it - most projects will want to look for something more
specific. Using a different browser would also require code changes.
