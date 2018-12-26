import re
from urllib.parse import urlparse

# SPARQL query result row subset used for enrichment of JSON results
# (or any other further processing)
class Pellet:
    datetime_rx = re.compile('^([0-9]{4}-[0-9]{2}-[0-9]{2})T00:00:00Z$')

    def __init__(self, wikidataId, birthDate, aboutLink):
        self.wikidataId = wikidataId
        self.birthDate = birthDate
        self.aboutLink = aboutLink

    def get_key(self):
        major = -1
        if self.aboutLink:
            pr = urlparse(self.aboutLink)
            major = 1 if pr.hostname == "cs.wikipedia.org" else 0

        return ( major, self.wikidataId )
