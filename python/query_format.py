from urllib.parse import urlencode, urlunparse

# search URLs from twint (except with sorted query params)

QUERY_FROM = 1
QUERY_RE = 2
QUERY_TO = 3


def format_home(town_name):
    return "https://mobile.twitter.com/" + town_name


class QueryFormat:
    def __init__(self, since):
        self.since = since

    def format_all(self, town_name):
        templ = [ "https", "twitter.com", "/i/search/timeline", "", "", "" ]
        urls = [ format_home(town_name) ]
        for query_mode in (QUERY_FROM, QUERY_RE, QUERY_TO):
            templ[4] = self.format_params(town_name, query_mode)
            urls.append(urlunparse(templ))

        return urls

    def format_params(self, town_name, query_mode):
        params = [
            ('f', 'tweets'),
            ('include_available_features', '1'),
            ('include_entities', '1'),
            ('max_position', '-1')
        ]

        params.append(('q', self.format_query(town_name, query_mode)))
        params.extend([
            ('reset_error_state', 'false'),
            ('src', 'unkn'),
            ('vertical', 'default'),
        ])

        return urlencode(params)

    def format_query(self, town_name, query_mode):
        if query_mode == QUERY_TO:
            q = "to:%s since:%d" % (town_name, self.since.timestamp())
        else:
            q = "from:%s since:%d" % (town_name, self.since.timestamp())
            if query_mode == QUERY_RE:
                q += " filter:nativeretweets"

        return q
