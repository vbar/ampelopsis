#!/usr/bin/python3

# requires download with funnel_links set to 3 and database filled by
# running condensate.py

from lxml import etree
import re
import sys
from urllib.parse import urljoin
from common import get_option, make_connection
from distance_args import ConfigArgs
from pinhole_base import PinholeBase
from url_heads import alt_town_url_head

rel_town_rx = re.compile("^/([a-zA-Z0-9_./-]+)")


def compute_set_score(a, b):
    union = a | b
    den = len(union)
    if not den:
        return None

    intersection = a & b
    nom = len(intersection)
    return nom / den


class Processor(PinholeBase):
    def __init__(self, cur):
        PinholeBase.__init__(self, cur, False, '*')
        self.silent = True
        self.link_threshold = float(get_option("inverse_distance_threshold", "0.01"))
        self.html_parser = etree.HTMLParser()
        self.hamlet2followers = {}

    def load_item(self, et):
        hamlet_name = et['osobaid']
        town_name = self.hamlet2town.get(hamlet_name)
        if town_name:
            self.extend_date(et)
            if hamlet_name not in self.hamlet2followers:
                self.hamlet2followers[hamlet_name] = self.make_followers_set(town_name)

    def enrich(self, gd):
        PinholeBase.enrich(self, gd)

        for gn in gd['nodes']:
            node_idx = gn['node']
            hamlet_name = self.node2variant[node_idx]
            following = self.hamlet2followers[hamlet_name]
            gn['doc_count'] = len(following)

    def process(self):
        persons = []
        vector = []
        for hamlet_name, fset in sorted(self.hamlet2followers.items(), key=lambda p: (-1 * len(p[1]), p[0])):
            persons.append(hamlet_name)
            vector.append(fset)

        l = len(vector)
        for i in range(l):
            for j in range(i + 1, l):
                print("measuring similarity between %s and %s..." % (persons[i], persons[j]), file=sys.stderr)
                sim = compute_set_score(vector[i], vector[j])
                if (sim is not None) and (sim > self.link_threshold):
                    # hamlet name is-a variant
                    low_node = self.introduce_node(persons[i], False)
                    high_node = self.introduce_node(persons[j], False)
                    edge = (low_node, high_node)
                    self.ref_map[edge] = 1 / sim

    def make_followers_set(self, town_name):
        town_set = set()
        url = "%s/%s/followers" % (alt_town_url_head, town_name)
        while url:
            url_id = self.get_url_id(url)
            if not url_id:
                return town_set

            root = self.get_html_document(url_id)
            if not root:
                print("cannot parse: " + url, file=sys.stderr)
                return town_set

            town_set = town_set | self.get_followers_set(root)
            url = self.get_followers_next(url, root)

        return frozenset(town_set)

    @staticmethod
    def get_followers_set(root):
        page_set = set()
        attrs = root.xpath("//a[@data-scribe-action='profile_click']/@href")
        for a in attrs:
            m = rel_town_rx.match(a)
            if m:
                name = m.group(1)
                page_set.add(name.lower())

        return page_set

    @staticmethod
    def get_followers_next(base_url, root):
        next_url = None
        attrs = root.xpath("//div[@class='w-button-more']/a/@href")
        for a in attrs:
            nu = urljoin(base_url, a)
            if next_url is None:
                next_url = nu
            elif next_url != nu:
                raise Exception("%s has multiple more buttons" % base_url)

        return next_url

    def get_html_document(self, url_id):
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            return None

        try:
            return etree.parse(reader, self.html_parser)
        finally:
            reader.close()


def main():
    ca = ConfigArgs()
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
                processor.process()
                processor.dump_undirected()
                if ca.histogram:
                    processor.dump_distance_histogram(ca.histogram)
            finally:
                processor.close()


if __name__ == "__main__":
    main()
