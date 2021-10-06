#!/usr/bin/python3

# requires database filled by running condensate.py

import collections
import random
import re
import sys
from common import get_option, make_connection
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs
from pinhole_base import PinholeBase

VertexOcc = collections.namedtuple('VertexOcc', 'vertex count')

class RefNet(PinholeBase):
    def __init__(self, cur, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        random.seed()
        self.sample_max = int(get_option("chord_sample_max", "3"))
        self.nick_rx = re.compile('@([-\\w]+)')
        self.link_nest = {} # source variant -> target variant -> source town name -> target town name -> list of URLs

    def load_item(self, et):
        self.extend_date(et)
        source_hamlet_name = et.get('osobaid')
        source_variant = self.get_variant(source_hamlet_name)
        if source_variant:
            self.do_load_item(et, source_hamlet_name, source_variant)

    def do_load_item(self, et, source_hamlet_name, source_variant):
        link_map = {}
        edge_set = set()
        txt = et.get('text')
        for m in self.nick_rx.finditer(txt):
            target_town_name = m.group(1)
            target_hamlet_name = self.town2hamlet.get(target_town_name)
            if target_hamlet_name and (target_hamlet_name != source_hamlet_name):
                target_variant = self.get_variant(target_hamlet_name)
                if target_variant:
                    pair_set = link_map.setdefault((source_variant, target_variant), set())
                    pair_set.add((source_hamlet_name, target_town_name))

                    source_node = self.introduce_node(source_variant, self.distinguish)
                    target_node = self.introduce_node(target_variant, False)
                    edge_set.add((source_node, target_node))

        for var_pair, pair_set in link_map.items():
            for name_pair in pair_set:
                source_town_name = self.hamlet2town.get(name_pair[0])
                if source_town_name:
                    target_town_name = name_pair[1]
                    outer_source = self.link_nest.setdefault(var_pair[0], {})
                    outer_target = outer_source.setdefault(var_pair[1], {})
                    inner_source = outer_target.setdefault(source_town_name, {})
                    inner_target = inner_source.setdefault(target_town_name, [])
                    inner_target.append(et['url'])

        for edge in edge_set:
            weight = self.ref_map.get(edge, 0)
            self.ref_map[edge] = weight + 1

    def make_matrix_desc(self):
        matrix_desc = {}
        for source_variant, outer_source in self.link_nest.items():
            source_name = self.get_presentation_name(source_variant)
            for target_variant, outer_target in outer_source.items():
                target_name = self.get_presentation_name(target_variant)
                vertices = self.get_cell_vertices(outer_target)
                row = matrix_desc.setdefault(source_name, {})
                row[target_name] = "\n".join(vertices[:3])

        return matrix_desc

    def make_samples(self):
        sample_matrix = {}
        for source_variant, outer_source in self.link_nest.items():
            source_name = self.get_presentation_name(source_variant)
            for target_variant, outer_target in outer_source.items():
                target_name = self.get_presentation_name(target_variant)
                sample_list = self.get_cell_urls(outer_target)
                if len(sample_list) > self.sample_max:
                    random.shuffle(sample_list)

                row = sample_matrix.setdefault(source_name, {})
                row[target_name] = sample_list[0:self.sample_max]

        return sample_matrix

    def get_cell_vertices(self, outer_target):
        occurences = []
        for source_town_name, inner_source in outer_target.items():
            for target_town_name, inner_target in inner_source.items():
                vtx = "@%s \u2192 @%s" % (source_town_name, target_town_name)
                cnt = len(inner_target)
                occurences.append(VertexOcc(vertex=vtx, count=cnt))

        occurences.sort(key=lambda vo: (-1 * vo.count, vo.vertex))
        return [ vo.vertex for vo in occurences ]

    def get_cell_urls(self, outer_target):
        url_set = set()
        for source_town_name, inner_source in outer_target.items():
            for target_town_name, inner_target in inner_source.items():
                url_set.update(inner_target)

        return list(url_set)


def main():
    ca = ConfigArgs()
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            ref_net = RefNet(cur, not ca.chord, parties)
            try:
                ref_net.run()
                if not ca.chord:
                    ref_net.dump_standard()
                else:
                    ref_net.dump_chord(ca.chord)
            finally:
                ref_net.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
