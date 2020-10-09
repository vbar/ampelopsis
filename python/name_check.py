import re

# we could include single quote, but there probably aren't any Czech
# politicians named O'Something...
name_char_rx = re.compile("[^\\w ./-]")

def normalize_name(raw):
    name = name_char_rx.sub("", raw.strip())
    return name.lower()

class NameCheck:
    def __init__(self):
        self.names = None

    def walk(self, tree):
        self.names = set()
        self.do_walk(tree, True)
        return self.names

    def do_walk(self, in_node, check):
        if type(in_node) is dict:
            if check:
                pair_dict = {}
                for k, v in in_node.items():
                    if (k in ('firstName', 'lastName')) and (type(v) is str):
                        pair_dict[k] = v
                        if len(pair_dict) > 1:
                            self.add_name(pair_dict)
                    else:
                        self.do_walk(v, k == 'statementOfficial')
            else:
                for k, v in in_node.items():
                    self.do_walk(v, k == 'statementOfficial')

        elif type(in_node) is list:
            for it in in_node:
                self.do_walk(it, False)

    def add_name(self, pair_dict):
        pair_list = [ normalize_name(pair_dict[n]) for n in ('firstName', 'lastName') ]
        if all(pair_list):
            norm_name = "%s %s" % tuple(pair_list)
            self.names.add(norm_name)
