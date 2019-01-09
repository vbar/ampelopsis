class JsonTreeCheck:
    def __init__(self, node_name, check_rx):
        self.node_name = node_name
        self.check_rx = check_rx

    def walk(self, in_node):
        if type(in_node) is dict:
            for k, v in in_node.items():
                w = None
                if k == self.node_name:
                    w = self.normalize(v)
                    if w and self.check_rx.search(w):
                        return True

                if (w is None) and self.walk(v):
                    return True
        elif type(in_node) is list:
            for it in in_node:
                if self.walk(it):
                    return True

        return False

    def normalize(self, v):
        if not type(v) is str:
            return None

        l = v.lower()
        return l.strip()
