import sys
from common import get_option

class ConfigArgs:
    def __init__(self):
        meta = False
        matrix = False
        for a in sys.argv[1:]:
            if meta is True:
                meta = a
            elif a == '--meta':
                meta = True
            if matrix is True:
                matrix = a
            elif a == '--matrix':
                matrix = True

        if (meta is True) or (matrix is True):
            raise Exception("command-line option missing argument")

        if meta is False:
            meta = get_option("chord_meta", "")

        if matrix is False:
            matrix = get_option("chord_matrix", "")

        self.meta = meta
        self.matrix = matrix

    @property
    def distinguish(self):
        return not(self.matrix or self.meta)
