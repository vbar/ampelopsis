import sys
from common import get_option

class ConfigArgs:
    def __init__(self):
        chord = False
        for a in sys.argv[1:]:
            if chord is True:
                chord = a
            elif a == '--chord':
                chord = True

        if chord is True:
            raise Exception("command-line option missing argument")

        if chord is False:
            chord = get_option("chord_output", "")

        self.chord = chord
