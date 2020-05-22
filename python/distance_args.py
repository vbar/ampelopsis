import sys

class ConfigArgs:
    def __init__(self):
        histogram = False
        for a in sys.argv[1:]:
            if histogram is True:
                histogram = a
            elif a == '--histogram':
                histogram = True

        if histogram is True:
            raise Exception("command-line option missing argument")

        if histogram is False:
            histogram = None

        self.histogram = histogram
