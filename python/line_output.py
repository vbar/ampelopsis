import csv
import matplotlib.pyplot as plt
import sys
from common import get_option

class TextLineOutput:
    def __init__(self, series):
        self.series = series

    def output(self):
        writer = csv.writer(sys.stdout, delimiter=",")
        writer.writerow(["date", "value"])
        for point in self.series:
            writer.writerow(point)


class GraphicLineOutput:
    def __init__(self, series):
        self.xseries = []
        self.yseries = []
        for point in series:
            self.xseries.append(point[0])
            self.yseries.append(point[1])

    def output(self):
        plt.plot(self.xseries, self.yseries)
        plt.show()


class ConfigLineOutput:
    def __init__(self, series):
        fmt = get_option("line_output_format", "both")
        if fmt not in ("none", "stream", "screen", "both"):
            raise Exception("option line_output_format has invalid value " + fmt)

        self.outputs = []
        if fmt in ("stream", "both"):
            self.outputs.append(TextLineOutput(series))

        if fmt in ("screen", "both"):
            self.outputs.append(GraphicLineOutput(series))

    def output(self):
        for output in self.outputs:
            output.output()
