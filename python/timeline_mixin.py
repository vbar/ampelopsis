import datetime

class TimelineMixin:
    def __init__(self, bin_scale):
        if bin_scale == 'minutes':
            self.quantize = self.quantize_to_minutes
            self.get_step = self.get_step_minutes
        elif bin_scale == 'hours':
            self.quantize = self.quantize_to_hours
            self.get_step = self.get_step_hours
        else:
            raise Exception("invalid time scale: " + bin_scale)

        self.key2timeline = {} # key -> list of datetime
        self.now_sorted = True # empty is sorted
        self.xseries = None # opt list of datetime
        self.value_series = None # opt key -> list of int

    def add_sample(self, key, rdt):
        dt = self.quantize(rdt)

        timeline = self.key2timeline.get(key)
        if not timeline:
            timeline = [ dt ]
            self.key2timeline[key] = timeline
        else:
            timeline.append(dt)
            self.now_sorted = False

    def is_empty(self):
        return not len(self.key2timeline)

    def get_xseries(self):
        self.lazy_model()
        return self.xseries

    def get_value_series(self):
        self.lazy_model()
        return self.value_series

    def lazy_model(self):
        if self.xseries:
            return

        xseries = []
        self.value_series = {}
        delta = self.get_step()
        dt = self.get_min_date()
        maxdt = self.get_max_date()
        idx_map = {}
        while dt <= maxdt:
            xseries.append(dt)
            for key, timeline in self.key2timeline.items(): # now sorted
                l = len(timeline)
                idx = idx_map.get(key, 0)
                freq = 0
                while (idx < l) and (dt == timeline[idx]):
                    freq += 1
                    idx += 1

                vseries = self.value_series.get(key)
                if not vseries:
                    vseries = [ freq ]
                    self.value_series[key] = vseries
                else:
                    vseries.append(freq)

                idx_map[key] = idx

            dt += delta

        self.xseries = xseries

    def get_step_minutes(self):
        return datetime.timedelta(minutes=1)

    def get_step_hours(self):
        return datetime.timedelta(hours=1)

    def quantize_to_minutes(self, pdt):
        return pdt.replace(microsecond=0, second=0)

    def quantize_to_hours(self, pdt):
        return pdt.replace(microsecond=0, second=0, minute=0)

    def get_min_date(self):
        self.lazy_sort()
        dt = None
        for dummy, timeline in self.key2timeline.items():
            if (dt is None) or (timeline[0] < dt):
                dt = timeline[0]

        return dt

    def get_max_date(self):
        self.lazy_sort()
        dt = None
        for dummy, timeline in self.key2timeline.items():
            if (dt is None) or (timeline[-1] > dt):
                dt = timeline[-1]

        return dt

    def lazy_sort(self):
        if self.now_sorted:
            return

        key2timeline = {}
        for key, timeline in self.key2timeline.items():
            key2timeline[key] = sorted(timeline)

        self.key2timeline = key2timeline
        self.now_sorted = True
