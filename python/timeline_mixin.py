from timeline_helper_mixin import TimelineHelperMixin

class TimelineMixin(TimelineHelperMixin):
    def __init__(self, bin_scale, puff=0):
        TimelineHelperMixin.__init__(self, bin_scale)
        self.puff = puff
        self.key2timeline = {} # key -> list of datetime
        self.now_sorted = True # empty is sorted
        self.xseries = None # opt list of datetime
        self.value_series = None # opt key -> list of int

    def add_sample(self, key, rdt):
        dt = self.quantize(rdt)

        timeline = self.key2timeline.get(key)
        if not timeline:
            timeline = []
            self.key2timeline[key] = timeline

        timeline.append(dt)

        # inspired by http://dl.ifip.org/db/conf/im/im2019-ws1-annet/191658.pdf
        if self.puff > 0:
            delta = self.get_step()
            before = dt
            after = dt
            for i in range(self.puff):
                before -= delta
                timeline.append(before)
                after += delta
                timeline.append(after)

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
