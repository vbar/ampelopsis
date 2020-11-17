from common import get_option

granularity = {
    'weekday': ( 7, lambda dt: dt.weekday() ),
    'hour': ( 24, lambda dt: dt.hour ),
    'minute': ( 60, lambda dt: dt.minute ),
    'second': ( 60, lambda dt: dt.second )
}

class TimecycleMixin:
    def __init__(self):
        self.cycle = get_option('time_cycle_period', 'weekday')
        spec = granularity.get(self.cycle)
        if not spec:
            raise Exception("Unknown time_cycle_period" + self.cycle)

        self.period_size = spec[0]
        self.part_extractor = spec[1]
        self.hamlet2per = {} # str hamlet name -> array of period_size ints
