import datetime

class TimelineHelperMixin:
    def __init__(self, bin_scale):
        if bin_scale == 'minutes':
            self.quantize = self.quantize_to_minutes
            self.get_step = self.get_step_minutes
        elif bin_scale == 'hours':
            self.quantize = self.quantize_to_hours
            self.get_step = self.get_step_hours
        else:
            raise Exception("invalid time scale: " + bin_scale)

    def get_step_minutes(self):
        return datetime.timedelta(minutes=1)

    def get_step_hours(self):
        return datetime.timedelta(hours=1)

    def quantize_to_minutes(self, pdt):
        return pdt.replace(microsecond=0, second=0)

    def quantize_to_hours(self, pdt):
        return pdt.replace(microsecond=0, second=0, minute=0)
