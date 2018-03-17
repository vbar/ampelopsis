import re

class BreathPreference:
    def __init__(self):
        self.round = 0

    def prioritize(self, url):
        return self.round

    def mark_batch(self):
        self.round += 1
        
class NoveltyPreference:
    def __init__(self, high_mark, low_mark):
        assert high_mark > low_mark
        assert low_mark > 0
        self.high_mark = high_mark
        self.low_mark = low_mark
        self.segment_rx = re.compile("[:/.?&=,;]")
        self.occurence = {}
        
    def prioritize(self, url):
        raw_segments = self.segment_rx.split(url)
        raw_segments.pop(0) # ignore protocol
        segments = [ s for s in raw_segments if s ]
        l = len(segments)
        i = 1
        prio = 1 # 0 is for seeds
        while i < l:
            head = tuple(segments[0:i])
            cnt = self.occurence.get(head, 0)
            prio += (i * cnt)
            self.occurence[head] = cnt + 1
            i += 1

        if len(self.occurence) > self.high_mark:
            self.prune()

        if url.find('?') >= 0:
            prio *= 100
            
        return prio
        
    def mark_batch(self):
        pass
    
    def prune(self):
        occurence = {}
        # FIXME: should use heap
        lst = sorted([ (v, k) for k, v in self.occurence.items() ], reverse=True)
        for v, k in lst[:self.low_mark]:
            occurence[k] = v

        self.occurence = occurence
