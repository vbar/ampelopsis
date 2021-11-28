from contextlib import contextmanager
from threading import Lock

class Pool:
    def __init__(self, ctor, max_size=3):
        assert max_size > 0
        self.ctor = ctor
        self.max_size = max_size
        self.lock = Lock()
        self.pool = []

    @contextmanager
    def get_resource(self):
        obj = None
        try:
            obj = self.summon()
            yield obj
        finally:
            if obj:
                self.dismiss(obj)

    def summon(self):
        while self.get_size():
            with self.lock:
                if len(self.pool):
                    return self.pool.pop()

        return self.ctor()

    def dismiss(self, obj):
        with self.lock:
            while len(self.pool) >= self.max_size:
                self.pool.pop()

            self.pool.append(obj)

    def get_size(self):
        with self.lock:
            return len(self.pool)
