"""A clock that returns system time by default, but can be overridden for testing purposes."""
import datetime

class FakeClock:
    def __init__(self, now):
        self.now = now

    def __enter__(self):
        CLOCK.set_now(self.now)
        return self.now

    def __exit__(self, type, value, traceback):
        CLOCK.set_now(None)

class Clock:
  def __init__(self):
    self.t = None

  def set_now(self, now):
    self.t = now

  def now(self):
    return self.t or datetime.datetime.now()

CLOCK = Clock()