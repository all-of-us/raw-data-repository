"""A clock that returns system time by default, but can be overridden for testing purposes.

Clocks are timezone naive, and operate in UTC.
"""
import datetime


class FakeClock:
  def __init__(self, now):
    self.now = now

  def __enter__(self):
    CLOCK.set_now(self.now)
    return self.now

  def __exit__(self, t, value, traceback):
    CLOCK.set_now(None)

  def advance(self, delta=None):
    self.now += delta or datetime.timedelta(minutes=1)


class Clock:
  def __init__(self):
    self.t = None

  def set_now(self, now):
    self.t = now

  def now(self):
    return self.t or datetime.datetime.utcnow()


CLOCK = Clock()
