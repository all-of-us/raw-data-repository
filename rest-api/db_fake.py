"""Used to replace standard db connection with a fake."""

import db


def db_fake():
  """Causes all subsequent usages of the db module to use a fake connection."""
  mock_connection = FakeConnection()
  db.get_connection = lambda: mock_connection
  return mock_connection


class FakeConnection(object):
  """Used to fake out all db access.

  A Fake object that masquerades as a db.Connection, a mySQLdb connection and a
  cursor.  It is very limited in functionality, as of yet it only supports
  commit, execute, and fetchall.
  """
  def __init__(self):
    self.conn = self  # Hey, we're already a fake, who cares!
    self.expectations = {}
    self.committed = False
    self.results = None

  def release(self):
    pass

  def commit(self):
    self.committed = False

  def rollback(self):
    pass

  def cursor(self):
    return self

  def add_expectation(self, query, values, results=None):
    results = results or []
    q = (query, values)
    self.expectations[q] = results

  def execute(self, query, values):
    q = (query, tuple(values))
    self.results = self.expectations.get(q, None)
    if self.results == None:
      raise BaseException('Unexpected query {} with values {}'.format(*q))

  def fetchall(self):
    if not self.results:
      raise BaseException("No results in fake db")
    results = self.results
    self.results = None
    return results
