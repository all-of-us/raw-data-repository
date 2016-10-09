"""A way to get a unique id.

Goals:
    - IDs are short enough that they can be written down.
    - IDs are globally unique.
"""

import random
import time

from google.appengine.api.datastore_errors import TransactionFailedError
from google.appengine.ext import ndb


# Start with a range between 1M and 100M.
MIN_ID = 100000
MAX_ID = 9999999


class IdReservation(ndb.Model):
  pass

def get_id():
  """Reserve a globally unique ID.

  The system will create a random number between MIN_ID and MAX_ID.  It then
  attempts to create a record in datastore reserving that ID.  If the attempt
  succeeds, the ID is handed out.  If it fails, it tries again.
  """
  while True:
    try:
      candidate = random.randint(MIN_ID, MAX_ID)
      # _check_and_create_record will create the record without using a
      # transaction.  Then _reserve_candidate will flip the reserved flag within
      # a transaction.  Unfortunately, it doesn't appear that transactional
      # gurantees don't extend to two threads creating the same entity at the
      # same time.
      if _reserve_candidate(candidate):
        return candidate
    except TransactionFailedError:
      pass

@ndb.transactional(retries=0)
def _reserve_candidate(candidate, testing_sleep=0):
  """Make sure that a given id is available.

  Args:
    candidate: The integer id to try and reserve.

  Returns:
    True if the reservation succeeded.
  """
  key = ndb.Key(IdReservation, candidate)
  existing = key.get()
  if existing:
    return False

  reservation = IdReservation(key=key)

  time.sleep(testing_sleep)
  reservation.put()
  return True


random.seed()
