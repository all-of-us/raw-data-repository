import email.utils
import logging
import pytz
import time

from flask import request

import clock

_GMT = pytz.timezone('GMT')


def add_headers(response):
  """Add uniform headers to all API responses.

  All responses are JSON, so we tag them as such at the app level to provide uniform protection
  against content-sniffing-based attacks.
  """
  response.headers['Content-Disposition'] = 'attachment; filename="f.txt"'
  response.headers['X-Content-Type-Options'] = 'nosniff'
  response.headers['Content-Type'] = 'application/json; charset=utf-8'
  response.headers['Date'] = email.utils.formatdate(
      time.mktime(pytz.utc.localize(clock.CLOCK.now()).astimezone(_GMT).timetuple()),
      usegmt=True)
  response.headers['Pragma'] = 'no-cache'
  response.headers['Cache-control'] = 'no-cache, must-revalidate'
  return response


def request_logging():
  """Some uniform logging of request characteristics before any checks are applied."""
  logging.info('Request protocol: HTTPS={}'.format(request.environ.get('HTTPS')))
