import logging

from flask import request


def add_headers(response):
  """Add uniform headers to all API responses.

  All responses are JSON, so we tag them as such at the app level to provide uniform protection
  against content-sniffing-based attacks.
  """
  response.headers['Content-Disposition'] = 'attachment'
  response.headers['X-Content-Type-Options'] = 'nosniff'
  response.headers['Content-Type'] = 'application/json'
  return response

def request_logging():
  """Some uniform logging of request characteristics before any checks are applied."""
  logging.info('Request protocol: HTTPS={}'.format(request.environ.get('HTTPS')))
