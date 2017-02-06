"""Utility methods for setting up apps."""

import logging

from flask import request

# All responses are json, so we tag them as such at the app level to
# provide uniform protection against content-sniffing-based attacks.
def add_headers(response):
  response.headers['Content-Disposition'] = 'attachment'
  response.headers['X-Content-Type-Options'] = 'nosniff'
  response.headers['Content-Type'] = 'application/json'
  return response

# Some uniform logging of request characteristics before any checks are applied.
def request_logging():
  logging.info('Request protocol: HTTPS={}'.format(request.environ.get('HTTPS')))
