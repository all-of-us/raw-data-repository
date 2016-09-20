"""Utilities used by the API definition.
"""

import endpoints

import config

def check_auth():
  current_user = endpoints.get_current_user()
  if current_user is None or current_user.email() not in config.ALLOWED_USERS:
    raise endpoints.UnauthorizedException('Forbidden.')
