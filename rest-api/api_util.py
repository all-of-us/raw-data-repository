"""Utilities used by the API definition.
"""

import endpoints

import config

def check_auth():
  user = endpoints.get_current_user()
  if user and user.email() in config.getSettingList(config.ALLOWED_USER):
    return

  raise endpoints.UnauthorizedException('Forbidden.')
