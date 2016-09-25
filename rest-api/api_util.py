"""Utilities used by the API definition.
"""

import endpoints
import config
import os

from google.appengine.api import oauth

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'


def check_auth():
  user = endpoints.get_current_user()
  return is_user_whitelisted(user)

def check_auth_flask():
  user = oauth.get_current_user()
  return is_user_whitelisted(user)

def is_user_whitelisted(user):
  if user and user.email() in config.getSettingList(config.ALLOWED_USER):
    return

  raise endpoints.UnauthorizedException('Forbidden.')
