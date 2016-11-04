from google.appengine.ext import ndb
from config import Config, ALLOWED_USER, ALLOW_INSECURE

example_user = 'example@example.com'
Config(config_key=ALLOWED_USER, value=example_user).put()
Config(config_key=ALLOW_INSECURE, value='True').put()
print("Added %s"%example_user)
