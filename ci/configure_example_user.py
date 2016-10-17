from google.appengine.ext import ndb
from config import Config, ALLOWED_USER

example_user = 'example@example.com'
Config(config_key=ALLOWED_USER,
       value=example_user).put()

print("Added %s"%example_user)
