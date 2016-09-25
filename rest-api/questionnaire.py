'''The definition of the questionnaire object and DB marshalling.
'''

from google.appengine.ext import ndb


class Questionnaire(ndb.Model):
  """The questionnaire."""
  resource = ndb.JsonProperty()
