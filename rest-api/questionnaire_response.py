from google.appengine.ext import ndb


class QuestionnaireResponse(ndb.Model):
  """The questionnaire response."""
  resource = ndb.JsonProperty()
