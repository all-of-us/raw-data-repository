'''The definition of the participant object.
'''
import fhir_datatypes

from google.appengine.ext import ndb

class ProviderLink(ndb.Model):
  """A link between a participant and an outside institution."""
  primary = ndb.BooleanProperty()
  organization = ndb.StructuredProperty(fhir_datatypes.FHIRReference, repeated=False)
  site = ndb.LocalStructuredProperty(fhir_datatypes.FHIRReference, repeated=True)
  identifier = ndb.LocalStructuredProperty(fhir_datatypes.FHIRIdentifier, repeated=True)

class Participant(ndb.Model):
  """The participant resource definition"""
  participantId = ndb.StringProperty()
  biobankId = ndb.StringProperty()
  # TODO: rename to lastModified (with data_access_object)
  last_modified = ndb.DateTimeProperty(auto_now=True)
  # Should this be indexed? If so, switch to StructuredProperty here and above
  # Should this be provider_link?
  providerLink = ndb.LocalStructuredProperty(ProviderLink, repeated=True)

  def get_primary_provider_link(self):
    if self.providerLink:
      for provider in self.providerLink:
        if provider.primary:
          return provider
    return None

# Fake history entry created to represent the age of the participant
class BirthdayEvent(object):
  def __init__(self, date_of_birth, date):
    self.date_of_birth = date_of_birth
    self.date = date
    self.key = ndb.Key('AgeHistory', '')
