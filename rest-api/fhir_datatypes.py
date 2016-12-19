from google.appengine.ext import ndb

class FHIRCoding(ndb.Model):
  """An FHIR coding"""
  system = ndb.StringProperty()
  code = ndb.StringProperty()

class FHIRIdentifier(ndb.Model):
  """An FHIR identifier"""
  system = ndb.StringProperty()
  value = ndb.StringProperty()

class FHIRReference(ndb.Model):
  """An FHIR reference"""
  reference = ndb.StringProperty()
  display = ndb.StringProperty()
