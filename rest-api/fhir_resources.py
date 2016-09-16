"""Resource objects for FHIR types that are not persisted to database tables.
"""

from protorpc import message_types
from protorpc import messages


class AttachmentResource(messages.Message):
  contentType = messages.StringField(1)
  language = messages.StringField(2)
  data = messages.StringField(3)
  url = messages.StringField(4)
  size = messages.IntegerField(5)
  hash = messages.StringField(6)
  title = messages.StringField(7)
  creation = message_types.DateTimeField(8)

class CodingResource(messages.Message):
  system = messages.StringField(1)
  version = messages.StringField(2)
  code = messages.StringField(3)
  display = messages.StringField(4)
  userSelected = messages.StringField(5)

class CodeableConceptResource(messages.Message):
  coding = messages.MessageField(CodingResource, 1, repeated=True)
  text = messages.StringField(2)

class QuantityResource(messages.Message):
  value = messages.FloatField(1)
  comparator = messages.StringField(2)
  unit = messages.StringField(3)
  system = messages.StringField(4)
  code = messages.StringField(5)

class PeriodResource(messages.Message):
  origin = messages.MessageField(QuantityResource, 1, repeated=False)
  period = messages.FloatField(2)
  factor = messages.FloatField(3)
  lowerLimit = messages.FloatField(4)
  upperLimit = messages.FloatField(5)
  dimensions = messages.FloatField(6)
  data = messages.StringField(7)

class ContactPointResource(messages.Message):
  system = messages.StringField(1)
  value = messages.StringField(2)
  use = messages.StringField(3)
  rank = messages.IntegerField(4)
  period = messages.MessageField(PeriodResource, 5, repeated=False)

class MetaResource(messages.Message):
  version_id = messages.StringField(1)
  last_updated = message_types.DateTimeField(2)
  profile = messages.StringField(3, repeated=True)
  security = messages.MessageField(CodingResource, 4, repeated=True)
  tag = messages.MessageField(CodingResource, 5, repeated=True)

class NarrativeResource(messages.Message):
  status = messages.StringField(1)
  div = messages.StringField(2)

class DomainUsageResourceResource(messages.Message):
  resourceType = messages.StringField(1)
  id = messages.StringField(2)
  meta = messages.MessageField(MetaResource, 3, repeated=False)
  implicitRules = messages.StringField(4)
  language = messages.StringField(5)
  text = messages.MessageField(NarrativeResource, 6, repeated=False)

class ReferenceResource(messages.Message):
  reference = messages.StringField(1)
  display = messages.StringField(2)

class IdentifierResource(messages.Message):
  use = messages.StringField(1)
  type_ = messages.MessageField(CodeableConceptResource, 2, repeated=True)
  system = messages.StringField(3)
  value = messages.StringField(4)
  period = messages.MessageField(PeriodResource, 5, repeated=False)
  assigner = messages.MessageField(ReferenceResource, 6, repeated=False)
