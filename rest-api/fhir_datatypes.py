"""Resource objects for FHIR types that are not persisted to database tables.
"""

from protorpc import message_types
from protorpc import messages


class Attachment(messages.Message):
  contentType = messages.StringField(1)
  language = messages.StringField(2)
  data = messages.StringField(3)
  url = messages.StringField(4)
  size = messages.IntegerField(5)
  hash = messages.StringField(6)
  title = messages.StringField(7)
  creation = message_types.DateTimeField(8)

class Coding(messages.Message):
  system = messages.StringField(1)
  version = messages.StringField(2)
  code = messages.StringField(3)
  display = messages.StringField(4)
  userSelected = messages.StringField(5)

class CodeableConcept(messages.Message):
  coding = messages.MessageField(Coding, 1, repeated=True)
  text = messages.StringField(2)

class Quantity(messages.Message):
  value = messages.FloatField(1)
  comparator = messages.StringField(2)
  unit = messages.StringField(3)
  system = messages.StringField(4)
  code = messages.StringField(5)

class Period(messages.Message):
  origin = messages.MessageField(Quantity, 1, repeated=False)
  period = messages.FloatField(2)
  factor = messages.FloatField(3)
  lowerLimit = messages.FloatField(4)
  upperLimit = messages.FloatField(5)
  dimensions = messages.FloatField(6)
  data = messages.StringField(7)

class ContactPoint(messages.Message):
  system = messages.StringField(1)
  value = messages.StringField(2)
  use = messages.StringField(3)
  rank = messages.IntegerField(4)
  period = messages.MessageField(Period, 5, repeated=False)

class Meta(messages.Message):
  version_id = messages.StringField(1)
  last_updated = message_types.DateTimeField(2)
  profile = messages.StringField(3, repeated=True)
  security = messages.MessageField(Coding, 4, repeated=True)
  tag = messages.MessageField(Coding, 5, repeated=True)

class Narrative(messages.Message):
  status = messages.StringField(1)
  div = messages.StringField(2)

class DomainUsageResource(messages.Message):
  resourceType = messages.StringField(1)
  id = messages.StringField(2)
  meta = messages.MessageField(Meta, 3, repeated=False)
  implicitRules = messages.StringField(4)
  language = messages.StringField(5)
  text = messages.MessageField(Narrative, 6, repeated=False)

class Reference(messages.Message):
  reference = messages.StringField(1)
  display = messages.StringField(2)

class Identifier(messages.Message):
  use = messages.StringField(1)
  type_ = messages.MessageField(CodeableConcept, 2, repeated=True)
  system = messages.StringField(3)
  value = messages.StringField(4)
  period = messages.MessageField(Period, 5, repeated=False)
  assigner = messages.MessageField(Reference, 6, repeated=False)
