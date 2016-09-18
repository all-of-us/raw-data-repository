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


class Range(messages.Message):
  start = message_types.DateTimeField(1)
  end = message_types.DateTimeField(2)

class Ratio(messages.Message):
  numerator = messages.MessageField(Quantity, 1, repeated=False)
  denominator = messages.MessageField(Quantity, 2, repeated=False)

class HumanName(messages.Message):
  resourceType = messages.StringField(1)
  use = messages.StringField(2)
  text = messages.StringField(3)
  family = messages.StringField(4, repeated=True)
  given = messages.StringField(5, repeated=True)
  prefix = messages.StringField(6, repeated=True)
  suffix = messages.StringField(7, repeated=True)
  period = messages.MessageField(Period, 8, repeated=False)

class Address(messages.Message):
  use = messages.StringField(1)
  type = messages.StringField(2)
  text = messages.StringField(3)
  line = messages.StringField(4, repeated=True)
  city = messages.StringField(5)
  district = messages.StringField(6)
  state = messages.StringField(7)
  postalCode = messages.StringField(8)
  country = messages.StringField(9)
  period = messages.MessageField(Period, 10, repeated=False)

class Schedule(messages.Message):
  resourceType = messages.StringField(1)
  text = messages.StringField(2)
  extension = messages.MessageField('Extension', 3, repeated=True)
  identifier = messages.MessageField(Identifier, 4, repeated=True)
  type = messages.MessageField(CodeableConcept, 5, repeated=True)
  actor = messages.MessageField(Reference, 6, repeated=True)
  planningHorizon = messages.MessageField(Period, 7, repeated=True)
  comment = messages.StringField(8)


class Extension(messages.Message):
  url = messages.StringField(1)
  valueInteger = messages.IntegerField(2)
  valueDecimal = messages.FloatField(3)
  valueDateTime = message_types.DateTimeField(4)
  valueDate = message_types.DateTimeField(5)
  valueInstant = message_types.DateTimeField(6)
  valueString = messages.StringField(7)
  valueUri = messages.StringField(8)
  valueBoolean = messages.BooleanField(9)
  valueCode = messages.StringField(10)
  valueBase64Binary = messages.StringField(11)
  valueCoding = messages.MessageField(Coding, 12, repeated=False)
  valueCodeableConcept = messages.MessageField(CodeableConcept, 13,
                                               repeated=False)
  valueAttachment = messages.MessageField(Attachment, 14, repeated=False)
  valueIdentifier = messages.MessageField(Identifier, 15, repeated=False)
  valueQuantity = messages.MessageField(Quantity, 16, repeated=False)
  valueRange = messages.MessageField(Range, 17, repeated=False)
  valuePeriod = messages.MessageField(Period, 18, repeated=False)
  valueRatio = messages.MessageField(Ratio, 19, repeated=False)
  valueHumanName = messages.MessageField(HumanName, 20, repeated=False)
  valueAddress = messages.MessageField(Address, 21, repeated=False)
  valueContactPoint = messages.MessageField(ContactPoint, 22, repeated=False)
  valueSchedule = messages.MessageField(Schedule, 23, repeated=False)
  valueReference = messages.MessageField(Reference, 24, repeated=False)
