'''The definition of the questionnaire object and DB marshalling.
'''
import collections
import uuid

import fhir_datatypes

from data_access_object import DataAccessObject
from protorpc import message_types
from protorpc import messages
from google.appengine.ext import ndb
from endpoints_proto_datastore.ndb import EndpointsModel


class Questionnaire(EndpointsModel):
  """The questionnaire resource definition"""
  resourceType = ndb.StringProperty()
  id = ndb.StringProperty()
  identifier = ndb.JsonProperty()
  version = ndb.StringProperty()
  status = ndb.StringProperty()
  date = ndb.StringProperty()
  publisher = ndb.StringProperty()
  telecom = ndb.JsonProperty
  subjectType = ndb.StringProperty()
  group = ndb.TextProperty()
  text = ndb.JsonProperty()
  contained = ndb.JsonProperty()
  extension = ndb.JsonProperty()
