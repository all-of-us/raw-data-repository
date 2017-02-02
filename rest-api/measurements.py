"""The definition of the measurements object and DB marshalling."""

import concepts
import data_access_object
import executors
import extraction
import participant
import singletons
import sync_log

import fhirclient.models.bundle

from extraction import extract_concept
from google.appengine.ext import ndb
from werkzeug.exceptions import BadRequest

class PhysicalMeasurements(ndb.Model):
  """The physical measurements resource definition"""
  resource = ndb.JsonProperty()
  last_modified = ndb.DateTimeProperty(auto_now=True)

  @classmethod
  def write_to_sync_log(cls, participantId, resource):
    sync_log.DAO().write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, participantId, resource)

  def _post_put_hook(self, _):
    executors.defer(PhysicalMeasurements.write_to_sync_log,
                    self.key.parent().id(), self.resource, _transactional=ndb.in_transaction())

class PhysicalMeasurementsDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(PhysicalMeasurementsDAO, self).__init__(PhysicalMeasurements, participant.Participant)

  def validate_query(self, query_definition):
    for field_filter in query_definition.field_filters:
      if field_filter.field_name != 'last_modified':
        raise BadRequest("Invalid filter on field %s" % field_filter.field_name)

  def properties_from_json(self, dict_, ancestor_id, id_):
    model = fhirclient.models.bundle.Bundle(dict_)
    model.id = id_
    return {
      "resource": model.as_json()
    }

  def properties_to_json(self, dict_):
    return dict_['resource']

  def list(self, participant_id):
    p_key = ndb.Key(participant.Participant, participant_id)
    query = PhysicalMeasurements.query(ancestor=p_key)
    return {"items": [self.to_json(p) for p in query.fetch()]}

def DAO():
  return singletons.get(PhysicalMeasurementsDAO)

class PhysicalMeasurementsExtractor(extraction.FhirExtractor):
  def __init__(self, resource):
    super(PhysicalMeasurementsExtractor, self).__init__(resource)
    self.values = {}

    # The first entry should be a Composition, then Observations follow.
    if not self.r_fhir.entry or self.r_fhir.entry[0].resource.resource_name != "Composition":
      raise BadRequest('The first entry should be a Composition. It is {}'.format(
          self.r_fhir.entry and self.r_fhir.entry[0].resource.resource_name))

    composition = self.r_fhir.entry[0].resource
    codings = {c.system: c.code for c in composition.type.coding}
    code = codings.get(concepts.SYSTEM_PHYSICAL_MEASUREMENTS, None)
    if not code:
      raise BadRequest('Physical measurements does not have a composition node with system: {}.'
                       .format(concepts.SYSTEM_PHYSICAL_MEASUREMENTS))
    if not code.startswith(concepts.PHYSICAL_MEASUREMENTS_CONCEPT_CODE_PREFIX):
      raise BadRequest('Invalid Composition code: {} should start with: {}.'.format(
          code, concepts.PHYSICAL_MEASUREMENTS_CONCEPT_CODE_PREFIX))

    for entry in (e.resource for e in self.r_fhir.entry[1:]):
      if entry.resource_name == "Observation":
        value = extraction.extract_value(entry)
        self.values.update({extract_concept(coding): value for coding in entry.code.coding})
        for component in entry.component or []:
          value = extraction.extract_value(component)
          self.values.update({extract_concept(coding): value for coding in component.code.coding})

  def extract_value(self, concept):
    return self.values.get(concept, None)
