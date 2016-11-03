"""The definition of the evaluation object and DB marshalling."""

import concepts
import data_access_object
import extraction
import participant

import fhirclient.models.bundle

from extraction import extract_concept
from google.appengine.ext import ndb
from werkzeug.exceptions import BadRequest

class Evaluation(ndb.Model):
  """The evaluation resource definition"""
  resource = ndb.JsonProperty()

class EvaluationDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(EvaluationDAO, self).__init__(Evaluation, participant.Participant)

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
    query = Evaluation.query(ancestor=p_key)
    return {"items": [self.to_json(p) for p in query.fetch()]}

DAO = EvaluationDAO()

class EvaluationExtractor(extraction.FhirExtractor):
  def __init__(self, resource):
    super(EvaluationExtractor, self).__init__(resource)
    self.values = {}

    # The first entry should be a Composition, then Observations follow.
    if not self.r_fhir.entry or self.r_fhir.entry[0].resource.resource_name != "Composition":
      raise BadRequest('The first entry should be a Composition. It is {}'.format(
          self.r_fhir.entry and self.r_fhir.entry[0].resource.resource_name))

    composition = self.r_fhir.entry[0].resource
    codings = {c.system: c.code for c in composition.type.coding}
    code = codings.get(concepts.SYSTEM_EVALUATION, None)
    if not code:
      raise BadRequest('Evaluation does not have a composition node with system: {}.'.format(
          concepts.SYSTEM_EVALUATION))
    if not code.startswith(concepts.EVALUATION_CONCEPT_CODE_PREFIX):
      raise BadRequest('Invalid Composition code: {} should start with: {}.'.format(
          code, concepts.EVALUATION_CONCEPT_CODE_PREFIX))

    for entry in (e.resource for e in self.r_fhir.entry[1:]):
      if entry.resource_name == "Observation":
        value = extraction.extract_value(entry)
        self.values.update({extract_concept(coding): value for coding in entry.code.coding})
        for component in entry.component or []:
          value = extraction.extract_value(component)
          self.values.update({extract_concept(coding): value for coding in component.code.coding})

  def extract_value(self, concept):
    return self.values.get(concept, None)
