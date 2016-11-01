"""The definition of the evaluation object and DB marshalling."""
import data_access_object
import extraction
import participant

import fhirclient.models.bundle

from extraction import extract_concept
from google.appengine.ext import ndb
from werkzeug.exceptions import BadRequest

EVALUATION_CONCEPT_SYSTEM = "http://terminology.pmi-ops.org/document-types"
EVALUATION_CONCEPT_CODE_PREFIX = "intake-exam-v"


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

    has_composition = False
    for entry in (e.resource for e in self.r_fhir.entry):
      if entry.resource_name == "Observation":
        value = extraction.extract_value(entry)
        self.values.update({extract_concept(coding): value for coding in entry.code.coding})
        for component in entry.component or []:
          value = extraction.extract_value(component)
          self.values.update({extract_concept(coding): value for coding in component.code.coding})
      elif entry.resource_name == "Composition":
        has_composition = True
        codings = {c.system: c.code for c in entry.type.coding}
        code = codings.get(EVALUATION_CONCEPT_SYSTEM, None)
        if not code:
          raise BadRequest('Evaluation does not have a composition node with system: {}.'.format(
              EVALUATION_CONCEPT_SYSTEM))
        if not code.startswith(EVALUATION_CONCEPT_CODE_PREFIX):
          raise BadRequest('Invalid Composition code: {} should start with: {}.'.format(
              code, EVALUATION_CONCEPT_CODE_PREFIX))

    if not has_composition:
      raise BadRequest("Evaluation has no Composition node.")

  def extract_value(self, concept):
    return self.values.get(concept, None)
