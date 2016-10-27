'''The definition of the questionnaire object and DB marshalling.
'''

import extraction
import fhirclient.models.questionnaire

import data_access_object

from google.appengine.ext import ndb


class Questionnaire(ndb.Model):
  """The questionnaire."""
  resource = ndb.JsonProperty()

class QuestionnaireDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(QuestionnaireDAO, self).__init__(Questionnaire)

  def properties_to_json(self, m):
    return m['resource']

  def properties_from_json(self, dict_, ancestor_id, id_):
    model = fhirclient.models.questionnaire.Questionnaire(dict_)
    model.id = id_
    return {
        "resource": model.as_json()
    }

DAO = QuestionnaireDAO()

class QuestionnaireExtractor(extraction.FhirExtractor):
  def extract_link_id_for_concept(self, concept):
    """Returns list of link ids in questionnaire that address the concept."""
    assert isinstance(concept, extraction.Concept)
    return self.extract_link_id_for_concept_(self.r_fhir.group, concept)

  def extract_link_id_for_concept_(self, qr, concept):
    # Sometimes concept is an existing attr with a value of None.
    for node in qr.concept or []:
      if concept == extraction.Concept(node.system, node.code):
        return [qr.linkId]

    ret = []
    for prop in ('question', 'group'):
      if getattr(qr, prop, None):
        ret += [v
                for q in extraction.as_list(getattr(qr, prop))
                for v in self.extract_link_id_for_concept_(q, concept)]
    return ret
