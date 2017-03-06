import extraction

class QuestionnaireExtractor(extraction.FhirExtractor):
  """FHIR extractor for questionnaires."""

  def extract_root_group_concepts(self):
    return [extraction.Concept(node.system, node.code) for node in self.r_fhir.group.concept or []]

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
