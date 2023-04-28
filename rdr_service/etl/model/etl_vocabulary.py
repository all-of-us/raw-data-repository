# These models are only used for unit tests.
from sqlalchemy import (
    Column, Integer, String, Date, DateTime
)

from rdr_service.model.base import VocBase, CdmBase

class Concept(VocBase):
    __tablename__ = "concept"
    concept_id       = Column(Integer, primary_key=True)
    concept_name     = Column(String(1000))
    domain_id        = Column(String(1000))
    vocabulary_id    = Column(String(1000))
    concept_class_id = Column(String(1000))
    standard_concept = Column(String(1000))
    concept_code     = Column(String(1000))
    valid_start_date = Column(Date)
    valid_end_date   = Column(Date)
    invalid_reason  = Column(String(1000))

class ConceptRelationship(VocBase):
    __tablename__ = "concept_relationship"
    concept_id_1     = Column(Integer, primary_key=True)
    concept_id_2     = Column(Integer)
    relationship_id  = Column(String(1000), primary_key=True)
    valid_start_date = Column(Date)
    valid_end_date   = Column(Date)
    invalid_reason = Column(String(1000))

class CombinedSurveyFilter(CdmBase):
    __tablename__ = "combined_survey_filter"
    survey_name = Column(String(80), primary_key=True)

class CombinedQuestionFilter(CdmBase):
    __tablename__ = "combined_question_filter"
    question_ppi_code = Column(String(80), primary_key=True)

class SourceToConceptMap(CdmBase):
    __tablename__ = "source_to_concept_map"
    id = Column(Integer, primary_key=True)
    source_code                 = Column(String(1000))
    source_concept_id           = Column(Integer)
    source_vocabulary_id        = Column(String(1000))
    source_code_description     = Column(String(1000))
    target_concept_id           = Column(Integer)
    target_vocabulary_id        = Column(String(20))
    valid_start_date            = Column(DateTime)
    valid_end_date              = Column(DateTime)
    invalid_reason              = Column(String(1))
    priority                    = Column(Integer)

class TempConceptRelationshipMapsTo(VocBase):
    __tablename__ = "tmp_con_rel_mapsto"
    id = Column(Integer, primary_key=True)
    concept_id_1 = Column(Integer)
    concept_id_2 = Column(Integer)

class TempConceptRelationshipMapsToValue(VocBase):
    __tablename__ = "tmp_con_rel_mapstoval"
    id = Column(Integer, primary_key=True)
    concept_id_1 = Column(Integer)
    concept_id_2 = Column(Integer)


class TempVocConcept(VocBase):
    __tablename__ = "tmp_voc_concept"
    concept_id = Column(Integer, primary_key=True)
    concept_code = Column(String(100))

class TempVocConceptS(VocBase):
    __tablename__ = "tmp_voc_concept_s"
    concept_id = Column(Integer, primary_key=True)
