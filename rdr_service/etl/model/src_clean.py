from sqlalchemy import BigInteger, Boolean, Column, DateTime, Index, String, SmallInteger, Integer, Date, Float
from sqlalchemy.dialects.mysql import DECIMAL, TINYINT, TEXT

from rdr_service.model.base import CdmBase


class QuestionnaireAnswersByModule(CdmBase):
    __tablename__ = 'questionnaire_answers_by_module'
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    participant_id = Column(BigInteger)
    authored = Column(DateTime)
    created = Column(DateTime)
    survey = Column(String(200))
    response_id = Column(BigInteger)
    question_code_id = Column(BigInteger)
    __table_args__ = (Index(
        'idx_participant_questionnaire_answers_by_module_and_code',
        participant_id,
        survey,
        question_code_id
    ),)


class SrcClean(CdmBase):
    __tablename__ = 'src_clean'
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    participant_id = Column(BigInteger)
    research_id = Column(BigInteger)
    external_id = Column(BigInteger)
    survey_name = Column(String(200))
    date_of_survey = Column(DateTime)
    question_ppi_code = Column(String(200))
    question_code_id = Column(BigInteger)
    value_ppi_code = Column(String(200))
    topic_value = Column(String(200))
    value_code_id = Column(BigInteger)
    value_number = Column(DECIMAL(precision=20, scale=6))
    value_boolean = Column(TINYINT)
    value_date = Column(DateTime)
    value_string = Column(String(1024))
    questionnaire_response_id = Column(BigInteger)
    unit_id = Column(String(50))
    filter = Column(SmallInteger)
    is_invalid = Column(Boolean)
    src_id = Column(String(50))
    __table_args__ = (Index('idx_src_clean_participant_id', participant_id),
                      Index('idx_src_clean_questionnaire_response_id', questionnaire_response_id))


class Location(CdmBase):
    __tablename__ = "location"
    location_id = Column(BigInteger, autoincrement=True, primary_key=True)
    address_1 = Column(String(255))
    address_2 = Column(String(255))
    city = Column(String(255))
    state = Column(String(255))
    zip = Column(String(255))
    county = Column(String(255))
    location_source_value = Column(String(255))
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class CareSite(CdmBase):
    __tablename__ = "care_site"
    id = Column(BigInteger, unique=True, nullable=False)
    care_site_id = Column(BigInteger, primary_key=True)
    care_site_name = Column(String(255))
    place_of_service_concept_id = Column(BigInteger, nullable=False)
    location_id = Column(BigInteger)
    care_site_source_value = Column(String(50), nullable=False)
    place_of_service_source_value = Column(String(50))
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class Provider(CdmBase):
    __tablename__ = "provider"
    id = Column(BigInteger, unique=True, nullable=False)
    provider_id = Column(BigInteger, primary_key=True)
    provider_name = Column(String(50))
    npi = Column(String(20))
    dea = Column(String(20))
    specialty_concept_id = Column(BigInteger, nullable=False)
    care_site_id = Column(BigInteger)
    year_of_birth = Column(Integer)
    gender_concept_id = Column(BigInteger, nullable=False)
    provider_source_value = Column(String(50), nullable=False)
    specialty_source_value = Column(String(50))
    specialty_source_concept_id = Column(BigInteger, nullable=False)
    gender_source_value = Column(String(50))
    gender_source_concept_id = Column(BigInteger, nullable=False)
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class Person(CdmBase):
    __tablename__ = "person"
    id = Column(BigInteger, nullable=False, unique=True)
    person_id = Column(BigInteger, nullable=False, primary_key=True)
    gender_concept_id = Column(BigInteger, nullable=False)
    year_of_birth = Column(Integer, nullable=False)
    month_of_birth = Column(Integer)
    day_of_birth = Column(Integer)
    birth_datetime = Column(DateTime)
    race_concept_id = Column(BigInteger, nullable=False)
    ethnicity_concept_id = Column(BigInteger, nullable=False)
    location_id = Column(BigInteger)
    provider_id = Column(BigInteger)
    care_site_id = Column(BigInteger)
    person_source_value = Column(String(50), nullable=False)
    gender_source_value = Column(String(50))
    gender_source_concept_id = Column(BigInteger, nullable=False)
    race_source_value = Column(String(50))
    race_source_concept_id = Column(BigInteger, nullable=False)
    ethnicity_source_value = Column(String(50))
    ethnicity_source_concept_id = Column(BigInteger, nullable=False)
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class Death(CdmBase):
    __tablename__ = "death"
    id = Column(BigInteger, primary_key=True)
    person_id = Column(BigInteger)
    death_date = Column(Date)
    death_datetime = Column(DateTime)
    death_type_concept_id = Column(BigInteger, )
    cause_concept_id = Column(String(50))
    cause_source_value = Column(String(50))
    cause_source_concept_id = Column(String(50))
    src_id = Column(String(50))


class ObservationPeriod(CdmBase):
    __tablename__ = "observation_period"
    observation_period_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    observation_period_start_date = Column(Date, nullable=False)
    observation_period_end_date = Column(Date, nullable=False)
    period_type_concept_id = Column(BigInteger, nullable=False)
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class PayerPlanPeriod(CdmBase):
    __tablename__ = "payer_plan_period"
    id = Column(BigInteger, nullable=False, unique=True)
    payer_plan_period_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    payer_plan_period_start_date = Column(Date, nullable=False)
    payer_plan_period_end_date = Column(Date, nullable=False)
    payer_concept_id = Column(BigInteger)
    payer_source_value = Column(String(50))
    payer_source_concept_id = Column(BigInteger)
    plan_concept_id = Column(BigInteger)
    plan_source_value = Column(String(50))
    plan_source_concept_id = Column(BigInteger)
    sponsor_concept_id = Column(BigInteger)
    sponsor_source_concept_id = Column(BigInteger)
    family_source_value = Column(String(50))
    stop_reason_concept_id = Column(BigInteger)
    stop_reason_source_value = Column(String(50))
    stop_reason_source_concept_id = Column(BigInteger)
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class VisitOccurrence(CdmBase):
    __tablename__ = "visit_occurrence"
    id = Column(BigInteger, nullable=False, unique=True)
    visit_occurrence_id = Column(BigInteger, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    visit_concept_id = Column(BigInteger, nullable=False)
    visit_start_date = Column(Date, nullable=False)
    visit_start_datetime = Column(DateTime, nullable=False)
    visit_end_date = Column(Date, nullable=False)
    visit_end_datetime = Column(DateTime, nullable=False)
    visit_type_concept_id = Column(BigInteger, nullable=False)
    provider_id = Column(BigInteger)
    care_site_id = Column(BigInteger)
    visit_source_value = Column(String(150))
    visit_source_concept_id = Column(BigInteger, nullable=False)
    admitting_source_concept_id = Column(BigInteger, nullable=False)
    admitting_source_value = Column(String(50))
    discharge_to_concept_id = Column(BigInteger, nullable=False)
    discharge_to_source_value = Column(String(50))
    preceding_visit_occurrence_id = Column(BigInteger)
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class ConditionOccurrence(CdmBase):
    __tablename__ = "condition_occurrence"
    id = Column(BigInteger, nullable=False, unique=True)
    condition_occurrence_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    condition_concept_id = Column(BigInteger, nullable=False)
    condition_start_date = Column(Date, nullable=False)
    condition_start_datetime = Column(DateTime)
    condition_end_date = Column(Date)
    condition_end_datetime = Column(DateTime)
    condition_type_concept_id = Column(BigInteger, nullable=False)
    condition_status_concept_id = Column(BigInteger)
    stop_reason = Column(String(20))
    provider_id = Column(BigInteger)
    visit_occurrence_id = Column(BigInteger)
    visit_detail_id = Column(BigInteger)
    condition_source_value = Column(String(50), nullable=False)
    condition_source_concept_id = Column(BigInteger, nullable=False)
    condition_status_source_value = Column(String(50))
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class ProcedureOccurrence(CdmBase):
    __tablename__ = "procedure_occurrence"
    procedure_occurrence_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    procedure_concept_id = Column(BigInteger, nullable=False)
    procedure_date = Column(Date, nullable=False)
    procedure_datetime = Column(DateTime)
    procedure_type_concept_id = Column(BigInteger, nullable=False)
    modifier_concept_id = Column(BigInteger, nullable=False)
    quantity = Column(Integer)
    provider_id = Column(BigInteger)
    visit_occurrence_id = Column(BigInteger)
    visit_detail_id = Column(BigInteger)
    procedure_source_value = Column(String(1024), nullable=False)
    procedure_source_concept_id = Column(BigInteger, nullable=False)
    modifier_source_value = Column(String(50))
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class Observation(CdmBase):
    __tablename__ = "observation"
    observation_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    observation_concept_id = Column(BigInteger, nullable=False)
    observation_date = Column(Date, nullable=False)
    observation_datetime = Column(DateTime)
    observation_type_concept_id = Column(BigInteger, nullable=False)
    value_as_number = Column(DECIMAL(20, 6))
    value_as_string = Column(String(1024))
    value_as_concept_id = Column(BigInteger, nullable=False)
    qualifier_concept_id = Column(BigInteger, nullable=False)
    unit_concept_id = Column(BigInteger, nullable=False)
    provider_id = Column(BigInteger)
    visit_occurrence_id = Column(BigInteger)
    visit_detail_id = Column(BigInteger)
    observation_source_value = Column(String(255), nullable=False)
    observation_source_concept_id = Column(BigInteger, nullable=False)
    unit_source_value = Column(String(50))
    qualifier_source_value = Column(String(50))
    # -- specific to this ETL
    value_source_concept_id = Column(BigInteger)
    value_source_value = Column(String(255))
    questionnaire_response_id = Column(BigInteger)
    meas_id = Column(BigInteger)
    # --
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class Measurement(CdmBase):
    __tablename__ = "measurement"
    id = Column(BigInteger, nullable=False, unique=True)
    measurement_id = Column(BigInteger, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    measurement_concept_id = Column(BigInteger, nullable=False)
    measurement_date = Column(Date, nullable=False)
    measurement_datetime = Column(DateTime)
    measurement_time = Column(String(50))
    measurement_type_concept_id = Column(BigInteger, nullable=False)
    operator_concept_id = Column(BigInteger, nullable=False)
    value_as_number = Column(DECIMAL(20, 6))
    value_as_concept_id = Column(BigInteger, nullable=False)
    unit_concept_id = Column(BigInteger, nullable=False)
    range_low = Column(DECIMAL(20, 6))
    range_high = Column(DECIMAL(20, 6))
    provider_id = Column(BigInteger)
    visit_occurrence_id = Column(BigInteger)
    visit_detail_id = Column(BigInteger)
    measurement_source_value = Column(String(50))
    measurement_source_concept_id = Column(BigInteger, nullable=False)
    unit_source_value = Column(String(50))
    # -- specific for this ETL
    value_source_value = Column(String(50))
    parent_id = Column(BigInteger)
    # --
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class Note(CdmBase):
    __tablename__ = "note"
    id = Column(BigInteger, nullable=False, unique=True)
    note_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    note_date = Column(Date, nullable=False)
    note_datetime = Column(DateTime)
    note_type_concept_id = Column(BigInteger, nullable=False)
    note_class_concept_id = Column(BigInteger, nullable=False)
    note_title = Column(String(250))
    note_text = Column(TEXT, nullable=False)
    encoding_concept_id = Column(BigInteger, nullable=False)
    language_concept_id = Column(BigInteger, nullable=False)
    provider_id = Column(BigInteger)
    visit_detail_id = Column(BigInteger)
    note_source_value = Column(String(50))
    visit_occurrence_id = Column(BigInteger)
    unit_id = Column(String(50), nullable=False)


class DrugExposure(CdmBase):
    __tablename__ = "drug_exposure"
    id = Column(BigInteger, nullable=False, unique=True)
    drug_exposure_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    drug_concept_id = Column(BigInteger, nullable=False)
    drug_exposure_start_date = Column(Date, nullable=False)
    drug_exposure_start_datetime = Column(DateTime)
    drug_exposure_end_date = Column(Date)
    drug_exposure_end_datetime = Column(DateTime)
    verbatim_end_date = Column(Date)
    drug_type_concept_id = Column(BigInteger, nullable=False)
    stop_reason = Column(String(20))
    refills = Column(Integer)
    quantity = Column(DECIMAL(20, 6))
    days_supply = Column(Integer)
    sig = Column(String(1024))
    route_concept_id = Column(BigInteger)
    lot_number = Column(String(50))
    provider_id = Column(BigInteger)
    visit_occurrence_id = Column(BigInteger)
    visit_detail_id = Column(BigInteger)
    drug_source_value = Column(String(50), nullable=False)
    drug_source_concept_id = Column(BigInteger)
    route_source_value = Column(String(50))
    dose_unit_source_value = Column(String(50))
    unit_id = Column(String(50), nullable=False)


class DeviceExposure(CdmBase):
    __tablename__ = "device_exposure"
    id = Column(BigInteger, nullable=False, unique=True)
    device_exposure_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    device_concept_id = Column(BigInteger, nullable=False)
    device_exposure_start_date = Column(Date, nullable=False)
    device_exposure_start_datetime = Column(DateTime)
    device_exposure_end_date = Column(Date)
    device_exposure_end_datetime = Column(DateTime)
    device_type_concept_id = Column(BigInteger, nullable=False)
    unique_device_id = Column(String(50))
    quantity = Column(DECIMAL(20, 6))
    provider_id = Column(BigInteger)
    visit_occurrence_id = Column(BigInteger)
    visit_detail_id = Column(BigInteger)
    device_source_value = Column(String(50), nullable=False)
    device_source_concept_id = Column(BigInteger)
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class Cost(CdmBase):
    __tablename__ = "cost"
    id = Column(BigInteger, nullable=False, unique=True)
    cost_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    cost_event_id = Column(BigInteger, nullable=False)
    cost_domain_id = Column(String(20), nullable=False)
    cost_type_concept_id = Column(BigInteger, nullable=False)
    currency_concept_id = Column(BigInteger, nullable=False)
    total_charge = Column(DECIMAL(20, 6))
    total_cost = Column(DECIMAL(20, 6))
    total_paid = Column(DECIMAL(20, 6))
    paid_by_payer = Column(DECIMAL(20, 6))
    paid_by_patient = Column(DECIMAL(20, 6))
    paid_patient_copay = Column(DECIMAL(20, 6))
    paid_patient_coinsurance = Column(DECIMAL(20, 6))
    paid_patient_deductible = Column(DECIMAL(20, 6))
    paid_by_primary = Column(DECIMAL(20, 6))
    paid_ingredient_cost = Column(DECIMAL(20, 6))
    paid_dispensing_fee = Column(DECIMAL(20, 6))
    payer_plan_period_id = Column(BigInteger)
    amount_allowed = Column(DECIMAL(20, 6))
    revenue_code_concept_id = Column(BigInteger, nullable=False)
    revenue_code_source_value = Column(String(50))
    drg_concept_id = Column(BigInteger)
    drg_source_value = Column(String(50))
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class FactRelationship(CdmBase):
    __tablename__ = "fact_relationship"
    id = Column(BigInteger, primary_key=True)
    domain_concept_id_1 = Column(Integer, nullable=False)
    fact_id_1 = Column(BigInteger, nullable=False)
    domain_concept_id_2 = Column(Integer, nullable=False)
    fact_id_2 = Column(BigInteger, nullable=False)
    relationship_concept_id = Column(BigInteger, nullable=False)
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class ConditionEra(CdmBase):
    __tablename__ = "condition_era"
    id = Column(BigInteger, nullable=False, unique=True)
    condition_era_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    condition_concept_id = Column(BigInteger, nullable=False)
    condition_era_start_date = Column(Date, nullable=False)
    condition_era_end_date = Column(Date, nullable=False)
    condition_occurrence_count = Column(Integer)
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class DrugEra(CdmBase):
    __tablename__ = "drug_era"
    id = Column(BigInteger, nullable=False, unique=True)
    drug_era_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    drug_concept_id = Column(BigInteger, nullable=False)
    drug_era_start_date = Column(Date, nullable=False)
    drug_era_end_date = Column(Date, nullable=False)
    drug_exposure_count = Column(Integer)
    gap_days = Column(Integer)
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class DoseEra(CdmBase):
    __tablename__ = "dose_era"
    id = Column(BigInteger, nullable=False, unique=True)
    dose_era_id = Column(BigInteger, autoincrement=True, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    drug_concept_id = Column(BigInteger, nullable=False)
    unit_concept_id = Column(BigInteger, nullable=False)
    dose_value = Column(DECIMAL(20, 6), nullable=False)
    dose_era_start_date = Column(Date, nullable=False)
    dose_era_end_date = Column(Date, nullable=False)
    unit_id = Column(String(50), nullable=False)
    src_id = Column(String(50))


class Metadata(CdmBase):
    __tablename__ = "metadata"
    id = Column(BigInteger, primary_key=True)
    metadata_concept_id = Column(BigInteger, nullable=False)
    metadata_type_concept_id = Column(BigInteger, nullable=False)
    name = Column(String(256), nullable=False)
    value_as_string = Column(String(1024))
    value_as_concept_id = Column(BigInteger)
    metadata_date = Column(Date)
    metadata_datetime = Column(DateTime)
    src_id = Column(String(50))


class NoteNlp(CdmBase):
    __tablename__ = "note_nlp"
    note_nlp_id = Column(BigInteger, nullable=False, primary_key=True)
    note_id = Column(BigInteger, nullable=False)
    section_concept_id = Column(BigInteger)
    snippet = Column(String(512))
    offset = Column(String(256))
    lexical_variant = Column(String(1024), nullable=False)
    note_nlp_concept_id = Column(BigInteger)
    note_nlp_source_concept_id = Column(BigInteger)
    nlp_system = Column(String(256))
    nlp_date = Column(Date, nullable=False)
    nlp_datetime = Column(DateTime)
    term_exists = Column(String(256))
    term_temporal = Column(String(256))
    term_modifiers = Column(String(256))
    src_id = Column(String(50))


class VisitDetail(CdmBase):
    __tablename__ = "visit_detail"
    visit_detail_id = Column(BigInteger, nullable=False, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    visit_detail_concept_id = Column(BigInteger, nullable=False)
    visit_detail_start_date = Column(Date, nullable=False)
    visit_detail_start_datetime = Column(DateTime)
    visit_detail_end_date = Column(Date, nullable=False)
    visit_detail_end_datetime = Column(DateTime)
    visit_detail_type_concept_id = Column(BigInteger, nullable=False)
    provider_id = Column(BigInteger)
    care_site_id = Column(BigInteger)
    visit_detail_source_value = Column(String(256))
    visit_detail_source_concept_id = Column(BigInteger)
    admitting_source_value = Column(String(256))
    admitting_source_concept_id = Column(BigInteger)
    discharge_to_source_value = Column(String(256))
    discharge_to_concept_id = Column(BigInteger)
    preceding_visit_detail_id = Column(BigInteger)
    visit_detail_parent_id = Column(BigInteger)
    visit_occurrence_id = Column(BigInteger, nullable=False)
    src_id = Column(String(50))


class SrcParticipant(CdmBase):
    __tablename__ = "src_participant"
    participant_id = Column(BigInteger, primary_key=True)
    latest_date_of_survey = Column(DateTime)
    date_of_birth = Column(Date)
    src_id = Column(String(50))


class SrcMapped(CdmBase):
    """ This is src_clean, mapped to standard concepts.
        'question_source_concept_id' must be related to
        'question_concept_id' as 'Maps To' relation.
        'value_source_concept_id' and 'value_concept_id' - the same relation."""
    __tablename__ = "src_mapped"
    id = Column(BigInteger, primary_key=True)
    participant_id = Column(BigInteger)
    date_of_survey = Column(DateTime)
    question_ppi_code = Column(String(200))
    question_code_id = Column(BigInteger)
    question_source_concept_id = Column(BigInteger)
    question_concept_id = Column(BigInteger)
    value_ppi_code = Column(String(200))
    topic_value = Column(String(200))
    value_code_id = Column(BigInteger)
    value_source_concept_id = Column(BigInteger)
    value_concept_id = Column(BigInteger)
    value_number = Column(DECIMAL(20, 6))
    value_boolean = Column(TINYINT)
    value_boolean_concept_id = Column(BigInteger)
    value_date = Column(DateTime)
    value_string = Column(String(1024))
    questionnaire_response_id = Column(BigInteger)
    unit_id = Column(String(50))
    is_invalid = Column(TINYINT(1))
    src_id = Column(String(50))


class SrcPersonLocation(CdmBase):
    """ Address is taken as answer to address-related questions during last survey. """
    __tablename__ = "src_person_location"
    participant_id = Column(BigInteger, primary_key=True)
    address_1 = Column(String(255))
    address_2 = Column(String(255))
    city = Column(String(255))
    zip = Column(String(255))
    state_ppi_code = Column(String(255))
    state = Column(String(255))
    location_id = Column(BigInteger)


class SrcGender(CdmBase):
    """ Contains gender information from patient surveys. """
    __tablename__ = "src_gender"
    person_id = Column(BigInteger, primary_key=True)
    ppi_code = Column(String(255))
    gender_source_concept_id = Column(BigInteger)
    gender_target_concept_id = Column(BigInteger)


class SrcRace(CdmBase):
    """ Contains racial information from patient surveys. """
    __tablename__ = "src_race"
    person_id = Column(BigInteger, primary_key=True)
    ppi_code = Column(String(255))
    race_source_concept_id = Column(BigInteger)
    race_target_concept_id = Column(BigInteger)


class SrcEthnicity(CdmBase):
    """ Contains ethnicity information from patient surveys. """
    __tablename__ = "src_ethnicity"
    person_id = Column(BigInteger, primary_key=True)
    ppi_code = Column(String(255))
    ethnicity_source_concept_id = Column(BigInteger)
    ethnicity_target_concept_id = Column(BigInteger)


class SrcMeas(CdmBase):
    """ Contains information about physical measurements of patient, which is necessary for filling OMOP CDM tables."""
    __tablename__ = "src_meas"
    id = Column(BigInteger, primary_key=True)
    participant_id = Column(BigInteger, nullable=False)
    finalized_site_id = Column(Integer)
    code_value = Column(String(255), nullable=False)
    measurement_time = Column(DateTime, nullable=False)
    value_decimal = Column(Float)
    value_unit = Column(String(255))
    value_code_value = Column(String(255))
    value_string = Column(String(1024))
    measurement_id = Column(BigInteger)
    physical_measurements_id = Column(Integer, nullable=False)
    parent_id = Column(BigInteger)
    src_id = Column(String(50))
    collect_type = Column(Integer)


class MeasurementCodeMap(CdmBase):
    """ Maps measurements code values to standard concept_ids. """
    __tablename__ = "tmp_cv_concept_lk"
    code_value = Column(String(500), primary_key=True)
    cv_source_concept_id = Column(BigInteger)
    cv_concept_id = Column(BigInteger)
    cv_domain_id = Column(String(50))


class MeasurementValueCodeMap(CdmBase):
    """ Maps measurement results value_code_value to standard concept_ids. """
    __tablename__ = "tmp_vcv_concept_lk"
    value_code_value = Column(String(500), primary_key=True)
    vcv_source_concept_id = Column(BigInteger)
    vcv_concept_id = Column(BigInteger)
    vcv_domain_id = Column(String(50))


class SrcMeasMapped(CdmBase):
    """ Joins altogether patient measurements information in source and cdm codes from 'tmp_cv_concept_lk' and
        'tmp_vcv_concept_lk', excluding notes, because notes will migrate to cdm.note.
    """
    __tablename__ = "src_meas_mapped"
    id = Column(BigInteger, primary_key=True)
    participant_id = Column(BigInteger, nullable=False)
    finalized_site_id = Column(Integer)
    code_value = Column(String(255), nullable=False)
    cv_source_concept_id = Column(BigInteger)
    cv_concept_id = Column(BigInteger)
    cv_domain_id = Column(String(50))
    measurement_time = Column(DateTime, nullable=False)
    value_decimal = Column(Float)
    value_unit = Column(String(255))
    vu_concept_id = Column(BigInteger)
    value_code_value = Column(String(255))
    vcv_source_concept_id = Column(BigInteger)
    vcv_concept_id = Column(BigInteger)
    measurement_id = Column(BigInteger)
    physical_measurements_id = Column(Integer, nullable=False)
    parent_id = Column(BigInteger)
    src_id = Column(String(50))
    collect_type = Column(Integer)


class SrcVisits(CdmBase):
    __tablename__ = "tmp_visits_src"
    visit_occurrence_id = Column(BigInteger, primary_key=True)
    person_id = Column(BigInteger, nullable=False)
    visit_start_datetime = Column(DateTime, nullable=False)
    visit_end_datetime = Column(DateTime, nullable=False)
    care_site_id = Column(BigInteger)
    src_id = Column(String(50))


class TempObsTarget(CdmBase):
    """ tmp_obs_target contains dates of all person's clinical events """
    __tablename__ = "temp_obs_target"
    id = Column(BigInteger, primary_key=True)
    person_id = Column(BigInteger)
    start_date = Column(Date)
    end_date = Column(Date)


class TempObsEndUnion(CdmBase):
    """ In 'temp_obs_end_union' we number observations from 'tmp_obs_target' by start_date.
        start_ordinal column contains number of start patient's observation events: first
        is 1, subsequent is 2, 3 and so on.
        End person observation events contains null in start_ordinal column.
        It is necessary for finding possibly intersecting patient observations intervals.
    """
    __tablename__ = "temp_obs_end_union"
    id = Column(BigInteger, primary_key=True)
    person_id = Column(BigInteger)
    event_date = Column(Date)
    event_type = Column(Integer)
    start_ordinal = Column(Integer)


class TempObsEndUnionPart(CdmBase):
    __tablename__ = "temp_obs_end_union_part"
    id = Column(BigInteger, primary_key=True)
    person_id = Column(BigInteger)
    event_date = Column(Date)
    event_type = Column(Integer)
    start_ordinal = Column(Integer)
    overall_ord = Column(Integer)


class TempObsEnd(CdmBase):
    __tablename__ = "temp_obs_end"
    id = Column(BigInteger, primary_key=True)
    person_id = Column(BigInteger)
    end_date = Column(Date)
    start_ordinal = Column(Integer)
    overall_ord = Column(Integer)


class TempObs(CdmBase):
    __tablename__ = "temp_obs"
    id = Column(BigInteger, primary_key=True)
    person_id = Column(BigInteger)
    observation_start_date = Column(Date)
    observation_end_date = Column(Date)


class TempFactRelSd(CdmBase):
    """ tmp_fact_rel_sd contains blood pressure measurements """
    __tablename__ = "tmp_fact_rel_sd"
    id = Column(BigInteger, primary_key=True)
    measurement_id = Column(BigInteger, nullable=False)
    systolic_blood_pressure_ind = Column(Integer, nullable=False)
    diastolic_blood_pressure_ind = Column(Integer, nullable=False)
    person_id = Column(BigInteger, nullable=False)
    parent_id = Column(BigInteger)
    src_id = Column(String(50))


class PidRidMapping(CdmBase):
    __tablename__ = "pid_rid_mapping"
    person_id = Column(BigInteger, primary_key=True)
    research_id = Column(BigInteger)
    external_id = Column(BigInteger)
    src_id = Column(String(50))


class QuestionnaireResponseAdditionalInfo(CdmBase):
    __tablename__ = "questionnaire_response_additional_info"
    id = Column(BigInteger, primary_key=True)
    questionnaire_response_id = Column(BigInteger)
    type = Column(String(255))
    value = Column(String(255))
    src_id = Column(String(50))

class EHRConsentStatus(CdmBase):
    __tablename__ = "consent"
    id = Column(BigInteger, primary_key=True)
    person_id = Column(BigInteger)
    research_id = Column(BigInteger)
    consent_for_electronic_health_records = Column(String(50))
    consent_for_electronic_health_records_authored = Column(DateTime)
    src_id = Column(String(50))

class WearConsent(CdmBase):
    __tablename__ = "wear_consent"
    id = Column(BigInteger, primary_key=True)
    person_id = Column(BigInteger)
    research_id = Column(BigInteger)
    authored = Column(DateTime)
    consent_status = Column(String(50))
    src_id = Column(String(50))
