'''Configuration for the metrics pipeline.

CONFIG contains all the fields that will appear in metrics, with functions
returning their possible valid values. Summary fields are used to derive values
from other field values.
'''
import config
import participant_enums

from census_regions import census_regions
from code_constants import BASE_VALUES, UNSET
from code_constants import PPI_SYSTEM
from dao.code_dao import CodeDao
from dao.hpo_dao import HPODao
from dao.participant_summary_dao import ParticipantSummaryDao
from field_mappings import QUESTIONNAIRE_MODULE_FIELD_NAMES, NON_EHR_QUESTIONNAIRE_MODULE_FIELD_NAMES
from field_mappings import CONSENT_FOR_STUDY_ENROLLMENT_FIELD, FIELD_TO_QUESTION_CODE
from field_mappings import CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_FIELD
from model.base import get_column_name
from model.code import CodeType
from model.participant_summary import ParticipantSummary
from participant_enums import QuestionnaireStatus, PhysicalMeasurementsStatus, SampleStatus

PARTICIPANT_KIND = 'Participant'
FULL_PARTICIPANT_KIND = 'FullParticipant'
HPO_ID_METRIC = 'hpoId'
CENSUS_REGION_METRIC = 'censusRegion'
BIOSPECIMEN_METRIC = 'biospecimen'
BIOSPECIMEN_SAMPLES_METRIC = 'biospecimenSamples'
PHYSICAL_MEASUREMENTS_METRIC = 'physicalMeasurements'
RACE_METRIC = 'race'
AGE_RANGE_METRIC = 'ageRange'
SAMPLES_TO_ISOLATE_DNA_METRIC = 'samplesToIsolateDNA'
BIOSPECIMEN_SUMMARY_METRIC = 'biospecimenSummary'
NUM_COMPLETED_BASELINE_PPI_MODULES_METRIC = 'numCompletedBaselinePPIModules'
CONSENT_FOR_STUDY_ENROLLMENT_AND_EHR_METRIC = 'consentForStudyEnrollmentAndEHR'
ENROLLMENT_STATUS_METRIC = 'enrollmentStatus'

# This isn't reported as its own metric, but feeds into the  consentForElectronicHealthRecords
# metric.
EHR_CONSENT_ANSWER_METRIC = 'ehrConsent'

SPECIMEN_COLLECTED_VALUE = 'SPECIMEN_COLLECTED'
SAMPLES_ARRIVED_VALUE = 'SAMPLES_ARRIVED'
SUBMITTED_VALUE = str(QuestionnaireStatus.SUBMITTED)

class FieldDef(object):
  def __init__(self, name, values_func, participant_summary_field=None):
    self.name = name
    self.values_func = values_func
    self.participant_summary_field = participant_summary_field

# Fields for codes have a participant summary field name that ends with "Id".
class CodeIdFieldDef(FieldDef):
  def __init__(self, name, values_func):
    super(CodeIdFieldDef, self).__init__(name[0:len(name) - 2], values_func, name)

class SummaryFieldDef(object):
  def __init__(self, name, compute_func, values_func):
    self.name = name
    self.compute_func = compute_func
    self.values_func = values_func

def _biospecimen_summary(summary):
  '''Summarizes the two biospecimen statuses into one.'''
  samples = summary.get(BIOSPECIMEN_SAMPLES_METRIC, UNSET)
  order = summary.get(BIOSPECIMEN_METRIC, UNSET)
  if samples != UNSET:
    return samples
  return order

def _consent_for_study_enrollment_and_ehr(summary):
  '''True when both the study and EHR have been consented to.'''
  consent_for_study = summary.get(CONSENT_FOR_STUDY_ENROLLMENT_FIELD, UNSET)
  consent_for_ehr = summary.get(CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_FIELD, UNSET)
  if consent_for_study == SUBMITTED_VALUE and consent_for_ehr == SUBMITTED_VALUE:
    return SUBMITTED_VALUE
  return UNSET

def _get_baseline_ppi_module_fields():
  return config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS, [])

def _num_completed_baseline_ppi_modules(summary):
  baseline_ppi_module_fields = _get_baseline_ppi_module_fields()
  return sum(1 for field in baseline_ppi_module_fields if summary.get(field) == SUBMITTED_VALUE)

def _enrollment_status(summary):
  ps_dao = ParticipantSummaryDao()
  consent = summary.get(CONSENT_FOR_STUDY_ENROLLMENT_AND_EHR_METRIC) == SUBMITTED_VALUE
  num_completed_baseline_ppi_modules = summary.get(NUM_COMPLETED_BASELINE_PPI_MODULES_METRIC)
  physical_measurements = PhysicalMeasurementsStatus(summary.get(PHYSICAL_MEASUREMENTS_METRIC))
  samples_to_isolate_dna = SampleStatus(summary.get(SAMPLES_TO_ISOLATE_DNA_METRIC))
  return ps_dao.calculate_enrollment_status(consent, num_completed_baseline_ppi_modules,
                                            physical_measurements, samples_to_isolate_dna)

def _get_hpo_ids():
  return [hpo.name for hpo in HPODao().get_all()]

def _get_race_values():
  return [str(race) for race in participant_enums.Race]

def _get_age_buckets():
  return [UNSET] + participant_enums.AGE_BUCKETS

def _get_census_regions():
  return [UNSET] + sorted(list(set(census_regions.values())))

def _get_sample_statuses():
  return [str(status) for status in participant_enums.SampleStatus]

def _get_completed_baseline_ppi_modules_values():
  return range(0, len(_get_baseline_ppi_module_fields()) + 1)

def _get_submission_statuses():
  return [str(status) for status in participant_enums.QuestionnaireStatus]

def _get_physical_measurements_values():
  return [str(status) for status in participant_enums.PhysicalMeasurementsStatus]

def _get_enrollment_statuses():
  return [str(status) for status in participant_enums.EnrollmentStatus]

def _get_biospecimen_values():
  return [UNSET, SPECIMEN_COLLECTED_VALUE]

def _get_biospecimen_samples_values():
  return [UNSET, SAMPLES_ARRIVED_VALUE]

def _get_biospecimen_summary_values():
  return [UNSET, SPECIMEN_COLLECTED_VALUE, SAMPLES_ARRIVED_VALUE]

def _get_answer_values(question_code_value):
  q_code = CodeDao().get_code(PPI_SYSTEM, question_code_value)
  if not q_code:
    return []
  return [answer_code.value for answer_code in q_code.children
          if answer_code.codeType == CodeType.ANSWER]

def _get_answer_values_func(question_code_value):
  return lambda: BASE_VALUES + _get_answer_values(question_code_value)

# These questionnaire answer fields are used to generate metrics.
ANSWER_FIELDS = ['genderIdentityId', 'stateId']

ANSWER_FIELD_TO_QUESTION_CODE = {k: FIELD_TO_QUESTION_CODE[k][0] for k in ANSWER_FIELDS}

# Fields generated by our metrics export.
BASE_PARTICIPANT_FIELDS = ['participant_id', 'date_of_birth', 'first_order_date',
                           'first_samples_arrived_date', 'first_physical_measurements_date',
                           'first_samples_to_isolate_dna_date']
HPO_ID_FIELDS = ['participant_id', 'hpo', 'last_modified']
ANSWER_FIELDS = ['participant_id', 'start_time', 'question_code', 'answer_code', 'answer_string']

def get_participant_fields():
  return BASE_PARTICIPANT_FIELDS + [get_column_name(ParticipantSummary, field_name + 'Time')
                                    for field_name in NON_EHR_QUESTIONNAIRE_MODULE_FIELD_NAMES]

#TODO(danrodney): handle membership tier
# CONFIG defines the fields that can appear in metrics, and functions that can be used
# to determine the valid values for each field.
CONFIG = {
   # These fields are set by logic defined in metrics_pipeline directly, and emitted to metrics
   # buckets. (Any fields mapped to in the pipeline that are *not* in this list will *not* be
   # emitted to metrics buckets, but can be used to calculate other fields.)
  'fields': [
    FieldDef(HPO_ID_METRIC, _get_hpo_ids),
    FieldDef(AGE_RANGE_METRIC, _get_age_buckets),
    FieldDef(CENSUS_REGION_METRIC, _get_census_regions),
    FieldDef(PHYSICAL_MEASUREMENTS_METRIC, _get_physical_measurements_values),
    FieldDef(BIOSPECIMEN_METRIC, _get_biospecimen_values),
    FieldDef(BIOSPECIMEN_SAMPLES_METRIC, _get_biospecimen_samples_values),
    FieldDef(RACE_METRIC, _get_race_values),
    FieldDef(SAMPLES_TO_ISOLATE_DNA_METRIC, _get_sample_statuses),
  ] + [FieldDef(fieldname, _get_submission_statuses) for fieldname in
       QUESTIONNAIRE_MODULE_FIELD_NAMES]
    + [CodeIdFieldDef(fieldname, _get_answer_values_func(question_code)) for
       fieldname, question_code in ANSWER_FIELD_TO_QUESTION_CODE.iteritems()],
  # These fields are computed using the first provided function in the definition,
  # based on the state of other fields.
  'summary_fields': [
    SummaryFieldDef(BIOSPECIMEN_SUMMARY_METRIC, _biospecimen_summary,
                    _get_biospecimen_summary_values),
    SummaryFieldDef(CONSENT_FOR_STUDY_ENROLLMENT_AND_EHR_METRIC,
                    _consent_for_study_enrollment_and_ehr,
                    _get_submission_statuses),
    SummaryFieldDef(NUM_COMPLETED_BASELINE_PPI_MODULES_METRIC, _num_completed_baseline_ppi_modules,
                    _get_completed_baseline_ppi_modules_values),
    SummaryFieldDef(ENROLLMENT_STATUS_METRIC, _enrollment_status,
                    _get_enrollment_statuses)
  ]
}

PARTICIPANT_SUMMARY_FIELD_TO_METRIC_FIELD = {f.participant_summary_field: f.name
                                             for f in CONFIG['fields']
                                             if f.participant_summary_field}

def transform_participant_summary_field(f):
  metric_field = PARTICIPANT_SUMMARY_FIELD_TO_METRIC_FIELD.get(f)
  return metric_field or f

def get_fieldnames():
  return set([field.name for field in get_config()['fields']])

def get_fields():
  fields = []
  conf = get_config()
  for field in conf['fields'] + conf['summary_fields']:
    field_dict = {'name': PARTICIPANT_KIND + '.' + field.name,
                  'values': field.values_func()}
    fields.append(field_dict)
  return sorted(fields, key=lambda field: field['name'])

def get_config():
  return CONFIG
