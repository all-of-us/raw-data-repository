"""Creates a participant, physical measurements, questionnaire responses, and biobank orders."""
import collections
import csv
import datetime
import json
import logging
import random
import string

from dateutil.parser import parse
from rdr_service.config import GAE_PROJECT
from werkzeug.exceptions import BadRequest

from rdr_service import clock
from rdr_service.api_util import open_cloud_file
from rdr_service.code_constants import (
    BIOBANK_TESTS,
    CABOR_SIGNATURE_QUESTION_CODE,
    CITY_QUESTION_CODE,
    CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE,
    CONSENT_FOR_STUDY_ENROLLMENT_MODULE,
    DATE_OF_BIRTH_QUESTION_CODE,
    EDUCATION_QUESTION_CODE,
    EMAIL_QUESTION_CODE,
    FIRST_NAME_QUESTION_CODE,
    GENDER_IDENTITY_QUESTION_CODE,
    HEALTHPRO_USERNAME_SYSTEM,
    INCOME_QUESTION_CODE,
    LANGUAGE_QUESTION_CODE,
    LAST_NAME_QUESTION_CODE,
    LIFESTYLE_PPI_MODULE,
    MIDDLE_NAME_QUESTION_CODE,
    OVERALL_HEALTH_PPI_MODULE,
    PHONE_NUMBER_QUESTION_CODE,
    PMI_OTHER_CODE,
    PMI_PREFER_NOT_TO_ANSWER_CODE,
    PPI_SYSTEM,
    RACE_QUESTION_CODE,
    RECONTACT_METHOD_QUESTION_CODE,
    SEXUAL_ORIENTATION_QUESTION_CODE,
    SEX_QUESTION_CODE,
    SITE_ID_SYSTEM,
    STATE_QUESTION_CODE,
    STREET_ADDRESS2_QUESTION_CODE,
    STREET_ADDRESS_QUESTION_CODE,
    THE_BASICS_PPI_MODULE,
    ZIPCODE_QUESTION_CODE,
)
from rdr_service.concepts import Concept
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.physical_measurements_dao import (
    _AUTHORING_STEP,
    _AUTHOR_PREFIX,
    _CREATED_LOC_EXTENSION,
    _CREATED_STATUS,
    _FINALIZED_LOC_EXTENSION,
    _FINALIZED_STATUS,
    _LOCATION_PREFIX,
)
from rdr_service.dao.questionnaire_dao import QuestionnaireDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.field_mappings import QUESTION_CODE_TO_FIELD
from rdr_service.model.code import CodeType
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.participant_enums import UNSET_HPO_ID, make_primary_provider_link_for_hpo, RetentionStatus, \
    RetentionType

_ANSWER_SPECS_BUCKET = "all-of-us-rdr-fake-data-spec"
_ANSWER_SPECS_FILE = "answer_specs.csv"

_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# 30%+ of participants have no primary provider link / HPO set
_NO_HPO_PERCENT = 0.3
# 20%+ of participants have no questionnaires submitted (including consent)
_NO_QUESTIONNAIRES_SUBMITTED = 0.2
# 20% of consented participants that submit the basics questionnaire have no biobank orders
_NO_BIOBANK_ORDERS = 0.2
# 20% of consented participants that submit the basics questionnaire have no physical measurements
_NO_PHYSICAL_MEASUREMENTS = 0.2
# 10% of individual physical measurements are absent
_PHYSICAL_MEASUREMENT_ABSENT = 0.1
# 20% of eligible physical measurements have qualifiers
_PHYSICAL_MEASURMENT_QUALIFIED = 0.2
# 80% of consented participants have no changes to their HPO
_NO_HPO_CHANGE = 0.8
# 5% of participants with biobank orders have multiple
_MULTIPLE_BIOBANK_ORDERS = 0.05
# 20% of participants with biobank orders have no biobank samples
_NO_BIOBANK_SAMPLES = 0.2
# Any other questionnaire has a 40% chance of not being submitted
_QUESTIONNAIRE_NOT_SUBMITTED = 0.4
# Any given question on a submitted questionnaire has a 10% chance of not being answered
_QUESTION_NOT_ANSWERED = 0.1
# The maximum percentage deviation of repeated measurements.
_PERCENT_DEVIATION_FOR_REPEATED_MEASUREMENTS = 0.01
# The system used with PMI codes in physical measurements
_PMI_MEASUREMENTS_SYSTEM = "http://terminology.pmi-ops.org/CodeSystem/physical-measurements"
# Maximum number of days between a participant consenting and submitting physical measurements
_MAX_DAYS_BEFORE_PHYSICAL_MEASUREMENTS = 60
# Maximum number of days between a participant consenting and submitting a biobank order.
_MAX_DAYS_BEFORE_BIOBANK_ORDER = 60
# Maximum number of days between a participant consenting and changing their HPO
_MAX_DAYS_BEFORE_HPO_CHANGE = 60
# Maximum number of days between the last request and the participant withdrawing from the study
_MAX_DAYS_BEFORE_WITHDRAWAL = 30
# Maximum number of days between the last request and the participant suspending their account
_MAX_DAYS_BEFORE_SUSPENSION = 30
# Max amount of time between created biobank orders and collected time for a sample.
_MAX_MINUTES_BETWEEN_ORDER_CREATED_AND_SAMPLE_COLLECTED = 72 * 60
# Max amount of time between collected and processed biobank order samples.
_MAX_MINUTES_BETWEEN_SAMPLE_COLLECTED_AND_PROCESSED = 72 * 60
# Max amount of time between processed and finalized biobank order samples.
_MAX_MINUTES_BETWEEN_SAMPLE_PROCESSED_AND_FINALIZED = 72 * 60
# Max amount of time between processed and finalized biobank orders.
# Random amount of time between questionnaire submissions
_MAX_DAYS_BETWEEN_SUBMISSIONS = 30

# Start creating participants from 1 years ago
_MAX_DAYS_HISTORY = 365

# Percentage of participants with multiple race answers
_MULTIPLE_RACE_ANSWERS = 0.2

# Maximum number of race answers
_MAX_RACE_ANSWERS = 3

# Maximum age of participants
_MAX_PARTICIPANT_AGE = 102

# Minimum age of participants
_MIN_PARTICIPANT_AGE = 12

_QUESTIONNAIRE_CONCEPTS = [
    CONSENT_FOR_STUDY_ENROLLMENT_MODULE,
    CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE,
    OVERALL_HEALTH_PPI_MODULE,
    LIFESTYLE_PPI_MODULE,
    THE_BASICS_PPI_MODULE,
]
_CALIFORNIA_HPOS = ["CAL_PMC", "SAN_YSIDRO"]

_QUESTION_CODES = list(QUESTION_CODE_TO_FIELD.keys()) + [RACE_QUESTION_CODE, CABOR_SIGNATURE_QUESTION_CODE]

_CONSTANT_CODES = [PMI_PREFER_NOT_TO_ANSWER_CODE, PMI_OTHER_CODE]


class FakeParticipantGenerator(object):
    def __init__(self, client, use_local_files=None, withdrawn_percent=0.05, suspended_percent=0.05):
        self._use_local_files = use_local_files
        self._client = client
        self._hpos = HPODao().get_all()
        self._sites = SiteDao().get_all()
        if not self._sites:
            raise BadRequest("No sites found; import sites before running generator.")
        self._now = clock.CLOCK.now()
        self._consent_questionnaire_id_and_version = None
        self._setup_data()
        self._setup_questionnaires()
        self._min_birth_date = self._now - datetime.timedelta(days=_MAX_PARTICIPANT_AGE * 365)
        self._max_days_for_birth_date = 365 * (_MAX_PARTICIPANT_AGE - _MIN_PARTICIPANT_AGE)

        # n% of participants withdraw from the study. Default is 5%
        self.withdrawn_percent = withdrawn_percent
        # n% of participants suspend their account. Default is 5%
        self.suspended_percent = suspended_percent

    def _days_ago(self, num_days):
        return self._now - datetime.timedelta(days=num_days)

    def _get_answer_codes(self, code):
        result = []
        for child in code.children:
            if child.codeType == CodeType.ANSWER:
                result.append(child.value)
                result.extend(self._get_answer_codes(child))
        return result

    def _setup_questionnaires(self):
        """Locates questionnaires and verifies that they have the appropriate questions in them."""
        questionnaire_dao = QuestionnaireDao()
        code_dao = CodeDao()
        question_code_to_questionnaire_id = {}
        self._questionnaire_to_questions = collections.defaultdict(list)
        self._question_code_to_answer_codes = {}
        # Populate maps of questionnaire ID/version to [(question_code, link ID)] and
        # question code to answer codes.
        for concept in _QUESTIONNAIRE_CONCEPTS:
            code = code_dao.get_code(PPI_SYSTEM, concept)
            if code is None:
                raise BadRequest("Code missing: %s; import data and clear cache." % concept)
            questionnaire = questionnaire_dao.get_latest_questionnaire_with_concept(code.codeId)
            if questionnaire is None:
                raise BadRequest("Questionnaire for code %s missing; import data." % concept)
            questionnaire_id_and_version = (questionnaire.questionnaireId, questionnaire.semanticVersion)
            if concept == CONSENT_FOR_STUDY_ENROLLMENT_MODULE:
                self._consent_questionnaire_id_and_version = questionnaire_id_and_version
            elif concept == THE_BASICS_PPI_MODULE:
                self._the_basics_questionnaire_id_and_version = questionnaire_id_and_version
            questions = self._questionnaire_to_questions[questionnaire_id_and_version]
            if questions:
                # We already handled this questionnaire.
                continue

            for question in questionnaire.questions:
                question_code = code_dao.get(question.codeId)
                if (question_code.value in _QUESTION_CODES) or (question_code.value in self._answer_specs):
                    question_code_to_questionnaire_id[question_code.value] = questionnaire.questionnaireId
                    questions.append((question_code.value, question.linkId))
                    if question_code.value in _QUESTION_CODES:
                        answer_codes = self._get_answer_codes(question_code)
                        all_codes = (answer_codes + _CONSTANT_CODES) if answer_codes else _CONSTANT_CODES
                        self._question_code_to_answer_codes[question_code.value] = all_codes
        # Log warnings for any question codes not found in the questionnaires.
        for code_value in _QUESTION_CODES + list(self._answer_specs.keys()):
            questionnaire_id = question_code_to_questionnaire_id.get(code_value)
            if not questionnaire_id:
                logging.warning("Question for code %s missing; import questionnaires", code_value)

    def _read_all_lines(self, filename):
        with open("rdr_service/app_data/%s" % filename) as f:
            reader = csv.reader(f)
            return [line[0].strip() for line in reader]

    def _read_json(self, filename):
        with open("rdr_service/app_data/%s" % filename) as f:
            return json.load(f)

    def _read_csv_from_gcs(self, bucket_name, file_name):
        with open_cloud_file("/%s/%s" % (bucket_name, file_name)) as infile:
            return list(csv.DictReader(infile))

    def _read_csv_from_file(self, file_name):
        with open("rdr_service/app_data/%s" % file_name, mode="r") as infile:
            return list(csv.DictReader(infile))

    def _setup_data(self):
        self._zip_code_to_state = {}
        with open("rdr_service/app_data/zipcodes.txt") as zipcodes:
            reader = csv.reader(zipcodes)
            for zipcode, state in reader:
                self._zip_code_to_state[zipcode] = state
        self._first_names = self._read_all_lines("first_names.txt")
        self._middle_names = self._read_all_lines("middle_names.txt")
        self._last_names = self._read_all_lines("last_names.txt")
        self._city_names = self._read_all_lines("city_names.txt")
        self._street_names = self._read_all_lines("street_names.txt")
        measurement_specs = self._read_json("measurement_specs.json")
        if self._use_local_files or GAE_PROJECT == 'localhost':
            # Read CSV from a local file when running dev_appserver.
            answer_specs = self._read_csv_from_file(_ANSWER_SPECS_FILE)
        else:
            # Read from GCS when running in Cloud environments.
            answer_specs = self._read_csv_from_gcs(_ANSWER_SPECS_BUCKET, _ANSWER_SPECS_FILE)
        # Save all the answer specs for questions that don't have special handling already.
        self._answer_specs = {
            answer_spec["question_code"]: answer_spec
            for answer_spec in answer_specs
            if answer_spec["question_code"] not in _QUESTION_CODES
        }
        # Serves as the denominator when deciding whether to answer a question.
        self._answer_specs_max_participants = max(
            [int(answer_spec["num_participants"]) for answer_spec in answer_specs]
        )
        qualifier_concepts = set()
        for measurement in measurement_specs:
            for qualifier in measurement["qualifiers"]:
                qualifier_concepts.add(Concept(qualifier["system"], qualifier["code"]))
        measurement_map = {
            Concept(measurement["code"]["system"], measurement["code"]["code"]): measurement
            for measurement in measurement_specs
        }
        self._measurement_specs = [
            measurement
            for measurement in measurement_specs
            if Concept(measurement["code"]["system"], measurement["code"]["code"]) not in qualifier_concepts
        ]
        self._qualifier_map = {
            qualifier_concept: measurement_map[qualifier_concept] for qualifier_concept in qualifier_concepts
        }

    def _make_full_url(self, concept):
        return "urn:example:%s" % concept["code"]

    def _make_base_measurement_resource(self, measurement, mean, measurement_count):
        resource = {
            "code": {
                "coding": [
                    {
                        "code": measurement["code"]["code"],
                        "display": "measurement",
                        "system": measurement["code"]["system"],
                    }
                ],
                "text": "text",
            }
        }
        pmi_code = measurement.get("pmiCode")
        if pmi_code:
            pmi_code_json = {"code": pmi_code, "display": "measurement", "system": _PMI_MEASUREMENTS_SYSTEM}
            resource["code"]["coding"].append(pmi_code_json)
        else:
            pmi_code_prefix = measurement.get("pmiCodePrefix")
            if pmi_code_prefix:
                code_suffix = None
                if mean:
                    code_suffix = "mean"
                else:
                    code_suffix = str(measurement_count)
                pmi_code_json = {
                    "code": pmi_code_prefix + code_suffix,
                    "display": "measurement",
                    "system": _PMI_MEASUREMENTS_SYSTEM,
                }
                resource["code"]["coding"].append(pmi_code_json)
        return resource

    def _make_measurement_resource(self, measurement, qualifier_set, previous_resource, measurement_count):
        resource = self._make_base_measurement_resource(measurement, False, measurement_count)
        if "decimal" in measurement["types"]:
            previous_value = None
            previous_unit = None
            if previous_resource:
                previous_value_quantity = previous_resource["valueQuantity"]
                previous_unit = previous_value_quantity["unit"]
                previous_value = previous_value_quantity["value"]

            # Arguably min and max should vary with units, but in our data there's only one unit
            # so we won't bother for now.
            min_value = measurement["min"]
            max_value = measurement["max"]
            if previous_unit:
                unit = previous_unit
            else:
                unit = random.choice(measurement["units"])
            if min_value == int(min_value) and max_value == int(max_value):
                # Assume int min and max means an integer
                if previous_value:
                    # Find a value that is up to some percent of the possible range higher or lower.
                    delta = int(_PERCENT_DEVIATION_FOR_REPEATED_MEASUREMENTS * (max_value - min_value))
                    value = random.randint(previous_value - delta, previous_value + delta)
                else:
                    value = random.randint(min_value, max_value)
            else:
                # Otherwise assume a floating point number with one digit after the decimal place
                if previous_value:
                    delta = _PERCENT_DEVIATION_FOR_REPEATED_MEASUREMENTS * (max_value - min_value)
                    value = round(random.uniform(previous_value - delta, previous_value + delta), 1)
                else:
                    value = round(random.uniform(min_value, max_value), 1)
            resource["valueQuantity"] = {
                "code": unit,
                "system": "http://unitsofmeasure.org",
                "unit": unit,
                "value": value,
            }
        if "string" in measurement["types"]:
            resource["valueString"] = "".join([random.choice(string.ascii_lowercase) for _ in range(20)])
        if measurement["valueCodes"]:
            value_code = random.choice(measurement["valueCodes"])
            resource["valueCodeableConcept"] = {
                "coding": [{"system": value_code["system"], "code": value_code["code"], "display": "value"}],
                "text": "value text",
            }
        if measurement["qualifiers"]:
            if random.random() <= _PHYSICAL_MEASURMENT_QUALIFIED:
                qualifiers = random.sample(
                    measurement["qualifiers"], random.randint(1, len(measurement["qualifiers"]))
                )
                qualifier_set.update(Concept(qualifier["system"], qualifier["code"]) for qualifier in qualifiers)
                resource["related"] = [
                    {"type": "qualified-by", "target": {"reference": self._make_full_url(qualifier)}}
                    for qualifier in qualifiers
                ]
        return resource

    def _get_measurement_values(self, measurement_resources):
        return [resource["valueQuantity"]["value"] for resource in measurement_resources]

    def _get_related(self, measurement_resources):
        related_list = []
        related_urls = set()
        for measurement_resource in measurement_resources:
            for related in measurement_resource.get("related", []):
                # Make sure we don't repeat references to the same qualifier.
                url = related["target"]["reference"]
                if url not in related_urls:
                    related_list.append(related)
                    related_urls.add(url)
        return related_list

    def _mean(self, values):
        return round(sum(values) / len(values), 1)

    def _find_closest_two_mean(self, measurement_values):
        # Find the mean of the two closest values; or if they are equally distant from each other,
        # find the mean of all three.
        delta_0_1 = abs(measurement_values[0] - measurement_values[1])
        delta_0_2 = abs(measurement_values[0] - measurement_values[2])
        delta_1_2 = abs(measurement_values[1] - measurement_values[2])
        if delta_0_1 < delta_0_2:
            if delta_0_1 < delta_1_2:
                return self._mean(measurement_values[0:2])
            else:
                return self._mean(measurement_values[1:])
        elif delta_0_1 == delta_0_2:
            return self._mean(measurement_values)
        else:
            if delta_0_2 < delta_1_2:
                return self._mean([measurement_values[0], measurement_values[2]])
            else:
                return self._mean(measurement_values[1:])

    def _calculate_last_two_mean(self, measurement_values):
        return self._mean(measurement_values[1:])

    def _find_components_by_coding(self, measurement_resources, submeasurement_coding):
        components = []
        for resource in measurement_resources:
            for subcomponent in resource.get("component", []):
                for coding in subcomponent["code"]["coding"]:
                    if (
                        coding["system"] == submeasurement_coding["system"]
                        and coding["code"] == submeasurement_coding["code"]
                    ):
                        components.append(subcomponent)
        return components

    def _make_mean_resource(self, mean_type, measurement, measurement_resources):
        resource = self._make_base_measurement_resource(measurement, True, None)
        if measurement_resources[0].get("valueQuantity"):
            measurement_values = self._get_measurement_values(measurement_resources)
            if len(measurement_values) < 3:
                raise BadRequest("Bad measurement resources: %s" % measurement_resources)
            elif mean_type == "closestTwo":
                value = self._find_closest_two_mean(measurement_values)
            elif mean_type == "lastTwo":
                value = self._calculate_last_two_mean(measurement_values)
            else:
                raise BadRequest("Unknown meanType: %s" % mean_type)
            unit = measurement_resources[0]["valueQuantity"]["unit"]
            resource["valueQuantity"] = {
                "code": unit,
                "system": "http://unitsofmeasure.org",
                "unit": unit,
                "value": value,
            }
        # Include all qualifiers on the measurements being averaged.
        related = self._get_related(measurement_resources)
        if related:
            resource["related"] = related
        return resource

    def _make_mean_entry(self, measurement, time_str, participant_id, measurement_resources):
        mean_type = measurement.get("meanType")
        resource = self._make_mean_resource(mean_type, measurement, measurement_resources)
        self._populate_measurement_entry(resource, time_str, participant_id)
        body_site = measurement_resources[0].get("bodySite")
        if body_site:
            resource["bodySite"] = body_site
        if measurement["submeasurements"]:
            components = []
            for submeasurement in measurement["submeasurements"]:
                measurement_components = self._find_components_by_coding(measurement_resources, submeasurement["code"])
                if measurement_components:
                    components.append(self._make_mean_resource(mean_type, submeasurement, measurement_components))
            if components:
                resource["component"] = components
        return {"fullUrl": self._make_full_url(measurement["code"]), "resource": resource}

    def _populate_measurement_entry(self, resource, time_str, participant_id):
        resource["effectiveDateTime"] = time_str
        resource["resourceType"] = "Observation"
        resource["status"] = "final"
        resource["subject"] = {"reference": "Patient/%s" % participant_id}

    def _make_measurement_entry(
        self, measurement, time_str, participant_id, qualifier_set, previous_resource=None, measurement_count=None
    ):
        resource = self._make_measurement_resource(measurement, qualifier_set, previous_resource, measurement_count)
        self._populate_measurement_entry(resource, time_str, participant_id)
        if measurement["bodySites"]:
            if previous_resource:
                resource["bodySite"] = previous_resource["bodySite"]
            else:
                body_site = random.choice(measurement["bodySites"])
                resource["bodySite"] = {
                    "coding": [{"code": body_site["code"], "display": "body site", "system": body_site["system"]}],
                    "text": "text",
                }
        if measurement["submeasurements"]:
            components = []
            for submeasurement in measurement["submeasurements"]:
                previous_component = None
                if previous_resource:
                    subcomponents = self._find_components_by_coding([previous_resource], submeasurement["code"])
                    if subcomponents:
                        previous_component = subcomponents[0]
                components.append(
                    self._make_measurement_resource(
                        submeasurement, qualifier_set, previous_component, measurement_count
                    )
                )
            if components:
                resource["component"] = components
        return {"fullUrl": self._make_full_url(measurement["code"]), "resource": resource}

    def _make_author(self, username, authoring_step):
        return {
            "reference": "%s%s" % (_AUTHOR_PREFIX, username),
            "extension": {"url": _AUTHORING_STEP, "valueCode": authoring_step},
        }

    def _make_physical_measurements(self, participant_id, measurements_time, force_measurement=False):
        time_str = measurements_time.isoformat()
        site = random.choice(self._sites)
        entries = [
            {
                "fullUrl": "urn:example:report",
                "resource": {
                    "author": [
                        self._make_author("creator@pmi-ops.org", _CREATED_STATUS),
                        self._make_author("finalizer@pmi-ops.org", _FINALIZED_STATUS),
                    ],
                    "extension": [
                        {
                            "url": _CREATED_LOC_EXTENSION,
                            "valueReference": "%s%s" % (_LOCATION_PREFIX, site.googleGroup),
                        },
                        {
                            "url": _FINALIZED_LOC_EXTENSION,
                            "valueReference": "%s%s" % (_LOCATION_PREFIX, site.googleGroup),
                        },
                    ],
                    "date": time_str,
                    "resourceType": "Composition",
                    "section": [{"entry": [{"reference": "urn:example:blood-pressure-1"}]}],
                    "status": "final",
                    "subject": {"reference": "Patient/%s" % participant_id},
                    "title": "PMI Intake Evaluation",
                    "type": {
                        "coding": [
                            {
                                "code": "intake-exam-v0.0.1",
                                "display": "PMI Intake Evaluation v0.0.1",
                                "system": "http://terminology.pmi-ops.org/CodeSystem/document-type",
                            }
                        ],
                        "text": "PMI Intake Evaluation v0.0.1",
                    },
                },
            }
        ]

        qualifier_set = set()
        for measurement in self._measurement_specs:
            if random.random() <= _PHYSICAL_MEASUREMENT_ABSENT and not force_measurement:
                continue
            num_measurements = 1
            num_measurements_str = measurement.get("numMeasurements")
            if num_measurements_str is not None:
                num_measurements = int(num_measurements_str)
            measurement_resources = []
            first_entry = self._make_measurement_entry(measurement, time_str, participant_id, qualifier_set, None, 1)
            entries.append(first_entry)
            if num_measurements > 1:
                measurement_resources.append(first_entry["resource"])
                for i in range(1, num_measurements):
                    entry = self._make_measurement_entry(
                        measurement, time_str, participant_id, qualifier_set, first_entry["resource"], i + 1
                    )
                    measurement_resources.append(entry["resource"])
                    entries.append(entry)
                entries.append(self._make_mean_entry(measurement, time_str, participant_id, measurement_resources))

        # Add any qualifiers that were specified for other measurements.
        for qualifier in qualifier_set:
            qualifier_measurement = self._qualifier_map[qualifier]
            entry = self._make_measurement_entry(qualifier_measurement, time_str, participant_id, qualifier_set)
            entries.append(entry)
        return {"resourceType": "Bundle", "type": "document", "entry": entries}

    def _submit_physical_measurements(self, participant_id, consent_time, force_measurement=False):
        if random.random() <= _NO_PHYSICAL_MEASUREMENTS and not force_measurement:
            return consent_time
        days_delta = random.randint(0, _MAX_DAYS_BEFORE_BIOBANK_ORDER)
        measurements_time = consent_time + datetime.timedelta(days=days_delta)
        request_json = self._make_physical_measurements(participant_id, measurements_time)
        self._client.request_json(
            _physical_measurements_url(participant_id),
            method="POST",
            body=request_json,
            pretend_date=measurements_time,
        )
        return measurements_time

    def _make_biobank_order_request(self, participant_id, sample_tests, created_time):
        samples = []
        order_id_suffix = "%s-%d" % (participant_id, random.randint(0, 100000000))
        site = random.choice(self._sites)
        handling_info = {
            "author": {"system": HEALTHPRO_USERNAME_SYSTEM, "value": "nobody@pmi-ops.org"},
            "site": {"system": SITE_ID_SYSTEM, "value": site.googleGroup},
        }
        request = {
            "subject": "Patient/%s" % participant_id,
            "identifier": [
                {"system": "https://www.pmi-ops.org", "value": "healthpro-order-id-123%s" % order_id_suffix},
                {"system": "https://orders.mayomedicallaboratories.com", "value": "WEB1YLHV%s" % order_id_suffix},
                {"system": "https://orders.mayomedicallaboratories.com/kit-id", "value": "KIT-%s" % order_id_suffix},
                {
                    "system": "https://orders.mayomedicallaboratories.com/tracking-number",
                    "value": "177%s" % order_id_suffix,
                },
            ],
            "createdInfo": handling_info,
            "processedInfo": handling_info,
            "collectedInfo": handling_info,
            "finalizedInfo": handling_info,
            "created": created_time.strftime(_TIME_FORMAT),
            "samples": samples,
            "notes": {"collected": "Collected notes", "processed": "Processed notes", "finalized": "Finalized notes"},
        }
        for sample_test in sample_tests:
            minutes_delta = random.randint(0, _MAX_MINUTES_BETWEEN_ORDER_CREATED_AND_SAMPLE_COLLECTED)
            collected_time = created_time + datetime.timedelta(minutes=minutes_delta)
            minutes_delta = random.randint(0, _MAX_MINUTES_BETWEEN_SAMPLE_COLLECTED_AND_PROCESSED)
            processed_time = collected_time + datetime.timedelta(minutes=minutes_delta)
            minutes_delta = random.randint(0, _MAX_MINUTES_BETWEEN_SAMPLE_PROCESSED_AND_FINALIZED)
            finalized_time = processed_time + datetime.timedelta(minutes=minutes_delta)
            processing_required = True if random.random() <= 0.5 else False
            samples.append(
                {
                    "test": sample_test,
                    "description": "Description for %s" % sample_test,
                    "collected": collected_time.strftime(_TIME_FORMAT),
                    "processed": processed_time.strftime(_TIME_FORMAT),
                    "finalized": finalized_time.strftime(_TIME_FORMAT),
                    "processingRequired": processing_required,
                }
            )
        return request

    def _submit_biobank_order(self, participant_id, start_time):
        num_samples = random.randint(1, len(BIOBANK_TESTS))
        order_tests = random.sample(BIOBANK_TESTS, num_samples)
        days_delta = random.randint(0, _MAX_DAYS_BEFORE_BIOBANK_ORDER)
        created_time = start_time + datetime.timedelta(days=days_delta)
        order_json = self._make_biobank_order_request(participant_id, order_tests, created_time)
        self._client.request_json(
            _biobank_order_url(participant_id), method="POST", body=order_json, pretend_date=created_time
        )
        return created_time

    def _submit_biobank_data(self, participant_id, consent_time, force_measurement=False):
        if random.random() <= _NO_BIOBANK_ORDERS and not force_measurement:
            return consent_time
        last_request_time = self._submit_biobank_order(participant_id, consent_time)
        if random.random() <= _MULTIPLE_BIOBANK_ORDERS:
            last_request_time = self._submit_biobank_order(participant_id, last_request_time)
        return last_request_time

    def _update_participant(self, change_time, participant_response, participant_id):
        return self._client.request_json(
            _participant_url(participant_id),
            method="PUT",
            body=participant_response,
            headers={"If-Match": participant_response["meta"]["versionId"]},
            pretend_date=change_time,
        )

    def _submit_hpo_changes(self, participant_response, participant_id, consent_time):
        if random.random() <= _NO_HPO_CHANGE:
            return consent_time, participant_response
        # Re-fetch the participant to make sure we have the up-to-date version.
        participant_response = self._client.request_json(_participant_url(participant_id), method="GET")
        hpo = random.choice(self._hpos)
        participant_response["providerLink"] = json.loads(make_primary_provider_link_for_hpo(hpo))
        days_delta = random.randint(0, _MAX_DAYS_BEFORE_HPO_CHANGE)
        change_time = consent_time + datetime.timedelta(days=days_delta)
        result = self._update_participant(change_time, participant_response, participant_id)
        return change_time, result

    def _submit_status_changes(self, participant_id, last_request_time, force_measurement=False):
        if random.random() <= self.suspended_percent and not force_measurement:
            # Fetch the participant to ensure its version is up-to-date.
            participant_response = self._client.request_json(_participant_url(participant_id), method="GET")
            participant_response["suspensionStatus"] = "NO_CONTACT"
            days_delta = random.randint(0, _MAX_DAYS_BEFORE_SUSPENSION)
            change_time = last_request_time + datetime.timedelta(days=days_delta)
            participant_response = self._update_participant(change_time, participant_response, participant_id)
            last_request_time = change_time
        if random.random() <= self.withdrawn_percent and not force_measurement:
            # Fetch the participant to ensure its version is up-to-date.
            participant_response = self._client.request_json(_participant_url(participant_id), method="GET")
            participant_response["withdrawalStatus"] = "NO_USE"
            days_delta = random.randint(0, _MAX_DAYS_BEFORE_WITHDRAWAL)
            change_time = last_request_time + datetime.timedelta(days=days_delta)
            self._update_participant(change_time, participant_response, participant_id)

    def generate_participant(self, include_physical_measurements,
                             include_biobank_orders,
                             requested_hpo=None,
                             requested_site=None):
        participant_response, creation_time, hpo = self._create_participant(requested_hpo,
                                                                            site=requested_site)
        participant_id = participant_response["participantId"]
        retention_time = creation_time

        if requested_site is None:
            california_hpo = hpo is not None and hpo.name in _CALIFORNIA_HPOS
            consent_time, last_qr_time, the_basics_submission_time = self._submit_questionnaire_responses(
                participant_id, california_hpo, creation_time
            )

            if consent_time:
                last_request_time = retention_time = last_qr_time
                # Potentially include physical measurements and biobank orders if the client requested it
                # and the participant has submitted the basics questionnaire.
                if include_physical_measurements and the_basics_submission_time:
                    last_measurement_time = self._submit_physical_measurements(participant_id,
                                                                               the_basics_submission_time)
                    last_request_time = max(last_request_time, last_measurement_time)
                if include_biobank_orders and the_basics_submission_time:
                    last_biobank_time = self._submit_biobank_data(participant_id, the_basics_submission_time)
                    last_request_time = max(last_request_time, last_biobank_time)
                if not requested_hpo:
                    last_hpo_change_time, participant_response = self._submit_hpo_changes(
                        participant_response, participant_id, consent_time
                    )
                    last_request_time = max(last_request_time, last_hpo_change_time)
                self._submit_status_changes(participant_id, last_request_time)

        # Create a retention_eligible_metrics record for the participant.
        rec = RetentionEligibleMetrics()
        rec.participantId = int(participant_id[1:])
        rec.retentionEligible = True if random.randint(0, 1) else False
        rec.retentionEligibleStatus = \
            RetentionStatus.NOT_ELIGIBLE if rec.retentionEligible else RetentionStatus.ELIGIBLE
        rec.retentionType = RetentionType.UNSET
        if rec.retentionEligible:
            rec.retentionEligibleTime = retention_time
            rec.activelyRetained = True if random.randint(0, 1) else False
            rec.passivelyRetained = True if random.randint(0, 1) else False
            rec.fileUploadDate = retention_time
            if rec.activelyRetained and not rec.passivelyRetained:
                rec.retentionType = RetentionType.ACTIVE
            elif not rec.activelyRetained and rec.passivelyRetained:
                rec.retentionType = RetentionType.PASSIVE
            elif rec.activelyRetained and rec.passivelyRetained:
                rec.retentionType = RetentionType.ACTIVE_AND_PASSIVE

        with HPODao().session() as session:
            session.add(rec)
            session.commit()

    def add_pm_and_biospecimens_to_participants(self, participant_id):
        logging.info("Adding PM&B for %s", participant_id)
        _, last_qr_time, the_basics_submission_time = self._submit_questionnaire_responses(
            participant_id, False, self._now, force_measurement=True
        )
        if the_basics_submission_time is None:
            the_basics_submission_time = self._now
        last_request_time = last_qr_time
        last_measurement_time = self._submit_physical_measurements(
            participant_id, the_basics_submission_time, force_measurement=True
        )
        if last_measurement_time is None:
            last_measurement_time = self._now
        last_request_time = max(last_request_time, last_measurement_time)
        last_biobank_time = self._submit_biobank_data(
            participant_id, the_basics_submission_time, force_measurement=True
        )
        last_request_time = max(last_request_time, last_biobank_time)
        if last_request_time is None:
            last_request_time = self._now
        logging.info("submitting physical measurements and biospecimen for %s" % participant_id)
        self._submit_status_changes(participant_id, last_request_time, force_measurement=True)

    def _create_participant(self, hpo_name, site=None):
        participant_json = {}
        creation_time = self._days_ago(random.randint(0, _MAX_DAYS_HISTORY))

        hpo = None
        if hpo_name and isinstance(hpo_name, str):
            hpo = HPODao().get_by_name(hpo_name)
        else:
            if random.random() > _NO_HPO_PERCENT and site is None:
                hpo = random.choice(self._hpos)
        if site is not None:
            hpo = HPODao().get(site.hpoId)
            creation_time = self._now
            participant_json["site"] = site.googleGroup
        if hpo:
            if hpo.hpoId != UNSET_HPO_ID:
                participant_json["providerLink"] = json.loads(make_primary_provider_link_for_hpo(hpo))

        participant_response = self._client.request_json(
            "Participant", method="POST", body=participant_json, pretend_date=creation_time
        )

        return (participant_response, creation_time, hpo)

    def _random_code_answer(self, question_code):
        code = random.choice(self._question_code_to_answer_codes[question_code])
        return [_code_answer(code)]

    def _choose_answer_code(self, question_code):
        answer_codes = self._question_code_to_answer_codes.get(question_code)
        if not answer_codes:
            # There is no question in questionnaires for this code; skip.
            return None
        if random.random() <= _QUESTION_NOT_ANSWERED:
            return None
        return self._random_code_answer(question_code)

    def _choose_answer_codes(self, question_code, percent_with_multiple, max_answers):
        answer_codes = self._question_code_to_answer_codes.get(question_code)
        if not answer_codes:
            # There is no question in questionnaires for this code; skip.
            return None
        if random.random() <= _QUESTION_NOT_ANSWERED:
            return None
        if random.random() > percent_with_multiple:
            return self._random_code_answer(question_code)
        num_answers = random.randint(2, max_answers)
        codes = random.sample(self._question_code_to_answer_codes[question_code], num_answers)
        return [_code_answer(code) for code in codes]

    def _choose_street_address(self):
        if random.random() <= _QUESTION_NOT_ANSWERED:
            return None
        return "%d %s" % (random.randint(100, 9999), random.choice(self._street_names))

    def _choose_city(self):
        if random.random() <= _QUESTION_NOT_ANSWERED:
            return None
        return random.choice(self._city_names)

    def _choose_phone_number(self):
        if random.random() <= _QUESTION_NOT_ANSWERED:
            return None
        return "(%d) %d-%d" % (random.randint(200, 999), random.randint(200, 999), random.randint(0, 9999))

    def _choose_state_and_zip(self, answer_map):
        if random.random() <= _QUESTION_NOT_ANSWERED:
            return
        zip_code = random.choice(list(self._zip_code_to_state.keys()))
        state = self._zip_code_to_state.get(zip_code)
        answer_map[ZIPCODE_QUESTION_CODE] = _string_answer(zip_code)
        answer_map[STATE_QUESTION_CODE] = [_code_answer("PIIState_%s" % state)]

    def _choose_name(self, answer_map):
        first_name = random.choice(self._first_names)
        middle_name = random.choice(self._middle_names)
        last_name = random.choice(self._last_names)
        email = first_name + last_name + "@fakeexample.com"
        answer_map[FIRST_NAME_QUESTION_CODE] = _string_answer(first_name)
        answer_map[MIDDLE_NAME_QUESTION_CODE] = _string_answer(middle_name)
        answer_map[LAST_NAME_QUESTION_CODE] = _string_answer(last_name)
        answer_map[EMAIL_QUESTION_CODE] = _string_answer(email)

    def _choose_date_of_birth(self, answer_map):
        delta = datetime.timedelta(days=random.randint(0, self._max_days_for_birth_date))
        date_of_birth = (self._min_birth_date + delta).date()
        answer_map[DATE_OF_BIRTH_QUESTION_CODE] = [{"valueDate": date_of_birth.isoformat()}]

    def _choose_answer_for_spec(self, answer_spec, answer_count):
        # We assume that each question only has one type of answer
        if int(answer_spec["code_answer_count"]) > 0:
            codes = answer_spec["code_answers"].split(",")
            if answer_count == 1:
                return [_code_answer(random.choice(codes))]
            else:
                return [_code_answer(code) for code in random.sample(codes, answer_count)]
        if int(answer_spec["decimal_answer_count"]) > 0:
            return [
                {
                    "valueDecimal": round(
                        random.uniform(
                            float(answer_spec["min_decimal_answer"]), float(answer_spec["max_decimal_answer"])
                        ),
                        1,
                    )
                }
                for _ in range(answer_count)
            ]
        if int(answer_spec["integer_answer_count"]) > 0:
            return [
                {
                    "valueInteger": random.randint(
                        int(answer_spec["min_integer_answer"]), int(answer_spec["max_integer_answer"])
                    )
                }
                for _ in range(answer_count)
            ]
        if int(answer_spec["date_answer_count"]) > 0:
            min_date = parse(answer_spec["min_date_answer"])
            max_date = parse(answer_spec["max_date_answer"])
            days_diff = (max_date - min_date).days
            return [
                {"valueDate": (min_date + datetime.timedelta(days=random.randint(0, days_diff))).isoformat()}
                for _ in range(answer_count)
            ]
        if int(answer_spec["datetime_answer_count"]) > 0:
            min_date = parse(answer_spec["min_datetime_answer"])
            max_date = parse(answer_spec["max_datetime_answer"])
            seconds_diff = (max_date - min_date).total_seconds()
            return [
                {"valueDateTime": (min_date + datetime.timedelta(seconds=random.randint(0, seconds_diff))).isoformat()}
                for _ in range(answer_count)
            ]
        if int(answer_spec["boolean_answer_count"]) > 0:
            return [{"valueBoolean": random.random() < 0.5}]
        if int(answer_spec["string_answer_count"]) > 0:
            return [
                {"valueString": "".join([random.choice(string.ascii_lowercase) for _ in range(20)])}
                for _ in range(answer_count)
            ]
        if int(answer_spec["uri_answer_count"]) > 0:
            return [
                {
                    "valueUri": "gs://notarealbucket.example.com/%s"
                    % "".join([random.choice(string.ascii_lowercase) for _ in range(20)])
                }
                for _ in range(answer_count)
            ]
        logging.warning("No answer type found for %s, skipping..." % answer_spec["question_code"])
        return None

    def _choose_answers_for_other_questions(self, answer_map):
        for question_code, answer_spec in list(self._answer_specs.items()):
            num_participants = int(answer_spec["num_participants"])
            # Skip answering this question based on the percentage of participants that answered it.
            if random.random() > float(num_participants) / float(self._answer_specs_max_participants):
                continue
            num_questionnaire_responses = int(answer_spec["num_questionnaire_responses"])
            num_answers = int(answer_spec["num_answers"])
            answer_count = 1
            if num_answers > num_questionnaire_responses:
                rand_val = random.random() * num_answers / num_questionnaire_responses
                if rand_val > 1:
                    # Set the answer count to 2 or more
                    answer_count = int(1 + max(1, rand_val))
            answer_map[question_code] = self._choose_answer_for_spec(answer_spec, answer_count)

    def _make_answer_map(self, california_hpo):
        answer_map = {}
        answer_map[RACE_QUESTION_CODE] = self._choose_answer_codes(
            RACE_QUESTION_CODE, _MULTIPLE_RACE_ANSWERS, _MAX_RACE_ANSWERS
        )

        street_parts = [None, None]
        street = self._choose_street_address()
        if street:
            street_parts = street.split("|")
        answer_map[STREET_ADDRESS_QUESTION_CODE] = _string_answer(street_parts[0])
        answer_map[STREET_ADDRESS2_QUESTION_CODE] = _string_answer("" if len(street_parts) < 2 else street_parts[1])
        answer_map[CITY_QUESTION_CODE] = _string_answer(self._choose_city())
        answer_map[PHONE_NUMBER_QUESTION_CODE] = _string_answer(self._choose_phone_number())
        for question_code in [
            GENDER_IDENTITY_QUESTION_CODE,
            RECONTACT_METHOD_QUESTION_CODE,
            LANGUAGE_QUESTION_CODE,
            SEX_QUESTION_CODE,
            SEXUAL_ORIENTATION_QUESTION_CODE,
            EDUCATION_QUESTION_CODE,
            INCOME_QUESTION_CODE,
        ]:
            answer_map[question_code] = self._choose_answer_code(question_code)

        self._choose_state_and_zip(answer_map)
        self._choose_name(answer_map)
        if california_hpo:
            answer_map[CABOR_SIGNATURE_QUESTION_CODE] = _string_answer("signature")
        self._choose_date_of_birth(answer_map)
        self._choose_answers_for_other_questions(answer_map)
        return answer_map

    def _submit_questionnaire_responses(self, participant_id, california_hpo, start_time, force_measurement=False):
        """We may want to ignore failures in some instances. Such as when running
    add_pm_and_biospecimens_to_participants. In this instance the existing test participant may
    have been withdrawn and we don't want the script to fail on a large data set for that case."""
        ignore_failure = force_measurement
        if not force_measurement and random.random() <= _NO_QUESTIONNAIRES_SUBMITTED:
            return None, None, None
        submission_time = start_time
        answer_map = self._make_answer_map(california_hpo)

        delta = datetime.timedelta(days=random.randint(0, _MAX_DAYS_BETWEEN_SUBMISSIONS))
        submission_time = submission_time + delta
        consent_time = submission_time
        # Submit the consent questionnaire always and other questionnaires at random.
        questions = self._questionnaire_to_questions[self._consent_questionnaire_id_and_version]
        self._submit_questionnaire_response(
            participant_id,
            self._consent_questionnaire_id_and_version,
            questions,
            submission_time,
            answer_map,
            ignore_failure,
        )

        the_basics_submission_time = None
        for questionnaire_id_and_version, questions in list(self._questionnaire_to_questions.items()):
            if questionnaire_id_and_version != self._consent_questionnaire_id_and_version and (
                random.random() > _QUESTIONNAIRE_NOT_SUBMITTED or force_measurement
            ):

                delta = datetime.timedelta(days=random.randint(0, _MAX_DAYS_BETWEEN_SUBMISSIONS))
                submission_time = submission_time + delta
                self._submit_questionnaire_response(
                    participant_id,
                    questionnaire_id_and_version,
                    questions,
                    submission_time,
                    answer_map,
                    ignore_failure,
                )
                if questionnaire_id_and_version == self._the_basics_questionnaire_id_and_version:
                    the_basics_submission_time = submission_time
                elif force_measurement:
                    the_basics_submission_time = start_time
        if submission_time is None and force_measurement:
            submission_time = self._now
        return consent_time, submission_time, the_basics_submission_time

    def _create_question_answer(self, link_id, answers):
        return {"linkId": link_id, "answer": answers}

    def _submit_questionnaire_response(
        self, participant_id, q_id_and_version, questions, submission_time, answer_map, ignore_failure=False
    ):
        questions_with_answers = []
        for question_code, link_id in questions:
            answer = answer_map.get(question_code)
            if answer:
                questions_with_answers.append(self._create_question_answer(link_id, answer))
        qr_json = self._create_questionnaire_response(participant_id, q_id_and_version, questions_with_answers)
        # Generate an authored time that has a nine in ten chance of being different than the
        # create datetime value.
        delta = datetime.timedelta(seconds=random.randint(0, 9) * random.randint(1, 30))
        qr_json["authored"] = (submission_time - delta).isoformat()

        extensions = list()
        extensions.append(
            {
                "url": "http://hl7.org/fhir/StructureDefinition/iso21090-ST-language",
                "valueCode": random.choice(["en", "en", "en", "en", "en", "es"]),
            }
        )
        qr_json["extension"] = extensions

        try:
            self._client.request_json(
                _questionnaire_response_url(participant_id), method="POST", body=qr_json, pretend_date=submission_time
            )
        except RuntimeError:
            logging.warn("Questionnaire not submitted for participant %s", participant_id)
            if not ignore_failure:
                raise

    def _create_questionnaire_response(self, participant_id, q_id_and_version, questions_with_answers):
        qr_json = {
            "resourceType": "QuestionnaireResponse",
            "status": "completed",
            "subject": {"reference": "Patient/%s" % participant_id},
            "questionnaire": {
                "reference": "Questionnaire/%d/_history/%s" % (q_id_and_version[0], q_id_and_version[1])
            },
            "group": {},
        }
        if questions_with_answers:
            qr_json["group"]["question"] = questions_with_answers
        return qr_json


def _questionnaire_response_url(participant_id):
    return "Participant/%s/QuestionnaireResponse" % participant_id


def _biobank_order_url(participant_id):
    return "Participant/%s/BiobankOrder" % participant_id


def _physical_measurements_url(participant_id):
    return "Participant/%s/PhysicalMeasurements" % participant_id


def _participant_url(participant_id):
    return "Participant/%s" % participant_id


def _string_answer(value):
    return [{"valueString": value}]


def _code_answer(code):
    return {"valueCoding": {"system": PPI_SYSTEM, "code": code}}
