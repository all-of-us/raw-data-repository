import json
import logging
import os
import re
import copy
from datetime import datetime
from dateutil import parser
from hashlib import md5
import pytz
from typing import Dict, List, Optional

from sqlalchemy import and_, or_
from sqlalchemy.orm import aliased, joinedload, Session, subqueryload
from werkzeug.exceptions import BadRequest

from rdr_service import singletons
from rdr_service.api_util import dispatch_task
from rdr_service.dao.database_utils import format_datetime, parse_datetime
from rdr_service.lib_fhir.fhirclient_1_0_6.models import questionnaireresponse as fhir_questionnaireresponse
from rdr_service.participant_enums import QuestionnaireResponseStatus, PARTICIPANT_COHORT_2_START_TIME,\
    PARTICIPANT_COHORT_3_START_TIME
from rdr_service.app_util import get_account_origin_id, is_self_request
from rdr_service import storage
from rdr_service import clock, code_constants, config
from rdr_service.code_constants import (
    CABOR_SIGNATURE_QUESTION_CODE,
    CONSENT_COHORT_GROUP_CODE,
    CONSENT_FOR_DVEHR_MODULE,
    CONSENT_FOR_GENOMICS_ROR_MODULE,
    CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE,
    CONSENT_FOR_STUDY_ENROLLMENT_MODULE,
    CONSENT_PERMISSION_YES_CODE,
    SENSITIVE_EHR_YES,
    DATE_OF_BIRTH_QUESTION_CODE,
    DVEHRSHARING_CONSENT_CODE_NOT_SURE,
    DVEHRSHARING_CONSENT_CODE_YES,
    DVEHR_SHARING_QUESTION_CODE,
    EHR_CONSENT_QUESTION_CODE,
    EHR_SENSITIVE_CONSENT_QUESTION_CODE,
    EHR_CONSENT_EXPIRED_QUESTION_CODE,
    GENDER_IDENTITY_QUESTION_CODE,
    LANGUAGE_OF_CONSENT,
    PMI_SKIP_CODE,
    PPI_EXTRA_SYSTEM,
    PPI_SYSTEM,
    RACE_QUESTION_CODE,
    CONSENT_GROR_YES_CODE,
    CONSENT_GROR_NO_CODE,
    CONSENT_GROR_NOT_SURE,
    GROR_CONSENT_QUESTION_CODE,
    CONSENT_COPE_YES_CODE,
    CONSENT_COPE_NO_CODE,
    CONSENT_COPE_DEFERRED_CODE,
    COPE_CONSENT_QUESTION_CODE,
    WEAR_CONSENT_QUESTION_CODE,
    WEAR_YES_ANSWER_CODE,
    STREET_ADDRESS_QUESTION_CODE,
    STREET_ADDRESS2_QUESTION_CODE,
    EHR_CONSENT_EXPIRED_YES,
    PRIMARY_CONSENT_UPDATE_QUESTION_CODE,
    COHORT_1_REVIEW_CONSENT_YES_CODE,
    COPE_VACCINE_MINUTE_1_MODULE_CODE,
    COPE_VACCINE_MINUTE_2_MODULE_CODE,
    COPE_VACCINE_MINUTE_3_MODULE_CODE,
    COPE_VACCINE_MINUTE_4_MODULE_CODE,
    APPLE_EHR_SHARING_MODULE,
    APPLE_EHR_STOP_SHARING_MODULE,
    APPLE_HEALTH_KIT_SHARING_MODULE,
    APPLE_HEALTH_KIT_STOP_SHARING_MODULE,
    FITBIT_SHARING_MODULE,
    FITBIT_STOP_SHARING_MODULE,
    THE_BASICS_PPI_MODULE,
    BASICS_PROFILE_UPDATE_QUESTION_CODES,
    REMOTE_PM_MODULE,
    REMOTE_PM_UNIT,
    MEASUREMENT_SYS,
    VA_PRIMARY_RECONSENT_C1_C2_QUESTION,
    VA_PRIMARY_RECONSENT_C3_QUESTION,
    VA_EHR_RECONSENT,
    NON_VA_PRIMARY_RECONSENT_QUESTION,
    VA_EHR_RECONSENT_QUESTION_CODE,
    AGREE_YES,
    AGREE_NO,
    ETM_CONSENT_QUESTION_CODE,
    ETM_YES_ANSWER_CODE,
    ETM_NO_ANSWER_CODE
)
from rdr_service.dao.base_dao import BaseDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.dao.participant_summary_dao import (
    ParticipantGenderAnswersDao,
    ParticipantRaceAnswersDao,
    ParticipantSummaryDao,
)
from rdr_service.model.log_position import LogPosition
from rdr_service.dao.questionnaire_dao import QuestionnaireHistoryDao, QuestionnaireQuestionDao
from rdr_service.field_mappings import FieldType, QUESTIONNAIRE_MODULE_CODE_TO_FIELD, QUESTION_CODE_TO_FIELD, \
    QUESTIONNAIRE_ON_DIGITAL_HEALTH_SHARING_FIELD
from rdr_service.model.code import Code, CodeType
from rdr_service.model.consent_response import ConsentResponse, ConsentType
from rdr_service.model.measurements import PhysicalMeasurements, Measurement
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireHistory, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer,\
    QuestionnaireResponseExtension, QuestionnaireResponseClassificationType
from rdr_service.model.survey import Survey, SurveyQuestion, SurveyQuestionOption, SurveyQuestionType
from rdr_service.participant_enums import (
    QuestionnaireDefinitionStatus,
    QuestionnaireStatus,
    TEST_LOGIN_PHONE_NUMBER_PREFIX,
    get_gender_identity,
    get_race,
    ParticipantCohort,
    ConsentExpireStatus,
    OriginMeasurementUnit,
    PhysicalMeasurementsCollectType
)

_QUESTIONNAIRE_PREFIX = "Questionnaire/"
_QUESTIONNAIRE_HISTORY_SEGMENT = "/_history/"
_QUESTIONNAIRE_REFERENCE_FORMAT = _QUESTIONNAIRE_PREFIX + "{}" + _QUESTIONNAIRE_HISTORY_SEGMENT + "{}"

_SIGNED_CONSENT_EXTENSION = "http://terminology.pmi-ops.org/StructureDefinition/consent-form-signed-pdf"

_LANGUAGE_EXTENSION = "http://hl7.org/fhir/StructureDefinition/iso21090-ST-language"

_CATI_EXTENSION = "http://all-of-us.org/fhir/forms/non-participant-author"


def count_completed_baseline_ppi_modules(participant_summary):
    baseline_ppi_module_fields = config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS, [])
    return sum(
        1
        for field in baseline_ppi_module_fields
        if getattr(participant_summary, field) == QuestionnaireStatus.SUBMITTED
    )


def count_completed_ppi_modules(participant_summary):
    ppi_module_fields = config.getSettingList(config.PPI_QUESTIONNAIRE_FIELDS, [])
    return sum(
        1 for field in ppi_module_fields if getattr(participant_summary, field, None) == QuestionnaireStatus.SUBMITTED
    )


def get_first_completed_baseline_time(participant_summary):
    baseline_authored = getattr(participant_summary, 'baselineQuestionnairesFirstCompleteAuthored')
    if baseline_authored:
        return baseline_authored
    baseline_ppi_module_fields = config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS, [])
    baseline_time = datetime(1000, 1, 1)
    for field in baseline_ppi_module_fields:
        field_value = getattr(participant_summary, field + "Authored")
        if not field_value:
            return None
        else:
            if field_value > baseline_time:
                baseline_time = field_value
    return baseline_time


class ResponseValidator:
    def __init__(self, questionnaire_history: QuestionnaireHistory, session):
        self.session = session
        self._questionnaire_question_map = self._build_question_id_map(questionnaire_history)

        self.survey = self._get_survey_for_questionnaire_history(questionnaire_history)
        if self.survey is not None:
            self._code_to_question_map = self._build_code_to_question_map()
            if self.survey.redcapProjectId is not None:
                logging.info('Validating imported survey')

        # Get the skip code id
        self.skip_code_id = self.session.query(Code.codeId).filter(Code.value == PMI_SKIP_CODE).scalar()
        if self.skip_code_id is None:
            logging.error('Unable to load PMI_SKIP code')

    def _get_survey_for_questionnaire_history(self, questionnaire_history: QuestionnaireHistory):
        survey_query = self.session.query(Survey).filter(
            Survey.codeId.in_([concept.codeId for concept in questionnaire_history.concepts]),
            Survey.importTime < questionnaire_history.created,
            or_(
                Survey.replacedTime.is_(None),
                Survey.replacedTime > questionnaire_history.created
            )
        ).options(
            joinedload(Survey.questions).joinedload(SurveyQuestion.options).joinedload(SurveyQuestionOption.code)
        )
        num_surveys_found = survey_query.count()
        if num_surveys_found == 0:
            logging.warning(
                f'No survey definition found for questionnaire id "{questionnaire_history.questionnaireId}" '
                f'version "{questionnaire_history.version}"'
            )
        elif num_surveys_found > 1:
            logging.warning(
                f'Multiple survey definitions found for questionnaire id "{questionnaire_history.questionnaireId}" '
                f'version "{questionnaire_history.version}"'
            )
        return survey_query.first()

    def _build_code_to_question_map(self) -> Dict[int, SurveyQuestion]:
        return {survey_question.code.codeId: survey_question for survey_question in self.survey.questions}

    @classmethod
    def _build_question_id_map(cls, questionnaire_history: QuestionnaireHistory) -> Dict[int, QuestionnaireQuestion]:
        return {question.questionnaireQuestionId: question for question in questionnaire_history.questions}

    @classmethod
    def _validate_min_max(cls, answer, min_str, max_str, parser_function, question_code):
        try:
            if min_str:
                min_parsed = parser_function(min_str)
                if answer < min_parsed:
                    logging.warning(
                        f'Given answer "{answer}" is less than expected min "{min_str}" for question {question_code}'
                    )
            if max_str:
                max_parsed = parser_function(max_str)
                if answer > max_parsed:
                    logging.warning(
                        f'Given answer "{answer}" is greater than expected max "{max_str}" for question {question_code}'
                    )
        except (parser.ParserError, ValueError):
            logging.error(f'Unable to parse validation string for question {question_code}', exc_info=True)

    def _check_answer_has_expected_data_type(self, answer: QuestionnaireResponseAnswer,
                                             question_definition: SurveyQuestion,
                                             questionnaire_question: QuestionnaireQuestion):
        question_code_value = questionnaire_question.code.value

        if answer.valueCodeId == self.skip_code_id:
            # Any questions can be answered with a skip, there's isn't anything to check in that case
            return

        if question_definition.questionType in (SurveyQuestionType.UNKNOWN,
                                                SurveyQuestionType.DROPDOWN,
                                                SurveyQuestionType.RADIO,
                                                SurveyQuestionType.CHECKBOX):
            number_of_selectable_options = len(question_definition.options)
            if number_of_selectable_options == 0 and answer.valueCodeId is not None:
                logging.warning(
                    f'Answer for {question_code_value} gives a value code id when no options are defined'
                )
            elif number_of_selectable_options > 0:
                if answer.valueCodeId is None:
                    logging.warning(
                        f'Answer for {question_code_value} gives no value code id when the question has options defined'
                    )
                elif answer.valueCodeId not in [option.codeId for option in question_definition.options]:
                    logging.warning(f'Code ID {answer.valueCodeId} is an invalid answer to {question_code_value}')

        elif question_definition.questionType in (SurveyQuestionType.TEXT, SurveyQuestionType.NOTES):
            if question_definition.validation is None and answer.valueString is None:
                logging.warning(f'No valueString answer given for text-based question {question_code_value}')
            elif question_definition.validation is not None and question_definition.validation != '':
                if question_definition.validation.startswith('date'):
                    if answer.valueDate is None:
                        logging.warning(f'No valueDate answer given for date-based question {question_code_value}')
                    else:
                        self._validate_min_max(
                            answer.valueDate,
                            question_definition.validation_min,
                            question_definition.validation_max,
                            lambda validation_str: parser.parse(validation_str).date(),
                            question_code_value
                        )
                elif question_definition.validation == 'integer':
                    if answer.valueInteger is None:
                        logging.warning(
                            f'No valueInteger answer given for integer-based question {question_code_value}'
                        )
                    else:
                        self._validate_min_max(
                            answer.valueInteger,
                            question_definition.validation_min,
                            question_definition.validation_max,
                            int,
                            question_code_value
                        )
                else:
                    logging.warning(
                        f'Unrecognized validation string "{question_definition.validation}" '
                        f'for question {question_code_value}'
                    )
        else:
            # There aren't alot of surveys in redcap right now, so it's unclear how
            # some of the other types would be answered
            logging.warning(f'No validation check implemented for answer to {question_code_value} '
                            f'with question type {question_definition.questionType}')

    def check_response(self, response: QuestionnaireResponse):
        if self.survey is None:
            return None

        question_codes_answered = set()
        for answer in response.answers:
            questionnaire_question = self._questionnaire_question_map.get(answer.questionId)
            if questionnaire_question is None:
                # This is less validation, and more getting the object that should ideally already be linked
                logging.error(f'Unable to find question {answer.questionId} in questionnaire history')
            else:
                survey_question = self._code_to_question_map.get(questionnaire_question.codeId)
                if not survey_question:
                    logging.error(f'Question code used by the answer to question {answer.questionId} does not match a '
                                  f'code found on the survey definition')
                else:
                    self._check_answer_has_expected_data_type(answer, survey_question, questionnaire_question)

                    if survey_question.codeId in question_codes_answered:
                        logging.error(f'Too many answers given for {survey_question.code.value}')
                    elif survey_question.questionType != SurveyQuestionType.CHECKBOX:
                        if not (
                            survey_question.questionType == SurveyQuestionType.UNKNOWN and len(survey_question.options)
                        ):  # UNKNOWN question types could be for a Checkbox, so multiple answers should be allowed
                            question_codes_answered.add(survey_question.codeId)


class QuestionnaireResponseDao(BaseDao):
    def __init__(self):
        super(QuestionnaireResponseDao, self).__init__(QuestionnaireResponse)
        # DA-2419:  For classifying TheBasics payloads as they are received.  Cache TTL set to 24 hours
        self.thebasics_profile_update_codes = singletons.get(singletons.BASICS_PROFILE_UPDATE_CODES_CACHE_INDEX,
                                                             lambda: self._load_thebasics_profile_update_codes(),
                                                             cache_ttl_seconds=86400
                                                             )

        # Need to record what types of consents are provided by the response when walking the answers
        self.consents_provided = []

    @staticmethod
    def _load_thebasics_profile_update_codes():
        """
        Invoked when the singleton cache needs to load the list of TheBasics profile update codes
        :return:  List of code id values for the profile update / secondary contact questions
        """
        results = []
        code_dao = CodeDao()
        for code_value in BASICS_PROFILE_UPDATE_QUESTION_CODES:
            code = code_dao.get_code(PPI_SYSTEM, code_value)
            if code:
                results.append(code.codeId)

        return results

    def get_id(self, obj):
        return obj.questionnaireResponseId

    def get_with_session(self, session, obj_id, **kwargs):
        result = super(QuestionnaireResponseDao, self).get_with_session(session, obj_id, **kwargs)
        if result:
            ParticipantDao().validate_participant_reference(session, result)
        return result

    def get_with_children(self, questionnaire_response_id):
        with self.session() as session:
            query = session.query(QuestionnaireResponse).options(subqueryload(QuestionnaireResponse.answers))
            result = query.get(questionnaire_response_id)
            if result:
                ParticipantDao().validate_participant_reference(session, result)
            return result

    def _validate_model(self, session, obj):  # pylint: disable=unused-argument
        if not obj.questionnaireId:
            raise BadRequest("QuestionnaireResponse.questionnaireId is required.")
        if not obj.questionnaireVersion:
            raise BadRequest("QuestionnaireResponse.questionnaireVersion is required.")
        if not obj.answers:
            logging.error("QuestionnaireResponse model has no answers. This is harmless but probably an error.")

    def _validate_link_ids_from_resource_json_group(self, resource, link_ids):
        """
    Look for question sections and validate the linkid in each answer. If there is a response
    answer link id that does not exist in the questionnaire, then log a message. In
    the future this may be changed to raising an exception.
    This is a recursive function because answer groups can be nested.
    :param resource: A group section of the response json.
    :param link_ids: List of link ids to validate against.
    """
        # note: resource can be either a dict or a list.
        # if this is a dict and 'group' is found, always call ourselves.
        if "group" in resource:
            self._validate_link_ids_from_resource_json_group(resource["group"], link_ids)

        if "question" not in resource and isinstance(resource, list):
            for item in resource:
                self._validate_link_ids_from_resource_json_group(item, link_ids)

        # once we have a question section, iterate through list of answers.
        if "question" in resource:
            for section in resource["question"]:
                link_id = section.get('linkId', None)
                # Do not log warning or raise exception when link id is 'ignoreThis' for unit tests.
                if (
                    link_id is not None
                    and link_id.lower() != "ignorethis"
                    and link_id not in link_ids
                ):
                    # The link_ids list being checked is a list of questions that have been answered,
                    #  the list doesn't include valid link_ids that don't have answers
                    if "answer" in section:
                        logging.error(f'Questionnaire response contains invalid link ID "{link_id}"')

    @staticmethod
    def _get_module_name(questionnaire_history: QuestionnaireHistory):
        """ Use the questionnaire_history to determine the module name """
        # Unittest/lower environments may not have expected questionnaire_history content, so allow for missing data
        result = None
        if isinstance(questionnaire_history, QuestionnaireHistory):
            concepts = questionnaire_history.concepts
            if concepts:
                concept_code = concepts[0].codeId
                code_obj = CodeDao().get(concept_code)
                result = code_obj.value if code_obj else None
        else:
            logging.error(f'Unexpected questionnaire_history parameter type {type(questionnaire_history)}')

        return result

    @staticmethod
    def _imply_street_address_2_from_street_address_1(code_ids):
        code_dao = CodeDao()
        street_address_1_code = code_dao.get_code(PPI_SYSTEM, STREET_ADDRESS_QUESTION_CODE)
        if street_address_1_code and street_address_1_code.codeId in code_ids:
            street_address_2_code = code_dao.get_code(PPI_SYSTEM, STREET_ADDRESS2_QUESTION_CODE)
            if street_address_2_code and street_address_2_code.codeId not in code_ids:
                code_ids.append(street_address_2_code.codeId)

    def insert_with_session(self, session, questionnaire_response):

        # Look for a questionnaire that matches any of the questionnaire history records.
        questionnaire_history = QuestionnaireHistoryDao().get_with_children_with_session(
            session, [questionnaire_response.questionnaireId, questionnaire_response.questionnaireSemanticVersion]
        )

        if not questionnaire_history:
            raise BadRequest(
                f"Questionnaire with ID {questionnaire_response.questionnaireId}, \
                semantic version {questionnaire_response.questionnaireSemanticVersion} is not found"
            )

        try:
            answer_validator = ResponseValidator(questionnaire_history, session)
            answer_validator.check_response(questionnaire_response)
        except (AttributeError, ValueError, TypeError, LookupError):
            logging.error('Code error encountered when validating the response', exc_info=True)

        module = self._get_module_name(questionnaire_history)
        questionnaire_response.created = clock.CLOCK.now()
        questionnaire_response.classificationType = QuestionnaireResponseClassificationType.COMPLETE  # Default
        if not questionnaire_response.authored:
            questionnaire_response.authored = questionnaire_response.created

        # Put the ID into the resource.
        resource_json = json.loads(questionnaire_response.resource)
        resource_json["id"] = str(questionnaire_response.questionnaireResponseId)
        questionnaire_response.resource = json.dumps(resource_json)
        super().validate_origin(questionnaire_response)

        # Gather the question ids and records that match the questions in the response
        question_ids = [answer.questionId for answer in questionnaire_response.answers]
        questions = QuestionnaireQuestionDao().get_all_with_session(session, question_ids)

        # DA-623: raise error when response link ids do not match our question link ids.
        # Gather the valid link ids for this question
        link_ids = [question.linkId for question in questions]
        # look through the response and verify each link id is valid for each question.
        self._validate_link_ids_from_resource_json_group(resource_json, link_ids)

        code_ids = [question.codeId for question in questions]
        self._imply_street_address_2_from_street_address_1(code_ids)
        current_answers = QuestionnaireResponseAnswerDao().get_current_answers_for_concepts(
            session, questionnaire_response.participantId, code_ids
        )

        # DA-2419: participant_summary update will not be triggered by a TheBasics response if it only contains
        # profile update data (not a full survey). PTSC may eventually start marking TheBasics profile update
        # payloads with in-progress FHIR status;  for now, must inspect the response content/code ids to determine
        if (module == THE_BASICS_PPI_MODULE and
                    (questionnaire_response.status == QuestionnaireResponseStatus.IN_PROGRESS or
                     all(c in self.thebasics_profile_update_codes for c in code_ids))
        ):
            questionnaire_response.classificationType = QuestionnaireResponseClassificationType.PROFILE_UPDATE
        # in-progress status can also denote other partial/incomplete payloads, such as incomplete COPE survey responses
        elif questionnaire_response.status == QuestionnaireResponseStatus.IN_PROGRESS:
            questionnaire_response.classificationType = QuestionnaireResponseClassificationType.PARTIAL

        # TODO:  If we later classify ConsentPII payloads that contain updates to participant's own profile data
        #  (name, address, email, etc.) as PROFILE_UPDATE (vs. only classifying secondary contact profile updates via
        #  TheBasics), then this check must allow those ConsentPII responses trigger _update_participant_summary()
        if (questionnaire_response.status == QuestionnaireResponseStatus.COMPLETED and
               questionnaire_response.classificationType == QuestionnaireResponseClassificationType.COMPLETE):
            with self.session() as new_session:
                self._update_participant_summary(
                    new_session, questionnaire_response, code_ids, questions, questionnaire_history, resource_json
                )

        self.create_consent_responses(
            questionnaire_response=questionnaire_response,
            session=session
        )

        super(QuestionnaireResponseDao, self).insert_with_session(session, questionnaire_response)
        # Mark existing answers for the questions in this response given previously by this participant
        # as ended.
        for answer in current_answers:
            answer.endTime = questionnaire_response.created
            session.merge(answer)

        summary = ParticipantSummaryDao().get_for_update(session, questionnaire_response.participantId)
        ParticipantSummaryDao().update_enrollment_status(summary, session=session)

        return questionnaire_response

    def _get_field_value(self, field_type, answer):
        if field_type == FieldType.CODE:
            return answer.valueCodeId
        if field_type == FieldType.STRING:
            return answer.valueString
        if field_type == FieldType.DATE:
            return answer.valueDate
        raise BadRequest(f"Don't know how to map field of type {field_type}")

    def _update_field(self, participant_summary, field_name, field_type, answer):
        value = getattr(participant_summary, field_name)
        new_value = self._get_field_value(field_type, answer)
        if new_value is not None and value != new_value:
            setattr(participant_summary, field_name, new_value)
            return True
        return False

    @staticmethod
    def _find_cope_month(questionnaire_history: QuestionnaireHistory, response_authored_date):
        cope_form_id_map = config.getSettingJson(config.COPE_FORM_ID_MAP)
        for form_ids_str, month_name in cope_form_id_map.items():
            if questionnaire_history.externalId in form_ids_str.split(','):
                return month_name

        # If the questionnaire identifier isn't in the COPE map then using response authored date as a fallback
        logging.error('Unrecognized identifier for COPE survey response '
                      f'(questionnaire_id: "{questionnaire_history.questionnaireId}", '
                      f'version: "{questionnaire_history.version}", identifier: "{questionnaire_history.externalId}"')

        if response_authored_date < datetime(2020, 6, 4):
            return 'May'
        elif response_authored_date < datetime(2020, 7, 1):
            return 'June'
        elif response_authored_date < datetime(2020, 10, 5):
            return 'July'
        elif response_authored_date < datetime(2020, 12, 5):  # Nov scheduled to close on Dec 3rd
            return 'Nov'
        elif response_authored_date < datetime(2021, 2, 8):  # Feb scheduled to open on Feb 9th
            return 'Dec'
        else:
            return 'Feb'

    def _add_physical_measurement(self, questionnaire_response):
        with self.session() as session:
            questionnaire_history = QuestionnaireHistoryDao().get_with_children_with_session(
                session, [questionnaire_response.questionnaireId, questionnaire_response.questionnaireSemanticVersion]
            )

            module = self._get_module_name(questionnaire_history)
            if module != REMOTE_PM_MODULE:
                return

            question_ids = [answer.questionId for answer in questionnaire_response.answers]
            questions = QuestionnaireQuestionDao().get_all_with_session(session, question_ids)
            code_ids = [question.codeId for question in questions]

        code_dao = CodeDao()
        pm_unite_code = code_dao.get_code(PPI_SYSTEM, REMOTE_PM_UNIT)
        if not pm_unite_code:
            raise BadRequest("No measurement unit code found in code table")
        if pm_unite_code.codeId not in code_ids:
            raise BadRequest(
                f"Can't update physical measurement data for participant {questionnaire_response.participantId}, "
                f"no measurement unit answer found"
            )

        participant_id = questionnaire_response.participantId
        authored = questionnaire_response.authored.replace(tzinfo=None)
        pm_dao = PhysicalMeasurementsDao()
        exist_pm = pm_dao.get_exist_remote_pm(participant_id, authored)
        if exist_pm:
            logging.info(f'Remote physical measurement for pid {participant_id} finalized at '
                         f'{str(authored)} already exist')
            return

        origin_measurement_unit = OriginMeasurementUnit.UNSET
        self_reported_int_value_map = {
            'self_reported_height_ft': None,
            'self_reported_height_in': None,
            'self_reported_weight_pounds': None,
            'self_reported_height_cm': None,
            'self_reported_weight_kg': None
        }

        codes = code_dao.get_with_ids(code_ids)
        code_map = {code.codeId: code for code in codes if code.system == PPI_SYSTEM}
        question_map = {question.questionnaireQuestionId: question for question in questions}
        for answer in questionnaire_response.answers:
            question = question_map.get(answer.questionId)
            if question:
                question_code = code_map.get(question.codeId)
                if question_code:
                    if question_code.value == REMOTE_PM_UNIT:
                        answer_code_value = code_dao.get(answer.valueCodeId).value
                        if answer_code_value == 'pm_1':
                            origin_measurement_unit = OriginMeasurementUnit.IMPERIAL
                        elif answer_code_value == 'pm_2':
                            origin_measurement_unit = OriginMeasurementUnit.METRIC
                        else:
                            raise BadRequest(f'unknown measurement unit {answer_code_value} for participant '
                                             f'{participant_id}')
                    elif (
                        question_code.value in['self_reported_height_ft', 'self_reported_height_in',
                                               'self_reported_height_cm']
                        and answer.valueInteger is not None
                    ):
                        self_reported_int_value_map[question_code.value] = round(answer.valueInteger, 1)
                    elif (
                        question_code.value in ['self_reported_weight_pounds', 'self_reported_weight_kg']
                        and answer.valueString is not None
                    ):
                        self_reported_int_value_map[question_code.value] = round(float(answer.valueString), 1)

        if origin_measurement_unit == OriginMeasurementUnit.IMPERIAL:
            # convert to METRIC
            height_ft = self_reported_int_value_map.get('self_reported_height_ft')
            height_in = self_reported_int_value_map.get('self_reported_height_in')
            height_cm_decimal = None
            if not (height_ft is None or height_in is None):
                height_cm_decimal = round((height_ft * 30.48) + (height_in * 2.54), 1)

            weight_pounds = self_reported_int_value_map.get('self_reported_weight_pounds')
            weight_kg_decimal = None
            if weight_pounds is not None:
                weight_kg_decimal = round(weight_pounds * 0.453592, 1)
        else:
            height_cm_decimal = self_reported_int_value_map.get('self_reported_height_cm')
            weight_kg_decimal = self_reported_int_value_map.get('self_reported_weight_kg')

        measurements = []
        if height_cm_decimal is not None:
            measurements.append(
                Measurement(
                    codeSystem=MEASUREMENT_SYS,
                    codeValue='height',
                    measurementTime=authored,
                    valueDecimal=height_cm_decimal,
                    valueUnit='cm',
                )
            )
        if weight_kg_decimal is not None:
            measurements.append(
                Measurement(
                    codeSystem=MEASUREMENT_SYS,
                    codeValue='weight',
                    measurementTime=authored,
                    valueDecimal=weight_kg_decimal,
                    valueUnit='kg',
                )
            )

        pm = PhysicalMeasurements(
            participantId=participant_id,
            created=clock.CLOCK.now(),
            final=True,
            logPosition=LogPosition(),
            finalized=authored,
            measurements=measurements,
            origin='vibrent',
            collectType=PhysicalMeasurementsCollectType.SELF_REPORTED,
            originMeasurementUnit=origin_measurement_unit,
            questionnaireResponseId=questionnaire_response.questionnaireResponseId
        )
        pm_dao.insert_remote_pm(pm)

    def _update_participant_summary(
        self, session, questionnaire_response, code_ids, questions, questionnaire_history, resource_json
    ):
        """Updates the participant summary based on questions answered and modules completed
    in the questionnaire response.

    If no participant summary exists already, only a response to the study enrollment consent
    questionnaire can be submitted, and it must include first and last name and e-mail address.
    """

        # Block on other threads modifying the participant or participant summary.
        participant = ParticipantDao().get_for_update(session, questionnaire_response.participantId)

        if participant is None:
            raise BadRequest(f"Participant with ID {questionnaire_response.participantId} is not found.")

        participant_summary = participant.participantSummary

        authored = questionnaire_response.authored
        # If authored is a datetime and has tzinfo, convert to utc and remove tzinfo.
        # The authored timestamps in the participant summary will already be in utc, but lack tzinfo.
        if authored and isinstance(authored, datetime) and authored.tzinfo:
            authored = authored.astimezone(pytz.utc).replace(tzinfo=None)

        code_ids.extend([concept.codeId for concept in questionnaire_history.concepts])

        code_dao = CodeDao()

        something_changed = False
        module_changed = False
        # If no participant summary exists, make sure this is the study enrollment consent.
        if not participant_summary:
            consent_code = code_dao.get_code(PPI_SYSTEM, CONSENT_FOR_STUDY_ENROLLMENT_MODULE)
            if not consent_code:
                raise BadRequest("No study enrollment consent code found; import codebook.")
            if not consent_code.codeId in code_ids:
                raise BadRequest(
                    f"Can't submit order for participant {questionnaire_response.participantId} without consent"
                )
            if not _validate_consent_pdfs(resource_json):
                raise BadRequest(
                    f"Unable to find signed consent-for-enrollment file for participant"
                )
            participant_summary = ParticipantDao.create_summary_for_participant(participant)
            something_changed = True

        # Fetch the codes for all questions and concepts
        codes = code_dao.get_with_ids(code_ids)

        code_map = {code.codeId: code for code in codes if code.system == PPI_SYSTEM}
        question_map = {question.questionnaireQuestionId: question for question in questions}
        race_code_ids = []
        gender_code_ids = []
        ehr_consent = False
        gror_consent = None
        dvehr_consent = QuestionnaireStatus.SUBMITTED_NO_CONSENT
        street_address_submitted = False
        street_address2_submitted = False

        rejected_reconsent = False

        # Skip updating the summary if the response being stored has an authored
        # date earlier than one that's already been recorded
        if questionnaire_history.concepts:
            concept = questionnaire_history.concepts[0]
            module_code = code_map.get(concept.codeId)
            if module_code:
                survey_name = (
                    module_code.value.lower() if self._is_digital_health_share_code(module_code.value)
                    else module_code.value
                )
                if survey_name in QUESTIONNAIRE_MODULE_CODE_TO_FIELD:
                    summary_field_name = QUESTIONNAIRE_MODULE_CODE_TO_FIELD.get(survey_name) + 'Authored'
                    existing_authored_datetime = getattr(participant_summary, summary_field_name, None)
                    if existing_authored_datetime and authored < existing_authored_datetime:
                        logging.warning(
                            f'Skipping summary update for {module_code.value} response authored on {authored} '
                            f'(previous response recorded was authored {existing_authored_datetime})'
                        )
                        return

        # Set summary fields for answers that have questions with codes found in QUESTION_CODE_TO_FIELD
        for answer in questionnaire_response.answers:
            question = question_map.get(answer.questionId)
            if question:
                code = code_map.get(question.codeId)
                if code:
                    if code.value == GENDER_IDENTITY_QUESTION_CODE:
                        gender_code_ids.append(answer.valueCodeId)
                    elif code.value == STREET_ADDRESS_QUESTION_CODE:
                        street_address_submitted = answer.valueString is not None
                    elif code.value == STREET_ADDRESS2_QUESTION_CODE:
                        street_address2_submitted = answer.valueString is not None
                    elif code.value == DATE_OF_BIRTH_QUESTION_CODE:
                        dispatch_task(
                            endpoint='check_date_of_birth',
                            payload={
                                'participant_id': participant.participantId,
                                'date_of_birth': answer.valueDate
                            }
                        )

                    summary_field = QUESTION_CODE_TO_FIELD.get(code.value)
                    if summary_field:
                        if something_changed:
                            self._update_field(participant_summary, summary_field[0], summary_field[1], answer)
                        else:
                            something_changed = self._update_field(
                                participant_summary, summary_field[0], summary_field[1], answer
                            )
                    elif code.value == RACE_QUESTION_CODE:
                        race_code_ids.append(answer.valueCodeId)

                    elif code.value == DVEHR_SHARING_QUESTION_CODE:
                        code = code_dao.get(answer.valueCodeId)
                        if code and code.value == DVEHRSHARING_CONSENT_CODE_YES:
                            dvehr_consent = QuestionnaireStatus.SUBMITTED
                        elif code and code.value == DVEHRSHARING_CONSENT_CODE_NOT_SURE:
                            dvehr_consent = QuestionnaireStatus.SUBMITTED_NOT_SURE
                    elif code.value in [EHR_CONSENT_QUESTION_CODE, EHR_SENSITIVE_CONSENT_QUESTION_CODE]:
                        code = code_dao.get(answer.valueCodeId)
                        if participant_summary.ehrConsentExpireStatus == ConsentExpireStatus.EXPIRED and \
                                authored > participant_summary.ehrConsentExpireAuthored:
                            participant_summary.ehrConsentExpireStatus = ConsentExpireStatus.UNSET
                            participant_summary.ehrConsentExpireAuthored = None
                            participant_summary.ehrConsentExpireTime = None
                        if code and code.value in [CONSENT_PERMISSION_YES_CODE, SENSITIVE_EHR_YES]:
                            self.consents_provided.append(ConsentType.EHR)
                            ehr_consent = True
                            if participant_summary.consentForElectronicHealthRecordsFirstYesAuthored is None:
                                participant_summary.consentForElectronicHealthRecordsFirstYesAuthored = authored
                            if participant_summary.ehrConsentExpireStatus == ConsentExpireStatus.EXPIRED and \
                                    authored < participant_summary.ehrConsentExpireAuthored:
                                ehr_consent = False
                    elif code.value == EHR_CONSENT_EXPIRED_QUESTION_CODE:
                        if answer.valueString and answer.valueString == EHR_CONSENT_EXPIRED_YES:
                            participant_summary.ehrConsentExpireStatus = ConsentExpireStatus.EXPIRED
                            participant_summary.ehrConsentExpireAuthored = authored
                            participant_summary.ehrConsentExpireTime = questionnaire_response.created
                            something_changed = True
                    elif code.value == CABOR_SIGNATURE_QUESTION_CODE:
                        if answer.valueUri or answer.valueString:
                            # TODO: validate the URI? [DA-326]
                            self.consents_provided.append(ConsentType.CABOR)
                            if not participant_summary.consentForCABoR:
                                participant_summary.consentForCABoR = True
                                participant_summary.consentForCABoRTime = questionnaire_response.created
                                participant_summary.consentForCABoRAuthored = authored
                                something_changed = True
                    elif code.value == GROR_CONSENT_QUESTION_CODE:
                        if code_dao.get(answer.valueCodeId).value == CONSENT_GROR_YES_CODE:
                            self.consents_provided.append(ConsentType.GROR)
                            gror_consent = QuestionnaireStatus.SUBMITTED
                        elif code_dao.get(answer.valueCodeId).value == CONSENT_GROR_NO_CODE:
                            gror_consent = QuestionnaireStatus.SUBMITTED_NO_CONSENT
                        elif code_dao.get(answer.valueCodeId).value == CONSENT_GROR_NOT_SURE:
                            gror_consent = QuestionnaireStatus.SUBMITTED_NOT_SURE
                    elif code.value == COPE_CONSENT_QUESTION_CODE:
                        answer_value = code_dao.get(answer.valueCodeId).value
                        if answer_value == CONSENT_COPE_YES_CODE:
                            submission_status = QuestionnaireStatus.SUBMITTED
                        elif answer_value in [CONSENT_COPE_NO_CODE, CONSENT_COPE_DEFERRED_CODE]:
                            submission_status = QuestionnaireStatus.SUBMITTED_NO_CONSENT
                        else:
                            submission_status = QuestionnaireStatus.SUBMITTED_INVALID

                        month_name = self._find_cope_month(questionnaire_history, authored)
                        setattr(participant_summary, f'questionnaireOnCope{month_name}', submission_status)
                        setattr(participant_summary, f'questionnaireOnCope{month_name}Time',
                                questionnaire_response.created)
                        setattr(participant_summary, f'questionnaireOnCope{month_name}Authored', authored)

                        # COPE Survey changes need to update number of modules complete in summary
                        module_changed = True
                    elif code.value == PRIMARY_CONSENT_UPDATE_QUESTION_CODE:
                        answer_value = code_dao.get(answer.valueCodeId).value
                        if answer_value == COHORT_1_REVIEW_CONSENT_YES_CODE:
                            self.consents_provided.append(ConsentType.PRIMARY_UPDATE)
                            participant_summary.consentForStudyEnrollmentAuthored = authored
                    elif code.value == CONSENT_COHORT_GROUP_CODE:
                        try:
                            cohort_group = int(answer.valueString)

                            # Only checking that we know of the cohort group so we don't crash when
                            # storing in the Enum column
                            cohort_numbers = ParticipantCohort.numbers()
                            if cohort_group not in cohort_numbers:
                                raise ValueError
                            else:
                                participant_summary.consentCohort = answer.valueString
                                something_changed = True
                        except ValueError:
                            logging.error(f'Invalid value given for cohort group: received "{answer.valueString}"')
                    elif code.value.lower() == WEAR_CONSENT_QUESTION_CODE:
                        answer_value = code_dao.get(answer.valueCodeId).value
                        if answer_value.lower() == WEAR_YES_ANSWER_CODE:
                            self.consents_provided.append(ConsentType.WEAR)
                    elif self._code_in_list(
                        code.value,
                        [
                            VA_PRIMARY_RECONSENT_C1_C2_QUESTION,
                            VA_PRIMARY_RECONSENT_C3_QUESTION,
                            NON_VA_PRIMARY_RECONSENT_QUESTION
                        ]
                    ):
                        answer_value = code_dao.get(answer.valueCodeId).value
                        if answer_value.lower() == AGREE_YES:
                            self.consents_provided.append(ConsentType.PRIMARY_RECONSENT)
                            participant_summary.reconsentForStudyEnrollmentAuthored = authored
                    elif code.value.lower() == VA_EHR_RECONSENT_QUESTION_CODE:
                        answer_value = code_dao.get(answer.valueCodeId).value
                        if answer_value.lower() == AGREE_NO:
                            rejected_reconsent = module_changed = something_changed = True
                            participant_summary.consentForElectronicHealthRecords = \
                                QuestionnaireStatus.SUBMITTED_NO_CONSENT
                            participant_summary.consentForElectronicHealthRecordsAuthored = authored
                            participant_summary.consentForElectronicHealthRecordsTime = questionnaire_response.created
                    elif code.value.lower() == ETM_CONSENT_QUESTION_CODE:
                        answer_value = code_dao.get(answer.valueCodeId).value
                        if answer_value.lower() == ETM_YES_ANSWER_CODE:
                            self.consents_provided.append(ConsentType.ETM)
                            participant_summary.consentForEtM = QuestionnaireStatus.SUBMITTED
                        elif answer_value.lower() == ETM_NO_ANSWER_CODE:
                            participant_summary.consentForEtM = QuestionnaireStatus.SUBMITTED_NO_CONSENT

                        participant_summary.consentForEtMTime = questionnaire_response.created
                        participant_summary.consentForEtMAuthored = authored

        # If the answer for line 2 of the street address was left out then it needs to be clear on summary.
        # So when it hasn't been submitted and there is something set for streetAddress2 we want to clear it out.
        summary_has_street_line_two = participant_summary.streetAddress2 is not None \
                                      and participant_summary.streetAddress2 != ""
        if street_address_submitted and not street_address2_submitted and summary_has_street_line_two:
            something_changed = True
            participant_summary.streetAddress2 = None

        # If race was provided in the response in one or more answers, set the new value.
        if race_code_ids:
            race_codes = [code_dao.get(code_id) for code_id in race_code_ids]
            race = get_race(race_codes)
            if race != participant_summary.race:
                participant_summary.race = race
                something_changed = True

        if gender_code_ids:
            gender_codes = [code_dao.get(code_id) for code_id in gender_code_ids]
            gender = get_gender_identity(gender_codes)
            if gender != participant_summary.genderIdentity:
                participant_summary.genderIdentity = gender
                something_changed = True

        dna_program_consent_update_code = config.getSettingJson(config.DNA_PROGRAM_CONSENT_UPDATE_CODE, None)

        # Set summary fields to SUBMITTED for questionnaire concepts that are found in
        # QUESTIONNAIRE_MODULE_CODE_TO_FIELD
        for concept in questionnaire_history.concepts:
            code = code_map.get(concept.codeId)
            if code:
                # the digital health code in code table is in lowercase, but in questionnaire payload is in CamelCased
                summary_field = QUESTIONNAIRE_MODULE_CODE_TO_FIELD.get(
                    code.value.lower() if self._is_digital_health_share_code(code.value) else code.value)
                if summary_field:
                    new_status = QuestionnaireStatus.SUBMITTED
                    if code.value == CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE and not ehr_consent:
                        new_status = QuestionnaireStatus.SUBMITTED_NO_CONSENT
                    elif code.value == CONSENT_FOR_DVEHR_MODULE:
                        new_status = dvehr_consent
                    elif code.value == CONSENT_FOR_GENOMICS_ROR_MODULE:
                        if gror_consent is None:
                            raise BadRequest(
                                "GROR Consent answer is required to match code {}."
                                    .format([CONSENT_GROR_YES_CODE, CONSENT_GROR_NO_CODE, CONSENT_GROR_NOT_SURE])
                            )
                        new_status = gror_consent
                    elif code.value == CONSENT_FOR_STUDY_ENROLLMENT_MODULE:
                        self.consents_provided.append(ConsentType.PRIMARY)
                        participant_summary.semanticVersionForPrimaryConsent = \
                            questionnaire_response.questionnaireSemanticVersion
                        if participant_summary.consentCohort is None or \
                                participant_summary.consentCohort == ParticipantCohort.UNSET:

                            if participant_summary.participantOrigin == 'vibrent':
                                logging.warning(f'Missing expected consent cohort information for participant '
                                                f'{participant_summary.participantId}')

                            if authored >= PARTICIPANT_COHORT_3_START_TIME:
                                participant_summary.consentCohort = ParticipantCohort.COHORT_3
                            elif PARTICIPANT_COHORT_2_START_TIME <= authored < PARTICIPANT_COHORT_3_START_TIME:
                                participant_summary.consentCohort = ParticipantCohort.COHORT_2
                            elif authored < PARTICIPANT_COHORT_2_START_TIME:
                                participant_summary.consentCohort = ParticipantCohort.COHORT_1
                        if participant_summary.consentForStudyEnrollmentFirstYesAuthored is None:
                            participant_summary.consentForStudyEnrollmentFirstYesAuthored = authored
                        # set language of consent to participant summary
                        for extension in resource_json.get("extension", []):
                            if (
                                extension.get("url") == _LANGUAGE_EXTENSION
                                and extension.get("valueCode") in LANGUAGE_OF_CONSENT
                            ):
                                if participant_summary.primaryLanguage != extension.get("valueCode"):
                                    participant_summary.primaryLanguage = extension.get("valueCode")
                                    something_changed = True
                                break
                            elif (
                                extension.get("url") == _LANGUAGE_EXTENSION
                                and extension.get("valueCode") not in LANGUAGE_OF_CONSENT
                            ):
                                logging.warning(f"consent language {extension.get('valueCode')} not recognized.")
                    elif self._is_digital_health_share_code(code.value):
                        digital_health_sharing_status, something_changed = self._update_digital_health_status_field(
                            participant_summary.digitalHealthSharingStatus, code.value.lower(), authored)
                        if something_changed:
                            setattr(participant_summary, summary_field, digital_health_sharing_status)

                    if summary_field != QUESTIONNAIRE_ON_DIGITAL_HEALTH_SHARING_FIELD \
                        and getattr(participant_summary, summary_field) != new_status:
                        setattr(participant_summary, summary_field, new_status)
                        setattr(participant_summary, summary_field + "Time", questionnaire_response.created)
                        setattr(participant_summary, summary_field + "Authored", authored)
                        something_changed = True
                        module_changed = True
                elif dna_program_consent_update_code is not None and code.value == dna_program_consent_update_code:
                    # If we receive a questionnaire response it means they've viewed the update and we should mark
                    # them as submitted
                    participant_summary.questionnaireOnDnaProgram = QuestionnaireStatus.SUBMITTED
                    participant_summary.questionnaireOnDnaProgramAuthored = authored
                # cope vaccines
                elif code.value in (
                    COPE_VACCINE_MINUTE_1_MODULE_CODE,
                    COPE_VACCINE_MINUTE_2_MODULE_CODE,
                    COPE_VACCINE_MINUTE_3_MODULE_CODE,
                    COPE_VACCINE_MINUTE_4_MODULE_CODE
                ):
                    cope_vaccine_map = {
                        COPE_VACCINE_MINUTE_1_MODULE_CODE: {
                            'submitted': 'questionnaireOnCopeVaccineMinute1',
                            'authored': 'questionnaireOnCopeVaccineMinute1Authored'
                        },
                        COPE_VACCINE_MINUTE_2_MODULE_CODE: {
                            'submitted': 'questionnaireOnCopeVaccineMinute2',
                            'authored': 'questionnaireOnCopeVaccineMinute2Authored'
                        },
                        COPE_VACCINE_MINUTE_3_MODULE_CODE: {
                            'submitted': 'questionnaireOnCopeVaccineMinute3',
                            'authored': 'questionnaireOnCopeVaccineMinute3Authored'
                        },
                        COPE_VACCINE_MINUTE_4_MODULE_CODE: {
                            'submitted': 'questionnaireOnCopeVaccineMinute4',
                            'authored': 'questionnaireOnCopeVaccineMinute4Authored'
                        }
                    }

                    module = cope_vaccine_map[code.value]
                    mod_submitted = module['submitted']
                    mod_authored = module['authored']

                    if getattr(participant_summary, mod_submitted) \
                            != QuestionnaireStatus.SUBMITTED:
                        setattr(participant_summary, mod_submitted, QuestionnaireStatus.SUBMITTED)
                        setattr(participant_summary, mod_authored, authored)
                        module_changed = True
                elif self._code_in_list(code.value, [VA_EHR_RECONSENT]) and not rejected_reconsent:
                    self.consents_provided.append(ConsentType.EHR_RECONSENT)
                    participant_summary.reconsentForElectronicHealthRecordsAuthored = authored

        if module_changed:
            participant_summary.numCompletedBaselinePPIModules = count_completed_baseline_ppi_modules(
                participant_summary
            )
            participant_summary.baselineQuestionnairesFirstCompleteAuthored = get_first_completed_baseline_time(
                participant_summary
            )
            participant_summary.numCompletedPPIModules = count_completed_ppi_modules(participant_summary)

        if something_changed:
            first_last = (participant_summary.firstName, participant_summary.lastName)
            email_phone = (participant_summary.email, participant_summary.loginPhoneNumber)
            if not all(first_last):
                raise BadRequest(
                    "First name ({:s}), and last name ({:s}) required for consenting."
                        .format(*["present" if part else "missing" for part in first_last])
                )
            if not any(email_phone):
                raise BadRequest(
                    "Email address ({:s}), or phone number ({:s}) required for consenting."
                        .format(*["present" if part else "missing" for part in email_phone])
                )

            participant_summary.lastModified = clock.CLOCK.now()
            session.merge(participant_summary)

            # switch account to test account if the phone number starts with 4442
            # this is a requirement from PTSC
            ph = getattr(participant_summary, 'loginPhoneNumber') or \
                 getattr(participant_summary, 'phoneNumber') or 'None'

            ph_clean = re.sub('[\(|\)|\-|\s]', '', ph)

            if ph_clean.startswith(TEST_LOGIN_PHONE_NUMBER_PREFIX):
                ParticipantDao().switch_to_test_account(session, participant)

            # update participant gender/race answers table
            if race_code_ids:
                participant_race_answer_dao = ParticipantRaceAnswersDao()
                participant_race_answer_dao.update_race_answers_with_session(
                    session, participant.participantId, race_code_ids
                )
            if gender_code_ids:
                participant_gender_race_dao = ParticipantGenderAnswersDao()
                participant_gender_race_dao.update_gender_answers_with_session(
                    session, participant.participantId, gender_code_ids
                )

    def create_consent_responses(self, questionnaire_response: QuestionnaireResponse, session: Session):
        """
        Analyzes the current ConsentResponses for a participant, and the response that was just received
        to determine if the new response is a new consent for the participant.
        """
        if len(self.consents_provided) == 0:
            # If the new response doesn't give any consent at all, then there's no need to validate a PDF
            return

        # Load previously received consent authored dates for the participant
        previous_consent_dates = ConsentDao.get_consent_authored_times_for_participant(
            session=session,
            participant_id=questionnaire_response.participantId
        )

        # Check authored dates to see if it's a new consent response,
        # or if it's potentially just a replay of a previous questionnaire response
        for consent_type in self.consents_provided:
            is_new_consent = True
            previous_authored_times = previous_consent_dates.get(consent_type)
            for previous_consent_authored_time in (previous_authored_times or []):
                if self._authored_times_match(
                    new_authored_time=questionnaire_response.authored,
                    current_authored_item=previous_consent_authored_time
                ):
                    is_new_consent = False
                    break

            if is_new_consent:
                session.add(ConsentResponse(response=questionnaire_response, type=consent_type))

    @classmethod
    def _authored_times_match(cls, new_authored_time: datetime, current_authored_item: datetime):
        if new_authored_time.tzinfo is None:
            new_authored_time = new_authored_time.replace(tzinfo=pytz.utc)
        if current_authored_item.tzinfo is None:
            current_authored_item = current_authored_item.replace(tzinfo=pytz.utc)
        difference_in_seconds = abs((new_authored_time - current_authored_item).total_seconds())
        return difference_in_seconds < 300  # Allowing 5 minutes of difference between authored dates

    def _is_digital_health_share_code(self, code_value):
        return code_value.lower() in [APPLE_EHR_SHARING_MODULE, APPLE_EHR_STOP_SHARING_MODULE,
                                      APPLE_HEALTH_KIT_SHARING_MODULE, APPLE_HEALTH_KIT_STOP_SHARING_MODULE,
                                      FITBIT_SHARING_MODULE, FITBIT_STOP_SHARING_MODULE]

    def _update_digital_health_status_field(self, current_value, code_value, authored):
        something_changed = False
        authored_str = format_datetime(authored)
        field_mapping = {
            APPLE_HEALTH_KIT_SHARING_MODULE: ('appleHealthKit', 'YES'),
            APPLE_HEALTH_KIT_STOP_SHARING_MODULE: ('appleHealthKit', 'NO'),
            APPLE_EHR_SHARING_MODULE: ('appleEHR', 'YES'),
            APPLE_EHR_STOP_SHARING_MODULE: ('appleEHR', 'NO'),
            FITBIT_SHARING_MODULE: ('fitbit', 'YES'),
            FITBIT_STOP_SHARING_MODULE: ('fitbit', 'NO')
        }
        current_value = current_value if current_value is not None else {}
        # sqlalchemy can't update the Json field directly, need a deepcopy to replace the old value
        new_value = copy.deepcopy(current_value)
        health_module = field_mapping[code_value][0]
        health_module_status = field_mapping[code_value][1]
        if health_module in new_value:
            current_history = new_value[health_module]['history']
            exist = False
            for item in current_history:
                if item['status'] == health_module_status and item['authoredTime'] == authored_str:
                    exist = True
                    break
            if not exist:
                if authored > parse_datetime(new_value[health_module]['authoredTime']):
                    new_value[health_module]['status'] = health_module_status
                    new_value[health_module]['authoredTime'] = authored_str

                new_value[health_module]['history'].insert(0, {
                        'status': health_module_status,
                        'authoredTime': authored_str
                    })
                new_value[health_module]['history'] = sorted(new_value[health_module]['history'],
                                                             key=lambda i: parse_datetime(i['authoredTime']),
                                                             reverse=True)
                something_changed = True
        else:
            new_value[health_module] = {
                'status': health_module_status,
                'authoredTime': authored_str,
                'history': [
                    {
                        'status': health_module_status,
                        'authoredTime': authored_str
                    }
                ]
            }
            something_changed = True

        return new_value, something_changed

    def insert(self, obj):
        if obj.questionnaireResponseId:
            response = super(QuestionnaireResponseDao, self).insert(obj)
        else:
            response = self._insert_with_random_id(obj, ["questionnaireResponseId"])

        # add physical measurement record for remote self reported physical measurement response
        self._add_physical_measurement(response)

        return response

    def read_status(self, fhir_response: fhir_questionnaireresponse.QuestionnaireResponse):
        status_map = {
            'in-progress': QuestionnaireResponseStatus.IN_PROGRESS,
            'completed': QuestionnaireResponseStatus.COMPLETED,
            'amended': QuestionnaireResponseStatus.AMENDED,
            'entered-in-error': QuestionnaireResponseStatus.ENTERED_IN_ERROR,
            'stopped': QuestionnaireResponseStatus.STOPPED
        }

        if fhir_response.status not in status_map:
            raise BadRequest(f'Unrecognized status "{fhir_response.status}"')
        else:
            return status_map[fhir_response.status]

    @classmethod
    def calculate_answer_hash(cls, response_json):
        answer_list_json = response_json.get('group', '')
        answer_list_str = json.dumps(answer_list_json)
        return md5(answer_list_str.encode('utf-8')).hexdigest()

    @classmethod
    def _extension_from_fhir_object(cls, fhir_extension):
        # Get the non-empty values from the FHIR extension object for the url field and
        # any field with a name that starts with "value"
        fhir_fields = fhir_extension.__dict__
        filtered_values = {}
        for name, value in fhir_fields.items():
            if value is not None and (name == 'url' or name.startswith('value')):
                filtered_values[name] = value

        return QuestionnaireResponseExtension(**filtered_values)

    @classmethod
    def _parse_external_identifier(cls, fhir_qr):
        external_id = None
        if fhir_qr.identifier:
            external_id = fhir_qr.identifier.value
            if external_id and len(external_id) > QuestionnaireResponse.externalId.type.length:
                logging.warning('External id was larger than expected, unable to save it to the database.')
                external_id = None
        return external_id

    @classmethod
    def extension_models_from_fhir_objects(cls, fhir_extensions):
        if fhir_extensions:
            try:
                return [cls._extension_from_fhir_object(extension) for extension in fhir_extensions]
            except TypeError:
                logging.warning('Unexpected extension value', exc_info=True)
                return []
        else:
            return []

    def from_client_json(self, resource_json, participant_id=None, client_id=None):
        # pylint: disable=unused-argument
        # Parse the questionnaire response, but preserve the original response when persisting
        fhir_qr = fhir_questionnaireresponse.QuestionnaireResponse(resource_json)
        patient_id = fhir_qr.subject.reference
        if patient_id != "Patient/P{}".format(participant_id):
            msg = "Questionnaire response subject reference does not match participant_id {}"
            raise BadRequest(msg.format(participant_id))
        questionnaire = self._get_questionnaire(fhir_qr.questionnaire, resource_json)
        if questionnaire.status == QuestionnaireDefinitionStatus.INVALID:
            raise BadRequest(
                f"Submitted questionnaire that is marked as invalid: questionnaire ID {questionnaire.questionnaireId}"
            )
        authored = None
        if fhir_qr.authored and fhir_qr.authored.date:
            authored = fhir_qr.authored.date
        else:
            logging.error(
                f'Response by P{participant_id} to questionnaire {questionnaire.questionnaireId} '
                f'has missing or invalid authored date'
            )

        language = None
        non_participant_author = None
        if fhir_qr.extension:
            for ext in fhir_qr.extension:
                if "iso21090-ST-language" in ext.url:
                    language = ext.valueCode[:2]
                if ext.url == _CATI_EXTENSION:
                    non_participant_author = ext.valueString

        qr = QuestionnaireResponse(
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            questionnaireSemanticVersion=questionnaire.semanticVersion,
            participantId=participant_id,
            nonParticipantAuthor=non_participant_author,
            authored=authored,
            language=language,
            resource=json.dumps(resource_json),
            status=self.read_status(fhir_qr),
            answerHash=self.calculate_answer_hash(resource_json),
            externalId=self._parse_external_identifier(fhir_qr)
        )

        if fhir_qr.group is not None:
            # Extract a code map and answers from the questionnaire response.
            code_map, answers = self._extract_codes_and_answers(fhir_qr.group, questionnaire)
            if not answers:
                logging.error("No answers from QuestionnaireResponse JSON. This is harmless but probably an error.")
            # Get or insert codes, and retrieve their database IDs.
            code_id_map = CodeDao().get_internal_id_code_map(code_map)

            # Now add the child answers, using the IDs in code_id_map
            self._add_answers(qr, code_id_map, answers)

        qr.extensions = self.extension_models_from_fhir_objects(fhir_qr.extension)
        return qr

    @staticmethod
    def _get_questionnaire(questionnaire, resource_json):
        """Retrieves the questionnaire referenced by this response; mutates the resource JSON to include
    the version if it doesn't already.
    If a questionnaire has a history element it goes into the if block here."""
        # if history...
        if not questionnaire.reference.startswith(_QUESTIONNAIRE_PREFIX):
            raise BadRequest(f"Questionnaire reference {questionnaire.reference} is invalid")
        questionnaire_reference = questionnaire.reference[len(_QUESTIONNAIRE_PREFIX):]
        # If the questionnaire response specifies the version of the questionnaire it's for, use it.
        if _QUESTIONNAIRE_HISTORY_SEGMENT in questionnaire_reference:
            questionnaire_ref_parts = questionnaire_reference.split(_QUESTIONNAIRE_HISTORY_SEGMENT)
            if len(questionnaire_ref_parts) != 2:
                raise BadRequest(f"Questionnaire id {questionnaire_reference} is invalid")
            try:
                questionnaire_id = int(questionnaire_ref_parts[0])
                semantic_version = questionnaire_ref_parts[1]
                q = QuestionnaireHistoryDao().get_with_children((questionnaire_id, semantic_version))
                if not q:
                    raise BadRequest(f"Questionnaire with id {questionnaire_id}, semantic version {semantic_version} "
                                     f"is not found")
                return q
            except ValueError:
                raise BadRequest(f"Questionnaire id {questionnaire_reference} is invalid")
        else:
            # if no questionnaire/history...
            try:
                questionnaire_id = int(questionnaire_reference)
                from rdr_service.dao.questionnaire_dao import QuestionnaireDao

                q = QuestionnaireDao().get_with_children(questionnaire_id)
                if not q:
                    raise BadRequest(f"Questionnaire with id {questionnaire_id} is not found")
                # Mutate the questionnaire reference to include the version.
                questionnaire_reference = _QUESTIONNAIRE_REFERENCE_FORMAT.format(questionnaire_id, q.semanticVersion)
                resource_json["questionnaire"]["reference"] = questionnaire_reference
                return q
            except ValueError:
                raise BadRequest(f"Questionnaire id {questionnaire_reference} is invalid")

    @classmethod
    def _extract_codes_and_answers(cls, group, q):
        """Returns (system, code) -> (display, code type, question code id) code map
    and (QuestionnaireResponseAnswer, (system, code)) answer pairs.
    """
        code_map = {}
        answers = []
        link_id_to_question = {}
        if q.questions:
            link_id_to_question = {question.linkId: question for question in q.questions}
        cls._populate_codes_and_answers(group, code_map, answers, link_id_to_question, q.questionnaireId)
        return (code_map, answers)

    @classmethod
    def _populate_codes_and_answers(cls, group, code_map, answers, link_id_to_question, questionnaire_id):
        """Populates code_map with (system, code) -> (display, code type, question code id)
    and answers with (QuestionnaireResponseAnswer, (system, code)) pairs."""
        if group.question:
            for question in group.question:
                if question.linkId and question.answer:
                    qq = link_id_to_question.get(question.linkId)
                    if qq:
                        for answer in question.answer:
                            qr_answer = QuestionnaireResponseAnswer(questionId=qq.questionnaireQuestionId)
                            system_and_code = None
                            ignore_answer = False
                            if answer.valueCoding:
                                if not answer.valueCoding.system:
                                    raise BadRequest(f"No system provided for valueCoding: {question.linkId}")
                                if not answer.valueCoding.code:
                                    raise BadRequest(f"No code provided for valueCoding: {question.linkId}")
                                if answer.valueCoding.system == PPI_EXTRA_SYSTEM:
                                    # Ignore answers from the ppi-extra system, as they aren't used for analysis.
                                    ignore_answer = True
                                else:
                                    system_and_code = (answer.valueCoding.system, answer.valueCoding.code)
                                    if not system_and_code in code_map:
                                        code_map[system_and_code] = (
                                            answer.valueCoding.display,
                                            CodeType.ANSWER,
                                            qq.codeId,
                                        )
                            if not ignore_answer:
                                if answer.valueDecimal is not None:
                                    qr_answer.valueDecimal = answer.valueDecimal
                                if answer.valueInteger is not None:
                                    qr_answer.valueInteger = answer.valueInteger
                                if answer.valueString is not None:
                                    answer_length = len(answer.valueString)
                                    max_length = QuestionnaireResponseAnswer.VALUE_STRING_MAXLEN
                                    if answer_length > max_length:
                                        raise BadRequest(
                                            f"String value too long (len={answer_length}); "
                                            f"must be less than {max_length}"
                                        )
                                    qr_answer.valueString = answer.valueString
                                if answer.valueDate is not None:
                                    qr_answer.valueDate = answer.valueDate.date
                                if answer.valueDateTime is not None:
                                    qr_answer.valueDateTime = answer.valueDateTime.date
                                if answer.valueBoolean is not None:
                                    qr_answer.valueBoolean = answer.valueBoolean
                                if answer.valueUri is not None:
                                    qr_answer.valueUri = answer.valueUri
                                answers.append((qr_answer, system_and_code))
                            if answer.group:
                                for sub_group in answer.group:
                                    cls._populate_codes_and_answers(
                                        sub_group, code_map, answers, link_id_to_question, questionnaire_id
                                    )

        if group.group:
            for sub_group in group.group:
                cls._populate_codes_and_answers(sub_group, code_map, answers, link_id_to_question, questionnaire_id)

    @staticmethod
    def _add_answers(qr, code_id_map, answers):
        for answer, system_and_code in answers:
            if system_and_code:
                system, code = system_and_code
                answer.valueCodeId = code_id_map.get(system, code)
            qr.answers.append(answer)

    @classmethod
    def get_participant_ids_with_response_to_survey(
        cls,
        survey_code: str,
        session: Session,
        sent_statuses: Optional[List[QuestionnaireResponseStatus]] = None,
        classification_types: Optional[List[QuestionnaireResponseClassificationType]] = None
    ) -> List[int]:
        if sent_statuses is None:
            sent_statuses = [QuestionnaireResponseStatus.COMPLETED]
        if classification_types is None:
            classification_types = [QuestionnaireResponseClassificationType.COMPLETE]

        query = (
            session.query(QuestionnaireResponse.participantId)
            .join(
                QuestionnaireConcept,
                and_(
                    QuestionnaireConcept.questionnaireId == QuestionnaireResponse.questionnaireId,
                    QuestionnaireConcept.questionnaireVersion == QuestionnaireResponse.questionnaireVersion
                )
            ).join(
                Code,
                Code.codeId == QuestionnaireConcept.codeId
            ).filter(
                Code.value.ilike(survey_code),
                QuestionnaireResponse.status.in_(sent_statuses),
                QuestionnaireResponse.classificationType.in_(classification_types)
            )
        )

        return [result_row.participantId for result_row in query.all()]

    @classmethod
    def get_latest_answer_to_question(cls, session: Session, participant_id, question_code_value) -> str:
        answer_code = aliased(Code)
        question_code = aliased(Code)
        query = (
            session.query(answer_code.value)
            .select_from(QuestionnaireResponse)
            .join(QuestionnaireResponseAnswer)
            .join(QuestionnaireQuestion)
            .join(
                question_code,
                and_(
                    question_code.codeId == QuestionnaireQuestion.codeId,
                    question_code.value == question_code_value
                )
            ).join(
                answer_code,
                answer_code.codeId == QuestionnaireResponseAnswer.valueCodeId
            )
            .order_by(QuestionnaireResponse.authored.desc())
            .filter(QuestionnaireResponse.participantId == participant_id)
            .limit(1)
        )

        return query.scalar()

    @classmethod
    def get_latest_answer_for_state_of_residence(cls, session: Session, participant_id) -> str:
        return cls.get_latest_answer_to_question(
            session=session,
            participant_id=participant_id,
            question_code_value=code_constants.STATE_QUESTION_CODE
        )

    @classmethod
    def get_latest_answer_for_state_receiving_care(cls, session: Session, participant_id) -> str:
        return cls.get_latest_answer_to_question(
            session=session,
            participant_id=participant_id,
            question_code_value=code_constants.RECEIVE_CARE_STATE
        )

    @classmethod
    def _code_in_list(cls, code_value: str, code_list: List[str]):
        return code_value.lower in [list_value.lower() for list_value in code_list]


def _validate_consent_pdfs(resource):
    """Checks for any consent-form-signed-pdf extensions and validates their PDFs in GCS."""
    if resource.get("resourceType") != "QuestionnaireResponse":
        raise ValueError(f'Expected QuestionnaireResponse for "resourceType" in {resource}.')

    # We now lookup up consent bucket names by participant origin id.
    p_origin = get_account_origin_id()
    consent_bucket_config = config.getSettingJson(config.CONSENT_PDF_BUCKET)
    # If we don't match the origin id, just return the first bucket in the dict.
    try:
        consent_bucket = consent_bucket_config.get(p_origin, consent_bucket_config[next(iter(consent_bucket_config))])
    except AttributeError:
        pass

    found_pdf = False
    for extension in resource.get("extension", []):
        if extension["url"] != _SIGNED_CONSENT_EXTENSION:
            continue
        local_pdf_path = extension["valueString"]
        _, ext = os.path.splitext(local_pdf_path)
        if ext.lower() != ".pdf":
            raise BadRequest(f"Signed PDF must end in .pdf, found {ext} (from {local_pdf_path}).")
        # Treat the value as a bucket-relative path, allowing a leading slash or not.
        if not local_pdf_path.startswith("/"):
            local_pdf_path = "/" + local_pdf_path

        _raise_if_gcloud_file_missing("/{}{}".format(consent_bucket, local_pdf_path))
        found_pdf = True

    if config.GAE_PROJECT == 'localhost' or is_self_request():
        # Pretend we found a valid consent if we're running on a development machine
        # skip checking for self request from fake participant generating
        return True
    else:
        return found_pdf


def _raise_if_gcloud_file_missing(path):
    """Checks that a GCS file exists.

  Args:
    path: An absolute Google Cloud Storage path, starting with /$BUCKET/.
  Raises:
    BadRequest if the path does not reference a file.
  """
    storage_provier = storage.get_storage_provider()
    if not storage_provier.exists(path):
        raise BadRequest(f"Google Cloud Storage file not found in {path}.")


class QuestionnaireResponseAnswerDao(BaseDao):
    def __init__(self):
        super(QuestionnaireResponseAnswerDao, self).__init__(QuestionnaireResponseAnswer)

    def get_id(self, obj):
        return obj.questionnaireResponseAnswerId

    def get_current_answers_for_concepts(self, session, participant_id, code_ids):
        """ Return any answers the participant has previously given to questions with the specified code IDs."""
        if not code_ids:
            return []
        return (
            session.query(QuestionnaireResponseAnswer)
                .join(QuestionnaireResponse)
                .join(QuestionnaireQuestion)
                .filter(QuestionnaireResponse.participantId == participant_id)
                .filter(QuestionnaireResponseAnswer.endTime == None)
                .filter(QuestionnaireQuestion.codeId.in_(code_ids))
                .all()
        )
