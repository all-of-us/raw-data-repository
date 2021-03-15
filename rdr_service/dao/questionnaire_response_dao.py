import json
import logging
import os
import re
from datetime import datetime

import pytz
from sqlalchemy import or_
from sqlalchemy.orm import subqueryload
from werkzeug.exceptions import BadRequest

from rdr_service.lib_fhir.fhirclient_1_0_6.models import questionnaireresponse as fhir_questionnaireresponse
from rdr_service.participant_enums import QuestionnaireResponseStatus, PARTICIPANT_COHORT_2_START_TIME,\
    PARTICIPANT_COHORT_3_START_TIME
from rdr_service.app_util import get_account_origin_id
from rdr_service import storage
from rdr_service import clock, config
from rdr_service.code_constants import (
    CABOR_SIGNATURE_QUESTION_CODE,
    CONSENT_COHORT_GROUP_CODE,
    CONSENT_FOR_DVEHR_MODULE,
    CONSENT_FOR_GENOMICS_ROR_MODULE,
    CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE,
    CONSENT_FOR_STUDY_ENROLLMENT_MODULE,
    CONSENT_PERMISSION_YES_CODE,
    DVEHRSHARING_CONSENT_CODE_NOT_SURE,
    DVEHRSHARING_CONSENT_CODE_YES,
    DVEHR_SHARING_QUESTION_CODE,
    EHR_CONSENT_QUESTION_CODE,
    EHR_CONSENT_EXPIRED_QUESTION_CODE,
    GENDER_IDENTITY_QUESTION_CODE,
    LANGUAGE_OF_CONSENT,
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
    STREET_ADDRESS_QUESTION_CODE,
    STREET_ADDRESS2_QUESTION_CODE,
    EHR_CONSENT_EXPIRED_YES,
    PRIMARY_CONSENT_UPDATE_QUESTION_CODE,
    COHORT_1_REVIEW_CONSENT_YES_CODE)
from rdr_service.dao.base_dao import BaseDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_dao import ParticipantDao, raise_if_withdrawn
from rdr_service.dao.participant_summary_dao import (
    ParticipantGenderAnswersDao,
    ParticipantRaceAnswersDao,
    ParticipantSummaryDao,
)
from rdr_service.dao.questionnaire_dao import QuestionnaireHistoryDao, QuestionnaireQuestionDao
from rdr_service.field_mappings import FieldType, QUESTIONNAIRE_MODULE_CODE_TO_FIELD, QUESTION_CODE_TO_FIELD
from rdr_service.model.code import CodeType
from rdr_service.model.questionnaire import  QuestionnaireHistory, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer,\
    QuestionnaireResponseExtension
from rdr_service.model.survey import Survey, SurveyQuestion, SurveyQuestionType
from rdr_service.participant_enums import (
    QuestionnaireDefinitionStatus,
    QuestionnaireStatus,
    TEST_LOGIN_PHONE_NUMBER_PREFIX,
    get_gender_identity,
    get_race,
    ParticipantCohort,
    ConsentExpireStatus)

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

        self.survey = self._get_survey_for_questionnaire_history(questionnaire_history)
        self._code_to_question_map = self._build_code_to_question_map()

    def _get_survey_for_questionnaire_history(self, questionnaire_history: QuestionnaireHistory):
        survey_query = self.session.query(Survey).filter(
            Survey.codeId.in_([concept.codeId for concept in questionnaire_history.concepts]),
            Survey.importTime < questionnaire_history.created,
            or_(
                Survey.replacedTime.is_(None),
                Survey.replacedTime > questionnaire_history.created
            )
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
        # TODO: test these logs

            # TODO: does this join load the questions and codes, and question options?
        return survey_query.first()

    def _build_code_to_question_map(self):
        return {survey_question.code.codeId: survey_question for survey_question in self.survey.questions}

    @classmethod
    def _check_answer_has_expected_data_type(cls, answer: QuestionnaireResponseAnswer, question: SurveyQuestion):
        if question.questionType in (SurveyQuestionType.UNKNOWN,
                                     SurveyQuestionType.DROPDOWN,
                                     SurveyQuestionType.RADIO,
                                     SurveyQuestionType.CHECKBOX):
            number_of_selectable_options = len(question.options)
            if number_of_selectable_options == 0 and answer.valueCodeId is not None:
                # TODO: int test that the questionId is set for the answer
                logging.warning(
                    f'Answer for {answer.question.code.value} gives a value code id when no options are defined'
                )
            elif number_of_selectable_options > 0 and answer.valueCodeId is None:
                logging.warning(
                    f'Answer for {answer.question.code.value} gives no value code id '
                    f'when the question has options defined'
                )
        elif question.questionType in (SurveyQuestionType.CALC,
                                       SurveyQuestionType.YESNO,
                                       SurveyQuestionType.TRUEFALSE,
                                       SurveyQuestionType.FILE,
                                       SurveyQuestionType.SLIDER):
            # There aren't alot of surveys in redcap right now, so it's unclear how these would be answered
            logging.warning(f'No validation implemented for answer to {answer.question.code.value}')

    def check_response(self, response: QuestionnaireResponse):
        if self.survey is None:
            return None

        for answer in response.answers:  # todo: int test that the answers relationship is set
            survey_question = self._code_to_question_map.get(answer.question.codeId)
            if not survey_question:
                # TODO: write test for this
                logging.error(f'Question code used by the answer for question {answer.questionId} does not match a '
                              f'code found on the survey definition')
            else:
                self._check_answer_has_expected_data_type(answer, survey_question)

        # TODO: check that
        #   answers of the expected type (date, code for multi-select, integer, free-text)
        #   multi-select answers give an option that is valid for the question
        #    e
        #   that there aren't more answers than expected (there could be fewer answers than what's in the survey)
        #   (checkbox questions get multiple answers)
        #    e
        #   a question isn't answered multiple times
        #   if there isn't branching logic on a question, then we should reasonably be able to assume that it
        #                   was answered
        #   does every answer match a response on the survey

        logging.info('this is valid')


class QuestionnaireResponseDao(BaseDao):
    def __init__(self):
        super(QuestionnaireResponseDao, self).__init__(QuestionnaireResponse)

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

        # Get the questions from the questionnaire history record.
        q_question_ids = set([question.questionnaireQuestionId for question in questionnaire_history.questions])
        for answer in questionnaire_response.answers:
            if answer.questionId not in q_question_ids:
                raise BadRequest(
                    f"Questionnaire response contains question ID {answer.questionId} not in questionnaire."
                )

        questionnaire_response.created = clock.CLOCK.now()
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

        # IMPORTANT: update the participant summary first to grab an exclusive lock on the participant
        # row. If you instead do this after the insert of the questionnaire response, MySQL will get a
        # shared lock on the participant row due the foreign key, and potentially deadlock later trying
        # to get the exclusive lock if another thread is updating the participant. See DA-269.
        # (We need to lock both participant and participant summary because the summary row may not
        # exist yet.)
        if questionnaire_response.status == QuestionnaireResponseStatus.COMPLETED:
            with self.session() as new_session:
                self._update_participant_summary(
                    new_session, questionnaire_response, code_ids, questions, questionnaire_history, resource_json
                )

        super(QuestionnaireResponseDao, self).insert_with_session(session, questionnaire_response)
        # Mark existing answers for the questions in this response given previously by this participant
        # as ended.
        for answer in current_answers:
            answer.endTime = questionnaire_response.created
            session.merge(answer)

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
            raise_if_withdrawn(participant)
            participant_summary = ParticipantDao.create_summary_for_participant(participant)
            something_changed = True
        else:
            raise_if_withdrawn(participant_summary)

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
                    elif code.value == EHR_CONSENT_QUESTION_CODE:
                        code = code_dao.get(answer.valueCodeId)
                        if participant_summary.ehrConsentExpireStatus == ConsentExpireStatus.EXPIRED and \
                            authored > participant_summary.ehrConsentExpireAuthored:
                            participant_summary.ehrConsentExpireStatus = ConsentExpireStatus.UNSET
                            participant_summary.ehrConsentExpireAuthored = None
                            participant_summary.ehrConsentExpireTime = None
                        if code and code.value == CONSENT_PERMISSION_YES_CODE:
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
                            if not participant_summary.consentForCABoR:
                                participant_summary.consentForCABoR = True
                                participant_summary.consentForCABoRTime = questionnaire_response.created
                                participant_summary.consentForCABoRAuthored = authored
                                something_changed = True
                    elif code.value == GROR_CONSENT_QUESTION_CODE:
                        if code_dao.get(answer.valueCodeId).value == CONSENT_GROR_YES_CODE:
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
                summary_field = QUESTIONNAIRE_MODULE_CODE_TO_FIELD.get(code.value)
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
                    if getattr(participant_summary, summary_field) != new_status:
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

            ParticipantSummaryDao().update_enrollment_status(participant_summary)
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

    def insert(self, obj):
        if obj.questionnaireResponseId:
            return super(QuestionnaireResponseDao, self).insert(obj)
        return self._insert_with_random_id(obj, ["questionnaireResponseId"])

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
            status=self.read_status(fhir_qr)
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

    if config.GAE_PROJECT == 'localhost':
        # Pretend we found a valid consent if we're running on a development machine
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
        """Return any answers the participant has previously given to questions with the specified
    code IDs."""
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
