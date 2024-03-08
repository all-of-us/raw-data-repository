from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import aliased, joinedload, Session

from rdr_service import code_constants, config, participant_enums as enums
from rdr_service.domain_model import response as response_domain_model
from rdr_service.model.code import Code
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.model.consent_response import ConsentResponse
from rdr_service.model.participant import Participant
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.services.system_utils import DateRange


class QuestionnaireResponseRepository:

    @classmethod
    def get_responses_to_surveys(
        cls,
        session: Session,
        survey_codes: List[str] = None,
        participant_ids: List[int] = None,
        include_ignored_answers=False,
        sent_statuses: Optional[List[enums.QuestionnaireResponseStatus]] = None,
        classification_types: Optional[List[enums.QuestionnaireResponseClassificationType]] = None,
        created_start_datetime: datetime = None,
        created_end_datetime: datetime = None
    ) -> Dict[int, response_domain_model.ParticipantResponses]:
        """
        Retrieve questionnaire response data (returned as a domain model) for the specified participant ids
        and survey codes.

        :param survey_codes: Survey module code strings to get responses for
        :param session: Session to use for connecting to the database
        :param participant_ids: Participant ids to get responses for
        :param include_ignored_answers: Include response answers that have been ignored
        :param sent_statuses: List of QuestionnaireResponseStatus to use when filtering responses
            (defaults to QuestionnaireResponseStatus.COMPLETED)
        :param classification_types: List of QuestionnaireResponseClassificationTypes to filter results by
        :param created_start_datetime: Optional start date, if set only responses that were sent to
            the API after this date will be returned
        :param created_end_datetime: Optional end date, if set only responses that were sent to the
            API before this date will be returned
        :return: A dictionary keyed by participant ids with the value being the collection of responses for
            that participant
        """

        if sent_statuses is None:
            sent_statuses = [enums.QuestionnaireResponseStatus.COMPLETED]
        if classification_types is None:
            classification_types = [enums.QuestionnaireResponseClassificationType.COMPLETE]

        # Build query for all the questions answered by the given participants for the given survey codes
        question_code = aliased(Code)
        survey_code = aliased(Code)
        query = (
            session.query(
                func.lower(question_code.value),
                QuestionnaireResponse.participantId,
                QuestionnaireResponse.questionnaireResponseId,
                QuestionnaireResponse.authored,
                survey_code.value,
                survey_code.codeId,
                QuestionnaireResponseAnswer,
                QuestionnaireResponse.status
            )
            .select_from(QuestionnaireResponseAnswer)
            .join(QuestionnaireQuestion)
            .join(QuestionnaireResponse)
            .join(
                Participant,
                Participant.participantId == QuestionnaireResponse.participantId
            )
            .join(question_code, question_code.codeId == QuestionnaireQuestion.codeId)
            .join(
                QuestionnaireConcept,
                and_(
                    QuestionnaireConcept.questionnaireId == QuestionnaireResponse.questionnaireId,
                    QuestionnaireConcept.questionnaireVersion == QuestionnaireResponse.questionnaireVersion
                )
            ).join(survey_code, survey_code.codeId == QuestionnaireConcept.codeId)
            .options(joinedload(QuestionnaireResponseAnswer.code))
            .filter(
                QuestionnaireResponse.status.in_(sent_statuses),
                QuestionnaireResponse.classificationType.in_(classification_types),
                Participant.isTestParticipant != 1
            )
        )

        if survey_codes:
            query = query.filter(
                survey_code.value.in_(survey_codes)
            )
        if participant_ids:
            query = query.filter(
                QuestionnaireResponse.participantId.in_(participant_ids)
            )
        if not include_ignored_answers:
            query = query.filter(
                or_(
                    QuestionnaireResponseAnswer.ignore.is_(False),
                    QuestionnaireResponseAnswer.ignore.is_(None)
                )
            )
        if created_start_datetime:
            query = query.filter(
                QuestionnaireResponse.created >= created_start_datetime
            ).with_hint(
                QuestionnaireResponse,
                'USE INDEX (idx_created_q_id)'
            )
        if created_end_datetime:
            query = query.filter(
                QuestionnaireResponse.created <= created_end_datetime
            ).with_hint(
                QuestionnaireResponse,
                'USE INDEX (idx_created_q_id)'
            )

        # build dict with participant ids as keys and ParticipantResponse objects as values
        participant_response_map = defaultdict(response_domain_model.ParticipantResponses)
        for question_code_str, participant_id, response_id, authored_datetime, survey_code_str, survey_code_id, \
                answer, status in query.all():
            # Get the collection of responses for the participant
            response_collection_for_participant = participant_response_map[participant_id]

            # Get the response that this particular answer is for so we can store the answer
            response = response_collection_for_participant.responses.get(response_id)
            if not response:
                # This is the first time seeing an answer for this response, so create the Response structure for it
                response = response_domain_model.Response(
                    id=response_id,
                    survey_code_id=survey_code_id,
                    survey_code=survey_code_str,
                    authored_datetime=authored_datetime,
                    status=status
                )
                response_collection_for_participant.responses[response_id] = response

            response.answered_codes[question_code_str].append(
                response_domain_model.Answer.from_db_model(answer)
            )

        return dict(participant_response_map)

    @classmethod
    def get_validated_ehr_consent_ids(cls, participant_id, session: Session):
        query = (
            session.query(ConsentResponse.questionnaire_response_id)
            .join(ConsentFile)
            .filter(
                ConsentFile.type.in_([ConsentType.EHR, ConsentType.PEDIATRIC_EHR]),
                ConsentFile.sync_status.in_([ConsentSyncStatus.READY_FOR_SYNC, ConsentSyncStatus.SYNC_COMPLETE]),
                ConsentFile.participant_id == participant_id
            )
        )
        return [consent_response.questionnaire_response_id for consent_response in query.all()]

    @classmethod
    def get_interest_in_sharing_ehr_ranges(cls, participant_id, session: Session, default_authored_datetime=None,
                                           validation_not_required=False):
        """
        :param participant_id:  Participant id (integer)
        :param session:  A session object for querying data
        :param default_authored_datetime: An authored timestamp to match to an EHR consent response, if provided
        :param validation_not_required: A flag to disable enforcement of successful PDF validation.   When this
                                        function is called during calculation of retention eligibility, is set to True

        """
        # Load all EHR and DV_EHR responses
        sharing_response_list = cls.get_responses_to_surveys(
            session=session,
            survey_codes=[
                code_constants.CONSENT_FOR_DVEHR_MODULE,
                code_constants.CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE,
                code_constants.PEDIATRIC_EHR_CONSENT
            ],
            participant_ids=[participant_id],
            classification_types=[
                # The EHR response linked to a validated EHR file might be marked as duplicate of another response
                enums.QuestionnaireResponseClassificationType.COMPLETE,
                enums.QuestionnaireResponseClassificationType.DUPLICATE
            ]
        ).get(participant_id)
        validated_ehr_id_list = cls.get_validated_ehr_consent_ids(participant_id=participant_id, session=session)

        # Find all ranges where interest in sharing EHR was expressed (DV_EHR) or consent to share was provided
        ehr_interest_date_ranges = []

        skip_validation_check = (config.getSettingJson('ENROLLMENT_STATUS_SKIP_VALIDATION', False)
                                 or validation_not_required)
        if sharing_response_list:

            current_date_range = None
            for response in sharing_response_list.in_authored_order:
                dv_interest_answer = response.get_single_answer_for(code_constants.DVEHR_SHARING_QUESTION_CODE)
                # TODO: check if answer is null, and use a safe version of get_single_answer
                if dv_interest_answer:
                    if (
                        dv_interest_answer.value.lower() == code_constants.DVEHRSHARING_CONSENT_CODE_YES.lower()
                        and current_date_range is None
                    ):
                        current_date_range = DateRange(start=response.authored_datetime)
                    if (
                        dv_interest_answer.value.lower() != code_constants.DVEHRSHARING_CONSENT_CODE_YES.lower()
                        and current_date_range is not None
                    ):
                        current_date_range.end = response.authored_datetime
                        ehr_interest_date_ranges.append(current_date_range)
                        current_date_range = None

                consent_answer = response.get_single_answer_for(code_constants.EHR_CONSENT_QUESTION_CODE)
                if consent_answer:
                    # ignore any EHR responses that are not validated
                    # Note: only check for validated EHR if the consent was authored after the response->consent data
                    #       started being generated (2022-02-18).  Also, if a default_authored_datetime was specified,
                    #       consider a response with matching authored as validated.  (See ROC-1572/PDR-1699)
                    if (
                        response.id not in validated_ehr_id_list and not skip_validation_check
                        and response.authored_datetime != default_authored_datetime
                        and response.authored_datetime > datetime(2022, 2, 18)
                    ):
                        continue

                    if (
                        consent_answer.value.lower() == code_constants.CONSENT_PERMISSION_YES_CODE.lower()
                        and current_date_range is None
                    ):
                        current_date_range = DateRange(start=response.authored_datetime)
                    if (
                        consent_answer.value.lower() != code_constants.CONSENT_PERMISSION_YES_CODE.lower()
                        and current_date_range is not None
                    ):
                        current_date_range.end = response.authored_datetime
                        ehr_interest_date_ranges.append(current_date_range)
                        current_date_range = None

                consent_answer = response.get_single_answer_for(code_constants.EHR_PEDIATRIC_CONSENT_QUESTION_CODE)
                if consent_answer:
                    if (
                        response.id not in validated_ehr_id_list
                        and not skip_validation_check
                        and response.authored_datetime != default_authored_datetime
                    ):
                        continue

                    if (
                        consent_answer.value.lower() == code_constants.PEDIATRIC_SHARE_AGREE.lower()
                        and current_date_range is None
                    ):
                        current_date_range = DateRange(start=response.authored_datetime)
                    if (
                        consent_answer.value.lower() != code_constants.PEDIATRIC_SHARE_AGREE.lower()
                        and current_date_range is not None
                    ):
                        current_date_range.end = response.authored_datetime
                        ehr_interest_date_ranges.append(current_date_range)
                        current_date_range = None

                expire_answer = response.get_single_answer_for(code_constants.EHR_CONSENT_EXPIRED_QUESTION_CODE)
                if (
                    expire_answer
                    and expire_answer.value.lower() == code_constants.EHR_CONSENT_EXPIRED_YES
                    and current_date_range
                ):
                    current_date_range.end = response.authored_datetime
                    ehr_interest_date_ranges.append(current_date_range)
                    current_date_range = None

            if current_date_range is not None:
                ehr_interest_date_ranges.append(current_date_range)

        return ehr_interest_date_ranges
