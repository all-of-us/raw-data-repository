from collections import defaultdict
from datetime import datetime
from typing import Collection, Dict

from sqlalchemy import and_, or_
from sqlalchemy.orm import aliased, Session

from rdr_service.code_constants import PRIMARY_CONSENT_UPDATE_QUESTION_CODE
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.code import Code
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.model.consent_response import ConsentResponse
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire import QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.participant_enums import QuestionnaireStatus


class ConsentDao(BaseDao):
    def __init__(self):
        super(ConsentDao, self).__init__(ConsentFile)

    @classmethod
    def _get_query_non_test_summaries(cls, session):
        return session.query(
            ParticipantSummary
        ).join(
            Participant,
            Participant.participantId == ParticipantSummary.participantId
        ).filter(
            Participant.isGhostId.isnot(True),
            Participant.isTestParticipant.isnot(True),
            or_(
                ParticipantSummary.email.is_(None),
                ParticipantSummary.email.notlike('%@example.com')
            )
        )

    @classmethod
    def get_consent_responses_to_validate(cls, session) -> Dict[int, Collection[ConsentResponse]]:
        """
        Gets all the consent responses that need to be validated.
        :return: Dictionary with keys being participant ids and values being collections of ConsentResponses
        """
        # A ConsentResponse hasn't been validated yet if there aren't any ConsentFiles that link to the response
        db_results = session.query(ConsentResponse, QuestionnaireResponse.participantId).join(
            QuestionnaireResponse
        ).outerjoin(
            ConsentFile
        ).filter(
            ConsentFile.id.is_(None)
        ).all()

        grouped_results = defaultdict(list)
        for consent_response, participant_id in db_results:
            grouped_results[participant_id].append(consent_response)

        return dict(grouped_results)


    @classmethod
    def get_participants_with_unvalidated_files(cls, session) -> Collection[ParticipantSummary]:
        """
        Finds all participants that should have submitted a consent, but don't have the corresponding file validated
        """

        primary_consent = aliased(ConsentFile)
        cabor_consent = aliased(ConsentFile)
        ehr_consent = aliased(ConsentFile)
        gror_consent = aliased(ConsentFile)
        primary_update_consent = aliased(ConsentFile)
        query = (
            cls._get_query_non_test_summaries(session)
            .outerjoin(
                primary_consent, and_(
                    primary_consent.participant_id == ParticipantSummary.participantId,
                    primary_consent.type == ConsentType.PRIMARY
                )
            ).outerjoin(
                cabor_consent, and_(
                    cabor_consent.participant_id == ParticipantSummary.participantId,
                    cabor_consent.type == ConsentType.CABOR
                )
            ).outerjoin(
                ehr_consent, and_(
                    ehr_consent.participant_id == ParticipantSummary.participantId,
                    ehr_consent.type == ConsentType.EHR
                )
            ).outerjoin(
                gror_consent, and_(
                    gror_consent.participant_id == ParticipantSummary.participantId,
                    gror_consent.type == ConsentType.GROR
                )
            ).outerjoin(
                primary_update_consent, and_(
                    primary_update_consent.participant_id == ParticipantSummary.participantId,
                    primary_update_consent.type == ConsentType.PRIMARY_UPDATE
                )
            ).outerjoin(
                QuestionnaireResponse,
                QuestionnaireResponse.participantId == ParticipantSummary.participantId
            ).outerjoin(
                QuestionnaireResponseAnswer,
                QuestionnaireResponseAnswer.questionnaireResponseId == QuestionnaireResponse.questionnaireResponseId
            ).outerjoin(
                QuestionnaireQuestion,
                QuestionnaireQuestion.questionnaireQuestionId == QuestionnaireResponseAnswer.questionId
            ).outerjoin(
                Code,
                and_(
                    Code.codeId == QuestionnaireQuestion.codeId,
                    Code.value == PRIMARY_CONSENT_UPDATE_QUESTION_CODE
                )
            ).filter(
                or_(
                    primary_consent.id.is_(None),
                    and_(
                        ParticipantSummary.consentForCABoR == QuestionnaireStatus.SUBMITTED,
                        cabor_consent.id.is_(None)
                    ),
                    and_(
                        ParticipantSummary.consentForElectronicHealthRecords == QuestionnaireStatus.SUBMITTED,
                        ehr_consent.id.is_(None)
                    ),
                    and_(
                        ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED,
                        gror_consent.id.is_(None)
                    ),
                    and_(
                        Code.codeId.isnot(None),
                        primary_update_consent.id.is_(None)
                    )
                )
            )
        )
        return query.distinct().all()

    @classmethod
    def get_participants_needing_validation(cls, session) -> Collection[ParticipantSummary]:
        query = cls._get_query_non_test_summaries(session)
        query = query.outerjoin(
            ConsentFile,
            ConsentFile.participant_id == ParticipantSummary.participantId
        ).filter(
            ConsentFile.id.is_(None)
        ).limit(5000)
        return query.all()

    @classmethod
    def get_files_needing_correction(cls, session, min_modified_datetime: datetime = None) -> Collection[ConsentFile]:
        query = session.query(ConsentFile).filter(
            ConsentFile.sync_status == ConsentSyncStatus.NEEDS_CORRECTING
        )
        if min_modified_datetime:
            query = query.filter(ConsentFile.modified >= min_modified_datetime)
        return query.all()

    @classmethod
    def _batch_update_consent_files_with_session(cls, session, consent_files: Collection[ConsentFile]):
        for file_record in consent_files:
            if file_record.id:
                session.merge(file_record)
            else:
                session.add(file_record)

    def batch_update_consent_files(self, consent_files: Collection[ConsentFile], session=None):
        if session is None:
            with self.session() as dao_session:
                return self._batch_update_consent_files_with_session(dao_session, consent_files)
        else:
            return self._batch_update_consent_files_with_session(session, consent_files)

    @classmethod
    def get_validation_results_for_participants(cls, session,
                                                participant_ids: Collection[int]) -> Collection[ConsentFile]:
        return session.query(ConsentFile).filter(
            ConsentFile.participant_id.in_(participant_ids)
        ).all()

    @classmethod
    def _get_ready_to_sync_with_session(cls, session: Session, org_names, hpo_names):
        query = (
            session.query(ConsentFile)
            .join(Participant)
            .outerjoin(Organization)
            .join(HPO, Participant.hpoId == HPO.hpoId)
            .filter(
                ConsentFile.sync_status == ConsentSyncStatus.READY_FOR_SYNC,
                or_(
                    Organization.externalId.in_(org_names),
                    HPO.name.in_(hpo_names)
                )
            )
        )
        return query.all()

    def get_files_ready_to_sync(self, org_names, hpo_names, session: Session = None) -> Collection[ConsentFile]:
        if session is None:
            with self.session() as dao_session:
                return self._get_ready_to_sync_with_session(
                    session=dao_session,
                    org_names=org_names,
                    hpo_names=hpo_names
                )
        else:
            return self._get_ready_to_sync_with_session(session=session, org_names=org_names, hpo_names=hpo_names)

    @classmethod
    def set_previously_synced_files_as_ready(cls, session: Session, participant_id: int):
        session.query(
            ConsentFile
        ).filter(
            ConsentFile.sync_status == ConsentSyncStatus.SYNC_COMPLETE,
            ConsentFile.participant_id == participant_id
        ).update({
            ConsentFile.sync_status: ConsentSyncStatus.READY_FOR_SYNC,
            ConsentFile.sync_time: None
        })
