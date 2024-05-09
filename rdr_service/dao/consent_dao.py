from collections import defaultdict
from datetime import datetime
from typing import Collection, Dict, List, Optional

from sqlalchemy import and_, or_
from sqlalchemy.orm import joinedload, Session

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType, ConsentErrorReport
from rdr_service.model.consent_response import ConsentResponse
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire_response import QuestionnaireResponse


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
    def get_consent_responses_to_validate(
        cls, session, since_date: datetime = None
    ) -> (Dict[int, List[ConsentResponse]], bool):
        """
        Gets all the consent responses that need to be validated.
        :return: Dictionary with keys being participant ids and values being collections of ConsentResponses
        """
        consent_batch_limit = 500

        # A ConsentResponse will need to be validated if there are not yet any ConsentFile objects
        # that are of the same type and for the same participant.
        query = (
            session.query(ConsentResponse)
            .join(QuestionnaireResponse)
            .join(Participant)
            .outerjoin(
                ConsentFile,
                and_(
                    or_(
                        and_(
                            or_(
                                ConsentFile.type != ConsentType.EHR,
                                and_(
                                    ConsentResponse.type == ConsentType.EHR,
                                    QuestionnaireResponse.authored <= '2022-04-01',
                                )
                            ),
                            ConsentFile.type == ConsentResponse.type
                        ),
                        and_(
                            ConsentResponse.type == ConsentType.EHR,
                            QuestionnaireResponse.authored > '2022-04-01',
                            ConsentResponse.id == ConsentFile.consent_response_id
                        )
                    ),
                    ConsentFile.participant_id == QuestionnaireResponse.participantId
                )
            ).filter(
                ConsentFile.id.is_(None),
                Participant.participantOrigin != 'configurator'
            ).options(
                joinedload(ConsentResponse.response)
            )
        )

        if since_date:
            query = query.filter(
                QuestionnaireResponse.created >= since_date,
                QuestionnaireResponse.questionnaireId > 0  # needed to use prod's index of the created date
            ).with_hint(
                QuestionnaireResponse, 'USE INDEX (idx_created_q_id)'
            )

        consent_responses = query.limit(consent_batch_limit).all()
        grouped_results = defaultdict(list)
        for consent_response in consent_responses:
            grouped_results[consent_response.response.participantId].append(consent_response)

        return dict(grouped_results), len(consent_responses) < consent_batch_limit

    @classmethod
    def get_consent_authored_times_for_participant(cls, session, participant_id) -> Dict[ConsentType, List[datetime]]:
        """
        Gets all the consent authored times for a participant.
        :return: Dictionary with keys being consent type and values being lists of authored dates for that type
        """
        consent_responses = session.query(QuestionnaireResponse.authored, ConsentResponse.type).select_from(
            ConsentResponse
        ).join(
            QuestionnaireResponse
        ).filter(
            QuestionnaireResponse.participantId == participant_id
        )
        consent_responses = consent_responses.all()

        grouped_results = defaultdict(list)
        for authored_time, consent_type in consent_responses:
            grouped_results[consent_type].append(authored_time)

        return dict(grouped_results)

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
    def get_next_revalidate_batch(cls, session, limit=1000) -> Collection[ConsentFile]:
        query = (
            session.query(ConsentFile.participant_id, ConsentFile.type)
            .filter(
                ConsentFile.sync_status == ConsentSyncStatus.NEEDS_CORRECTING
            )
            .order_by(ConsentFile.last_checked, ConsentFile.created)
            .distinct().limit(limit)
        )
        return query

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
    def _get_ready_to_sync_with_session(cls, session: Session, org_names, hpo_names, additional_filters=None):
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
                ),
                ConsentFile.type != ConsentType.ETM
            )
        )

        if additional_filters:
            for hpo_name, filter_dict in additional_filters.items():
                exclude_type_list = filter_dict.get('exclude_types')
                if exclude_type_list:
                    query = query.filter(
                        or_(
                            HPO.name != hpo_name,
                            ConsentFile.type.notin_(exclude_type_list)
                        )
                    )

        return query.all()

    def get_files_ready_to_sync(
        self, org_names, hpo_names, session: Session = None, additional_filters=None
    ) -> Collection[ConsentFile]:
        if session is None:
            with self.session() as dao_session:
                return self._get_ready_to_sync_with_session(
                    session=dao_session,
                    org_names=org_names,
                    hpo_names=hpo_names,
                    additional_filters=additional_filters
                )
        else:
            return self._get_ready_to_sync_with_session(
                session=session, org_names=org_names, hpo_names=hpo_names, additional_filters=additional_filters
            )

    @classmethod
    def get_files_for_participant(cls, participant_id: int, session: Session,
                                  consent_type: Optional[ConsentType] = None) -> List[ConsentFile]:
        query = (
            session.query(ConsentFile)
            .filter(
                ConsentFile.participant_id == participant_id
            )
        )
        if consent_type:
            query = query.filter(
                ConsentFile.type == consent_type
            )

        return query.all()

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

class ConsentErrorReportDao(BaseDao):

    def __init__(self):
        super(ConsentErrorReportDao, self).__init__(ConsentErrorReport)

    @classmethod
    def _batch_update_consent_error_reports_with_session(cls, session, error_reports: Collection[ConsentErrorReport]):
        for rec in error_reports:
            if rec.id:
                session.merge(rec)
            else:
                session.add(rec)

    def batch_update_consent_error_reports(self, error_reports: Collection[ConsentErrorReport], session=None):
        if session is None:
            with self.session() as dao_session:
                return self._batch_update_consent_error_reports_with_session(dao_session, error_reports)
        else:
            return self._batch_update_consent_error_reports_with_session(session, error_reports)
