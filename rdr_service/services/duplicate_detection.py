from collections import defaultdict
from datetime import datetime
from typing import List

from sqlalchemy.orm import Session

from rdr_service.dao.base_dao import with_session
from rdr_service.dao.duplicate_account_dao import DuplicateAccountDao, DuplicationSource, DuplicateExistsException
from rdr_service.model.participant_summary import ParticipantSummary


class _SummaryCache:
    def __init__(self, session: Session):
        self._session = session
        self._name_dob_cache = defaultdict(         # top level dict for keying by first name
            lambda: defaultdict(                    # next level dict for keying by last name
                lambda: defaultdict(                # next level dict for keying by dob
                    list                            # list to hold any participant ids that have the above data
                )
            )
        )
        self._email_cache = defaultdict(list)
        self._phone_cache = defaultdict(list)
        self._load_data()

    def _load_data(self):
        for summary in DuplicateAccountDao.query_participant_duplication_data(self._session):
            self._name_dob_cache[summary.firstName][summary.lastName][summary.dateOfBirth].append(summary.participantId)
            if summary.email:
                self._email_cache[summary.email].append(summary.participantId)
            if summary.loginPhoneNumber:
                self._phone_cache[summary.loginPhoneNumber].append(summary.participantId)

    def get_matching_participant_ids(self, summary: ParticipantSummary) -> List[int]:
        result = set()
        name_dob_matches = (
            self._name_dob_cache
            .get(summary.firstName, {})
            .get(summary.lastName, {})
            .get(summary.dateOfBirth, [])
        )
        result.update(name_dob_matches)
        result.update(self._email_cache[summary.email])
        result.update(self._phone_cache[summary.loginPhoneNumber])
        return [participant_id for participant_id in result if participant_id != summary.participantId]


class DuplicateDetection:
    @classmethod
    @with_session
    def find_duplicates(cls, since: datetime, session: Session):
        """
        Find and record duplicate accounts since the provided timestamp.
        Will load any participants modified since the timestamp and compare
        them to all other participants to find and record any new duplicates.
        """

        cache = _SummaryCache(session)
        recently_modified_summaries = DuplicateAccountDao.query_participants_to_check(session=session, since=since)
        for summary in recently_modified_summaries:
            duplicate_id_list = cache.get_matching_participant_ids(summary)
            for duplicate_id in duplicate_id_list:
                cls._record_as_duplicate(
                    participant_a_id=summary.participantId,
                    participant_b_id=duplicate_id,
                    session=session
                )

    @classmethod
    def _record_as_duplicate(cls, participant_a_id: int, participant_b_id: int, session: Session):
        try:
            DuplicateAccountDao.store_duplication(
                participant_a_id=participant_a_id,
                participant_b_id=participant_b_id,
                authored=datetime.utcnow(),
                source=DuplicationSource.RDR,
                session=session
            )
        except DuplicateExistsException:
            pass  # ignore any that are already flagged as duplicates
