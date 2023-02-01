from rdr_service.dao.rex_dao import RexParticipantMappingDao
from rdr_service.dao.study_nph_dao import NphParticipantDao
from rdr_service.config import NPH_STUDY_ID, AOU_STUDY_ID


class EnrollmentInterface:
    def __init__(self, study_code):
        # Maps PTSC study IDs to ancillary study ID and study participant dao
        self.study_code = study_code

    def create_study_participant(self, aou_pid, ancillary_pid):
        cln_study_pid = int(ancillary_pid[4:])
        # TODO: Implement random biobank/research IDs.
        insert_params = {
            'id': cln_study_pid,
            'biobank_id': int(f"1{cln_study_pid}"),
            'research_id': int(f"2{cln_study_pid}"),
        }
        self.participant_dao.insert(self.participant_dao.model_type(**insert_params))
        self.create_rex_participant_mapping(aou_pid, cln_study_pid)

    def create_rex_participant_mapping(self, aou_pid, study_pid):
        rex_dao = RexParticipantMappingDao()
        insert_params = {
            'primary_study_id': AOU_STUDY_ID,
            'ancillary_study_id': self.ancillary_study_id,
            'primary_participant_id': aou_pid,
            'ancillary_participant_id': study_pid
        }
        rex_dao.insert(rex_dao.model_type(**insert_params))

    @property
    def participant_dao(self):
        if self.study_code == 'NPH-1000':
            return NphParticipantDao()

    @property
    def ancillary_study_id(self):
        if self.study_code == 'NPH-1000':
            return NPH_STUDY_ID
