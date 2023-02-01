from rdr_service.dao.study_nph_dao import NphParticipantDao
from rdr_service.config import NPH_STUDY_ID


class EnrollmentInterface:
    def __init__(self, study_code):
        # Maps PTSC study IDs to ancillary study ID and study participant dao
        self.study_code = study_code

    def create_study_participant(self, ancillary_pid):
        insert_params = {
            'id': ancillary_pid[4:],
            'biobank_id': '',
            'research_id': '',
        }
        ancillary_participant = self.study_dao.insert(self.study_dao.model_type(
            **insert_params
        ))
        self.create_rex_participant_mapping(ancillary_participant)


    def create_rex_participant_mapping(self, ancillary_participant):
        pass

    @property
    def study_dao(self):
        if self.study_code == 'NPH-1000':
            return NphParticipantDao()

    @property
    def ancillary_study_id(self):
        if self.study_code == 'NPH-1000':
            return NPH_STUDY_ID
