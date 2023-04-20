from rdr_service.ancillary_study_resources.nph.enums import Activity, EnrollmentEventTypes
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.rex_dao import RexParticipantMappingDao
from rdr_service.dao.study_nph_dao import NphParticipantDao
from rdr_service.config import NPH_STUDY_ID, AOU_STUDY_ID, GAE_PROJECT


class EnrollmentInterface:
    def __init__(self, study_code):
        self.study_code = study_code

    def create_study_participant(self, aou_pid, ancillary_pid, event_authored_time):
        # We have to use the AoU research ID
        aou_participant_dao = ParticipantDao()
        aou_participant = aou_participant_dao.get(aou_pid)
        insert_params = {
            'id': ancillary_pid,
            'research_id': aou_participant.researchId,
        }

        self.participant_dao.insert_participant_with_random_biobank_id(
            self.participant_dao.model_type(**insert_params))
        self.create_rex_participant_mapping(aou_pid, ancillary_pid)

        enrollment_data = {
            'study': 'nph',
            'participant_id': ancillary_pid,
            'activity_id': Activity.ENROLLMENT.number,
            'event_type_id': EnrollmentEventTypes.REFERRED.number,
            'event_authored_time': event_authored_time
        }

        if GAE_PROJECT != 'localhost':
            cloud_task = GCPCloudTask()
            cloud_task.execute(
                endpoint='insert_study_event_task',
                payload=enrollment_data,
                queue='nph'
            )

    def create_rex_participant_mapping(self, aou_pid, study_pid):
        rex_dao = RexParticipantMappingDao()
        insert_params = {
            'primary_study_id': AOU_STUDY_ID,
            'ancillary_study_id': self.ancillary_study_id,
            'primary_participant_id': aou_pid,
            'ancillary_participant_id': study_pid,
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
