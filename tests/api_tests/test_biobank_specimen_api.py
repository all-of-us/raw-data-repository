import datetime
from rdr_service.dao.biobank_specimen_dao import BiobankSpecimenDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant import Participant
from rdr_service.model.biobank_order import BiobankSpecimen
from tests.helpers.unittest_base import BaseTestCase

TIME_1 = datetime.datetime(2020, 4, 1)
TIME_2 = datetime.datetime(2020, 4, 2)


class BiobankOrderApiTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.participant = Participant(participantId=123, biobankId=555)
        self.participant_dao = ParticipantDao()
        self.participant_dao.insert(self.participant)
        self.summary_dao = ParticipantSummaryDao()
        self.dao = BiobankSpecimenDao()
        self.specimen = BiobankSpecimen(rlimsId='sabrina', participantId=self.participant.participantId,
                                        orderId='', testCode='1234567', repositoryId='repo id', studyId='study id',
                                        cohortId='cohort id', collectionDate=TIME_1, confirmedDate=TIME_2)
        self.specimen_path = f"Biobank/specimens/{self.specimen.rlimsId}"
        #self.specimen_path = f"Biobank/specimens"


    def test_put_specimen(self):
        payload = self.dao.to_client_json(self.specimen)
        result = self.send_put(self.specimen_path, request_data=payload, headers={"if-match": 'W/"1"' })
