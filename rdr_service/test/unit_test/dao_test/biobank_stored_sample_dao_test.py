from rdr_service import clock
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.participant import Participant
from rdr_service.test.unit_test.unit_test_util import SqlTestBase


# TODO: represent in new test suite
class BiobankStoredSampleDaoTest(SqlTestBase):
    """Tests only that a sample can be written and read; see the reconciliation pipeline."""

    def setUp(self):
        super(BiobankStoredSampleDaoTest, self).setUp()
        self.participant = Participant(participantId=123, biobankId=555)
        ParticipantDao().insert(self.participant)
        self.dao = BiobankStoredSampleDao()

    def test_insert_and_read_sample(self):
        sample_id = "WEB123456"
        test_code = "1U234"
        now = clock.CLOCK.now()
        created = self.dao.insert(
            BiobankStoredSample(
                biobankStoredSampleId=sample_id,
                biobankId=self.participant.biobankId,
                biobankOrderIdentifier="KIT",
                test=test_code,
                confirmed=now,
            )
        )
        fetched = self.dao.get(sample_id)
        self.assertEqual(test_code, created.test)
        self.assertEqual(test_code, fetched.test)
