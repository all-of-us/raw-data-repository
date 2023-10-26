from datetime import datetime

from rdr_service.clock import FakeClock
from rdr_service.dao.pediatric_data_log_dao import PediatricDataLogDao
from rdr_service.model.pediatric_data_log import PediatricDataLog, PediatricDataType
from tests.helpers.unittest_base import BaseTestCase


class PediatricDataLogDaoTest(BaseTestCase):
    def test_insert_updates_previous(self):
        """Any newly inserted records should replace the previous data of the same type"""
        participant = self.data_generator.create_database_participant()

        # set up some pediatric data records for the participant
        august_record = PediatricDataLog(
            participant_id=participant.participantId,
            created=datetime(2022, 8, 17),
            data_type=PediatricDataType.AGE_RANGE,
            value='Second'
        )
        self.session.add(august_record)
        march_record = PediatricDataLog(
            participant_id=participant.participantId,
            created=datetime(2022, 3, 20),
            data_type=PediatricDataType.AGE_RANGE,
            value='First',
            replaced_by=august_record
        )
        self.session.add(march_record)
        self.session.commit()

        # create a new record to replace the most recent
        latest_record = PediatricDataLog(
            participant_id=participant.participantId,
            data_type=PediatricDataType.AGE_RANGE,
            value='newest'
        )
        PediatricDataLogDao.insert(latest_record)

        # check that the last record inserted was set as the replacement
        self.session.refresh(august_record)
        self.assertEqual(latest_record.id, august_record.replaced_by_id)

    def test_insert_duplicate(self):
        """
        If the value being inserted matches the latest value (for the given data type)
        don't insert the duplicate record
        """
        participant = self.data_generator.create_database_participant()

        existing_record = PediatricDataLog(
            participant_id=participant.participantId,
            created=datetime(2022, 8, 17),
            data_type=PediatricDataType.AGE_RANGE,
            value='testing_duplicate'
        )
        self.session.add(existing_record)
        self.session.commit()

        PediatricDataLogDao.insert(
            PediatricDataLog(
                participant_id=participant.participantId,
                data_type=PediatricDataType.AGE_RANGE,
                value='testing_duplicate'
            )
        )

        pediatric_data = self.session.query(PediatricDataLog).filter(
            PediatricDataLog.participant_id == participant.participantId
        ).all()
        self.assertEqual(1, len(pediatric_data))
        self.assertEqual(existing_record.id, pediatric_data[0].id)

    def test_age_sets_last_modified(self):
        """
        Setting a new pediatric age range on a participant will set the summary's value of is_pediatric to True.
        So we should update the lastModified time as well.
        """
        pediatric_summary = self.data_generator.create_database_participant_summary()

        timestamp = datetime(2018, 7, 10)
        with FakeClock(timestamp):
            PediatricDataLogDao.record_age_range(
                participant_id=pediatric_summary.participantId,
                age_range_str='SIX_AND_BELOW',
                session=self.session
            )

        self.assertEqual(timestamp, pediatric_summary.lastModified)
