from datetime import datetime

from rdr_service.dao import database_factory
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
