#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from datetime import datetime
from dateutil import parser
from tests.helpers.unittest_base import BaseTestCase

from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.participant_enums import RetentionType, RetentionStatus
from rdr_service.resource.generators import RetentionEligibleMetricGenerator


class RetentionMetricGeneratorTest(BaseTestCase):

    def setUp(self, with_data=True, with_consent_codes=False) -> None:
        super().setUp(with_data, with_consent_codes)

        self.timestamp = datetime(2021, 7, 10, 10, 30, 0, 0)
        self.participant_id = int(self.create_participant()[1:])

        self.rem = RetentionEligibleMetrics(
            id = 1,
            created = None,
            modified = None,
            participantId = self.participant_id,
            retentionEligible = True,
            retentionEligibleTime = self.timestamp,
            activelyRetained = True,
            passivelyRetained = True,
            fileUploadDate = self.timestamp,
            retentionEligibleStatus = RetentionStatus.ELIGIBLE,
            retentionType = RetentionType.ACTIVE_AND_PASSIVE
        )

    def test_retention_metric_generator(self):
        """ Test the retention metric generator """

        # Test that the number of fields in the DAO model has not changed.
        # This test is to make sure the resource model is updated when the SA model has been changed.
        column_count = len(RetentionEligibleMetrics.__table__.columns)
        self.assertEqual(column_count, 11)

        self.session.add(self.rem)
        self.session.commit()

        gen = RetentionEligibleMetricGenerator()
        res = gen.make_resource(self.participant_id)
        self.assertIsNotNone(res)

        data = res.get_resource()
        self.assertIsInstance(data, dict)
        # Check the resource field count. note: enums fields count as 2 fields in a resource.
        self.assertEqual(len(data.keys()), 13)

        self.assertEqual(data['id'], 1)
        self.assertEqual(parser.parse(data['retention_eligible_time']), self.timestamp)
        self.assertEqual(parser.parse(data['file_upload_date']), self.timestamp)
        self.assertEqual(data['participant_id'], f'P{self.participant_id}')
