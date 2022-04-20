from datetime import datetime, timedelta

from rdr_service.model.biobank_order import BiobankSpecimen
from rdr_service.model.biobank_stored_sample import BiobankStoredSample, SampleStatus
from rdr_service.tools.tool_libs.biobank_data_comparison import BiobankSampleComparator, DifferenceType, SamplePair
from tests.helpers.unittest_base import BaseTestCase


class BiobankDataComparisonTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BiobankDataComparisonTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs):
        super(BiobankDataComparisonTest, self).setUp(*args, **kwargs)

        biobank_id = 123123123
        test_code = '1U25'
        order_id = 'KIT-7890'
        confirmed_time = datetime(2017, 11, 5, 18, 14)
        disposed_time = datetime(2017, 11, 7, 7, 0)

        self.api_data = BiobankSpecimen(
            biobankId=biobank_id,
            testCode=test_code,
            orderId=order_id,
            confirmedDate=confirmed_time,
            disposalDate=disposed_time,
            status='Disposed',
            disposalReason='Consumed'
        )
        self.report_data = BiobankStoredSample(
            biobankId=biobank_id,
            test=test_code,
            biobankOrderIdentifier=order_id,
            confirmed=confirmed_time,
            disposed=disposed_time,
            status=SampleStatus.CONSUMED,
            rdrCreated=confirmed_time
        )
        self.comparator = BiobankSampleComparator(SamplePair(api_data=self.api_data, report_data=self.report_data))

    def test_no_difference_in_defaults(self):
        """No discrepancies should be found in the default data"""
        self.assertEqual([], self.comparator.get_differences())

    def test_biobank_id_comparison(self):
        """Test finding difference in biobank id"""
        self.report_data.biobankId = 1122

        differences = self.comparator.get_differences()
        self.assertEqual(1, len(differences))
        self.assertEqual(DifferenceType.BIOBANK_ID, differences[0].type)

    def test_code_comparison(self):
        """Check that a difference of test codes is found"""
        self.api_data.testCode = '5bob'

        differences = self.comparator.get_differences()
        self.assertEqual(1, len(differences))
        self.assertEqual(DifferenceType.TEST_CODE, differences[0].type)

    def test_order_id_comparison(self):
        """Test that a difference of order ids is found"""
        self.api_data.orderId = '7878787'

        differences = self.comparator.get_differences()
        self.assertEqual(1, len(differences))
        self.assertEqual(DifferenceType.ORDER_ID, differences[0].type)

    def test_confirmed_date_comparison(self):
        # Check that the confirmed dates can be slightly different
        self.report_data.confirmed = self.api_data.confirmedDate + timedelta(minutes=3)
        self.assertEqual([], self.comparator.get_differences())

        # Check that a few hours difference is flagged
        self.report_data.confirmed = self.api_data.confirmedDate + timedelta(hours=3)
        differences = self.comparator.get_differences()
        self.assertEqual(1, len(differences))
        self.assertEqual(DifferenceType.CONFIRMED_DATE, differences[0].type)

    def test_disposed_date_comparison(self):
        self.report_data.disposed = self.api_data.disposalDate + timedelta(days=3)
        differences = self.comparator.get_differences()
        self.assertEqual(1, len(differences))
        self.assertEqual(DifferenceType.DISPOSAL_DATE, differences[0].type)

    def test_missing_from_report(self):
        comparator = BiobankSampleComparator(SamplePair(report_data=None, api_data=self.api_data))
        differences = comparator.get_differences()
        self.assertEqual(1, len(differences))
        self.assertEqual(DifferenceType.MISSING_FROM_SIR, differences[0].type)

    def test_missing_from_api(self):
        comparator = BiobankSampleComparator(SamplePair(report_data=self.report_data, api_data=None))
        differences = comparator.get_differences()
        self.assertEqual(1, len(differences))
        self.assertEqual(DifferenceType.MISSING_FROM_API_DATA, differences[0].type)

    def test_status_difference(self):
        """Check that a difference in status is found"""
        self.report_data.status = SampleStatus.ACCESSINGING_ERROR

        differences = self.comparator.get_differences()
        self.assertEqual(1, len(differences))
        self.assertEqual(DifferenceType.STATUS, differences[0].type)

    def test_status_differences_allowed(self):
        # 1PXR2 samples show as "Unknown" from the reports, but are in circulation according to the API
        self.api_data.testCode = self.report_data.test = '1PXR2'
        self.report_data.status = SampleStatus.UNKNOWN
        self.api_data.status = 'In Circulation'
        self.assertEqual([], self.comparator.get_differences())

    def test_ignoring_outdated_sir_status(self):
        # Samples that are disposed after 10 days of first appearing on the SIR will not be updated by the SIR
        # process. So the SIR data is showing these samples as received still, but the API data will show them
        # as disposed. This should be ignored from the comparison results.
        self.report_data.status = SampleStatus.RECEIVED
        self.report_data.rdrCreated = self.api_data.disposalDate - timedelta(days=13)

        differences = self.comparator.get_differences()
        self.assertEqual(0, len(differences))
