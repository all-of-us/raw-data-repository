# pylint: disable=unused-import
import datetime
from unittest import mock

from rdr_service import config
from rdr_service.dao.awardee_insite_dao import AwardeeInSiteDao
from rdr_service.model.awardee_insite import AwardeeInSite
from tests.service_tests.test_genomic_datagen import GenomicDataGenMixin
from rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed import InputFeed, Intake2SummaryFeed, \
    AwardeeInSiteFeed


class AwardeeInSiteDataFeedTest(GenomicDataGenMixin):
    def setUp(self, *args, **kwargs):
        # pylint: disable=unused-argument
        super().setUp()

    @mock.patch("rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed.AwardeeInSiteFeed.row_to_dict")
    @mock.patch("google.cloud.bigquery.Client")
    def test_run_datafeed(self, mock_bq_client, mock_row_to_dict):

        record = [{
            "participantId": 12345,
            "firstName": "John",
            "middleName": "Sam",
            "lastName": "Doe",
            "zipCode": "77454",
            "state": "TX",
            "city": "Houston",
            "streetAddress": "123 Lake Dr",
            "streetAddress2": "Apt 34",
            "phoneNumber": "1234567890",
            "email": "john@example.com",
            "dateOfBirth": "1992-06-08",
            "organization": "PA",
            "withdrawalStatus": "not_withdrawn",
            "withdrawalTime": "2024-11-21T18:12:00",
            "deactivationStatus": "not_deactivated",
            "deactivationTime": "2024-11-21T18:12:00",
            "deceasedStatus": "unset",
            "deceasedAuthored": "2024-11-21T18:12:00",
            "consentForElectronicHealthRecords": "yes",
            "consentForElectronicHealthRecordsAuthored": "2024-11-21T18:12:00",
            "firstEhrReceiptTime": "2024-11-25T18:12:00",
            "latestEhrReceiptTime": "2024-11-26T18:12:00",
            "consentForStudyEnrollment": "no",
            "patientStatus": []
        }]

        mock_bq_instance = mock_bq_client.return_value
        mock_bq_instance.query.return_value.result.return_value = record
        mock_row_to_dict.return_value = record[0]

        # Test if its correctly inserted
        awardee_insite_feed = AwardeeInSiteFeed()
        awardee_insite_feed.get_datafeed_definition = mock.Mock(return_value={
            "staging_data_sql": "staging_sql",
            "streaming_data_sql": "streaming_sql",
            "destination_model": AwardeeInSite
        })

        awardee_insite_feed.run_datafeed("awardee_insite")

        awardee_insite_dao = AwardeeInSiteDao()
        actual_rows = awardee_insite_dao.get_all()

        self.assertEqual(actual_rows[0].participantId, 12345)
        self.assertEqual(actual_rows[0].firstName, "John")
        self.assertEqual(actual_rows[0].middleName, "Sam")
        self.assertEqual(actual_rows[0].lastName, "Doe")
        self.assertEqual(actual_rows[0].zipCode, "77454")
        self.assertEqual(actual_rows[0].state, "TX")
        self.assertEqual(actual_rows[0].city, "Houston")
        self.assertEqual(actual_rows[0].streetAddress, "123 Lake Dr")
        self.assertEqual(actual_rows[0].streetAddress2, "Apt 34")
        self.assertEqual(actual_rows[0].phoneNumber, "1234567890")
        self.assertEqual(actual_rows[0].email, "john@example.com")
        self.assertEqual(actual_rows[0].dateOfBirth, datetime.date(1992, 6, 8))
        self.assertEqual(actual_rows[0].organization, "PA")
        self.assertEqual(actual_rows[0].withdrawalStatus, "not_withdrawn")
        self.assertEqual(actual_rows[0].withdrawalTime, datetime.datetime(2024, 11, 21, 18, 12))
        self.assertEqual(actual_rows[0].deactivationStatus, "not_deactivated")
        self.assertEqual(actual_rows[0].deactivationTime, datetime.datetime(2024, 11, 21, 18, 12))
        self.assertEqual(actual_rows[0].consentForElectronicHealthRecords, "yes")
        self.assertEqual(actual_rows[0].consentForElectronicHealthRecordsAuthored, datetime.datetime(2024, 11, 21, 18, 12))
        self.assertEqual(actual_rows[0].consentForStudyEnrollment,"no")
        self.assertEqual(actual_rows[0].deceasedStatus, "unset")
        self.assertEqual(actual_rows[0].deceasedAuthored, datetime.datetime(2024, 11, 21, 18, 12))
        self.assertEqual(actual_rows[0].patientStatus, [])

        ####################################################
        # Test updating the middle name of an existing record
        updated_record = [{
            "participantId": 12345,
            "firstName": "John",
            "middleName": "Samuel",
            "state": "NY",
            "patientStatus": []
        }]

        mock_bq_instance.query.return_value.result.return_value = updated_record
        mock_row_to_dict.return_value = updated_record[0]

        awardee_insite_feed.run_datafeed("awardee_insite")

        updated_rows = awardee_insite_dao.get_all()

        self.assertEqual(updated_rows[0].participantId, 12345)
        self.assertEqual(updated_rows[0].middleName, "Samuel")

