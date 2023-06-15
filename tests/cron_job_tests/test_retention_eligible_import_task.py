from datetime import datetime
import mock
import pytz
from typing import Optional

from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.offline import retention_eligible_import
from rdr_service.participant_enums import RetentionStatus, RetentionType
from tests.helpers.unittest_base import BaseTestCase

TIME_1 = datetime(2020, 4, 1)


class RetentionEligibleImportTest(BaseTestCase):

    @mock.patch('rdr_service.offline.retention_eligible_import.GoogleCloudStorageCSVReader')
    def test_retention_eligible_import(self, csv_reader):
        ps1 = self.data_generator.create_database_participant_summary()
        ps2 = self.data_generator.create_database_participant_summary()
        ps3 = self.data_generator.create_database_participant_summary()
        ps4 = self.data_generator.create_database_participant_summary()

        csv_reader.return_value = [
            self._build_csv_row(
                participant_id=ps1.participantId,
                retention_eligible='1',
                retention_eligible_date='2020-02-20',
                actively_retained='1',
                last_active_retention_activity_date='2020-02-10',
                passively_retained='0'
            ),
            self._build_csv_row(
                participant_id=ps2.participantId,
                retention_eligible='1',
                retention_eligible_date='2020-02-20',
                actively_retained='0',
                last_active_retention_activity_date='2020-02-10',
                passively_retained='1'
            ),
            self._build_csv_row(
                participant_id=ps3.participantId,
                retention_eligible='1',
                retention_eligible_date='2020-02-20',
                actively_retained='1',
                last_active_retention_activity_date='2020-02-10',
                passively_retained='1'
            ),
            self._build_csv_row(
                participant_id=ps4.participantId,
                retention_eligible='0',
                retention_eligible_date='NULL',
                actively_retained='0',
                last_active_retention_activity_date='NULL',
                passively_retained='0'
            )
        ]

        test_date = datetime(2020, 10, 13)
        pytz.timezone('US/Central').localize(test_date)

        retention_eligible_import.import_retention_eligible_metrics_file({
            "bucket": 'test_bucket',
            "upload_date": test_date.isoformat(),
            "file_path": 'test_bucket/test_file.csv'
        })

        eligible_date = datetime(2020, 2, 20)
        last_active_date = datetime(2020, 2, 10)
        self._assert_summary_retention_fields(
            summary=ps1,
            status=RetentionStatus.ELIGIBLE,
            eligible_time=eligible_date,
            last_activity_time=last_active_date,
            retention_type=RetentionType.ACTIVE
        )
        self._assert_summary_retention_fields(
            summary=ps2,
            status=RetentionStatus.ELIGIBLE,
            eligible_time=eligible_date,
            last_activity_time=last_active_date,
            retention_type=RetentionType.PASSIVE
        )
        self._assert_summary_retention_fields(
            summary=ps3,
            status=RetentionStatus.ELIGIBLE,
            eligible_time=eligible_date,
            last_activity_time=last_active_date,
            retention_type=RetentionType.ACTIVE_AND_PASSIVE
        )
        self._assert_summary_retention_fields(
            summary=ps4,
            status=RetentionStatus.NOT_ELIGIBLE,
            eligible_time=None,
            last_activity_time=None,
            retention_type=None
        )

        # closing anything the session has open to prep for the next phase of the test
        self.session.commit()

        # test update with new file
        csv_reader.return_value = [
            self._build_csv_row(
                participant_id=ps1.participantId,
                retention_eligible='1',
                retention_eligible_date='2020-03-20',
                actively_retained='0',
                last_active_retention_activity_date='2020-03-10',
                passively_retained='1'
            ),
            self._build_csv_row(
                participant_id=ps2.participantId,
                retention_eligible='0',
                retention_eligible_date='NULL',
                actively_retained='0',
                last_active_retention_activity_date='NULL',
                passively_retained='0'
            ),
            self._build_csv_row(
                participant_id=ps3.participantId,
                retention_eligible='1',
                retention_eligible_date='2020-02-20',
                actively_retained='1',
                last_active_retention_activity_date='2020-02-10',
                passively_retained='1'
            ),
            self._build_csv_row(
                participant_id=ps4.participantId,
                retention_eligible='1',
                retention_eligible_date='2020-03-20',
                actively_retained='1',
                last_active_retention_activity_date='2020-03-10',
                passively_retained='0'
            )
        ]

        test_date = datetime(2021, 10, 20)
        pytz.timezone('US/Central').localize(test_date)
        retention_eligible_import.import_retention_eligible_metrics_file({
            "bucket": 'test_bucket',
            "upload_date": test_date.isoformat(),
            "file_path": 'test_bucket/test_file.csv'
        })

        self._assert_summary_retention_fields(
            summary=ps1,
            status=RetentionStatus.ELIGIBLE,
            eligible_time=datetime(2020, 3, 20),
            last_activity_time=datetime(2020, 3, 10),
            retention_type=RetentionType.PASSIVE
        )
        self._assert_summary_retention_fields(
            summary=ps2,
            status=RetentionStatus.NOT_ELIGIBLE,
            eligible_time=None,
            last_activity_time=None,
            retention_type=None
        )
        self._assert_summary_retention_fields(
            summary=ps3,
            status=RetentionStatus.ELIGIBLE,
            eligible_time=datetime(2020, 2, 20),
            last_activity_time=datetime(2020, 2, 10),
            retention_type=RetentionType.ACTIVE_AND_PASSIVE
        )
        self._assert_summary_retention_fields(
            summary=ps4,
            status=RetentionStatus.ELIGIBLE,
            eligible_time=datetime(2020, 3, 20),
            last_activity_time=datetime(2020, 3, 10),
            retention_type=RetentionType.ACTIVE
        )

        # Check that the update didn't create a new metrics object
        first_metric_obj_list = self.session.query(RetentionEligibleMetrics).filter(
            RetentionEligibleMetrics.participantId == ps1.participantId
        ).all()
        self.assertEqual(1, len(first_metric_obj_list))

        ps = self.send_get("ParticipantSummary?retentionEligibleStatus=NOT_ELIGIBLE&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 1)
        ps = self.send_get("ParticipantSummary?retentionEligibleStatus=ELIGIBLE&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 3)
        ps = self.send_get("ParticipantSummary?retentionType=ACTIVE_AND_PASSIVE&retentionEligibleStatus=ELIGIBLE"
                           "&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 1)
        ps = self.send_get("ParticipantSummary?retentionType=PASSIVE&retentionEligibleStatus=ELIGIBLE"
                           "&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 1)
        ps = self.send_get("ParticipantSummary?retentionType=UNSET&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 1)
        ps = self.send_get("ParticipantSummary?retentionType=UNSET&retentionEligibleStatus=NOT_ELIGIBLE"
                           "&_includeTotal=TRUE")
        self.assertEqual(len(ps['entry']), 1)

    def test_lower_env_retention_metric_cronjob(self):
        ps1 = self.data_generator.create_database_participant_summary()
        ps2 = self.data_generator.create_database_participant_summary(
            consentForStudyEnrollmentAuthored=TIME_1,
            sampleStatus1ED10Time=TIME_1,
            questionnaireOnTheBasicsAuthored=TIME_1,
            questionnaireOnOverallHealthAuthored=TIME_1,
            questionnaireOnLifestyleAuthored=TIME_1,
            consentForElectronicHealthRecordsAuthored=TIME_1,
            consentForDvElectronicHealthRecordsSharingAuthored=TIME_1,
            consentForStudyEnrollment=1,
            consentForElectronicHealthRecords=1,
            questionnaireOnTheBasics=1,
            questionnaireOnOverallHealth=1,
            questionnaireOnLifestyle=1,
            withdrawalStatus=1,
            suspensionStatus=1,
            samplesToIsolateDNA=1
        )

        retention_window = datetime.timedelta(days=100)
        in_eighteen_month = datetime.now() - retention_window
        ps3 = self.data_generator.create_database_participant_summary(
            consentForStudyEnrollmentAuthored=TIME_1,
            sampleStatus1ED10Time=TIME_1,
            questionnaireOnTheBasicsAuthored=TIME_1,
            questionnaireOnOverallHealthAuthored=TIME_1,
            questionnaireOnLifestyleAuthored=TIME_1,
            consentForElectronicHealthRecordsAuthored=TIME_1,
            consentForDvElectronicHealthRecordsSharingAuthored=TIME_1,
            questionnaireOnHealthcareAccessAuthored=in_eighteen_month,
            consentForStudyEnrollment=1,
            consentForElectronicHealthRecords=1,
            questionnaireOnTheBasics=1,
            questionnaireOnOverallHealth=1,
            questionnaireOnLifestyle=1,
            withdrawalStatus=1,
            suspensionStatus=1,
            samplesToIsolateDNA=1
        )

        ps4 = self.data_generator.create_database_participant_summary(
            consentForStudyEnrollmentAuthored=TIME_1,
            sampleStatus1ED10Time=TIME_1,
            questionnaireOnTheBasicsAuthored=TIME_1,
            questionnaireOnOverallHealthAuthored=TIME_1,
            questionnaireOnLifestyleAuthored=TIME_1,
            consentForElectronicHealthRecordsAuthored=TIME_1,
            consentForDvElectronicHealthRecordsSharingAuthored=TIME_1,
            questionnaireOnHealthcareAccessAuthored=in_eighteen_month,
            ehrUpdateTime=in_eighteen_month,
            consentForStudyEnrollment=1,
            consentForElectronicHealthRecords=1,
            questionnaireOnTheBasics=1,
            questionnaireOnOverallHealth=1,
            questionnaireOnLifestyle=1,
            withdrawalStatus=1,
            suspensionStatus=1,
            samplesToIsolateDNA=1
        )

        retention_eligible_import.calculate_retention_eligible_metrics()

        p1 = self.send_get(f'Participant/P{ps1.participantId}/Summary')
        p2 = self.send_get(f'Participant/P{ps2.participantId}/Summary')
        p3 = self.send_get(f'Participant/P{ps3.participantId}/Summary')
        p4 = self.send_get(f'Participant/P{ps4.participantId}/Summary')

        self.assertEqual(p1['retentionEligibleStatus'], str(RetentionStatus.NOT_ELIGIBLE))
        self.assertEqual(p1['retentionType'], str(RetentionType.UNSET))

        self.assertEqual(p2['retentionEligibleStatus'], str(RetentionStatus.ELIGIBLE))
        self.assertEqual(p2['retentionEligibleTime'], TIME_1.strftime("%Y-%m-%dT%H:%M:%S"))
        self.assertEqual(p2['retentionType'], str(RetentionType.PASSIVE))

        self.assertEqual(p3['retentionEligibleStatus'], str(RetentionStatus.ELIGIBLE))
        self.assertEqual(p3['retentionEligibleTime'], TIME_1.strftime("%Y-%m-%dT%H:%M:%S"))
        self.assertEqual(p3['retentionType'], str(RetentionType.ACTIVE))

        self.assertEqual(p4['retentionEligibleStatus'], str(RetentionStatus.ELIGIBLE))
        self.assertEqual(p4['retentionEligibleTime'], TIME_1.strftime("%Y-%m-%dT%H:%M:%S"))
        self.assertEqual(p4['retentionType'], str(RetentionType.ACTIVE_AND_PASSIVE))

    @classmethod
    def _build_csv_row(
        cls, participant_id: str, retention_eligible: str, retention_eligible_date: str, actively_retained: str,
        last_active_retention_activity_date: str, passively_retained: str
    ):
        return {
            'participant_id': participant_id,
            'retention_eligible': retention_eligible,
            'retention_eligible_date': retention_eligible_date,
            'actively_retained': actively_retained,
            'last_active_retention_activity_date': last_active_retention_activity_date,
            'passively_retained': passively_retained,
            'UBR': '0',
            'UBR1_RaceEthnicity': '0',
            'UBR2_Age': '0',
            'UBR3_Sex': '0',
            'UBR4_SexualAndGenderMinorities': '0',
            'UBR5_Income': '0',
            'UBR6_Education': '0',
            'UBR7_Geography': '0',
            'UBR8_AccessToCare': '0',
            'UBR9_Disability': '0'
        }

    def _get_participant_summary(self, participant_id):
        return self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == participant_id
        ).one()

    def _assert_summary_retention_fields(
        self, summary: ParticipantSummary, status: RetentionStatus, eligible_time,
        last_activity_time, retention_type: Optional[RetentionType]
    ):
        self.session.refresh(summary)
        self.assertEqual(status, summary.retentionEligibleStatus)
        self.assertEqual(eligible_time, summary.retentionEligibleTime)
        self.assertEqual(last_activity_time, summary.lastActiveRetentionActivityTime)
        self.assertEqual(retention_type, summary.retentionType)
