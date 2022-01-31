from datetime import date
import mock

from rdr_service import config
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.tools.tool_libs.consents import ConsentTool
from tests.helpers.tool_test_mixin import ToolTestMixin
from tests.helpers.unittest_base import BaseTestCase


@mock.patch('rdr_service.tools.tool_libs.consents.logger')
class ConsentsTest(ToolTestMixin, BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(ConsentsTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs) -> None:
        super(ConsentsTest, self).setUp(*args, **kwargs)

        consent_dao_patcher = mock.patch('rdr_service.tools.tool_libs.consents.ConsentDao')
        self.consent_dao_mock = consent_dao_patcher.start().return_value
        self.addCleanup(consent_dao_patcher.stop)
        # Patching other DAOs to keep them from trying to connect to the DB
        for name_to_patch in ['ParticipantSummaryDao', 'HPODao']:
            dao_patch = mock.patch(f'rdr_service.tools.tool_libs.consents.{name_to_patch}')
            dao_patch.start()
            self.addCleanup(dao_patch.stop)

        self.consent_dao_mock.get_files_needing_correction.return_value = [
            ConsentFile(
                id=1, participant_id=123123123, type=ConsentType.PRIMARY, file_exists=False
            ),
            ConsentFile(
                id=2, participant_id=222333444, type=ConsentType.CABOR, file_exists=False
            ),
            ConsentFile(
                id=3, participant_id=222333444, type=ConsentType.GROR, file_exists=True,
                is_signature_valid=False, is_signing_date_valid=True, other_errors='missing checkmark',
                file_path='test_bucket/P222/GROR_no_checkmark_or_signature.pdf'
            ),
            ConsentFile(
                id=4, participant_id=654321123, type=ConsentType.CABOR, file_exists=True,
                is_signature_valid=True, is_signing_date_valid=False,
                signing_date=date(2021, 12, 1), expected_sign_date=date(2020, 12, 1),
                file_path='test_bucket/P654/Cabor_bad_date.pdf'
            ),
            ConsentFile(
                id=5, participant_id=901987345, type=ConsentType.EHR, file_exists=True,
                is_signature_valid=False, is_signing_date_valid=True,
                file_path='test_bucket/P901/EHR_no_signature.pdf'
            )
        ]

    def _run_consents_tool(self, command, verbose=False, additional_args=None):
        with mock.patch('rdr_service.tools.tool_libs.consents.GoogleCloudStorageProvider') as storage_provider_mock:
            def blob_that_gives_url(bucket_name, blob_name):
                blob_mock = mock.MagicMock()
                blob_mock.generate_signed_url.return_value = f'https://example.com/{bucket_name}/{blob_name}'
                return blob_mock
            storage_provider_mock.return_value.get_blob.side_effect = blob_that_gives_url

            tool_args = {
                'command': command,
                'since': None,
                'verbose': verbose
            }
            if additional_args:
                tool_args.update(additional_args)

            self.run_tool(
                ConsentTool,
                tool_args,
                mock_session=True,
                server_config={
                    config.CONSENT_PDF_BUCKET: {
                        'vibrent': 'test-bucket-name'
                    }
                }
            )

    def test_report_to_send_to_ptsc(self, logger_mock):
        """Check the basic report format, the one that would be sent to Vibrent or CE for correcting"""
        with mock.patch('rdr_service.tools.tool_libs.consents.input'):
            self._run_consents_tool(command='report-errors')
            logger_mock.info.assert_called_once_with('\n'.join([
                'P123123123 - PRIMARY    missing file',
                'P222333444 - CABOR      missing file',
                'P222333444 - GROR       invalid signature, missing checkmark',
                'P654321123 - CABOR      invalid signing date (expected 2020-12-01 but file has 2021-12-01)',
                'P901987345 - EHR        invalid signature',
            ]))

    def test_report_to_audit(self, logger_mock):
        """
        Check additional information and formatting helpful for looking into whether
        the files might have been mistakenly marked as incorrect
        """
        with mock.patch('rdr_service.tools.tool_libs.consents.input'):
            self._run_consents_tool(command='report-errors', verbose=True)
            logger_mock.info.assert_called_once_with('\n'.join([
                '',
                '1        - P123123123 - PRIMARY    missing file',
                '',
                '2        - P222333444 - CABOR      missing file',
                '3        - P222333444 - GROR       invalid signature, missing checkmark',
                'https://example.com/test_bucket/P222/GROR_no_checkmark_or_signature.pdf',
                '',
                '4        - P654321123 - CABOR      '
                'invalid signing date (expected 2020-12-01 but file has 2021-12-01, diff of 365 days)',
                'https://example.com/test_bucket/P654/Cabor_bad_date.pdf',
                '',
                '5        - P901987345 - EHR        invalid signature',
                'https://example.com/test_bucket/P901/EHR_no_signature.pdf',
            ]))

    def test_changing_existing_record(self, logger_mock):

        with mock.patch('rdr_service.tools.tool_libs.consents.input') as input_mock,\
             mock.patch('rdr_service.tools.tool_libs.consents.dispatch_rebuild_consent_metrics_tasks') as dispatch_mock:
            file_to_update = ConsentFile(
                id=24,
                participant_id=1234,
                file_path='test_bucket/file.pdf',
                type=ConsentType.PRIMARY,
                sync_status=ConsentSyncStatus.NEEDS_CORRECTING
            )
            self.consent_dao_mock.get_with_session.return_value = file_to_update

            input_mock.return_value = 'y'
            self._run_consents_tool(
                command='modify',
                additional_args={
                    'type': 'CABOR',
                    'sync_status': 'READY_FOR_SYNC'
                }
            )

            logger_mock.info.assert_has_calls([
                mock.call('File info:      P1234, test_bucket/file.pdf'),
                mock.call('type:           PRIMARY => CABOR'),
                mock.call('sync_status:    NEEDS_CORRECTING => READY_FOR_SYNC')
            ])

            updated_file = self.consent_dao_mock.batch_update_consent_files.call_args_list[0].args[0][0]
            self.assertEqual(file_to_update.id, updated_file.id)
            self.assertEqual(ConsentType.CABOR, updated_file.type)
            self.assertEqual(ConsentSyncStatus.READY_FOR_SYNC, updated_file.sync_status)
            # Verify the associated resource data consent metrics record was rebuilt
            dispatch_mock.assert_called_once()
            dispatch_rebuild_ids = dispatch_mock.call_args_list[0].args[0]
            self.assertEqual(dispatch_rebuild_ids, [updated_file.id])

    def test_validating_participants_from_file(self, _):
        self.temporarily_override_config_setting(
            key=config.CONSENT_SYNC_BUCKETS,
            value={}
        )

        with mock.patch('rdr_service.tools.tool_libs.consents.ConsentValidationController') as controller_class_mock,\
                mock.patch('rdr_service.tools.tool_libs.consents.open') as open_mock,\
                mock.patch('rdr_service.tools.tool_libs.consents.ParticipantSummaryDao') as summary_dao_class_mock:
            participant_id_list = [1, 45, 289, 3020]
            controller_mock = controller_class_mock.return_value

            # Set up file to list participant ids
            pid_file_handle = open_mock.return_value.__enter__.return_value
            pid_file_handle.__iter__.return_value = iter(participant_id_list)

            test_summaries = ['one', 'forty-five', 'two hundred eighty-nine', 'three thousand twenty']
            summary_dao_class_mock.get_by_ids_with_session.return_value = test_summaries

            # Verify that the script uses the participant ids from the file for validation
            # and specifies the consent type
            self._run_consents_tool(
                command='validate',
                additional_args={
                    'pid_file': 'pids.txt',
                    'type': 'EHR'
                }
            )

            # Make sure the id file was loaded
            summary_dao_class_mock.get_by_ids_with_session.assert_called_with(
                session=mock.ANY,
                obj_ids=participant_id_list
            )

            # Make sure the validation was done for the loaded summaries and for the specific consent type
            controller_mock.validate_participant_consents.assert_has_calls(
                calls=[
                    mock.call(summary=summary, output_strategy=mock.ANY, types_to_validate=[ConsentType.EHR])
                    for summary in test_summaries
                ],
                any_order=True
            )

    def test_record_upload(self, _):
        with mock.patch('rdr_service.tools.tool_libs.consents.csv') as csv_mock, \
                mock.patch('rdr_service.tools.tool_libs.consents.dispatch_rebuild_consent_metrics_tasks') \
                    as dispatch_consent_metrics_rebuild_mock, \
                mock.patch('rdr_service.tools.tool_libs.consents.open'):
            csv_file_mock = csv_mock.DictReader.return_value
            csv_file_mock.__iter__.return_value = [
                {
                    'id': 1,
                    'participant_id': '4567',
                    'file_exists': '1',
                    'file_path': 'bucket/valid_file.pdf',
                    'sync_status': '2'
                },
                {
                    'id': 2,
                    'participant_id': '1234',
                    'file_exists': '0',
                    'file_path': '',
                    'sync_status': '1'
                }
            ]

            # Set up mock so we can check the file changes correctly
            first_file_mock = mock.MagicMock()
            second_file_mock = mock.MagicMock()

            def get_file(obj_id, **_):
                if obj_id == 1:
                    return first_file_mock
                else:
                    return second_file_mock
            self.consent_dao_mock.get_with_session.side_effect = get_file

            # Check without max_date
            self._run_consents_tool(
                command='upload',
                additional_args={
                    'file': 'data.csv',
                }
            )

            # Make sure each file got the right updates
            self.assertEqual(4567, first_file_mock.participant_id)
            self.assertTrue(first_file_mock.file_exists)
            self.assertEqual('bucket/valid_file.pdf', first_file_mock.file_path)
            self.assertEqual(ConsentSyncStatus.READY_FOR_SYNC, first_file_mock.sync_status)

            self.assertEqual(1234, second_file_mock.participant_id)
            self.assertFalse(second_file_mock.file_exists)
            self.assertIsNone(second_file_mock.file_path)
            self.assertEqual(ConsentSyncStatus.NEEDS_CORRECTING, second_file_mock.sync_status)

            # Confirm the consent metrics resource data rebuild tasks were dispatched for updated records
            dispatch_consent_metrics_rebuild_mock.assert_called_once()
            dispatch_rebuild_ids = dispatch_consent_metrics_rebuild_mock.call_args_list[0].args[0]
            uploaded_ids = [1, 2]
            self.assertCountEqual(dispatch_rebuild_ids, uploaded_ids)

