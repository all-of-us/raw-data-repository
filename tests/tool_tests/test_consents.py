from datetime import date
import mock

from rdr_service.model.consent_file import ConsentFile, ConsentType
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

        self.invalid_files = [
            ConsentFile(
                participant_id=123123123, type=ConsentType.PRIMARY, file_exists=False
            ),
            ConsentFile(
                participant_id=222333444, type=ConsentType.CABOR, file_exists=False
            ),
            ConsentFile(
                participant_id=222333444, type=ConsentType.GROR, file_exists=True,
                is_signature_valid=False, is_signing_date_valid=True, other_errors='missing checkmark',
                file_path='test_bucket/P222/GROR_no_checkmark_or_signature.pdf'
            ),
            ConsentFile(
                participant_id=654321123, type=ConsentType.CABOR, file_exists=True,
                is_signature_valid=True, is_signing_date_valid=False,
                signing_date=date(2021, 12, 1), expected_sign_date=date(2020, 12, 1),
                file_path='test_bucket/P654/Cabor_bad_date.pdf'
            ),
            ConsentFile(
                participant_id=901987345, type=ConsentType.EHR, file_exists=True,
                is_signature_valid=False, is_signing_date_valid=True,
                file_path='test_bucket/P901/EHR_no_signature.pdf'
            )
        ]

    def _run_error_report(self, verbose=False):
        with mock.patch('rdr_service.tools.tool_libs.consents.ConsentDao') as consent_dao_class_mock,\
                mock.patch('rdr_service.tools.tool_libs.consents.GoogleCloudStorageProvider') as storage_provider_mock:
            consent_dao_instance_mock = consent_dao_class_mock.return_value
            consent_dao_instance_mock.get_files_needing_correction.return_value = self.invalid_files

            def blob_that_gives_url(bucket_name, blob_name):
                blob_mock = mock.MagicMock()
                blob_mock.generate_signed_url.return_value = f'https://example.com/{bucket_name}/{blob_name}'
                return blob_mock
            storage_provider_mock.return_value.get_blob.side_effect = blob_that_gives_url

            self.run_tool(ConsentTool, tool_args={
                'since': None,
                'verbose': verbose
            })

    def test_report_to_send_to_ptsc(self, logger_mock):
        """Check the basic report format, the one that would be sent to Vibrent or CE for correcting"""
        self._run_error_report()
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
        self._run_error_report(verbose=True)
        logger_mock.info.assert_called_once_with('\n'.join([
            'P123123123 - PRIMARY    missing file',
            '',
            'P222333444 - CABOR      missing file',
            'P222333444 - GROR       invalid signature, missing checkmark - '
            'https://example.com/test_bucket/P222/GROR_no_checkmark_or_signature.pdf',
            '',
            'P654321123 - CABOR      '
            'invalid signing date (expected 2020-12-01 but file has 2021-12-01, diff of 365 days) - '
            'https://example.com/test_bucket/P654/Cabor_bad_date.pdf',
            '',
            'P901987345 - EHR        invalid signature - https://example.com/test_bucket/P901/EHR_no_signature.pdf',
        ]))

