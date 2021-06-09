import mock

from rdr_service.services import consent_files
from tests.helpers.unittest_base import BaseTestCase


class ConsentFactoryTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(ConsentFactoryTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs) -> None:
        super(ConsentFactoryTest, self).setUp(*args, **kwargs)
        self.storage_provider_mock = mock.MagicMock()

        # Patch the PDF wrapper class to simply return the blob object that was meant to be parsed
        pdf_patcher = mock.patch('rdr_service.services.consent_files.Pdf.from_google_storage_blob')
        pdf_mock = pdf_patcher.start()
        pdf_mock.side_effect = lambda blob: blob
        self.addCleanup(pdf_patcher.stop)

        # Mock test consent data
        self.primary_file = self._mock_pdf(name='ConsentPII_1234.pdf')
        self.cabor_file = self._mock_pdf(
            name='ConsentPII_4567.pdf',
            text_in_file=consent_files.VibrentConsentFactory.CABOR_TEXT
        )
        self.another_primary = self._mock_pdf(name='ConsentPII_test.pdf')
        self.ehr_file = self._mock_pdf(name='EHRConsentPII.pdf')
        self.another_ehr = self._mock_pdf(name='EHRConsentPII_2.pdf')
        self.signature_image = self._mock_pdf(name='EHRConsentPII.png')
        self.gror_file = self._mock_pdf(name='GROR_234.pdf')

        self.storage_provider_mock.list.return_value = [
            self.primary_file,
            self.cabor_file,
            self.another_primary,
            self.ehr_file,
            self.another_ehr,
            self.signature_image,
            self.gror_file
        ]

        self.vibrent_factory = consent_files.ConsentFileAbstractFactory.get_file_factory(
            participant_id=1234,
            participant_origin='vibrent',
            storage_provider=self.storage_provider_mock
        )

    def test_consent_factory_returned(self):
        """Test the factory builder method to make sure it builds and returns the correct factory type"""
        participant_id = 123456789
        consent_factory = consent_files.ConsentFileAbstractFactory.get_file_factory(
            participant_id=participant_id,
            participant_origin='vibrent',
            storage_provider=self.storage_provider_mock
        )

        self.assertIsInstance(consent_factory, consent_files.VibrentConsentFactory)
        self.storage_provider_mock.list.assert_called_with(
            bucket_name=consent_factory._get_source_bucket(),
            prefix=f'Participant/P{participant_id}'
        )

    def test_vibrent_primary_consent(self):
        """Test that the factory correctly identifies the Primary consent files"""
        self.assertConsentListEquals(
            expected_class=consent_files.VibrentPrimaryConsentFile,
            expected_files=[self.primary_file, self.another_primary],
            actual_files=self.vibrent_factory.get_primary_consents()
        )

    def test_vibrent_cabor_consent(self):
        """Test that the factory correctly identifies the Cabor consent file"""
        self.assertConsentListEquals(
            expected_class=consent_files.VibrentCaborConsentFile,
            expected_files=[self.cabor_file],
            actual_files=self.vibrent_factory.get_cabor_consents()
        )

    def test_vibrent_ehr_consent(self):
        self.assertConsentListEquals(
            expected_class=consent_files.VibrentEhrConsentFile,
            expected_files=[self.ehr_file, self.another_ehr],
            actual_files=self.vibrent_factory.get_ehr_consents()
        )

    def test_vibrent_gror_consent(self):
        self.assertConsentListEquals(
            expected_class=consent_files.VibrentGrorConsentFile,
            expected_files=[self.gror_file],
            actual_files=self.vibrent_factory.get_gror_consents()
        )

    def assertConsentListEquals(self, expected_class, expected_files, actual_files):
        for file_object in actual_files:
            self.assertIsInstance(file_object, expected_class)

        self.assertEqual(
            [file.name for file in expected_files],
            [file.pdf.name for file in actual_files]
        )

    @classmethod
    def _mock_pdf(cls, name, text_in_file: str = None):
        pdf_mock = mock.MagicMock()
        pdf_mock.name = name

        pdf_mock.get_page_number_of_text.side_effect = lambda search_list: 1 if text_in_file in search_list else None

        return pdf_mock
