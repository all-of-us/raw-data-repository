from datetime import datetime
import mock

from rdr_service import config
from rdr_service.services.consent import files
from tests.helpers.unittest_base import BaseTestCase


class BaseConsentFactoryTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BaseConsentFactoryTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs) -> None:
        super(BaseConsentFactoryTest, self).setUp(*args, **kwargs)
        self.storage_provider_mock = mock.MagicMock()

        # Patch the PDF wrapper class to simply return the blob object that was meant to be parsed
        pdf_patcher = mock.patch('rdr_service.services.consent.files.Pdf.from_google_storage_blob')
        pdf_mock = pdf_patcher.start()
        pdf_mock.side_effect = lambda blob: blob
        self.addCleanup(pdf_patcher.stop)

        # Provide a config value that gives bucket names for the factories to read
        self.temporarily_override_config_setting(config.CONSENT_PDF_BUCKET, {
            'vibrent': 'vibrent_bucket',
            'careevolution': 'ce-bucket'
        })

    def assertConsentListEquals(self, expected_class, expected_files, actual_files):
        for file_object in actual_files:
            self.assertIsInstance(file_object, expected_class)

        self.assertEqual(
            [file.name for file in expected_files],
            [file.pdf.name for file in actual_files]
        )

    @classmethod
    def _translation_search_matches(cls, text_in_file, search_list):
        if text_in_file is None:
            return False

        # The code uses strings when there aren't translations, and tuples when there are
        for search_item in search_list:
            if isinstance(search_item, str) and search_item not in text_in_file:
                return False
            elif not any([search_str in text_in_file for search_str in search_item]):
                return False
        return True


class VibrentConsentFactoryTest(BaseConsentFactoryTest):
    def setUp(self, *args, **kwargs) -> None:
        super(VibrentConsentFactoryTest, self).setUp(*args, **kwargs)

        # Patch the PDF wrapper class to simply return the blob object that was meant to be parsed
        pdf_patcher = mock.patch('rdr_service.services.consent.files.Pdf.from_google_storage_blob')
        pdf_mock = pdf_patcher.start()
        pdf_mock.side_effect = lambda blob: blob
        self.addCleanup(pdf_patcher.stop)

        # Mock test consent data
        self.primary_file = self._mock_pdf(name='ConsentPII_1234.pdf')
        self.cabor_file = self._mock_pdf(
            name='ConsentPII_4567.pdf',
            text_in_file=files.VibrentConsentFactory.CABOR_TEXT
        )
        self.spanish_cabor_file = self._mock_pdf(
            name='ConsentPII_es.pdf',
            text_in_file='Declaración de Derechos del Sujeto de Investigación Experimental'
        )
        self.another_primary = self._mock_pdf(name='ConsentPII_test.pdf')
        self.ehr_file = self._mock_pdf(name='EHRConsentPII.pdf')
        self.another_ehr = self._mock_pdf(name='EHRConsentPII_2.pdf')
        self.signature_image = self._mock_pdf(name='EHRConsentPII.png')
        self.gror_file = self._mock_pdf(name='GROR_234.pdf')
        self.etm_file = self._mock_pdf(name='EtMConsent.pdf')
        self.primary_update_file = self._mock_pdf(
            name='PrimaryConsentUpdate_7890.pdf',
            text_in_file='Do you agree to this updated consent?'
        )
        self.spanish_primary_update_file = self._mock_pdf(
            name='PrimaryConsentUpdate_es21.pdf',
            text_in_file='¿Está de acuerdo con este consentimiento actualizado?'
        )
        self.older_update_file = self._mock_pdf(name='PrimaryConsentUpdate_v1.pdf')

        self.storage_provider_mock.list.return_value = [
            self.primary_file,
            self.cabor_file,
            self.spanish_cabor_file,
            self.another_primary,
            self.ehr_file,
            self.another_ehr,
            self.signature_image,
            self.gror_file,
            self.primary_update_file,
            self.spanish_primary_update_file,
            self.etm_file
        ]

        self.vibrent_factory = files.ConsentFileAbstractFactory.get_file_factory(
            participant_id=1234,
            participant_origin='vibrent',
            storage_provider=self.storage_provider_mock
        )

    def test_consent_factory_returned(self):
        """Test the factory builder method to make sure it builds and returns the correct factory type"""
        participant_id = 123456789
        consent_factory = files.ConsentFileAbstractFactory.get_file_factory(
            participant_id=participant_id,
            participant_origin='vibrent',
            storage_provider=self.storage_provider_mock
        )

        self.assertIsInstance(consent_factory, files.VibrentConsentFactory)
        self.storage_provider_mock.list.assert_called_with(
            bucket_name=consent_factory._get_source_bucket(),
            prefix=f'Participant/P{participant_id}'
        )

    def test_vibrent_primary_consent(self):
        """Test that the factory correctly identifies the Primary consent files"""
        self.assertConsentListEquals(
            expected_class=files.VibrentPrimaryConsentFile,
            expected_files=[self.primary_file, self.another_primary],
            actual_files=self.vibrent_factory.get_primary_consents()
        )

    def test_vibrent_cabor_consent(self):
        """Test that the factory correctly identifies the Cabor consent file"""
        self.assertConsentListEquals(
            expected_class=files.VibrentCaborConsentFile,
            expected_files=[self.cabor_file, self.spanish_cabor_file],
            actual_files=self.vibrent_factory.get_cabor_consents()
        )

    def test_vibrent_ehr_consent(self):
        self.assertConsentListEquals(
            expected_class=files.VibrentEhrConsentFile,
            expected_files=[self.ehr_file, self.another_ehr],
            actual_files=self.vibrent_factory.get_ehr_consents()
        )

    def test_vibrent_gror_consent(self):
        self.assertConsentListEquals(
            expected_class=files.VibrentGrorConsentFile,
            expected_files=[self.gror_file],
            actual_files=self.vibrent_factory.get_gror_consents()
        )

    def test_vibrent_primary_update_consent(self):
        self.assertConsentListEquals(
            expected_class=files.VibrentPrimaryConsentUpdateFile,
            expected_files=[self.primary_update_file, self.spanish_primary_update_file],
            actual_files=self.vibrent_factory.get_primary_update_consents(consent_date=datetime.now())
        )

    def test_vibrent_etm_consent(self):
        self.assertConsentListEquals(
            expected_class=files.VibrentEtmConsentFile,
            expected_files=[self.etm_file],
            actual_files=self.vibrent_factory.get_etm_consents()
        )

    @classmethod
    def _mock_pdf(cls, name, text_in_file: str = None):
        pdf_mock = mock.MagicMock()
        pdf_mock.name = name

        def page_number_for_text(search_list):
            if cls._translation_search_matches(
                text_in_file=text_in_file,
                search_list=search_list
            ):
                return 1
            else:
                return None
        pdf_mock.get_page_number_of_text.side_effect = page_number_for_text

        return pdf_mock


class CeConsentFactoryTest(BaseConsentFactoryTest):
    def setUp(self, *args, **kwargs) -> None:
        super(CeConsentFactoryTest, self).setUp(*args, **kwargs)
        self.storage_provider_mock = mock.MagicMock()

        # Mock test consent data
        self.primary_file = self._mock_pdf(
            name='primary_test.pdf',
            text_in_file='Consent to Join the All of Us Research Program'
        )
        self.spanish_primary = self._mock_pdf(
            name='spanish_primary.pdf',
            text_in_file='Consentimiento para Participar en el Programa Científico All of Us'
        )

        self.cabor_file = self._mock_pdf(
            name='cabor_test.pdf',
            text_in_file="California Experimental Subject's Bill of Rights"
        )
        self.spanish_cabor = self._mock_pdf(
            name='spanish_cabor.pdf',
            text_in_file='Declaración de Derechos del Sujeto de Investigación Experimental, de California'
        )

        self.ehr_file = self._mock_pdf(name='ehr_test.pdf', text_in_file='HIPAA Authorization for Research EHR')
        self.spanish_ehr = self._mock_pdf(
            name='spanish_ehr.pdf',
            text_in_file='Autorización para Investigación de HIPAA'
        )

        self.gror_file = self._mock_pdf(name='gror_test.pdf', text_in_file='Consent to Receive DNA Results')
        self.spanish_gror = self._mock_pdf(
            name='spanish_gror.pdf',
            text_in_file='Consentimiento para Recibir Resultados de ADN'
        )

        self.wear_file = self._mock_pdf(name='wear_test.pdf', text_in_file='All of Us WEAR Study')

        self.storage_provider_mock.list.return_value = [
            self.primary_file, self.spanish_primary,
            self.cabor_file, self.spanish_cabor,
            self.ehr_file, self.spanish_ehr,
            self.gror_file, self.spanish_gror,
            self.wear_file
        ]

        self.ce_factory = files.ConsentFileAbstractFactory.get_file_factory(
            participant_id=1234,
            participant_origin='careevolution',
            storage_provider=self.storage_provider_mock
        )

    def test_consent_factory_returned(self):
        """Test the factory builder method to make sure it builds and returns the correct factory type"""
        participant_id = 123456789
        consent_factory = files.ConsentFileAbstractFactory.get_file_factory(
            participant_id=participant_id,
            participant_origin='careevolution',
            storage_provider=self.storage_provider_mock
        )

        self.assertIsInstance(consent_factory, files.CeConsentFactory)
        self.storage_provider_mock.list.assert_called_with(
            bucket_name=consent_factory._get_source_bucket(),
            prefix=f'Participants/P{participant_id}'
        )

    def test_ce_primary_consent(self):
        """Test that the factory correctly identifies the Primary consent files"""
        self.assertConsentListEquals(
            expected_class=files.CePrimaryConsentFile,
            expected_files=[self.primary_file, self.spanish_primary],
            actual_files=self.ce_factory.get_primary_consents()
        )

    def test_ce_cabor_consent(self):
        """Test that the factory correctly identifies the Cabor consent file"""
        self.assertConsentListEquals(
            expected_class=files.CeCaborConsentFile,
            expected_files=[self.cabor_file, self.spanish_cabor],
            actual_files=self.ce_factory.get_cabor_consents()
        )

    def test_ce_ehr_consent(self):
        self.assertConsentListEquals(
            expected_class=files.CeEhrConsentFile,
            expected_files=[self.ehr_file, self.spanish_ehr],
            actual_files=self.ce_factory.get_ehr_consents()
        )

    def test_ce_gror_consent(self):
        self.assertConsentListEquals(
            expected_class=files.CeGrorConsentFile,
            expected_files=[self.gror_file, self.spanish_gror],
            actual_files=self.ce_factory.get_gror_consents()
        )

    def test_ce_wear_consent(self):
        self.assertConsentListEquals(
            expected_class=files.CeWearConsentFile,
            expected_files=[self.wear_file],
            actual_files=self.ce_factory.get_wear_consents()
        )

    @classmethod
    def _mock_pdf(cls, name, text_in_file: str = None):
        pdf_mock = mock.MagicMock()
        pdf_mock.name = name

        def has_text(search_list):
            return cls._translation_search_matches(
                text_in_file=text_in_file,
                search_list=search_list
            )
        pdf_mock.has_text.side_effect = has_text

        return pdf_mock
