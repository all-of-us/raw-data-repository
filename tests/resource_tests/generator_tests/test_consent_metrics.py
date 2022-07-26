#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import mock

from datetime import datetime, date
from tests.helpers.unittest_base import BaseTestCase

from rdr_service import config
from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.model.consent_file import ConsentSyncStatus, ConsentType, ConsentOtherErrors
from rdr_service.model.consent_response import ConsentResponse
import rdr_service.resource.generators

class ConsentMetricGeneratorTest(BaseTestCase):

    def setUp(self, *args, **kwargs) -> None:
        super(ConsentMetricGeneratorTest, self).setUp(*args, **kwargs)
        self.dao = ResourceDataDao()
        self._participant = self.data_generator.create_database_participant(participantOrigin='vibrent')
        self.consent_metric_resource_generator = rdr_service.resource.generators.ConsentMetricGenerator()
        self.consent_error_report_generator = rdr_service.resource.generators.ConsentErrorReportGenerator()

    def _create_participant_with_all_consents_authored(self, **kwargs):
        """ Populate a participant_summary record with provided data """
        # Tests using this setup method may create participants with specific origin or paired hpo_id values
        participant = self.data_generator.create_database_participant(
            participantOrigin=kwargs.get('participantOrigin','vibrent'),
            hpoId=kwargs.get('hpoId', 0)
        )
        defaults = {
            'consentForStudyEnrollmentAuthored': datetime.strptime('2020-01-01 01:00:00', "%Y-%m-%d %H:%M:%S"),
            'consentForStudyEnrollmentFirstYesAuthored': datetime.strptime('2020-01-01 01:00:00', "%Y-%m-%d %H:%M:%S"),
            'consentForCABoRAuthored': datetime.strptime('2020-01-01 02:00:00', "%Y-%m-%d %H:%M:%S"),
            'consentForElectronicHealthRecordsAuthored': datetime.strptime('2020-01-01 03:00:00', "%Y-%m-%d %H:%M:%S"),
            'consentForElectronicHealthRecordsFirstYesAuthored': \
                datetime.strptime('2020-01-01 03:00:00', "%Y-%m-%d %H:%M:%S"),
            'consentForGenomicsRORAuthored': datetime.strptime('2020-01-01 04:00:00', "%Y-%m-%d %H:%M:%S"),
            'participantOrigin': participant.participantOrigin,
            'participant': participant
        }

        # Merge the kwargs and defaults dicts; kwargs values take precedence over default values
        for key in defaults.keys():
            if key not in kwargs.keys():
                kwargs = dict(**{key: defaults[key]}, **kwargs)

        participant = self.data_generator.create_database_participant_summary(**kwargs)
        return participant

    def _create_participant_with_custom_primary_consent_authored(self, authored, **kwargs):

        participant = self.data_generator.create_database_participant_summary(
            consentForStudyEnrollmentAuthored=authored,
            consentForStudyEnrollmentFirstYesAuthored=authored,
            participant=self._participant,
            **kwargs
        )
        return participant

    @staticmethod
    def _create_expected_metrics_dict(participant, consent_type=ConsentType.PRIMARY,
                                      consent_status=ConsentSyncStatus.READY_FOR_SYNC, expected_errors=[]):
        """
        Set up a dictionary of values to compare against resource data dictionary from ConsentMetricGenerator;
        does not include created, modified, or id (auto-generated values)
        """
        expected_values_dict = {'hpo_id': participant.hpoId,
                                'organization_id': participant.organizationId,
                                'participant_id': f'P{participant.participantId}',
                                'consent_type': str(consent_type),
                                'consent_type_id': int(consent_type),
                                'sync_status': str(consent_status),
                                'sync_status_id': int(consent_status),
                                'missing_file': ('missing_file' in expected_errors),
                                'signature_missing': ('signature_missing' in expected_errors),
                                'invalid_signing_date': ('invalid_signing_date' in expected_errors),
                                'checkbox_unchecked': ('checkbox_unchecked' in expected_errors),
                                'non_va_consent_for_va': ('non_va_consent_for_va' in expected_errors),
                                'va_consent_for_non_va': ('va_consent_for_non_va' in expected_errors),
                                'invalid_dob': ('invalid_dob' in expected_errors),
                                'invalid_age_at_consent': ('invalid_age_at_consent' in expected_errors),
                                'invalid_printed_name': ('invalid_printed_name' in expected_errors),
                                'sensitive_ehr_expected': ('sensitive_ehr_expected' in expected_errors),
                                'non_sensitive_ehr_expected': ('non_sensitive_ehr_expected' in expected_errors),
                                'sensitive_ehr_missing_initials': ('sensitive_ehr_missing_initials' in expected_errors)
                                }

        return expected_values_dict

    @mock.patch('rdr_service.resource.generators.consent_metrics.ConsentErrorReportGenerator.send_consent_error_email')
    def test_consent_metrics_generator_no_errors(self, send_error_email_mock):
        """ Test the consent_metrics generator with no error conditions """

        # Use a valid datOfBirth for participant summary data
        participant = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
        )
        # Create consent_file record with no error conditions
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            participant_id=participant.participantId,
            signing_date=participant.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1
        )
        self.assertIsNotNone(consent_file_rec.id)

        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.assertIsNone(unreported_errors)
        self.consent_error_report_generator.create_error_reports(unreported_errors)

        # No expected_errors provided, all error conditions default to False
        expected = self._create_expected_metrics_dict(participant, expected_errors=[])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # Also check that the authored date matches the date from the participant_summary record
        self.assertEqual(resource_data.get('consent_authored_date', None),
                         datetime.date(participant.consentForElectronicHealthRecordsFirstYesAuthored))

        # No error emails to send
        self.assertEqual(0, send_error_email_mock.call_count)

    @mock.patch('rdr_service.resource.generators.consent_metrics.ConsentErrorReportGenerator.send_consent_error_email')
    def test_consent_metrics_generator_dob_invalid(self, send_error_email_mock):
        """
        invalid_dob error calculated from participant_summary data, sync_status can still be READY_TO_SYNC
        """

        # Create participant summary data with (1) DOB missing,  and (2) DOB > 124 years from primary consent authored
        invalid_dob = datetime.strptime('1894-12-31', '%Y-%m-%d')
        p1 = self.data_generator.create_database_participant(participantOrigin='vibrent')
        p2 = self.data_generator.create_database_participant(participantOrigin='vibrent')
        p1_consents = self._create_participant_with_all_consents_authored(participant=p1, dateOfBirth=None)
        p2_consents = self._create_participant_with_all_consents_authored(participant=p2, dateOfBirth=invalid_dob)
        # Create consent_file records for each participant's primary consent with no other error conditions
        consent_file_rec_1 = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            participant_id=p1_consents.participantId,
            signing_date=p1_consents.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1
        )
        consent_file_rec_2 = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            participant_id=p2_consents.participantId,
            signing_date=p2_consents.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1
        )
        self.assertIsNotNone(consent_file_rec_1.id)
        self.assertIsNotNone(consent_file_rec_2.id)

        # Expected: Invalid DOB because DOB is missing
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec_1.id).get_data()
        expected = self._create_expected_metrics_dict(p1_consents, expected_errors=['invalid_dob'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # Expected: Invalid DOB because DOB is > 124 years before primary consent authored date
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec_2.id).get_data()
        expected = self._create_expected_metrics_dict(p2_consents, expected_errors=['invalid_dob'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # DA-2611: READY_FOR_SYNC files with DOB/age_at_consent issues no longer trigger automated error reports
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.assertEqual(unreported_errors, None)
        self.consent_error_report_generator.create_error_reports(unreported_errors)
        self.assertEqual(0, send_error_email_mock.call_count)

    @mock.patch('rdr_service.resource.generators.consent_metrics.ConsentErrorReportGenerator.send_consent_error_email')
    def test_consent_metrics_generator_invalid_age_at_consent(self, send_error_email_mock):
        """
         invalid_age_at_consent errors come from participant_summary data, sync_status can still be READY_TO_SYNC
         """
        # Create participant summary data with a DOB less than 18 years from primary consent authored date
        participant = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('2014-01-01', '%Y-%m-%d')),
        )
        # Create consent_file record with no other error conditions
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            participant_id=participant.participantId,
            signing_date=participant.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1
        )
        self.assertIsNotNone(consent_file_rec.id)

        # Expected: invalid_age_at_consent (less than 18 years of age)
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        expected = self._create_expected_metrics_dict(participant, expected_errors=['invalid_age_at_consent'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # DA-2611: READY_FOR_SYNC files with DOB/age at consent errors no longer trigger automated reports
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.assertEqual(unreported_errors, None)
        self.consent_error_report_generator.create_error_reports(unreported_errors)
        self.assertEqual(0, send_error_email_mock.call_count)

    @mock.patch('rdr_service.resource.generators.consent_metrics.ConsentErrorReportGenerator.send_consent_error_email')
    def test_consent_metrics_generator_missing_file(self, send_error_email_mock):
        """
        Consent metrics missing_file error based on consent_file having file_exists = 0
        """
        # Create participant summary data (valid DOB)
        participant = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
        )
        # Create consent_file record with file_exists set to false, status NEEDS_CORRECTING
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            signing_date=participant.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=0,
            is_signature_valid=0,
            is_signing_date_valid=0
        )
        self.assertIsNotNone(consent_file_rec.id)

        # Expected: Missing file
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        # Note:  if file is missing, neither the signature_missing or invalid_signing_date errors should be set
        expected = self._create_expected_metrics_dict(participant,
                                                      consent_status=ConsentSyncStatus.NEEDS_CORRECTING,
                                                      expected_errors=['missing_file'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # Email generated for missing file
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.consent_error_report_generator.create_error_reports(unreported_errors)
        self.assertEqual(1, send_error_email_mock.call_count)
        subject = send_error_email_mock.call_args[0][0]
        body = send_error_email_mock.call_args[0][1]
        self.assertIn('Missing file', subject)
        self.assertEqual(1, body.count('Error Detected'))
        # Confirm after report generation that there are no unreported ids
        self.assertIsNone(self.consent_error_report_generator.get_unreported_error_ids())

    @mock.patch('rdr_service.resource.generators.consent_metrics.ConsentErrorReportGenerator.send_consent_error_email')
    def test_consent_metrics_generator_dob_and_file_errors(self, send_error_email_mock):
        """
         Consent metrics signature_missing error + invalid_age_at_consent error from primary consent
         """
        # Create participant summary data (DOB < 18 years from primary consent authored date)
        participant = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('2004-01-01', '%Y-%m-%d'))
        )
        # Create consent_file record with file_exists set to false, status NEEDS_CORRECTING
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            signing_date=participant.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=0,
            # Because there wasn't a signature detected, this downstream signing date error is ignored in metrics code
            is_signing_date_valid=0
        )
        self.assertIsNotNone(consent_file_rec.id)

        # Expected: invalid_age_at_consent and signature missing errors
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        expected = self._create_expected_metrics_dict(participant,
                                                      consent_status=ConsentSyncStatus.NEEDS_CORRECTING,
                                                      expected_errors=['invalid_age_at_consent', 'signature_missing'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # DA-2611: consent_file ids in NEEDS_CORRECTING for another PDF validation error can also
        # trigger DOB/age at consent error reports associated with the same ConsentPII payload
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.consent_error_report_generator.create_error_reports(unreported_errors)
        self.assertEqual(2, send_error_email_mock.call_count)

    @mock.patch('rdr_service.resource.generators.consent_metrics.ConsentErrorReportGenerator.send_consent_error_email')
    def test_consent_metrics_generator_other_errors(self, send_error_email_mock):
        """
        Consent metrics errors that are extracted from the consent_file other_errors string field
        """
        # Create participant summary data (valid DOB)
        participant = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
        )
        # Create consent_file record with missing check error,  status NEEDS_CORRECTING
        consent_file_rec_1 = self.data_generator.create_database_consent_file(
            type=ConsentType.GROR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            signing_date=participant.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=ConsentOtherErrors.MISSING_CONSENT_CHECK_MARK
        )

        # Create consent_file record with veteran consent for non-veteran error
        consent_file_rec_2 = self.data_generator.create_database_consent_file(
            type=ConsentType.EHR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            signing_date=participant.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=ConsentOtherErrors.VETERAN_CONSENT_FOR_NON_VETERAN
        )
        self.assertIsNotNone(consent_file_rec_2.id)

        # Create consent_file record with both missing check mark and non-veteran consent for veteran participant error
        consent_file_rec_3 = self.data_generator.create_database_consent_file(
            type=ConsentType.EHR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            signing_date=participant.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=", ".join([ConsentOtherErrors.NON_VETERAN_CONSENT_FOR_VETERAN,
                                   ConsentOtherErrors.MISSING_CONSENT_CHECK_MARK])
        )

        # Expected: checkbox_unchecked error for consent_file_rec_1
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec_1.id).get_data()
        expected = self._create_expected_metrics_dict(participant,
                                                      consent_type=ConsentType.GROR,
                                                      consent_status=ConsentSyncStatus.NEEDS_CORRECTING,
                                                      expected_errors=\
                                                          ['checkbox_unchecked']
                                                      )
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # Also validate the resource data consent_authored_date matches the participant_summary GROR authored date
        self.assertEqual(resource_data.get('consent_authored_date', None),
                         datetime.date(participant.consentForGenomicsRORAuthored))

        # Expected: non_va_consent_for_va for consent_file_rec_2
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec_2.id).get_data()
        expected = self._create_expected_metrics_dict(participant,
                                                      consent_type=ConsentType.EHR,
                                                      consent_status=ConsentSyncStatus.NEEDS_CORRECTING,
                                                      expected_errors=\
                                                          ['va_consent_for_non_va']
                                                      )
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # Also validate the resource data consent_authored_date matches the participant_summary EHR authored date
        self.assertEqual(resource_data.get('consent_authored_date', None),
                         datetime.date(participant.consentForElectronicHealthRecordsFirstYesAuthored))

        # Expected: checkbox_unchecked, non_va_for_va_consent for consent_file_rec_3
        resource_data= self.consent_metric_resource_generator.make_resource(consent_file_rec_3.id).get_data()
        expected = self._create_expected_metrics_dict(participant,
                                                      consent_type=ConsentType.EHR,
                                                      consent_status=ConsentSyncStatus.NEEDS_CORRECTING,
                                                      expected_errors=\
                                                          ['checkbox_unchecked', 'non_va_consent_for_va']
                                                      )
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # Also validate the resource data consent_authored_date matches the participant_summary EHR authored date
        self.assertEqual(resource_data.get('consent_authored_date', None),
                         datetime.date(participant.consentForElectronicHealthRecordsFirstYesAuthored))

        # Expected:  Three email reports for each of the three distinct error conditions
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.consent_error_report_generator.create_error_reports(unreported_errors)
        self.assertEqual(3, send_error_email_mock.call_count)

    def test_consent_metrics_generator_resolved_date(self):
        """
        For OBSOLETE sync status, confirm the resolved date equals the last modified date from consent_file record
        """
        # Create participant summary data (valid DOB)
        participant = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
        )
        # Create consent_file record with checkbox error, status NEEDS_CORRECTING
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.EHR,
            sync_status=ConsentSyncStatus.OBSOLETE,
            participant_id=participant.participantId,
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=ConsentOtherErrors.MISSING_CONSENT_CHECK_MARK
        )
        self.assertIsNotNone(consent_file_rec.id)

        # Expected: checkbox_unchecked error
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        expected = self._create_expected_metrics_dict(participant,
                                                      consent_type=ConsentType.EHR,
                                                      consent_status=ConsentSyncStatus.OBSOLETE,
                                                      expected_errors=['checkbox_unchecked'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # Confirm the consent authored date matches the date from the participant_summary record, and that the
        # resolved date matches the consent_file record modified date
        self.assertEqual(resource_data.get('consent_authored_date', None),
                         datetime.date(participant.consentForElectronicHealthRecordsFirstYesAuthored))

        self.assertEqual(resource_data.get('resolved_date', None),
                         datetime.date(consent_file_rec.modified))

    @mock.patch('rdr_service.resource.generators.consent_metrics.ConsentErrorReportGenerator.send_consent_error_email')
    def test_consent_metrics_generator_signature_missing_error_filtered(self, send_error_email_mock):
        """
        Ignore known potential false positives for missing signatures, for consents authored before 2018-07-13
        """
        # Create participant summary data with a primary consent authored date before the false positive cutoff
        participant = self._create_participant_with_custom_primary_consent_authored(
            datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
        )
        # Create consent_file record with signature missing/signing date missing, status NEEDS_CORRECTING
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            expected_sign_date=date(year=2018, month=1, day=1),
            file_exists=1,
            is_signature_valid=0,
            is_signing_date_valid=0
        )
        self.assertIsNotNone(consent_file_rec.id)
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.consent_error_report_generator.create_error_reports(unreported_errors)

        # Confirm this record's ignore flag was set due to filtering the false positive case for missing signature
        # and that no email report was generated
        self.assertEqual(resource_data['ignore'], True)
        self.assertEqual(0, send_error_email_mock.call_count)

    @mock.patch('rdr_service.resource.generators.consent_metrics.ConsentErrorReportGenerator.send_consent_error_email')
    def test_consent_metrics_generator_invalid_signing_date_error_filtered(self, send_error_email_mock):
        """
        Ignore known potential false positives for valid signature but missing signing dates
        for consents authored before 2018-07-13
        """
        # Create participant summary data with a primary consent authored date before the false positive cutoff
        participant = self._create_participant_with_custom_primary_consent_authored(
            datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
        )
        # Create consent_file record with the missing signing date condition
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            expected_sign_date=date(year=2018, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=0
        )
        self.assertIsNotNone(consent_file_rec.id)
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.consent_error_report_generator.create_error_reports(unreported_errors)

        # Confirm this record's ignore flag was set due to filtering the invalid_signing_date error, and that
        # no email report was generated
        self.assertEqual(resource_data['ignore'], True)
        self.assertEqual(0, send_error_email_mock.call_count)

    @mock.patch('rdr_service.resource.generators.consent_metrics.ConsentErrorReportGenerator.send_consent_error_email')
    def test_consent_metrics_generator_special_sync_status_filtered(self, send_error_email_mock):
        """
        Ignore consent records whose current sync_status is a special case status such as UNKNOWN or DELAYING_SYNC
        """
        participant = self._create_participant_with_all_consents_authored(
            consentForStudyEnrollmentAuthored=datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            consentForStudyEnrollmentFirstYesAuthored=datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d'))
        )
        # Create consent_file record with a "non-standard" sync_status
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.DELAYING_SYNC,
            participant_id=participant.participantId,
            expected_sign_date=date(year=2018, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1
        )
        self.assertIsNotNone(consent_file_rec.id)
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.consent_error_report_generator.create_error_reports(unreported_errors)

        # Confirm this record's ignore flag was set due to filtering on the special sync_status and that
        # no email report was generated
        self.assertEqual(resource_data['ignore'], True)
        self.assertEqual(0, send_error_email_mock.call_count)

    @mock.patch('rdr_service.resource.generators.consent_metrics.ConsentErrorReportGenerator.send_consent_error_email')
    def test_consent_metrics_generator_va_consent_for_non_va_filtered(self, send_error_email_mock):
        """
        Ignore va_consent_for_non_va errors if that's the only error and participant's current pairing is to the VA HPO
        """
        va_hpo = self.data_generator.create_database_hpo(hpoId=2000, name='VA')
        participant = self._create_participant_with_all_consents_authored(
            consentForStudyEnrollmentAuthored=datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            consentForStudyEnrollmentFirstYesAuthored=datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
            hpoId=va_hpo.hpoId
        )
        # Create consent_file record with file_exists set to false, status NEEDS_CORRECTING
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            expected_sign_date=date(year=2018, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=ConsentOtherErrors.VETERAN_CONSENT_FOR_NON_VETERAN
        )
        self.assertIsNotNone(consent_file_rec.id)
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.consent_error_report_generator.create_error_reports(unreported_errors)

        # Confirm this record's ignore flag was set due to filtering the va_consent_for_non_va error and no
        # error report email was sent
        self.assertEqual(resource_data['ignore'], True)
        self.assertEqual(0, send_error_email_mock.call_count)

    def test_consent_metrics_generator_test_participant(self):
        """ Confirm test_participant flag is set by generator if participant is paired to TEST hpo """
        # Create a test participant and a consent_file record for them
        test_hpo = self.data_generator.create_database_hpo(hpoId=2000, name='TEST')
        participant = self._create_participant_with_all_consents_authored(
            consentForStudyEnrollmentFirstYesAuthored=datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            consentForStudyEnrollmentAuthored=datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
            hpoId=test_hpo.hpoId
        )
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            participant_id=participant.participantId,
            expected_sign_date=date(year=2018, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1
        )
        self.assertIsNotNone(consent_file_rec.id)
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        # Confirm the consent_metric PDR data generator flagged the test participant
        self.assertTrue(resource_data.get('test_participant'))

    def test_consent_metrics_wear_no_authored(self):
        """ consent metric record will be missing authored date if the consent_response value is null """
        # Create a test participant and a consent_file record for them
        test_hpo = self.data_generator.create_database_hpo(hpoId=2000, name='TEST')
        participant = self._create_participant_with_all_consents_authored(
            consentForStudyEnrollmentFirstYesAuthored=datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            consentForStudyEnrollmentAuthored=datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
            hpoId=test_hpo.hpoId
        )
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.WEAR,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            participant_id=participant.participantId,
            expected_sign_date=date(year=2022, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1
        )
        self.assertIsNotNone(consent_file_rec.id)
        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        # Confirm no default authored timestamp is available
        self.assertIsNone(resource_data.get('consent_authored_date'))

    def test_consent_metric_authored_from_questionnaire_response(self):
        """ Confirm consent_metric record gets authored time from related questionnaire_response record """
        # Set up a QuestionnaireResponses, with a specific authored date
        test_authored_date = datetime(year=2022, month=6, day=1)
        response_to_validate = self.data_generator.create_database_questionnaire_response(authored=test_authored_date)

        # Create the related consent_response and consent_file entries
        consent_response = ConsentResponse(response=response_to_validate)
        self.session.add(ConsentResponse(response=response_to_validate))
        consent_file_rec = self.data_generator.create_database_consent_file(
            consent_response=consent_response,
            type=1,
            sync_status=1,
            file_exists=0,
            participant_id=response_to_validate.participantId
        )
        self.session.commit()

        resource_data = self.consent_metric_resource_generator.make_resource(consent_file_rec.id).get_data()
        self.assertEqual(resource_data.get('consent_authored_date'), datetime.date(test_authored_date))

    @mock.patch('rdr_service.services.email_service.EmailService.send_email')
    def test_consent_error_report_email_generation(self, email_mock):
        # Override config settings for test purposes
        test_key = 'test_key'
        test_email_config = {
            "recipients": ["test_error_report_recipient@test.com", ],
            "cc_recipients": ["test_error_report_cc_recipient1@test.com", "test_error_report_cc_recipient2@test.com"]
        }
        config.override_setting(config.SENDGRID_KEY, [test_key])
        config.override_setting(config.PTSC_SERVICE_DESK_EMAIL, test_email_config)

        participant = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
            participantOrigin='vibrent'
        )
        # Create consent_file record with missing check error,  status NEEDS_CORRECTING
        self.data_generator.create_database_consent_file(
            type=ConsentType.GROR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            signing_date=participant.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=ConsentOtherErrors.MISSING_CONSENT_CHECK_MARK
        )
        # Create consent_file record with consent version error,  status NEEDS_CORRECTING
        self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            expected_sign_date=date(year=2018, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=ConsentOtherErrors.VETERAN_CONSENT_FOR_NON_VETERAN
        )
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.consent_error_report_generator.create_error_reports(id_list=unreported_errors,
                                                                 participant_origin='vibrent')
        # Two email reports should be sent, one for each error type (order of emails indeterminate)
        self.assertEqual(email_mock.call_count, 2)
        call_args = email_mock.call_args_list
        subject_lines = []
        for call_arg in call_args:
            subject_lines.append(call_arg.args[0].subject)
            # Verify the to/from email addresses
            self.assertEqual('no-reply@pmi-ops.org', call_arg.args[0].from_email)
            self.assertEqual(test_email_config.get('recipients'), call_arg.args[0].recipients)
            self.assertEqual(test_email_config.get('cc_recipients'), call_arg.args[0].cc_recipients)
            self.assertIn('DRC Consent Validation Issue', call_arg.args[0].subject)

        # Verify the expected subject lines were generated
        self.assertIn('DRC Consent Validation Issue | GROR | Checkbox not checked', subject_lines)
        self.assertIn('DRC Consent Validation Issue | PRIMARY | VA consent version for non-VA participant',
                      subject_lines)

    @mock.patch('rdr_service.resource.generators.consent_metrics.ConsentErrorReportGenerator.send_consent_error_email')
    def test_get_unreported_errors(self, send_error_email_mock):
        participant = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
            participantOrigin='vibrent'
        )
        # Create consent_file record with missing check error,  status NEEDS_CORRECTING
        gror_consent = self.data_generator.create_database_consent_file(
            type=ConsentType.GROR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            signing_date=participant.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=ConsentOtherErrors.MISSING_CONSENT_CHECK_MARK
        )
        # Create consent_file record with consent version error,  status NEEDS_CORRECTING
        primary_consent = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            expected_sign_date=date(year=2018, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=ConsentOtherErrors.VETERAN_CONSENT_FOR_NON_VETERAN
        )
        # Report the NEEDS_CORRECTING records created so far (two distinct error types / two email reports)
        first_unreported_ids_list = self.consent_error_report_generator.get_unreported_error_ids()
        self.consent_error_report_generator.create_error_reports(id_list=first_unreported_ids_list,
                                                                 participant_origin='vibrent')
        self.assertEqual(2, send_error_email_mock.call_count)

        # Add checkbox error for an additional consent
        ehr_consent = self.data_generator.create_database_consent_file(
            type=ConsentType.EHR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            expected_sign_date=date(year=2018, month=1, day=31),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=ConsentOtherErrors.MISSING_CONSENT_CHECK_MARK
        )
        second_unreported_ids_list = self.consent_error_report_generator.get_unreported_error_ids()
        # The second check of get_unreported_error_ids() should only find the new/not yet reported error
        self.assertEqual(first_unreported_ids_list, [gror_consent.id, primary_consent.id])
        self.assertEqual(second_unreported_ids_list, [ehr_consent.id])

    @mock.patch('rdr_service.services.email_service.EmailService.send_email')
    def test_ce_error_email(self, email_mock):
        """
        For consent errors originating from CE, email report (temporarily) goes to same recipients who are
        configured as the cc: recipients in the app config for PTSC service desk tickets
        """
        # Override config settings for test purposes
        test_key = 'test_key'
        test_email_config = {
            "recipients": ["test_error_report_recipient@test.com", ],
            "cc_recipients": ["test_error_report_cc_recipient1@test.com", "test_error_report_cc_recipient2@test.com"]
        }
        config.override_setting(config.SENDGRID_KEY, [test_key])
        config.override_setting(config.PTSC_SERVICE_DESK_EMAIL, test_email_config)

        participant = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
            participantOrigin='careevolution'
        )
        # Create NEEDS_CORRECTING consent_file record with missing check error
        self.data_generator.create_database_consent_file(
            type=ConsentType.GROR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            signing_date=participant.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=ConsentOtherErrors.MISSING_CONSENT_CHECK_MARK
        )
        # Generate the report for the CE consent error
        unreported_errors = self.consent_error_report_generator.get_unreported_error_ids()
        self.consent_error_report_generator.create_error_reports(id_list=unreported_errors,
                                                                 participant_origin='careevolution')
        self.assertEqual(email_mock.call_count, 1)
        call_args = email_mock.call_args_list
        subject_lines = []
        for call_arg in call_args:
            subject_lines.append(call_arg.args[0].subject)
            # For CE reports, the 'cc_recipients' (DRC / RDR team) config item is currently used for the "to" list.
            # The email should have no cc: list
            self.assertEqual('no-reply@pmi-ops.org', call_arg.args[0].from_email)
            self.assertEqual(test_email_config.get('cc_recipients'), call_arg.args[0].recipients)
            self.assertEmpty(call_arg.args[0].cc_recipients)
            # Confirm CE notation prepended to subject line text
            self.assertIn('(CE)', call_arg.args[0].subject)
