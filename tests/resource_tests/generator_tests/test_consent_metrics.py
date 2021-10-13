#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from datetime import datetime, date
from tests.helpers.unittest_base import BaseTestCase

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.model.consent_file import ConsentSyncStatus, ConsentType, ConsentOtherErrors
import rdr_service.resource.generators

class ConsentMetricGeneratorTest(BaseTestCase):

    def setUp(self, *args, **kwargs) -> None:
        super(ConsentMetricGeneratorTest, self).setUp(*args, **kwargs)
        self.resource_data_dao = ResourceDataDao()

    def _create_participant_with_all_consents_authored(self, **kwargs):

        participant = self.data_generator.create_database_participant_summary(
            participantOrigin='vibrent',
            consentForStudyEnrollmentAuthored=datetime.strptime('2020-01-01 01:00:00', "%Y-%m-%d %H:%M:%S"),
            consentForStudyEnrollmentFirstYesAuthored=datetime.strptime('2020-01-01 01:00:00', "%Y-%m-%d %H:%M:%S"),
            consentForCABoRAuthored=datetime.strptime('2020-01-01 02:00:00', "%Y-%m-%d %H:%M:%S"),
            consentForElectronicHealthRecordsAuthored=datetime.strptime('2020-01-01 03:00:00', "%Y-%m-%d %H:%M:%S"),
            consentForElectronicHealthRecordsFirstYesAuthored=\
                datetime.strptime('2020-01-01 03:00:00', "%Y-%m-%d %H:%M:%S"),
            consentForGenomicsRORAuthored=datetime.strptime('2020-01-01 04:00:00', "%Y-%m-%d %H:%M:%S"),
            **kwargs
        )
        return participant

    def _create_participant_with_custom_primary_consent_authored(self, authored, **kwargs):
        participant = self.data_generator.create_database_participant_summary(
            participantOrigin='vibrent',
            consentForStudyEnrollmentAuthored=authored,
            consentForStudyEnrollmentFirstYesAuthored=authored,
            **kwargs
        )
        return participant

    @staticmethod
    def _create_expected_metrics_dict(participant, consent_type=ConsentType.PRIMARY,
                                      consent_status=ConsentSyncStatus.READY_FOR_SYNC, expected_errors=[]):
        """
        Set up a dictionary of values to compare against resource data dictionary from ConsentMetricsGenerator;
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
                                'invalid_age_at_consent': ('invalid_age_at_consent' in expected_errors)
                                }

        return expected_values_dict

    def test_consent_metrics_generator_no_errors(self):
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

        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()
        resource_data = res_gen.make_resource(consent_file_rec.id).get_data()

        # No expected_errors provided, all error conditions default to False
        expected = self._create_expected_metrics_dict(participant, expected_errors=[])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # Also check that the authored date matches the date from the participant_summary record
        self.assertEqual(resource_data.get('consent_authored_date', None),
                         datetime.date(participant.consentForElectronicHealthRecordsFirstYesAuthored))

    def test_consent_metrics_generator_dob_invalid(self):
        """
        invalid_dob error calculated from participant_summary data, sync_status can still be READY_TO_SYNC
        """

        # Create participant summary data with (1) DOB missing,  and (2) DOB > 124 years from primary consent authored
        participant_1 = self._create_participant_with_all_consents_authored(dateOfBirth=None)
        participant_2 = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('1895-12-31', '%Y-%m-%d'))
        )
        # Create consent_file records for each participant's primary consent with no other error conditions
        consent_file_rec_1 = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            participant_id=participant_1.participantId,
            signing_date=participant_1.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1
        )
        consent_file_rec_2 = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            participant_id=participant_2.participantId,
            signing_date=participant_2.consentForStudyEnrollmentFirstYesAuthored.date(),
            expected_sign_date=date(year=2020, month=1, day=1),
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1
        )
        self.assertIsNotNone(consent_file_rec_1.id)
        self.assertIsNotNone(consent_file_rec_2.id)
        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()

        # Expected: Invalid DOB because DOB is missing
        resource_data = res_gen.make_resource(consent_file_rec_1.id).get_data()
        expected = self._create_expected_metrics_dict(participant_1, expected_errors=['invalid_dob'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # Expected: Invalid DOB because DOB is > 124 years before primary consent authored date
        resource_data = res_gen.make_resource(consent_file_rec_2.id).get_data()
        expected = self._create_expected_metrics_dict(participant_2, expected_errors=['invalid_dob'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

    def test_consent_metrics_generator_invalid_age_at_consent(self):
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
        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()

        # Expected: invalid_age_at_consent (less than 18 years of age)
        resource_data = res_gen.make_resource(consent_file_rec.id).get_data()
        expected = self._create_expected_metrics_dict(participant, expected_errors=['invalid_age_at_consent'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

    def test_consent_metrics_generator_missing_file(self):
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
        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()

        # Expected: invalid_age_at_consent (less than 18 years of age)
        resource_data = res_gen.make_resource(consent_file_rec.id).get_data()
        # Note:  if file is missing, neither the signature_missing or invalid_signing_date errors should be set
        expected = self._create_expected_metrics_dict(participant,
                                                      consent_status=ConsentSyncStatus.NEEDS_CORRECTING,
                                                      expected_errors=['missing_file'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

    def test_consent_metrics_generator_dob_and_file_errors(self):
        """
         Consent metrics signature_missing error + invalid_age_at_consent error from primary consent
         """
        # Create participant summary data (DOB < 18 years from primary consent authored date)
        participant = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('2004-01-01', '%Y-%m-%d')),
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
        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()

        # Expected: invalid_age_at_consent and signature_missing errors
        resource_data = res_gen.make_resource(consent_file_rec.id).get_data()
        expected = self._create_expected_metrics_dict(participant,
                                                      consent_status=ConsentSyncStatus.NEEDS_CORRECTING,
                                                      expected_errors=['invalid_age_at_consent', 'signature_missing'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

    def test_consent_metrics_generator_other_errors(self):
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

        # Create consent_file record with non-veteran consent for veteran participant error
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

        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()

        # Expected: checkbox_unchecked error for consent_file_rec_1
        resource_data = res_gen.make_resource(consent_file_rec_1.id).get_data()
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
        resource_data = res_gen.make_resource(consent_file_rec_2.id).get_data()
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
        resource_data=res_gen.make_resource(consent_file_rec_3.id).get_data()
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

    def test_consent_metrics_generator_resolved_date(self):
        """
        For OBSOLETE sync status, confirm the resolved date equals the last modified date from consent_file record
        """
        # Create participant summary data (valid DOB)
        participant = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
        )
        # Create consent_file record with file_exists set to false, status NEEDS_CORRECTING
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
        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()

        # Expected: checkbox_unchecked error
        resource_data = res_gen.make_resource(consent_file_rec.id).get_data()
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

    def test_consent_metrics_generator_signature_missing_error_filtered(self):
        """
        Ignore known potential false positives for missing signatures, for consents authored before 2018-07-13
        """
        # Create participant summary data with a primary consent authored date before the false positive cutoff
        participant = self._create_participant_with_custom_primary_consent_authored(
            datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
        )
        # Create consent_file record with file_exists set to false, status NEEDS_CORRECTING
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            expected_sign_date=date(year=2018, month=1, day=1),
            file_exists=1,
            is_signature_valid=0,
        )
        self.assertIsNotNone(consent_file_rec.id)
        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()
        resource_data = res_gen.make_resource(consent_file_rec.id).get_data()

        # Confirm this record's ignore flag was set due to filtering the signature_missing error
        self.assertEqual(resource_data['ignore'], True)

    def test_consent_metrics_generator_special_sync_status_filtered(self):
        """
        Ignore consent records whose current sync_status is a special case status such as UNKNOWN or DELAYING_SYNC
        """
        # Create participant summary data with a primar consent authored date before the false positive cutoff
        participant = self._create_participant_with_custom_primary_consent_authored(
            datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            dateOfBirth=datetime.date(datetime.strptime('1999-01-01', '%Y-%m-%d')),
        )
        # Create consent_file record with file_exists set to false, status NEEDS_CORRECTING
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
        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()
        resource_data = res_gen.make_resource(consent_file_rec.id).get_data()

        # Confirm this record's ignore flag was set due to filtering on the special sync_status
        self.assertEqual(resource_data['ignore'], True)

    def test_consent_metrics_generator_va_consent_for_non_va_filtered(self):
        """
        Ignore va_consent_for_non_va errors if that's the only error and participant's current pairing is to the VA HPO
        """
        va_hpo = self.data_generator.create_database_hpo(hpoId=2000, name='VA')
        # Create participant summary data with a primary consent authored date before the false positive cutoff
        participant = self._create_participant_with_custom_primary_consent_authored(
            datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
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
        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()
        resource_data = res_gen.make_resource(consent_file_rec.id).get_data()

        # Confirm this record's ignore flag was set due to filtering the va_consent_for_non_va error
        self.assertEqual(resource_data['ignore'], True)


