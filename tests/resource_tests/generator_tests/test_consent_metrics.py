#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from datetime import datetime
from tests.helpers.unittest_base import BaseTestCase

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.model.consent_file import ConsentSyncStatus, ConsentType, ConsentErrors
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
                                'invalid_signature': ('invalid_signature' in expected_errors),
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
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1
        )
        self.assertIsNotNone(consent_file_rec.id)

        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()
        resource_data = res_gen.make_resource(consent_file_rec.id).get_data()

        # No expected_errors provided, all error conditions default to False
        expected = self._create_expected_metrics_dict(participant)
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # Also check that the authored date matches the date from the participant_summary record
        self.assertEqual(resource_data.get('consent_authored_date', None),
                         datetime.date(participant.consentForElectronicHealthRecordsFirstYesAuthored))

    def test_consent_metrics_generator_dob_invalid(self):
        """
        This error is calculated from participant_summary data,
        but the consent_file table record can still be READY_TO_SYNC if there were no other issues
        """

        # Create participant summary data with DOB missing and with DOB > 124 years from primary consent authored
        participant_1 = self._create_participant_with_all_consents_authored(dateOfBirth=None)
        participant_2 = self._create_participant_with_all_consents_authored(
            dateOfBirth=datetime.date(datetime.strptime('1000-01-01', '%Y-%m-%d'))
        )
        # Create consent_file records for each participant's primary consent with no other error conditions
        consent_file_rec_1 = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            participant_id=participant_1.participantId,
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1
        )
        consent_file_rec_2 = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            participant_id=participant_2.participantId,
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
         Consent metrics invalid_age_at_consent error is calculated from participant_summary data,
         but the consent_file table record can still be READY_TO_SYNC if there were no other issues
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
            file_exists=0,
            # Because file_exists is 0, these should not be flagged as additional errors
            is_signature_valid=0,
            is_signing_date_valid=0
        )
        self.assertIsNotNone(consent_file_rec.id)
        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()

        # Expected: invalid_age_at_consent (less than 18 years of age)
        resource_data = res_gen.make_resource(consent_file_rec.id).get_data()
        expected = self._create_expected_metrics_dict(participant,
                                                      consent_status=ConsentSyncStatus.NEEDS_CORRECTING,
                                                      expected_errors=['missing_file'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

    def test_consent_metrics_generator_dob_and_file_errors(self):
        """
         Consent metrics invalid_signature error + invalid_age_at_consent error from primary consent
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
        # Create consent_file record with file_exists set to false, status NEEDS_CORRECTING
        consent_file_rec = self.data_generator.create_database_consent_file(
            type=ConsentType.GROR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            participant_id=participant.participantId,
            file_exists=1,
            is_signature_valid=1,
            is_signing_date_valid=1,
            other_errors=ConsentErrors.MISSING_CONSENT_CHECK_MARK
        )
        self.assertIsNotNone(consent_file_rec.id)
        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()

        # Expected: invalid_age_at_consent and signature_missing errors
        resource_data = res_gen.make_resource(consent_file_rec.id).get_data()
        expected = self._create_expected_metrics_dict(participant,
                                                      consent_type=ConsentType.GROR,
                                                      consent_status=ConsentSyncStatus.NEEDS_CORRECTING,
                                                      expected_errors=['checkbox_unchecked'])
        generated = {k: v for k, v in resource_data.items() if k in expected}
        self.assertDictEqual(generated, expected)

        # Also validate the resource data consent_authored_date matches the participant_summary GROR authored date
        # Also check that the authored date matches the date from the participant_summary record
        self.assertEqual(resource_data.get('consent_authored_date', None),
                         datetime.date(participant.consentForGenomicsRORAuthored))

    def test_consent_metrics_generator_obsoleted_error(self):
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
            file_exists=1,
            is_signature_valid=0,
            is_signing_date_valid=1,
            other_errors=ConsentErrors.MISSING_CONSENT_CHECK_MARK
        )
        self.assertIsNotNone(consent_file_rec.id)
        res_gen = rdr_service.resource.generators.ConsentMetricsGenerator()

        # Expected: invalid_age_at_consent and signature_missing errors
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
