from datetime import datetime, timedelta
import mock

from rdr_service.clock import FakeClock
from rdr_service.offline.import_hpo_lite_pairing import HpoLitePairingImporter
from rdr_service.model.hpo_lite_pairing_import_record import HpoLitePairingImportRecord
from rdr_service.model.utils import to_client_participant_id
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from tests.helpers.unittest_base import BaseTestCase
from rdr_service import config


@mock.patch('rdr_service.offline.import_hpo_lite_pairing.RedcapClient')
class HpoLitePairingImporterTest(BaseTestCase):
    def setUp(self, **kwargs):
        super(HpoLitePairingImporterTest, self).setUp(**kwargs)
        self.test_api_key = '123ABC'
        config.override_setting(config.HPO_LITE_REDCAP_PROJECT_TOKEN, self.test_api_key)
        config.override_setting(config.HPO_LITE_ORG_NAME_MAPPING,
                                {
                                    '1': 'ORG_TEST'
                                })

        # Patching to prevent consent validation checks from running
        build_validator_patch = mock.patch(
            'rdr_service.services.consent.validation.ConsentValidationController.build_controller'
        )
        build_validator_patch.start()
        self.addCleanup(build_validator_patch.stop)

    @staticmethod
    def _redcap_hpo_lite_pairing_record(participant_id, paired_date, hpo_name, user_email, hpo_lite_pairing_complete):
        """Build out a record that matches the codes we're expecting from redcap"""
        return {
            'recordid': to_client_participant_id(participant_id),
            'paired_date': paired_date,
            'hpo_name': hpo_name,
            'user_email': user_email,
            'hpo_lite_pairing_complete': hpo_lite_pairing_complete
        }

    def test_report_import_uses_key_and_default_date_range(self, redcap_class):
        """Test the parameters that the importer uses for Redcap"""

        # The importer will import everything from the start of yesterday by default
        now_yesterday = datetime.now() - timedelta(days=1)
        start_of_yesterday = datetime(now_yesterday.year, now_yesterday.month, now_yesterday.day)

        importer = HpoLitePairingImporter()
        importer.import_pairing_data()

        redcap_instance = redcap_class.return_value
        redcap_instance.get_records.assert_called_with(self.test_api_key, start_of_yesterday)

    def test_hpo_lite_pairing_data_importing(self, redcap_class):
        unpaired_participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(participant=unpaired_participant)

        hpo = self.data_generator.create_database_hpo(name='HPO_TEST')
        org = self.data_generator.create_database_organization(hpoId=hpo.hpoId,
                                                               externalId='ORG_TEST')

        redcap_class.return_value.get_records.return_value = [
            self._redcap_hpo_lite_pairing_record(
                participant_id=unpaired_participant.participantId,
                paired_date='2020-01-01',
                hpo_name='1',
                hpo_lite_pairing_complete='2',
                user_email='reportauthor@test.com'
            )
        ]

        import_datetime = datetime(2020, 10, 24, 1, 27, 45)
        importer = HpoLitePairingImporter()
        with FakeClock(import_datetime):
            importer.import_pairing_data()

        record = self.session.query(HpoLitePairingImportRecord).filter(
            HpoLitePairingImportRecord.participantId == unpaired_participant.participantId
        ).first()

        self.assertEqual(record.participantId, unpaired_participant.participantId)
        self.assertEqual(record.orgId, org.organizationId)
        self.assertEqual(record.pairedDate, datetime(2020, 1, 1, 0, 0))
        self.assertNotEqual(record.uploadingUserId, None)

        p_dao = ParticipantDao()
        p = p_dao.get(unpaired_participant.participantId)
        self.assertEqual(p.hpoId, hpo.hpoId)
        self.assertEqual(p.organizationId, org.organizationId)
        self.assertEqual(p.siteId, None)
        ps_dao = ParticipantSummaryDao()
        ps = ps_dao.get(unpaired_participant.participantId)
        self.assertEqual(ps.hpoId, hpo.hpoId)
        self.assertEqual(ps.organizationId, org.organizationId)
        self.assertEqual(ps.siteId, None)

    @mock.patch('rdr_service.offline.import_hpo_lite_pairing.logging')
    def test_hpo_lite_pairing_data_importing_already_paired(self, mock_logging, redcap_class):
        hpo = self.data_generator.create_database_hpo(name='HPO_TEST')
        self.data_generator.create_database_organization(hpoId=hpo.hpoId, externalId='ORG_TEST')

        paired_participant = self.data_generator.create_database_participant(
            hpoId=hpo.hpoId
        )
        self.data_generator.create_database_participant_summary(participant=paired_participant)

        redcap_class.return_value.get_records.return_value = [
            self._redcap_hpo_lite_pairing_record(
                participant_id=paired_participant.participantId,
                paired_date='2020-01-01',
                hpo_name='1',
                hpo_lite_pairing_complete='2',
                user_email='reportauthor@test.com'
            )
        ]

        import_datetime = datetime(2020, 10, 24, 1, 27, 45)
        importer = HpoLitePairingImporter()
        with FakeClock(import_datetime):
            importer.import_pairing_data()

        mock_logging.error.assert_has_calls([
            mock.call(f'{paired_participant.participantId} is not exist or already paired with other HPO')
        ])

    @mock.patch('rdr_service.offline.import_hpo_lite_pairing.logging')
    def test_hpo_lite_pairing_data_importing_bad_input(self, mock_logging, redcap_class):
        unpaired_participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(participant=unpaired_participant)

        hpo = self.data_generator.create_database_hpo(name='HPO_TEST')
        self.data_generator.create_database_organization(hpoId=hpo.hpoId, externalId='ORG_TEST')

        redcap_class.return_value.get_records.return_value = [
            self._redcap_hpo_lite_pairing_record(
                participant_id=123456789, # not exist participant id
                paired_date='2020-01-01',
                hpo_name='1',
                hpo_lite_pairing_complete='2',
                user_email='reportauthor@test.com'
            ),
            self._redcap_hpo_lite_pairing_record(
                participant_id=unpaired_participant.participantId,
                paired_date='wrong_date_format',
                hpo_name='1',
                hpo_lite_pairing_complete='2',
                user_email='reportauthor@test.com'
            )
        ]

        import_datetime = datetime(2020, 10, 24, 1, 27, 45)
        importer = HpoLitePairingImporter()
        with FakeClock(import_datetime):
            importer.import_pairing_data()

        mock_logging.error.assert_has_calls([
            mock.call('123456789 is not exist or already paired with other HPO'),
            mock.call(f'Record for {unpaired_participant.participantId} encountered an error', exc_info=True)
        ])

    @mock.patch('rdr_service.offline.import_hpo_lite_pairing.logging')
    def test_hpo_lite_pairing_data_importing_org_not_found(self, mock_logging, redcap_class):
        unpaired_participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(participant=unpaired_participant)

        hpo = self.data_generator.create_database_hpo(name='HPO_TEST')
        self.data_generator.create_database_organization(hpoId=hpo.hpoId, externalId='ORG_NOT_EXIST')

        redcap_class.return_value.get_records.return_value = [
            self._redcap_hpo_lite_pairing_record(
                participant_id=unpaired_participant.participantId,
                paired_date='2020-01-01',
                hpo_name='1',
                hpo_lite_pairing_complete='2',
                user_email='reportauthor@test.com'
            )
        ]

        import_datetime = datetime(2020, 10, 24, 1, 27, 45)
        importer = HpoLitePairingImporter()
        with FakeClock(import_datetime):
            importer.import_pairing_data()

        mock_logging.error.assert_has_calls([
            mock.call(f'Organization ORG_TEST not found for {unpaired_participant.participantId}')
        ])
