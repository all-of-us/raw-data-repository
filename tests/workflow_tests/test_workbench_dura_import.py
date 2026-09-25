import mock

from datetime import datetime
from rdr_service import config
from rdr_service.clock import FakeClock
from rdr_service.dao.workbench_dao import WorkbenchInstitutionalDuraDao
from rdr_service.model.workbench_researcher import WorkbenchInstitutionalDura
from rdr_service.researchers_offline.import_workbench_dura_data import WorkbenchDuraImporter
from tests.helpers.unittest_base import BaseTestCase


@mock.patch('rdr_service.researchers_offline.import_workbench_dura_data.RedcapClient')
class WorkbenchDuraImporterTest(BaseTestCase):
    def setUp(self, **kwargs):
        super(WorkbenchDuraImporterTest, self).setUp(**kwargs)

        self.temporarily_override_config_setting(config.WB_INSTITUTIONAL_DURA_REDCAP_TOKEN, '123ABC')
        self.importer = WorkbenchDuraImporter()
        self.dao = WorkbenchInstitutionalDuraDao()

    def test_dura_data_imported(self, redcap_class):
        redcap_class.return_value.send_request.return_value = [
            {
                "record_id": "1",
                "access_method": "1",
                "agreement_end_date": "2024-05-01",
                "contractoutcome": "1",
                "country_institution": "US",
                "peer_integration_complete": "0",
                "document_status___1": "0",
                "document_status___2": "1",
                "tier_access___1": "1",
                "tier_access___2": "0",
                "tier_access___3": "1",
                "tier_access___4": "0",
                "preapproval_tier___1": "1",
                "preapproval_tier___2": "0",
                "original_dura_completion": "0",
                "dura_type_active": "1",
                "currentdura_agreementstatus": "1",
                "currentdura_closed_reason": None,
                "currentdura_other_reason": None,
                "currentdura_peerconfirmationdate": "2024-05-29",
                "registration_form_checklist___1": "1",
                "dura_checklist___3": "3",
                "dura_checklist_3___2": "2",
                "currentdura_dura_checklist___4": "4"
            },
            {
                "record_id": "2",
                "access_method": "1",
                "agreement_end_date": "2024-05-29",
                "contractoutcome": "1",
                "country_institution": "GB",
                "peer_integration_complete": "0",
                "document_status___1": "1",
                "document_status___2": "0",
                "tier_access___1": "0",
                "tier_access___2": "0",
                "tier_access___3": "1",
                "tier_access___4": "0",
                "preapproval_tier___1": "0",
                "preapproval_tier___2": "1",
                "original_dura_completion": "0",
                "dura_type_active": "1",
                "currentdura_agreementstatus": "3",
                "currentdura_closed_reason": "2",
                "currentdura_other_reason": None,
                "currentdura_peerconfirmationdate": ""
            }
        ]

        import_datetime = datetime(2026, 5, 26, 3, 30, 00)
        dura_datetime_1 = datetime(2024, 5, 1, 0, 0, 00)
        dura_datetime_2 = datetime(2024, 5, 29, 0, 0, 00)

        since_date = "2026-05-30"
        with FakeClock(import_datetime):
            self.importer.import_reports(since_date)

        dura_data_1: WorkbenchInstitutionalDura = self.session.query(WorkbenchInstitutionalDura).filter(
            WorkbenchInstitutionalDura.record_id == 1
        ).one()
        dura_data_2: WorkbenchInstitutionalDura = self.session.query(WorkbenchInstitutionalDura).filter(
            WorkbenchInstitutionalDura.record_id == 2
        ).one()

        self.assertEqual('1', dura_data_1.access_method)
        self.assertEqual(dura_datetime_1, dura_data_1.agreement_end_date)
        self.assertEqual('1', dura_data_1.contractoutcome)
        self.assertEqual('US', dura_data_1.country_institution_code)
        self.assertEqual('1', dura_data_1.document_status___2)
        self.assertEqual('1', dura_data_1.tier_access___1)
        self.assertEqual('0', dura_data_1.tier_access___2)
        self.assertEqual('1', dura_data_1.tier_access___3)
        self.assertEqual('1', dura_data_1.preapproval_tier___1)
        self.assertEqual('0', dura_data_1.preapproval_tier___2)
        self.assertEqual(0, dura_data_1.original_dura_completion)
        self.assertEqual(1, dura_data_1.currentdura_agreementstatus)
        self.assertEqual(None, dura_data_1.currentdura_closed_reason)
        self.assertEqual(dura_datetime_2, dura_data_1.currentdura_peerconfirmationdate)
        self.assertEqual('1', dura_data_1.registration_form_checklist___1)
        self.assertEqual('3', dura_data_1.dura_checklist___3)
        self.assertEqual('2', dura_data_1.dura_checklist_3___2)
        self.assertEqual('4', dura_data_1.currentdura_dura_checklist___4)

        self.assertEqual('1', dura_data_2.access_method)
        self.assertEqual(dura_datetime_2, dura_data_2.agreement_end_date)
        self.assertEqual('GB', dura_data_2.country_institution_code)
        self.assertEqual('0', dura_data_2.peer_integration_complete)
        self.assertEqual('1', dura_data_2.document_status___1)
        self.assertEqual('1', dura_data_2.tier_access___3)
        self.assertEqual('0', dura_data_2.tier_access___4)
        self.assertEqual('0', dura_data_2.preapproval_tier___1)
        self.assertEqual('1', dura_data_2.preapproval_tier___2)
        self.assertEqual(1, dura_data_2.dura_type_active)
        self.assertEqual(3, dura_data_2.currentdura_agreementstatus)
        self.assertEqual(2, dura_data_2.currentdura_closed_reason)
        self.assertEqual(None, dura_data_2.currentdura_other_reason)
        self.assertEqual(None, dura_data_2.currentdura_peerconfirmationdate)

    def test_dura_data_updates_imported(self, redcap_class):
        redcap_class.return_value.send_request.return_value = [
            {
                "record_id": "1",
                "access_method": "1",
                "agreement_end_date": "2024-05-01",
                "contractoutcome": "1",
                "country_institution": "US",
                "peer_integration_complete": "0"
            }
        ]

        dura_datetime_1 = datetime(2024, 5, 1, 0, 0, 00)
        dura_datetime_2 = datetime(2024, 5, 29, 0, 0, 00)

        import_time_1 = datetime(2024, 1, 1)
        with FakeClock(import_time_1):
            self.importer.import_reports()

        dura_data_1 = self.dao.get_all()

        self.assertEqual(1, len(dura_data_1))
        self.assertEqual('1', dura_data_1[0].access_method)
        self.assertEqual(dura_datetime_1, dura_data_1[0].agreement_end_date)
        self.assertEqual('1', dura_data_1[0].contractoutcome)
        self.assertEqual('US', dura_data_1[0].country_institution_code)
        self.assertEqual('0', dura_data_1[0].peer_integration_complete)

        # Test updates to records insertion
        redcap_class.return_value.send_request.return_value = [
            {
                "record_id": "1",
                "access_method": "1",
                "agreement_end_date": "2024-05-01",
                "contractoutcome": "1",
                "country_institution": "IN",
                "peer_integration_complete": "1"
            },
            {
                "record_id": "2",
                "access_method": "1",
                "agreement_end_date": "2024-05-29",
                "contractoutcome": "1",
                "country_institution": "GB",
                "peer_integration_complete": "0"
            }
        ]

        import_time_2 = datetime(2024, 6, 1)
        with FakeClock(import_time_2):
            self.importer.import_reports()

        dura_data_2 = self.dao.get_all()

        self.assertEqual(3, len(dura_data_2))
        self.assertEqual('1', dura_data_2[1].access_method)
        self.assertEqual(dura_datetime_1, dura_data_2[1].agreement_end_date)
        self.assertEqual('IN', dura_data_2[1].country_institution_code)
        self.assertEqual('1', dura_data_2[1].peer_integration_complete)
        self.assertEqual('1', dura_data_2[2].access_method)
        self.assertEqual(dura_datetime_2, dura_data_2[2].agreement_end_date)
        self.assertEqual('1', dura_data_2[2].contractoutcome)
        self.assertEqual('GB', dura_data_2[2].country_institution_code)
