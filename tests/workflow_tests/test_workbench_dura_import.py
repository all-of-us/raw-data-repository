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
                "peer_integration_complete": "0"
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

        import_datetime = datetime(2026, 5, 26, 3, 30, 00)
        dura_datetime_1 = datetime(2024, 5, 1, 0, 0, 00)
        dura_datetime_2 = datetime(2024, 5, 29, 0, 0, 00)

        with FakeClock(import_datetime):
            self.importer.import_reports()

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
        self.assertEqual('1', dura_data_2.access_method)
        self.assertEqual(dura_datetime_2, dura_data_2.agreement_end_date)
        self.assertEqual('GB', dura_data_2.country_institution_code)
        self.assertEqual('0', dura_data_2.peer_integration_complete)
