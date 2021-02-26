import mock

from rdr_service.services.data_dictionary_updater import DataDictionaryUpdater, dictionary_tab_id,\
    internal_tables_tab_id, hpo_key_tab_id, questionnaire_key_tab_id, site_key_tab_id
from tests.helpers.unittest_base import BaseTestCase


class DataDictionaryUpdaterTest(BaseTestCase):
    def setUp(self, **kwargs) -> None:
        super(DataDictionaryUpdaterTest, self).setUp(**kwargs)
        self.updater = DataDictionaryUpdater('', '', self.session)

        # Patch system calls that the sheets client uses
        self.patchers = []  # We'll need to stop the patchers when the tests are done
        for module_to_patch in [
            'rdr_service.services.google_sheets_client.gcp_get_iam_service_key_info',
            'rdr_service.services.google_sheets_client.ServiceAccountCredentials'
        ]:
            patcher = mock.patch(module_to_patch)
            patcher.start()
            self.patchers.append(patcher)

        # Get sheets api service mock so that the tests can check calls to the google api
        service_patcher = mock.patch('rdr_service.services.google_sheets_client.discovery')
        self.patchers.append(service_patcher)
        mock_discovery = service_patcher.start()
        self.mock_spreadsheets_return = mock_discovery.build.return_value.spreadsheets.return_value

        # We can't create tabs through code, so we need to initialize the tabs through the download
        self.tab_names = [
            dictionary_tab_id, internal_tables_tab_id, hpo_key_tab_id, questionnaire_key_tab_id, site_key_tab_id
        ]
        self.mock_spreadsheets_return.get.return_value.execute.return_value = {
            'sheets': [{
                'properties': {'title': tab_name},
                'data': [{}]
            } for tab_name in self.tab_names]
        }

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()

    def test_spreadsheet_downloads_values_from_drive(self):
        self.updater.run_update()
        print('bob')
