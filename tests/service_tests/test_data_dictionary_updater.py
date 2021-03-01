import mock

from rdr_service.services.data_dictionary_updater import DataDictionaryUpdater, dictionary_tab_id,\
    internal_tables_tab_id, hpo_key_tab_id, questionnaire_key_tab_id, site_key_tab_id
from tests.helpers.unittest_base import BaseTestCase
from tests.service_tests.test_google_sheets_client import GoogleSheetsTestBase


class DataDictionaryUpdaterTest(GoogleSheetsTestBase):
    def setUp(self, **kwargs) -> None:
        super(DataDictionaryUpdaterTest, self).setUp(**kwargs)
        self.updater = DataDictionaryUpdater('', '', self.session)

    @classmethod
    def _default_tab_names(cls):
        return [
            dictionary_tab_id, internal_tables_tab_id, hpo_key_tab_id, questionnaire_key_tab_id, site_key_tab_id
        ]

    def test_updating_data_dictionary_tab(self):
        self.updater.run_update()
        vals = self._get_uploaded_sheet_data()
        print('bob')
