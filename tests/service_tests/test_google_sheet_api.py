import mock

from rdr_service.services.google_sheet_api import GoogleSheetApi
from tests.helpers.unittest_base import BaseTestCase


class GoogleSheetsApiTest(BaseTestCase):
    def setUp(self, **kwargs):
        super(GoogleSheetsApiTest, self).setUp(**kwargs)

        # Patch system calls that the sheets api uses
        self.patchers = []  # We'll need to stop the patchers when the tests are done
        for module_to_patch in [
            'rdr_service.services.google_sheet_api.gcp_get_iam_service_key_info',
            'rdr_service.services.google_sheet_api.ServiceAccountCredentials'
        ]:
            patcher = mock.patch(module_to_patch)
            patcher.start()
            self.patchers.append(patcher)

        # Get sheets api service mock so that the tests can check calls to the google api
        service_patcher = mock.patch('rdr_service.services.google_sheet_api.discovery')
        self.patchers.append(service_patcher)

        mock_discovery = service_patcher.start()
        self.mock_spreadsheets_return = mock_discovery.build.return_value.spreadsheets.return_value

        # We can't create tabs through code, so have the spreadsheet download two empty tabs by default
        self.default_tab_names = ['one', 'two']
        self.mock_spreadsheets_return.get.return_value.execute.return_value = {
            'sheets': [{
                'properties': {'title': tab_name},
                'data': [{'rowData': [{'values': [self._mock_cell(None)]}]}]
            } for tab_name in self.default_tab_names]
        }

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()

    def test_spreadsheet_downloads_values_from_drive(self):
        """Check that the spreadsheet initializes with the values currently in the spreadsheet on drive"""

        # Mock a spreadsheet with three tabs on google drive
        # The test spreadsheet looks like this:
        # Tab title: 'first_tab'
        # _ _ '7' _ '9'
        # _ '5'
        # '3'
        # _
        # _ _ '4'
        # Tab title: 'another tab'
        # _
        # _ _ 'another tab for testing'
        # Tab title: 'Final tab'
        # 'test'

        empty_cell = self._mock_cell(None)
        first_tab_title = 'first_tab'
        first_tab_data = {
            'properties': {'title': first_tab_title},
            'data': [{
                'rowData': [
                    {'values': [empty_cell, empty_cell, self._mock_cell('7'), empty_cell, self._mock_cell('9')]},
                    {'values': [empty_cell, self._mock_cell('5')]},
                    {'values': [self._mock_cell('3')]},
                    {'values': [empty_cell]},
                    {'values': [empty_cell, empty_cell, self._mock_cell('4')]}
                ]
            }]
        }
        second_tab_title = 'another tab'
        second_tab_data = {
            'properties': {'title': second_tab_title},
            'data': [{
                'rowData': [
                    {'values': [empty_cell]},
                    {'values': [empty_cell, empty_cell, self._mock_cell('another tab for testing')]}
                ]
            }]
        }
        third_tab_title = 'Final tab'
        third_tab_data = {
            'properties': {'title': third_tab_title},
            'data': [{
                'rowData': [
                    {'values': [self._mock_cell('test')]}
                ]
            }]
        }
        self.mock_spreadsheets_return.get.return_value.execute.return_value = {
            'sheets': [first_tab_data, second_tab_data, third_tab_data]
        }

        expected_first_tab_values = [
            ['', '', '7', '', '9'],
            ['', '5'],
            ['3'],
            [''],
            ['', '', '4']
        ]
        expected_second_tab_values = [
            [''],
            ['', '', 'another tab for testing']
        ]
        expected_third_tab_values = [
            ['test']
        ]
        with GoogleSheetApi('', '') as sheet:
            self.assertEqual(expected_first_tab_values, sheet.get_tab_values())
            self.assertEqual(expected_second_tab_values, sheet.get_tab_values(second_tab_title))
            self.assertEqual(expected_third_tab_values, sheet.get_tab_values(third_tab_title))

    def test_modifying_sheet_values(self):
        # Construct a sheet that appears as follows (underscores represent a cell that will be blank):
        # Tab title: first_tab
        # _ _ 3
        # 4
        # Tab title: second_tab
        # _
        # _ 5
        # 7
        with GoogleSheetApi('', '') as sheet:
            sheet.update_cell(0, 2, '3')  # Set 3 on first tab
            sheet.update_cell(2, 0, '7', self.default_tab_names[1])  # Set 7 on second tab
            sheet.set_current_tab(self.default_tab_names[1])
            sheet.update_cell(1, 1, '5')  # Set 5 on second tab
            sheet.update_cell(1, 0, '4', self.default_tab_names[0])  # Set 4 on first tab

            # Check that the tab values are set as expected
            self.assertEqual([
                ['', '', '3'],
                ['4']
            ], sheet.get_tab_values(self.default_tab_names[0]))
            self.assertEqual([
                [''],
                ['', '5'],
                ['7']
            ], sheet.get_tab_values())  # Sheet should still be default to the second tab

    def test_tab_data_uploaded_to_api(self):
        """
        Test that the values sent to the API construct a matrix that will fill in the google spreadsheet as expected
        """

        # Construct a sheet that appears as follows (underscores represent a cell that will be blank):
        # Tab title: first_tab
        # _ _ 3
        # 4
        # Tab title: second_tab
        # _
        # _ 5
        # 7
        with GoogleSheetApi('', '') as sheet:
            sheet.update_cell(0, 2, '3')  # Set 3 on first tab
            sheet.update_cell(1, 0, '4')  # Set 4 on first tab
            sheet.set_current_tab(self.default_tab_names[1])
            sheet.update_cell(2, 0, '7')  # Set 7 on second tab
            sheet.update_cell(1, 1, '5')  # Set 5 on second tab

        tabs_uploaded = self.mock_spreadsheets_return.values.return_value.batchUpdate\
            .call_args.kwargs.get('body').get('data')

        first_uploaded_tab_data = tabs_uploaded[0]
        self.assertEqual(f"'{self.default_tab_names[0]}'!A1", first_uploaded_tab_data['range'])
        self.assertEqual([
            ['', '', '3'],
            ['4']
        ], first_uploaded_tab_data['values'])

        second_uploaded_tab_data = tabs_uploaded[1]
        self.assertEqual(f"'{self.default_tab_names[1]}'!A1", second_uploaded_tab_data['range'])
        self.assertEqual([
            [''],
            ['', '5'],
            ['7']
        ], second_uploaded_tab_data['values'])

    @classmethod
    def _mock_cell(cls, value):
        cell_dict = {}
        if value:
            cell_dict['formattedValue'] = value

        return cell_dict

    def test_values_are_only_mutable_through_object_interface(self):
        """
        To help keep future designs clean, we want to make sure that the internal structure of values for the sheet
        can only be modified by the sheet (and not through the data structure given when getting values for a tab).
        """

        # Set up a sheet with some values. The resulting sheet should appear as follows
        # 1
        # _
        # _ _ _ 8
        with GoogleSheetApi('', '') as sheet:
            sheet.update_cell(0, 0, '1')
            sheet.update_cell(2, 3, '8')

            # Get the value structure returned from the sheet and modify it
            values = sheet.get_tab_values()
            del values[1]  # remove the empty row
            values[0][0] = '3'  # Change the 1 to a 3

            # Check that the sheet values remain unchanged
            self.assertEqual([
                ['1'],
                [''],
                ['', '', '', '8']
            ], sheet.get_tab_values())
