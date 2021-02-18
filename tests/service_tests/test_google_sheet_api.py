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

    def tearDown(self):
        # Stop the patchers (in case other tests use them in different ways)
        for patcher in self.patchers:
            patcher.stop()

    def test_empty_call_structure(self):
        """
        Test the integrity of an empty call to the google api
        and make sure options other than cell values are as expected
        """
        expected_spreadsheet_id = '123ABC'
        with GoogleSheetApi(expected_spreadsheet_id, 'service_key_id'):
            pass

        self.mock_spreadsheets_return.values.return_value.update.assert_called_with(
            spreadsheetId=expected_spreadsheet_id,
            range='A1',
            valueInputOption='RAW',
            body={
                'values': []
            }
        )

    def _get_values_sent_to_spreadsheet(self):
        return self.mock_spreadsheets_return.values.return_value.update.call_args.kwargs.get('body').get('values')

    def test_value_arrays_sent_to_api(self):
        """
        Test that the values sent to the API construct a matrix that will fill in the google spreadsheet as expected
        """

        # This test will expect the resulting spreadsheet update to look like the grid below.
        # Underscores represent a cell that will be blank.
        # _ _ 7 _ 9
        # _ 5
        # 3
        # _
        # _ _ 4
        with GoogleSheetApi('', '') as spreadsheet:
            spreadsheet.change_cell(2, 0, '3')  # 3 on third row
            spreadsheet.change_cell(0, 4, '9')  # 9 on first row
            spreadsheet.change_cell(4, 2, '4')  # 4 on last row
            spreadsheet.change_cell(0, 2, '7')  # 7 on first row
            spreadsheet.change_cell(1, 1, '5')  # 5 on second row

        self.assertEqual([
            ['', '', '7', '', '9'],
            ['', '5'],
            ['3'],
            [''],
            ['', '', '4']
        ], self._get_values_sent_to_spreadsheet())

    @classmethod
    def _grid_value(cls, value):
        cell_dict = {}
        if value:
            cell_dict['formattedValue'] = value

        return cell_dict

    def test_spreadsheet_loads_values_from_drive(self):
        """Check that the spreadsheet initializes with the values currently in the spreadsheet on drive as expected"""
        expected_values_grid = [
            ['', '', '7', '', '9'],
            ['', '5'],
            ['3'],
            [''],
            ['', '', '4']
        ]
        empty_cell = self._grid_value(None)
        self.mock_spreadsheets_return.get.return_value.execute.return_value = {
            'sheets': [{
                'data': [{
                    'rowData': [
                        {'values': [empty_cell, empty_cell, self._grid_value('7'), empty_cell, self._grid_value('9')]},
                        {'values': [empty_cell, self._grid_value('5')]},
                        {'values': [self._grid_value('3')]},
                        {'values': [empty_cell]},
                        {'values': [empty_cell, empty_cell, self._grid_value('4')]}
                    ]
                }]
            }]
        }

        with GoogleSheetApi('', '') as sheet:
            self.assertEqual(expected_values_grid, sheet._values_grid)
