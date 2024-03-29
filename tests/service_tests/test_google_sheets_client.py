from googleapiclient.errors import HttpError
import mock

from rdr_service.services.google_sheets_client import GoogleSheetsClient
from tests.helpers.unittest_base import BaseTestCase


class GoogleSheetsTestBase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, **kwargs) -> None:
        super(GoogleSheetsTestBase, self).setUp(**kwargs)

        # Patch system calls that the sheets api uses
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

        self._empty_cell = self._mock_cell(None)

        # We can't create tabs through code, so have the spreadsheet download two empty tabs by default
        self.default_tab_names = self._default_tab_names()
        self.mock_spreadsheets_return.get.return_value.execute.return_value = {
            'sheets': [{
                'properties': {'title': tab_name},
                'data': [{'rowData': [{'values': [self._empty_cell]}]}]
            } for tab_name in self.default_tab_names]
        }

    def tearDown(self):
        super(GoogleSheetsTestBase, self).tearDown()
        for patcher in self.patchers:
            patcher.stop()

    @classmethod
    def _default_tab_names(cls):
        return ['one', 'two']

    @classmethod
    def _mock_cell(cls, value):
        cell_dict = {}
        if value:
            cell_dict['formattedValue'] = value

        return cell_dict

    def _get_uploaded_sheet_data(self):
        return self.mock_spreadsheets_return.values.return_value.batchUpdate.call_args.kwargs.get('body').get('data')


class GoogleSheetsApiTest(GoogleSheetsTestBase):
    def test_spreadsheet_downloads_values_from_drive(self):
        """Check that the spreadsheet initializes with the values currently in the spreadsheet on drive"""

        # Mock a spreadsheet with three tabs on google drive
        # The test spreadsheet looks like this (underscores represent a cell that will be blank):
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
        first_tab_title = 'first_tab'
        first_tab_data = {
            'properties': {'title': first_tab_title},
            'data': [{
                'rowData': [
                    {'values': [self._empty_cell, self._empty_cell, self._mock_cell('7'), self._empty_cell,
                                self._mock_cell('9')]},
                    {'values': [self._empty_cell, self._mock_cell('5')]},
                    {'values': [self._mock_cell('3')]},
                    {'values': [self._empty_cell]},
                    {'values': [self._empty_cell, self._empty_cell, self._mock_cell('4')]}
                ]
            }]
        }
        second_tab_title = 'another tab'
        second_tab_data = {
            'properties': {'title': second_tab_title},
            'data': [{
                'rowData': [
                    {'values': [self._empty_cell]},
                    {'values': [self._empty_cell, self._empty_cell, self._mock_cell('another tab for testing')]}
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
        with GoogleSheetsClient('', '') as sheet:
            self.assertEqual(expected_first_tab_values, sheet.get_tab_values())
            self.assertEqual(expected_second_tab_values, sheet.get_tab_values(second_tab_title))
            self.assertEqual(expected_third_tab_values, sheet.get_tab_values(third_tab_title))

    def test_modifying_sheet_values(self):
        # Construct a sheet that appears as follows:
        # Tab title: first_tab
        # _ _ 3
        # 4
        # Tab title: second_tab
        # _
        # _ 5
        # 7
        with GoogleSheetsClient('', '') as sheet:
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

        # Construct a sheet that appears as follows:
        # Tab title: first_tab
        # _ _ 3
        # 4
        # Tab title: second_tab
        # _
        # _ 5
        # 7
        with GoogleSheetsClient('', '') as sheet:
            sheet.update_cell(0, 2, '3')  # Set 3 on first tab
            sheet.update_cell(1, 0, '4')  # Set 4 on first tab
            sheet.set_current_tab(self.default_tab_names[1])
            sheet.update_cell(2, 0, '7')  # Set 7 on second tab
            sheet.update_cell(1, 1, '5')  # Set 5 on second tab

        tabs_uploaded = self._get_uploaded_sheet_data()

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

    def test_values_are_only_mutable_through_object_interface(self):
        """
        To help keep future designs clean, we want to make sure that the internal structure of values for the sheet
        can only be modified by the sheet (and not through the data structure given when getting values for a tab).
        """

        # Set up a sheet with some values. The resulting sheet should appear as follows
        # 1
        # _
        # _ _ _ 8
        with GoogleSheetsClient('', '') as sheet:
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

    def test_functionality_for_shifting_sheet_range(self):
        """
        To work with pre-existing sheets (that have pre-existing headers and special formatting) we should be
        able to offset the values that are uploaded as an update.
        """

        # Mock a spreadsheet with two tabs on google drive
        # The test spreadsheet looks like this (underscores represent a cell that will be blank):
        # Tab title: 'first_tab'
        # _ _ '7' _ '9'
        # _ '2' '3'
        # Tab title: 'another tab'
        # 'another tab for testing'
        # _
        # _
        # _ _ '7' _ '9'
        first_tab_title = 'first_tab'
        first_tab_data = {
            'properties': {'title': first_tab_title},
            'data': [{
                'rowData': [
                    {'values': [self._empty_cell, self._empty_cell, self._mock_cell('7'),
                                self._empty_cell, self._mock_cell('9')]},
                    {'values': [self._empty_cell, self._mock_cell('2'), self._mock_cell('3')]}
                ]
            }]
        }
        second_tab_title = 'another tab'
        second_tab_data = {
            'properties': {'title': second_tab_title},
            'data': [{
                'rowData': [
                    {'values': [self._mock_cell('another tab for testing')]},
                    {'values': [self._empty_cell]},
                    {'values': [self._empty_cell]},
                    {'values': [self._empty_cell, self._empty_cell, self._mock_cell('7'),
                                self._empty_cell, self._mock_cell('9')]}
                ]
            }]
        }
        self.mock_spreadsheets_return.get.return_value.execute.return_value = {
            'sheets': [first_tab_data, second_tab_data]
        }

        # Set offsets for the tabs and then update some values
        with GoogleSheetsClient('', '', tab_offsets={
            first_tab_title: 'B2',
            second_tab_title: 'D3'
        }) as sheet:
            sheet.update_cell(0, 0, 'two', tab_id=first_tab_title)
            sheet.update_cell(0, 1, '8', tab_id=second_tab_title)

        # Make sure that the offsets were used when uploading values
        tabs_uploaded = self._get_uploaded_sheet_data()

        first_uploaded_tab_data = tabs_uploaded[0]
        self.assertEqual(f"'{first_tab_title}'!B2", first_uploaded_tab_data['range'])
        self.assertEqual([
            ['two', '3']
        ], first_uploaded_tab_data['values'])

        second_uploaded_tab_data = tabs_uploaded[1]
        self.assertEqual(f"'{second_tab_title}'!D3", second_uploaded_tab_data['range'])
        self.assertEqual([
            ['', '8'],
            ['', '9']
        ], second_uploaded_tab_data['values'])

    def test_clearing_further_sheet_rows(self):
        """
        When updating sheets there should be a way to clear out the rest of the
        sheet when we know we're at the last row of the updates
        """

        # Mock a spreadsheet
        # The test spreadsheet looks like this:
        # Tab title: 'only_tab'
        # _
        # _
        # _
        # _ _ _ _ b
        # _ a
        tab_title = 'only_tab'
        self.mock_spreadsheets_return.get.return_value.execute.return_value = {
            'sheets': [{
                'properties': {'title': tab_title},
                'data': [{
                    'rowData': [
                        {'values': [self._empty_cell]},
                        {'values': [self._empty_cell]},
                        {'values': [self._empty_cell]},
                        {'values': [self._empty_cell, self._empty_cell, self._empty_cell, self._empty_cell,
                                    self._mock_cell('b')]},
                        {'values': [self._empty_cell, self._mock_cell('a')]}
                    ]
                }]
            }]
        }

        # Set offsets for the tabs and then update some values
        with GoogleSheetsClient('', '') as sheet:
            sheet.update_cell(0, 0, 'test')
            sheet.truncate_tab_at_row(1)

        # Make sure row 1 and on were cleared
        tab_data = self._get_uploaded_sheet_data()[0]
        self.assertEqual([
            ['test'],
            [''],
            [''],
            ['', '', '', '', ''],
            ['', ''],
        ], tab_data['values'])

    def test_inserting_new_rows(self):
        """Inserting new rows should push existing rows down"""
        with GoogleSheetsClient('', '') as sheet:
            # Set up a sheet with some values. The resulting sheet should appear as follows
            # 1
            # _
            # _ _ _ 8
            # _
            # _ 9
            sheet.update_cell(0, 0, '1')
            sheet.update_cell(2, 3, '8')
            sheet.update_cell(4, 1, '9')

            # Insert a new row at the third spot, resulting in a sheet that appears as
            # 1
            # _
            # _ _ _ _
            # _ _ _ 8
            # 9
            # NOTE: writing empty cells is needed to 'erase' what is google drive for the row that is moving down
            # (otherwise we would write the cells in the new row and miss updates needed for where it was).
            # 9 and 8 are examples of cells that need their old location cleared
            sheet.insert_new_row_at(2)

            # Check that the sheet has the new row
            self.assertEqual([
                ['1'],
                [''],
                ['', '', '', ''],
                ['', '', '', '8'],
                ['', ''],
                ['', '9']
            ], sheet.get_tab_values())

    def test_removing_rows(self):
        with GoogleSheetsClient('', '') as sheet:
            # Set up a sheet with some values. The resulting sheet should appear as follows
            # 1
            # 10
            # _ _ _ 8
            # 9
            # _ _ _ _ 7
            sheet.update_cell(0, 0, '1')
            sheet.update_cell(1, 0, '10')
            sheet.update_cell(2, 3, '8')
            sheet.update_cell(3, 0, '9')
            sheet.update_cell(4, 4, '7')

            # Remove the second and third row, resulting in a sheet that appears as
            # 1
            # _ _ _ 8
            # 9 _ _ _
            # _ _ _ _ 7
            # _ _ _ _ _
            # NOTE: writing empty cells is needed to 'erase' what is google drive for the rows that are moving up
            # (otherwise we would write the cells in the new row and miss updates needed for where it was).
            # 7 and 8 are examples of cells that need their old location cleared
            sheet.remove_row_at(1)

            # Check that the sheet looks right
            self.assertEqual([
                ['1'],
                ['', '', '', '8'],
                ['9', '', '', ''],
                ['', '', '', '', '7'],
                ['', '', '', '', '']
            ], sheet.get_tab_values())

    def test_retrieving_row_values(self):
        with GoogleSheetsClient('', '') as sheet:

            # Set up a sheet with some values. The resulting sheet should appear as follows
            # 1
            # _
            # _ 1 2 _ 8
            # 9
            sheet.update_cell(0, 0, '1')
            sheet.update_cell(2, 1, '1')
            sheet.update_cell(2, 2, '2')
            sheet.update_cell(2, 4, '8')
            sheet.update_cell(3, 0, '9')

            # Verify we can get the expected row
            self.assertEqual(['', '1', '2', '', '8'], sheet.get_row_at(2))

    @mock.patch('time.sleep', side_effect=lambda _: None)  # Keeping the backoff retries from waiting for any time
    def test_retry_on_google_api_error(self, _):
        """Check that the download retries if we get an error"""
        attempt_count = 0

        def raise_api_error():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count > 2:
                return mock.DEFAULT  # return the mock's default return_value
            else:
                raise HttpError(..., bytes())

        self.mock_spreadsheets_return.get.return_value.execute.side_effect = raise_api_error

        sheet_client = GoogleSheetsClient(..., ...)
        sheet_client.download_values()

        # Make sure more than one attempt was made (that an HttpError was raised)
        self.assertTrue(attempt_count > 2)

    @mock.patch('time.sleep', side_effect=lambda _: ...)  # Keeping the backoff retries from waiting for any time
    def test_api_error_retry_gives_up_eventually(self, _):
        """Make sure that the retries don't go on forever, and that we eventually get the error"""
        attempt_count = 0

        def raise_api_error():
            nonlocal attempt_count
            attempt_count += 1
            raise HttpError(..., bytes())

        self.mock_spreadsheets_return.get.return_value.execute.side_effect = raise_api_error

        sheet_client = GoogleSheetsClient(..., ...)
        with self.assertRaises(HttpError):
            sheet_client.download_values()

        # Make sure more than one attempt was made (that a BadGateway was raised)
        self.assertEqual(4, attempt_count)

    def test_new_tab_created(self):
        """Test that new tabs can be generated on the spreadsheet"""

        with GoogleSheetsClient('', '') as sheet:
            new_tab_name = 'new_tab_test'
            sheet.add_new_tab(new_tab_name)
            sheet.update_cell(0, 0, 'this is on the new tab!', tab_id=new_tab_name)

        # Check that the metadata updates generated a new tab on the spreadsheet
        batch_update_requests = self.mock_spreadsheets_return.batchUpdate.call_args.kwargs.get('body').get('requests')
        new_sheet_request = batch_update_requests[0]
        self.assertEqual(new_tab_name, new_sheet_request['addSheet']['properties']['title'])

        # Make sure the new tab data uploads
        tabs_uploaded = self._get_uploaded_sheet_data()
        new_tab_data = tabs_uploaded[2]  # New tab data is listed after the default tabs
        self.assertEqual(f"'{new_tab_name}'!A1", new_tab_data['range'])
        self.assertEqual([['this is on the new tab!']], new_tab_data['values'])
