from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
import socket

from rdr_service.services.gcp_utils import gcp_get_iam_service_key_info


class GoogleSheetsClient:
    """
    Allows for interacting with a spreadsheet in google drive. This class is designed to be used as a context manager
    and requires that:
        - A service account (with a json keyfile) is authenticated
        - The service account has the correct permissions to edit the google spreadsheet

    Please carefully verify that this works for your purpose if you re-use this. There are some things that don't
    currently work (such as formula manipulation and making new tabs).
    """

    def __init__(self, spreadsheet_id, service_key_id, tab_offsets=None):
        """
        :param spreadsheet_id: Google Drive id of the spreadsheet.
        :param service_key_id: Key id for the service account used.
        :type tab_offsets: Dictionary specifying tab names and offsets for them (defined in Google Sheet cell
                notation such as B4). Giving a cell value will specify that any changes for that tab use that cell
                as the origin. So with an origin of B4 an update to C5 would be given as row 1 and column 1.
                Used to prevent updating headers in the target spreadsheet.
                WARNING: Does not support columns past Z
        """

        # Load credentials from service key file
        service_key_info = gcp_get_iam_service_key_info(service_key_id)
        self._api_credentials = ServiceAccountCredentials.from_json_keyfile_name(service_key_info['key_path'])

        self._spreadsheet_id = spreadsheet_id
        self._default_tab_id = None
        self._tabs = None
        self._empty_cell_value = ''
        self._tab_offsets = {tab_name: {
            'row': int(offset[1:]) - 1,  # convert row number specified in a system of counting from 1
            'col': ord(offset[:1].upper()) - ord('A'),  # Get column number (A = 0, B = 1, ...)
            'offset_str': offset
        } for tab_name, offset in tab_offsets.items()} if tab_offsets else {}

    def _build_service(self):
        # The Google API client uses sockets, and the requests can take longer than the default timeout.
        # The proposed solution is to increase the default timeout manually
        # https://github.com/googleapis/google-api-python-client/issues/632
        # The socket seems to be created when calling discover.build, so this temporarily increases the timeout for
        # new sockets when the Google service creates its socket.
        default_socket_timeout = socket.getdefaulttimeout()
        num_seconds_in_five_minutes = 300
        socket.setdefaulttimeout(num_seconds_in_five_minutes)

        # Set up for being able to interact with the sheet in Drive
        sheets_api_service = discovery.build('sheets', 'v4', credentials=self._api_credentials)

        # Set the timeout back for anything else in the code that would use sockets
        socket.setdefaulttimeout(default_socket_timeout)

        return sheets_api_service

    def __enter__(self):
        self.download_values()
        return self

    def __exit__(self, *_):
        self.upload_values()

    @classmethod
    def _initialize_empty_tab(cls):
        return []

    def _get_offset_row_col(self, tab_id):
        tab_offset_data = self._tab_offsets.get(tab_id, {
            'row': 0,
            'col': 0
        })
        return tab_offset_data['row'], tab_offset_data['col']

    def _get_offset_string(self, tab_id):
        tab_offset_data = self._tab_offsets.get(tab_id, {
            'offset_str': 'A1'
        })
        return tab_offset_data['offset_str']

    def download_values(self):
        """
        Retrieve the values as they currently are in google drive.
        Note: this will overwrite any changes that have been made this instance of the document using `update_cell`.

        :return: None
        """
        self._tabs = {}

        # API call documented at https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
        request = self._build_service().spreadsheets().get(spreadsheetId=self._spreadsheet_id, includeGridData=True)
        response = request.execute()

        # Parse the retrieved spreadsheet
        tab_data = response['sheets']
        for tab in tab_data:
            tab_id = tab['properties'].get('title')

            # Set the default tab to the first tab
            if self._default_tab_id is None:
                self._default_tab_id = tab_id

            # Initialize the internal tab structure and parse the values from the response
            self._tabs[tab_id] = self._initialize_empty_tab()
            tab_grid_data = tab['data'][0].get('rowData', [])
            for row_number, row_data in enumerate(tab_grid_data):
                row_values = row_data.get('values')
                if row_values:
                    for col_number, cell_data in enumerate(row_values):
                        row_offset, col_offset = self._get_offset_row_col(tab_id)

                        if row_number >= row_offset and col_number >= col_offset:
                            cell_value = cell_data.get('formattedValue', self._empty_cell_value)
                            self.update_cell(row_number - row_offset, col_number - col_offset, cell_value, tab_id)

    def set_current_tab(self, tab_id):
        """
        Change the default tab. Used to make updating multiple fields on one tab cleaner
        (so the tab id doesn't need to be given with the location for each cell value).

        :param tab_id: Name of the tab to use as the default.
        :return: None
        """
        self._default_tab_id = tab_id

    def update_cell(self, row: int, col: int, value: str, tab_id=None):
        """
        Change the value of a cell.
        Any changes made will be stored locally until the next call to `upload_values`
        (or when the context ends).

        :param row: row number of the cell, starting from 0 at the top of the spreadsheet
        :param col: column number of the cell, starting from 0 at the left of the spreadsheet
        :param value: value to store
        :param tab_id: Name of the tab to modify. The default tab is used if this parameter isn't provided.
        :return: None
        """
        if not isinstance(col, int):
            col = int(col)

        values_grid = self._tabs.get(tab_id or self._default_tab_id)

        # Increase the number of rows we have if the caller is setting a cell on a
        # row farther out than what is initialized
        while row >= len(values_grid):
            values_grid.append([self._empty_cell_value])

        row_for_update = values_grid[row]

        # Increase the number of columns we have in the row if the caller is setting a
        # cell on a cell father out than what is initialized in the row
        while col >= len(row_for_update):
            row_for_update.append(self._empty_cell_value)

        row_for_update[col] = value

    def truncate_tab_at_row(self, row, tab_id=None):
        """
        Clears all values from the sheet at and below the given row (setting their cells equal to an empty string).

        :param row: Row to start clearing, starting from 0 at the top of the document
        :param tab_id: Tab to clear values from, defaults to the current tab if not provided
        """

        values_grid = self._tabs.get(tab_id or self._default_tab_id)

        current_row = row
        while current_row < len(values_grid):  # Iterate through the rows
            # Replace everything in the row with empty strings
            values_grid[current_row] = [self._empty_cell_value] * len(values_grid[current_row])
            current_row += 1

    def insert_new_row_at(self, row_index, tab_id=None):
        """
        Creates a new, empty row at the given row index. The current row at the given index will be moved down.
        :param row_index: Index, counting from 0, for the new row
        :param tab_id: Tab to add the new row to, defaults to the current tab if not provided
        """
        values_grid = self._tabs.get(tab_id or self._default_tab_id)
        values_grid.insert(row_index, [self._empty_cell_value])

        # All the following rows will be moved down.
        # Any row in front of a row that moves down will be uploaded to the document in the same position as the one
        # that moved down. Any row in front of one that moves down needs to have as many cells as the one it's
        # replacing, so that it will overwrite all the values left over from the row that it pushed down.
        while row_index < len(values_grid) - 1:  # The last row isn't replacing anything, so doesn't need to be checked
            row_to_expand = values_grid[row_index]
            number_of_cells_to_replace = len(values_grid[row_index + 1])
            while number_of_cells_to_replace > len(row_to_expand):
                row_to_expand.append(self._empty_cell_value)

            row_index += 1

    def remove_row_at(self, row_index, tab_id=None):
        """
        Removes a row from the sheet.
        :param row_index: Index, counting from 0, for the row to remove
        :param tab_id: Tab to remove the row from, defaults to the current tab if not provided
        """
        values_grid = self._tabs.get(tab_id or self._default_tab_id)
        number_of_cells_replaced = len(values_grid[row_index])
        del values_grid[row_index]

        # Removing a row in the document means every row moves up, including the last one.
        # So we need to insert a row at the end to overwrite the values left from when the original last row moves up.
        # (The number of cells is expanded later in this method).
        values_grid.append([self._empty_cell_value])

        # All following rows will be moved up.
        # Any rows after a row that moves up will be uploaded to the document in the same position as the one before it.
        # If the following row doesn't have as many cells as the row it's replacing, then it wouldn't
        # overwrite all the cells and some trailing values could be left over. All rows might need to have
        # extra cells added so they will overwrite all the cells left from the row they're replacing.
        while row_index < len(values_grid):
            next_row = values_grid[row_index]
            while number_of_cells_replaced > len(next_row):
                next_row.append(self._empty_cell_value)

            # Get the number of cells in this row, the row after it will be taking it's place in the document
            number_of_cells_replaced = len(next_row)
            row_index += 1

    def get_row_at(self, row_index, tab_id=None):
        """
        Retrieves the list of values at the given row. If the indexed row doesn't already exist, this method will
        expand the grid until it does.

        :param row_index: Index, counting from 0, for the row to retrieve
        :param tab_id: Tab to read the row from, defaults to the current tab if not provided
        :return: List of values that make up the given row
        """
        values_grid = self._tabs.get(tab_id or self._default_tab_id)

        while row_index >= len(values_grid):
            values_grid.append([self._empty_cell_value])

        return list(values_grid[row_index])

    def upload_values(self):
        """
        Upload the local data to the google drive spreadsheet.
        Note: any changes made to the target spreadsheet since the last call to `download_values` will be overwritten.
        """
        request = self._build_service().spreadsheets().values().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={
                'valueInputOption': 'RAW',
                'data': [{
                    'range': f"'{tab_id}'!{self._get_offset_string(tab_id)}",
                    'values': tab_data
                } for tab_id, tab_data in self._tabs.items()]
            }
        )
        request.execute()

    def get_tab_values(self, tab_id=None):
        """
        Returns the values of the specified tab (or the current tab if no tab was specified).
        Empty cells are represented by empty strings.

        :param tab_id: Identifier of the tab to retrieve values from.
        :return: A two dimensional list of strings that represent the cell values, organized by
            rows (from the top down) and then columns (from left to right).
        """

        if tab_id is None:
            tab_id = self._default_tab_id

        value_grid = self._tabs.get(tab_id)
        return [[value for value in row] for row in value_grid]
