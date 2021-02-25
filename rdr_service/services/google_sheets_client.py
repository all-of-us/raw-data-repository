from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

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

    def __init__(self, spreadsheet_id, service_key_id):
        """
        :param spreadsheet_id: Google Drive id of the spreadsheet.
        :param service_key_id: Key id for the service account used.
        """

        # Load credentials from service key file
        service_key_info = gcp_get_iam_service_key_info(service_key_id)
        credentials = ServiceAccountCredentials.from_json_keyfile_name(service_key_info['key_path'])

        # Set up for being able to interact with the sheet in Drive
        self._service = discovery.build('sheets', 'v4', credentials=credentials)
        self._spreadsheet_id = spreadsheet_id

        # Initialize internal fields
        self._default_tab_id = None
        self._tabs = None
        self._empty_cell_value = ''

    def __enter__(self):
        self.download_values()
        return self

    def __exit__(self, *_):
        self.upload_values()

    def download_values(self):
        """
        Retrieve the values as they currently are in google drive.
        Note: this will overwrite any changes that have been made this instance of the document using `update_cell`.

        :return: None
        """
        self._tabs = {}

        # API call documented at https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
        request = self._service.spreadsheets().get(spreadsheetId=self._spreadsheet_id, includeGridData=True)
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
            tab_grid_data = tab['data'][0]['rowData']
            for row_number, row_data in enumerate(tab_grid_data):
                row_values = row_data.get('values')
                if row_values:
                    for col_number, cell_data in enumerate(row_values):
                        cell_value = cell_data.get('formattedValue', self._empty_cell_value)
                        self.update_cell(row_number, col_number, cell_value, tab_id)

    @classmethod
    def _initialize_empty_tab(cls):
        return []

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

        if tab_id is None:
            tab_id = self._default_tab_id
        values_grid = self._tabs.get(tab_id)

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

    def upload_values(self):
        """
        Upload the local data to the google drive spreadsheet.
        Note: any changes made to the target spreadsheet since the last call to `download_values` will be overwritten.

        :return: None
        """
        request = self._service.spreadsheets().values().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={
                'valueInputOption': 'RAW',
                'data': [{
                    'range': f"'{tab_title}'!A1",
                    'values': tab_data
                } for tab_title, tab_data in self._tabs.items()]
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
