from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

from rdr_service.services.gcp_utils import gcp_get_iam_service_key_info


class GoogleSheetApi:
    """
    Allows for interacting with a spreadsheet in google drive.
    It is assumed that a service account (with a json keyfile) is being used,
    and that service account has the correct permissions to edit the spreadsheet.
    """

    def __init__(self, spreadsheet_id, service_key_id):
        # Load credentials from service key file
        service_key_info = gcp_get_iam_service_key_info(service_key_id)
        credentials = ServiceAccountCredentials.from_json_keyfile_name(service_key_info['key_path'])

        # Set up for being able to interact with the sheet in Drive
        self._service = discovery.build('sheets', 'v4', credentials=credentials)
        self._spreadsheet_id = spreadsheet_id

        # Initialize grid for storing values
        self._values_grid = []
        self._empty_cell_value = ''

    def __enter__(self):
        self.download_values()
        return self

    def __exit__(self, *_):
        self.upload_values()

    def download_values(self):
        """
        Retrieve the values as they currently are in google drive.
        Note: this will overwrite any changes that have been made this instance of the document using `change_cell`.

        :return: None
        """

        # API call documented at https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
        request = self._service.spreadsheets().get(spreadsheetId=self._spreadsheet_id, includeGridData=True)
        response = request.execute()

        # Parse the retrieved spreadsheet
        self._values_grid = []  # re-initialize our values
        grid_row_data = response['sheets'][0]['data'][0]['rowData']
        # todo: start working with sheets[1] and so on...
        for row_number, row in enumerate(grid_row_data):
            for col_number, cell in enumerate(row['values']):
                self.change_cell(row_number, col_number, cell.get('formattedValue', self._empty_cell_value))

    def change_cell(self, row: int, col: int, value: str):
        """
        Change the value of a cell.
        Any changes made will be stored locally until the next call to `upload_values`.

        :param row: number of the row the cell is in, starting from 0
        :param col: number of the column the cell is in, starting from 0
        :param value: value to store in the cell
        :return: None
        """

        # Increase the number of rows we have if the caller is setting a cell on a
        # row farther out than what is initialized
        while row >= len(self._values_grid):
            self._values_grid.append([self._empty_cell_value])

        update_row = self._values_grid[row]

        # Increase the number of columns we have in the row if the caller is setting a
        # cell on a cell father out than what is initialized in the row
        while col >= len(update_row):
            update_row.append(self._empty_cell_value)

        update_row[col] = value

        # todo: return whether the value in the cell was changed

    def upload_values(self):
        """
        Upload the current data to the google drive spreadsheet.
        Note: any changes made to the target spreadsheet since the last call to `download_values` will be overwritten.

        :return: None
        """
        # API call documented at https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/update
        request = self._service.spreadsheets().values().update(
            spreadsheetId=self._spreadsheet_id,
            range='A1',
            valueInputOption='RAW',
            body={
                'values': self._values_grid
            }
        )
        request.execute()
