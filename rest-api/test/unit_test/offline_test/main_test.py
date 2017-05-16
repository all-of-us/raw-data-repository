import clock
import json
import mock

from offline import main
from offline import biobank_samples_pipeline
from test.unit_test.unit_test_util import TestBase


class MainTest(TestBase):
  @mock.patch('offline.biobank_samples_pipeline.upsert_from_latest_csv')
  @mock.patch('offline.biobank_samples_pipeline.write_reconciliation_report')
  @mock.patch('api_util.check_cron')
  # pylint: disable=unused-argument
  def test_no_server_error_for_samples_data_error(self, mock_check_cron, mock_report, mock_upsert):
    mock_upsert.return_value = 25, clock.CLOCK.now()  # should be unused, clarifies errors to have a realistic value
    mock_upsert.side_effect = biobank_samples_pipeline.DataError('should be thrown/logged for test')
    response = json.loads(main.import_biobank_samples())
    self.assertIn('written', response)
    self.assertEquals(response['written'], None)
