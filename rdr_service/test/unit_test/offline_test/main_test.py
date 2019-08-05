import clock
import mock

import config
from offline import main
from test.unit_test.unit_test_util import TestBase


class MainTest(TestBase):
  def setUp(self):
    super(MainTest, self).setUp()
    config.override_setting(config.INTERNAL_STATUS_MAIL_SENDER, ['sender@googlegroups.com'])
    config.override_setting(config.INTERNAL_STATUS_MAIL_RECIPIENTS, ['to@googlegroups.com'])
    config.override_setting(config.BIOBANK_STATUS_MAIL_RECIPIENTS, ['ars@biobank.org'])

  @mock.patch('offline.biobank_samples_pipeline.upsert_from_latest_csv')
  @mock.patch('app_util.check_cron')
  @mock.patch('google.appengine.api.app_identity.get_application_id')
  @mock.patch('google.appengine.api.mail.send_mail')
  # pylint: disable=unused-argument
  def test_samples_data_error_sends_alert(
      self, mock_send_mail, mock_get_app_id, mock_check_cron, mock_upsert):
    mock_get_app_id.return_value = 'all-of-us-rdr-unittests'
    # The return value should be unused, but it clarifies errors to have a realistic value.
    mock_upsert.return_value = 25, clock.CLOCK.now()
    mock_upsert.side_effect = ValueError('should be thrown for test')
    with self.assertRaises(ValueError):
      main.import_biobank_samples()
    self.assertEquals(mock_send_mail.call_count, 1)
