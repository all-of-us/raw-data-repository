import unittest

from base import BaseClientTest


class BiobankSamplesPipelineTest(BaseClientTest):
  """Exercise the reconciliation MySQL query, which cannot be run on SqlLite (unit tests)."""
  def setUp(self):
    super(BiobankSamplesPipelineTest, self).setUp(base_path='offline')

  def test_reconciliation_empty(self):
    self.client.request('BiobankSamplesReconciliation', 'GET', cron=True)


if __name__ == '__main__':
  unittest.main()
