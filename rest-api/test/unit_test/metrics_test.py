"""Tests for metrics"""

import collections
import copy
import datetime

import metrics
import offline.metrics_pipeline
import unittest

from werkzeug.exceptions import NotFound

from test.unit_test.unit_test_util import NdbTestBase


class MetricsTest(NdbTestBase):
  def test_serving_version(self):

    with self.assertRaises(offline.metrics_pipeline.PipelineNotRunningException):
      offline.metrics_pipeline.set_serving_version()

    self.assertEqual(None, metrics.get_serving_version())

    metrics.set_pipeline_in_progress()
    self.assertEqual(None, metrics.get_serving_version())

    in_progress = metrics.get_in_progress_version()
    self.assertTrue(in_progress)

    offline.metrics_pipeline.set_serving_version()
    with self.assertRaises(offline.metrics_pipeline.PipelineNotRunningException):
      offline.metrics_pipeline.set_serving_version()

    expected = copy.deepcopy(in_progress)
    expected.in_progress = False
    expected.complete = True
    expected.data_version = offline.metrics_pipeline.PIPELINE_METRICS_DATA_VERSION
    serving_version = metrics.get_serving_version().get()
    # Don't compare the auto populated dates.
    expected.date = None
    serving_version.date = None
    self.assertEquals(expected, serving_version)

    # Pretend that the data version changed.
    serving_version.data_version = metrics.SERVING_METRICS_DATA_VERSION + 1
    serving_version.put()
    self.assertEqual(None, metrics.get_serving_version())

    # Make sure that a version mismatch results in a 404.
    with self.assertRaises(NotFound):
      metrics.MetricService().get_metrics(None)

if __name__ == '__main__':
  unittest.main()
