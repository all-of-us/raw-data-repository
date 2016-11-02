"""Tests for biobank_samples_pipeline."""

import os
import biobank_sample

from offline.biobank_samples_pipeline import BiobankSamplesPipeline

from cloudstorage import cloudstorage_api
from mapreduce import test_support
from testlib import testutil

class BiobankSamplesPipelineTest(testutil.CloudStorageTestBase):
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    
  def test_end_to_end(self):    
    with open(_data_path('biobank_samples_1.csv'), 'rb') as src, \
        cloudstorage_api.open('/pmi-drc-biobank-test.appspot.com/biobank_samples_1.csv', mode='w') as dest:
      while 1:
          copy_buffer = src.read(1024)
          if not copy_buffer:
              break
          dest.write(copy_buffer)    
    BiobankSamplesPipeline().start()
    test_support.execute_until_empty(self.taskqueue)
    
    biobank_samples_1 = biobank_sample.DAO.load('0', 'PMI-100001')
    expected_sample_dict_1 = {
        'family_id': 'SF160914-000001',
        'sample_id': '16258000008',
        'event_name': 'DRC-00123',
        'storage_status': 'In Prep',
        'type': 'Urine',
        'treatments': 'No Additive',
        'expected_volume': '10 mL',
        'quantity': '1 mL',
        'container_type': 'TS - Matrix 1.4mL',
        'collection_date': '2016/09/13 09:47:00',
        'parent_sample_id': '16258000001',
        'confirmed_date': '2016/09/14 09:49:00' }
    expected_samples_1 = biobank_sample.DAO.from_json(
        { 'samples': [ expected_sample_dict_1 ]}, 'PMI-100001', '0')
    self.assertEquals(expected_samples_1, biobank_samples_1)    

def _data_path(filename):
  return os.path.join(os.path.dirname(__file__), '..', 'test-data', filename)
