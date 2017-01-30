import sync_log

from google.appengine.ext import ndb
from unit_test_util import NdbTestBase

class ParticipantNdbTest(NdbTestBase):
  
  def tearDown(self):
    sync_log.DAO.set_num_shards(1)
    
  def test_sync_empty_one_shard(self):
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, None, 2)
    self.assertEquals([], resources)
    self.assertEquals('0', token)
    self.assertFalse(more_available)
    
  def test_sync_empty_two_shards(self):
    sync_log.DAO.set_num_shards(2)
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, None, 2)
    self.assertEquals([], resources)
    self.assertEquals('0|0', token)
    self.assertFalse(more_available)
  
  def test_sync_one_resource_one_shard(self):
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P123', 'foo')
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, None, 2)
    self.assertEquals(['foo'], resources)
    self.assertEquals('1', token)
    self.assertFalse(more_available)
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.BIOBANK_ORDERS, None, 2)
    self.assertEquals([], resources)
    self.assertEquals('0', token)
    self.assertFalse(more_available)
    
  def test_sync_one_resource_two_shards(self):
    sync_log.DAO.set_num_shards(2)
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P123', 'foo')    
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, None, 2)
    self.assertEquals(['foo'], resources)
    self.assertEquals('1|0', token)
    self.assertFalse(more_available)
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.BIOBANK_ORDERS, None, 2)
    self.assertEquals([], resources)
    self.assertEquals('0|0', token)
    self.assertFalse(more_available)
    
  def test_sync_two_resources_one_shard(self):
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P123', 'foo')
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P456', 'bar')
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, None, 2)
    self.assertEquals(['foo', 'bar'], resources)
    self.assertEquals('2', token)
    self.assertFalse(more_available)
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.BIOBANK_ORDERS, None, 2)
    self.assertEquals([], resources)
    self.assertEquals('0', token)
    self.assertFalse(more_available)
    
  def test_sync_two_resources_same_shard(self):
    sync_log.DAO.set_num_shards(2)
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P123', 'foo')    
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P123', 'bar')
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, None, 2)
    self.assertEquals(['foo'], resources)
    self.assertEquals('1|0', token)
    self.assertTrue(more_available)
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, token, 2)
    self.assertEquals(['bar'], resources)
    self.assertEquals('2|0', token)
    self.assertFalse(more_available)
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, token, 2)
    self.assertEquals([], resources)
    self.assertEquals('2|0', token)
    self.assertFalse(more_available)        
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.BIOBANK_ORDERS, None, 2)
    self.assertEquals([], resources)
    self.assertEquals('0|0', token)
    self.assertFalse(more_available)

    # Add two more log entries; verify that they get returned eventually as well
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P123', 'xxx')    
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P123', 'yyy')
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, None, 2)
    self.assertEquals(['foo'], resources)
    self.assertEquals('1|0', token)
    self.assertTrue(more_available)
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, token, 2)
    self.assertEquals(['bar'], resources)
    self.assertEquals('2|0', token)
    self.assertTrue(more_available)
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, token, 2)
    self.assertEquals(['xxx'], resources)
    self.assertEquals('3|0', token)
    self.assertTrue(more_available)
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, token, 2)
    self.assertEquals(['yyy'], resources)
    self.assertEquals('4|0', token)
    self.assertFalse(more_available)
    
  def test_sync_two_resources_different_shards(self):
    sync_log.DAO.set_num_shards(2)
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P123', 'foo')    
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P456', 'bar')
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, None, 2)
    self.assertEquals(['foo', 'bar'], resources)
    self.assertEquals('1|1', token)
    self.assertFalse(more_available)
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, token, 2)
    self.assertEquals([], resources)
    self.assertEquals('1|1', token)
    self.assertFalse(more_available)    
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.BIOBANK_ORDERS, None, 2)
    self.assertEquals([], resources)
    self.assertEquals('0|0', token)
    self.assertFalse(more_available)
    
    # Add two more log entries; verify that they get returned eventually as well
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P123', 'xxx')    
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P456', 'yyy')
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, None, 2)
    self.assertEquals(['foo', 'bar'], resources)
    self.assertEquals('1|1', token)
    self.assertTrue(more_available)    
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, token, 2)
    self.assertEquals(['xxx', 'yyy'], resources)
    self.assertEquals('2|2', token)
    self.assertFalse(more_available)
    
    
  def test_sync_wrong_number_of_shards(self):   
    sync_log.DAO.set_num_shards(2)
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P123', 'foo')    
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P456', 'bar')
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P123', 'xxx')    
    sync_log.DAO.write_log_entry(sync_log.PHYSICAL_MEASUREMENTS, 'P456', 'yyy')

    # When we sync with the wrong number of shards, we start from the beginning.
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, '3', 2)
    self.assertEquals(['foo', 'bar'], resources)
    self.assertEquals('1|1', token)
    self.assertTrue(more_available)
    (resources, token, more_available) = sync_log.DAO.sync(sync_log.PHYSICAL_MEASUREMENTS, 
                                                           '2|2|2', 2)
    self.assertEquals(['foo', 'bar'], resources)
    self.assertEquals('1|1', token)
    self.assertTrue(more_available)
    
