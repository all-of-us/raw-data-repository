"""NDB model and DAO for logs of writes that can be synced."""
import config

from protorpc import messages
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop
from __builtin__ import False

# Sync channel indexes
PHYSICAL_MEASUREMENTS = 1
BIOBANK_ORDERS = 2
  
class SyncCounter(ndb.Model):
  """A counter for a shard of a sync channel; increments and is used in SyncLogEntry.id."""
  value = ndb.IntegerProperty()
  
class SyncLogEntry(ndb.Model):
  """A log entry in the sync log; has a SyncCounter as its parent."""  
  participantId = ndb.StringProperty()
  resource = ndb.JsonProperty()
    
class SyncLogDao:
  """A DAO for writing and reading sync log entries."""
  
  def __init__(self):
    self.num_shards = None
    
  def get_num_shards(self):
    """Returns the number of shards to use when syncing."""
    if self.num_shards:
      return self.num_shards
    return config.getSetting(config.SYNC_SHARDS_PER_CHANNEL, 1)
  
  def set_num_shards(self, num_shards):
    """Overrides the number of shards. Used for testing."""
    self.num_shards = num_shards
  
  def get_shard_number(self, participantId):
    return (hash(participantId) & 0xffffffff) % self.get_num_shards()

  def make_sync_counter_key(self, channel_index, shard_number):  
    return ndb.Key('SyncCounter', '%d|%d' % (channel_index, shard_number))
  
  def make_log_entry_key(self, sync_counter_key, id):
    return ndb.Key('SyncLogEntry', id, parent=sync_counter_key)        
  
  @ndb.transactional
  def write_log_entry(self, channel_index, participantId, resource):    
    """Writes a log entry for the specified participant and channel."""
    counter_key = self.make_sync_counter_key(channel_index, self.get_shard_number(participantId))
    counter = counter_key.get()
    if counter:
      counter.value += 1      
    else:
      # Start the counter at 1
      counter = SyncCounter(key=counter_key, value=1)
       
    entry = SyncLogEntry(participantId=participantId, resource=resource, 
                         key=self.make_log_entry_key(counter_key, counter.value))
    counter.put()
    entry.put()
    
  def sync(self, channel_index, previous_token, max_results):
    """Syncs resources from a channel across all shards in ascending ID order.
    
    If previous_token is specified, the read starts from the ID values encoded in the token.
    
    max_results indicates the maximum number of results to return across all shards. Fewer results
    may be returned.
    
    Returns a tuple of (resource_list, next_token, more_available)
    """
    counter_values = previous_token.split('|') if previous_token else []
    
    num_shards = self.get_num_shards()
    # If we changed the number of shards, the previous sync token is invalid; start from the 
    # beginning of time.
    if len(counter_values) != num_shards:
      counter_values = []
    futures = []   
    
    # Fetch pages for each shards, and collect the futures
    for i in range(0, num_shards):
      sync_counter_key = self.make_sync_counter_key(channel_index, i)
      query = SyncLogEntry.query(ancestor=sync_counter_key)
      if len(counter_values) > i:
        if counter_values[i] != '0':        
          key = self.make_log_entry_key(sync_counter_key, int(counter_values[i]))
          query = query.filter(SyncLogEntry.key > key)
      else:
        counter_values.append('0')      
      query = query.order(SyncLogEntry.key)
      # Should we use cursors instead?      
      futures.append(query.fetch_page_async(max_results / num_shards))
    resources = []
    more_available = False
    
    # Get the results from the futures
    for i in range(0, len(futures)):
      f = futures[i]
      (results, cursor, more) = f.get_result()      
      if results:        
        for result in results:
          resources.append(result.resource)
        counter_values[i] = str(results[len(results) - 1].key.id())
      if more:
        more_available = True
    return (resources, '|'.join(counter_values), more_available)  

DAO = SyncLogDao()
