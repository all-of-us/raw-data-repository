"""NDB model and DAO for logs of writes that can be synced.

Sync log entries provide a strongly consistent feed of participant changes, so clients can keep 
their models up to date and notify end users of change events.

Each sync channel has a configurable number of shards, each with its own SyncCounter row.
These counters are incremented as records are written to the log; each record has the new value
of the counter as the ID part of its key. Each shard is its own entity group; this allows for 
higher write QPS than having only a single entity group per channel.

Calls to the sync() method retrieve resources from the log in increasing ID order, across all 
shards. Because they are in the same entity group, reads from a shard are guaranteed to reflect
all previous calls to write_log_entry for that shard. (This is in contrast to reads from secondary
indexes, where no such guarantees exist.)

Writes are sharded based on a hash of the ID for the participant being written about. This
guarantees that resources for the same participant will be returned to the client in the
order they were written. 
"""
import config

from google.appengine.ext import ndb

# Sync channel indexes -- provided in calls to write_log_entry() and sync() below as channel_index.
PHYSICAL_MEASUREMENTS = 1
BIOBANK_ORDERS = 2
  
class SyncCounter(ndb.Model):
  """A counter for a shard of a sync channel; increments and is used in SyncLogEntry.id."""
  value = ndb.IntegerProperty()
  
class SyncLogEntry(ndb.Model):
  """A log entry in the sync log; has a SyncCounter as its parent."""  
  participantId = ndb.StringProperty()
  resource = ndb.JsonProperty()
    
class SyncLogDao(object):
  """A DAO for writing and reading sync log entries."""
  
  def __init__(self):
    self._num_shards = None
    
  def _get_num_shards(self):
    """Returns the number of shards to use when syncing."""
    if self._num_shards:
      return self._num_shards
    return config.getSetting(config.SYNC_SHARDS_PER_CHANNEL, 1)
  
  def set_num_shards(self, num_shards):
    """Overrides the number of shards. Used for testing."""
    self._num_shards = num_shards
  
  def _get_shard_number(self, participantId):
    return hash(participantId) % self._get_num_shards()

  def _make_sync_counter_key(self, channel_index, shard_number):  
    return ndb.Key('SyncCounter', '%d|%d' % (channel_index, shard_number))
  
  def _make_log_entry_key(self, sync_counter_key, entry_id):
    return ndb.Key('SyncLogEntry', entry_id, parent=sync_counter_key)        
  
  @ndb.transactional
  def write_log_entry(self, channel_index, participantId, resource):    
    """Writes a log entry for the specified participant and channel."""
    counter_key = self._make_sync_counter_key(channel_index, self._get_shard_number(participantId))
    counter = counter_key.get()
    if counter:
      counter.value += 1      
    else:
      # Start the counter at 1
      counter = SyncCounter(key=counter_key, value=1)
       
    entry = SyncLogEntry(participantId=participantId, resource=resource, 
                         key=self._make_log_entry_key(counter_key, counter.value))
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
    
    num_shards = self._get_num_shards()
    assert num_shards <= max_results
    # If we changed the number of shards, the previous sync token is invalid; start from the 
    # beginning of time.
    if len(counter_values) != num_shards:
      counter_values = ['0'] * num_shards
    futures = []   
    
    # Fetch pages for each shards, and collect the futures
    for i in range(0, num_shards):
      sync_counter_key = self._make_sync_counter_key(channel_index, i)
      query = SyncLogEntry.query(ancestor=sync_counter_key)
      if counter_values[i] != '0':        
        key = self._make_log_entry_key(sync_counter_key, int(counter_values[i]))
        query = query.filter(SyncLogEntry.key > key)
      query = query.order(SyncLogEntry.key)          
      futures.append(query.fetch_page_async(max_results / num_shards))
    resources = []
    more_available = False
    
    # Get the results from the futures
    for i in range(0, len(futures)):
      f = futures[i]
      (results, _, more) = f.get_result()      
      if results:        
        for result in results:
          resources.append(result.resource)
        counter_values[i] = str(results[-1].key.id())
      if more:
        more_available = True
    return (resources, '|'.join(counter_values), more_available)  

_DAO = SyncLogDao()
def DAO():
  return _DAO
