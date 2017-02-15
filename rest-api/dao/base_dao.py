import dao.database_factory
from contextlib import contextmanager
from sqlalchemy.orm.session import make_transient
        
class BaseDao(object):
  """A data access object base class; defines common methods for inserting, updating, and retrieving
  objects using SQLAlchemy.
  """
  def __init__(self, model_type):
    self.model_type = model_type
    self.database = dao.database_factory.get_database()

  @contextmanager
  def session(self):
    sess = self.database.make_session()
    try:
        yield sess
        sess.commit()
    except Exception as ex:
        sess.rollback()
        raise ex
    finally:        
        sess.close()
    
  # Note: potentially mutates the calling obj when inserting.    
  def insert_with_session(self, session, obj):    
    session.add(obj)

  # Note: potentially mutates the calling obj when inserting.
  def insert(self, obj):
    with self.session() as session:      
      self.insert_with_session(session, obj)        
    
  def get_id(self, obj):
    '''Returns the ID (for single primary key column tables) or a tuple of IDs (for multiple
    primary key column tables). Must be overridden by subclasses.'''    
    raise "Not implemented"
  
  def get_with_session(self, session, id):
    return session.query(self.model_type).get(id)
    
  def get(self, id):
    with self.session() as session:
      result = self.get_with_session(session, id)          
    return result
          
  def make_version_id(self, last_modified):
    return 'W/"{}"'.format(api_util.unix_time_millis(last_modified))
  
  def do_update(self, session, obj, existing_obj):
    '''Performs the update after checking to make sure there is no version conflict and a
    previous version exists. Subclasses can override to modify behavior before merging.'''
    session.merge(obj)
    
  # Note: does NOT modify the specified object when updating            
  def update_with_session(self, session, obj, expected_version_id=None):
    id = self.get_id(obj)
    existing_obj = self.get(self.get_id(obj))
    if not existing_obj:
      raise NotFound('{} with id {} does not exist'.format(
          self.model_type.__name__, id))
    # If an expected version was provided, make sure it matches the last modified timestamp of 
    # the existing entity.
    if expected_version_id:
      if existing_obj.lastModified:
        version_id = self.make_version_id(existing_obj.lastModified)
      if version_id != expected_version_id:
        raise PreconditionFailed('If-Match header was {}; stored version was {}'
                                 .format(expected_version_id, version_id))    
    self.do_update(session, obj, existing_obj)
  
  # Note: does NOT modify the specified object when updating
  def update(self, obj, expected_version_id=None):
    with self.session() as session:
      return self.update_with_session(session, obj, expected_version_id)    
    

def as_dict(obj):  
  return {fieldname: getattr(obj, fieldname) for fieldname in 
          (fieldname for fieldname in dir(obj.__class__) if not fieldname.startswith('_')) }
