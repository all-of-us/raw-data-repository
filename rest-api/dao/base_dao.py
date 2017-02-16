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
        
  def insert_with_session(self, session, obj):    
    """Adds the object into the session to be inserted."""
    session.add(obj)
  
  def insert(self, obj):
    """Inserts an object into the database. The calling object may be mutated
    in the process."""
    with self.session() as session:      
      self.insert_with_session(session, obj)        
    
  def get_id(self, obj):
    """Returns the ID (for single primary key column tables) or a list of IDs (for multiple
    primary key column tables). Must be overridden by subclasses."""    
    raise NotImplementedError
  
  def get_with_session(self, session, id):
    """Gets an object with the specified ID for this type from the database using the specified
    session. Returns None if not found."""
    return session.query(self.model_type).get(id)
    
  def get(self, id):
    """Gets an object with the specified ID for this type from the database. 
    Returns None if not found."""
    with self.session() as session:
      result = self.get_with_session(session, id)          
    return result
          
  def _make_version_id(self, last_modified):
    return 'W/"{}"'.format(api_util.unix_time_millis(last_modified))
      
  def validate_update(self, session, obj, existing_obj, expected_version_id=None):
    """Validates that an update is OK before performing it. By default, validates that the 
    object already exists, and if an expected version ID is provided, that it matches."""
    if not existing_obj:
      raise NotFound('{} with id {} does not exist'.format(
          self.model_type.__name__, id))
    # If an expected version was provided, make sure it matches the last modified timestamp of 
    # the existing entity.
    if expected_version_id:
      if existing_obj.lastModified:
        version_id = self._make_version_id(existing_obj.lastModified)
      if version_id != expected_version_id:
        raise PreconditionFailed('If-Match header was {}; stored version was {}'
                                 .format(expected_version_id, version_id))    
  
  def do_update(self, session, obj, existing_obj):
    """Perform the update of the specified object. Subclasses can override to alter things."""
    session.add(obj)   
                
  def update_with_session(self, session, obj, expected_version_id=None):
    """Updates the object in the database with the specified session and (optionally) 
    expected version ID."""
    id = self.get_id(obj)
    existing_obj = self.get(self.get_id(obj))
    self.validate_update(session, obj, existing_obj, expected_version_id)
    self.do_update(session, obj, existing_obj)
  
  def update(self, obj, expected_version_id=None):
    """Updates the object in the database. Will fail if the object doesn't exist already, or
    if expected_version_id is provided but does not match the lastModified timestamp of the object.
    May modify the passed in object."""
    with self.session() as session:
      return self.update_with_session(session, obj, expected_version_id)    
  