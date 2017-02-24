import sys
import random

import dao.database_factory
from contextlib import contextmanager
from werkzeug.exceptions import NotFound, PreconditionFailed, ServiceUnavailable
from sqlalchemy.exc import IntegrityError

# Maximum number of times we will attempt to insert an entity with a random ID before 
# giving up.
MAX_INSERT_ATTEMPTS = 20

# Range of possible values for random IDs.
_MIN_ID = 100000000
_MAX_ID = 999999999

class BaseDao(object):
  """A data access object base class; defines common methods for inserting and retrieving
  objects using SQLAlchemy. 
  
  Extend directly from BaseDao if entities cannot be updated after being
  inserted; extend from UpdatableDao if they can be updated. 
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

  def _validate_model(self, session, obj):
    """Override to validate a model before any db write (insert or update)."""
    pass

  def _validate_insert(self, session, obj):
    """Override to validate a new model before inserting it (not applied to updates)."""
    self._validate_model(session, obj)

  def insert_with_session(self, session, obj):
    """Adds the object into the session to be inserted."""
    self._validate_insert(session, obj)
    session.add(obj)
    return obj

  def insert(self, obj):
    """Inserts an object into the database. The calling object may be mutated
    in the process."""
    with self.session() as session:
      return self.insert_with_session(session, obj)    

  def get_id(self, obj):
    """Returns the ID (for single primary key column tables) or a list of IDs (for multiple
    primary key column tables). Must be overridden by subclasses."""
    raise NotImplementedError

  def get_with_session(self, session, obj_id):
    """Gets an object by ID for this type using the specified session. Returns None if not found."""
    return session.query(self.model_type).get(obj_id)

  def get(self, obj_id):
    """Gets an object with the specified ID for this type from the database.

    Returns None if not found.
    """
    with self.session() as session:
      return self.get_with_session(session, obj_id)

  def get_with_children(self, obj_id):
    """Subclasses may override this to eagerly loads any child objects (using subqueryload)."""
    return self.get(self, obj_id)
  
  def _get_random_id(self, field):
    return random.randint(_MIN_ID, _MAX_ID)
    
  def _insert_with_random_id(self, obj, fields):    
    """Attempts to insert an entity with randomly assigned ID(s) repeatedly until success
    or a maximum number of attempts are performed."""    
    for i in range(0, MAX_INSERT_ATTEMPTS):
      for field in fields:
        setattr(obj, field, self._get_random_id(field))      
      try:
        with self.session() as session:
          return self.insert_with_session(session, obj)
      except IntegrityError:
        pass                  
    # We were unable to insert a participant (unlucky). Throw an error.
    raise ServiceUnavailable("Giving up after %d insert attempts" % MAX_INSERT_ATTEMPTS)

class UpdatableDao(BaseDao):
  """A DAO that allows updates to entities. 
  
  Extend from UpdatableDao if entities can be updated after being inserted.
  
  All model objects using this DAO must define a "version" field.
  """
  
  def _validate_update(self, session, obj, existing_obj):
    """Validates that an update is OK before performing it. (Not applied on insert.)

    By default, validates that the object already exists, and if an expected version ID is provided,
    that it matches.
    """
    if not existing_obj:
      raise NotFound('%s with id %s does not exist' % (self.model_type.__name__, id))
    # If an expected version was provided, make sure it matches the last modified timestamp of
    # the existing entity.
    if obj.version:
      if existing_obj.version != obj.version:
        raise PreconditionFailed('Expected version was %d; stored version was %d' % \
                                 (obj.version, existing_obj.version))
    self._validate_model(session, obj)

  # pylint: disable=unused-argument
  def _do_update(self, session, obj, existing_obj):
    """Perform the update of the specified object. Subclasses can override to alter things."""
    session.merge(obj)

  def update_with_session(self, session, obj):
    """Updates the object in the database with the specified session and (optionally)
    expected version ID."""
    existing_obj = self.get(self.get_id(obj))
    self._validate_update(session, obj, existing_obj)
    self._do_update(session, obj, existing_obj)

  def update(self, obj):
    """Updates the object in the database. Will fail if the object doesn't exist already, or
    if obj.version does not match the version of the existing object.
    May modify the passed in object."""
    with self.session() as session:
      return self.update_with_session(session, obj)
