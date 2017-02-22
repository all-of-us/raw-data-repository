import dao.database_factory
from contextlib import contextmanager
from sqlalchemy.orm.session import make_transient
from werkzeug.exceptions import NotFound, PreconditionFailed

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

  def insert(self, obj):
    """Inserts an object into the database. The calling object may be mutated
    in the process."""
    with self.session() as session:
      self.insert_with_session(session, obj)

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

  def _validate_update(self, session, obj, existing_obj, expected_version=None):
    """Validates that an update is OK before performing it. (Not applied on insert.)

    By default, validates that the object already exists, and if an expected version ID is provided,
    that it matches.
    """
    if not existing_obj:
      raise NotFound('%s with id %s does not exist' % (self.model_type.__name__, id))
    # If an expected version was provided, make sure it matches the last modified timestamp of
    # the existing entity.
    if expected_version:      
      if existing_obj.version != expected_version:
        raise PreconditionFailed('Expected version was %d; stored version was %d' % \
                                 (expected_version, existing_obj.version))
    self._validate_model(session, obj)

  def _do_update(self, session, obj, existing_obj):
    """Perform the update of the specified object. Subclasses can override to alter things."""
    session.merge(obj)

  def update_with_session(self, session, obj, expected_version=None):
    """Updates the object in the database with the specified session and (optionally)
    expected version ID."""
    id = self.get_id(obj)
    existing_obj = self.get(self.get_id(obj))
    self._validate_update(session, obj, existing_obj, expected_version)
    self._do_update(session, obj, existing_obj)

  def update(self, obj, expected_version=None):
    """Updates the object in the database. Will fail if the object doesn't exist already, or
    if expected_version is provided but does not match the version of the existing object.
    May modify the passed in object."""
    with self.session() as session:
      return self.update_with_session(session, obj, expected_version)
