"""Base class for API handlers."""

import api_util

from flask import Flask, request
from flask.ext.restful import Resource

class BaseApi(Resource):
  """Base class for API handlers.

  Provides a generic implementation for an API handler which is backed by a
  DataAccessObject.

  Subclasses should implement the list() function.  The generic implementations
  of get(), post() and patch() should be sufficient for most subclasses.
  """
  def __init__(self, dao):
    self.dao = dao

  @api_util.auth_required
  def get(self, id_=None, a_id=None):
    """Handle a GET request.

    Args:
      id_: If provided this is the id of the object to fetch.  If this is not
        present, this is assumed to be a "list" request, and the list() function
        will be called.
      a_id: The ancestor id.
    """
    if not id_:
      return self.list(a_id)
    return self.dao.to_json(self.dao.load(id_, a_id))

  @api_util.auth_required
  def list(self, a_id=None):
    """Handle a list request.

    Subclasses should pull the query parameters from the request with
    request.args.get().

    Args:
      a_id: The ancestor id.

    """
    pass

  def validate_object(self, obj):
    """Override this function to validate the passed object.

    This function should raise an exception if the object doesn't pass
    validation.
    """
    pass

  @api_util.auth_required
  def post(self, a_id=None):
    """Handles a POST request.

    Args:
      a_id: The ancestor id.
    """
    resource = request.get_json(force=True)
    m = self.dao.from_json(resource, a_id, self.dao.allocate_id())
    self.validate_object(m)
    self.dao.store(m, get_fake_date())
    return self.dao.to_json(m)

  @api_util.auth_required
  def patch(self, id_, a_id=None):
    """Handles a PATCH (update) request.

    Args:
      id_: The id of the object to update.
      a_id: The ancestor id.
    """
    old_m = self.dao.load(id_, a_id)
    new_m = self.dao.from_json(request.get_json(force=True), a_id, id_)
    self.validate_object(new_m)
    api_util.update_model(old_model=old_m, new_model=new_m)
    self.dao.store(old_m, get_fake_date())
    return self.dao.to_json(old_m)

def get_fake_date():
  date = request.headers.get('x-pretend-date', None)
  if date:
    date = api_util.parse_date(date)
    print("Pretending fake date is %s"%date)
  return date

