"""Base class for API handlers."""

import api_util
import uuid

from flask import Flask, request
from flask.ext.restful import Resource, reqparse, abort

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
  def get(self, id=None, a_id=None):
    if not id:
      return self.list(a_id)
    return self.dao.to_json(self.dao.load(id))

  @api_util.auth_required
  def list(self, a_id=None):
    pass

  def validate_object(self, obj):
    """Override this function to validate the passed object.

    This function should raise an exception if the object doesn't pass
    validation.
    """
    pass

  @api_util.auth_required
  def post(self, a_id=None):
    resource = request.get_json(force=True)
    m = self.dao.from_json(resource, a_id, str(uuid.uuid4()))
    self.validate_object(m)
    self.dao.store(m)
    return self.dao.to_json(m)

  @api_util.auth_required
  def patch(self, id, a_id=None):
    old_m = self.dao.load(id, a_id)
    new_m = self.dao.from_json(request.get_json(force=True), a_id, id)
    self.validate_object(new_m)
    api_util.update_model(old_model=old_m, new_model=new_m)
    self.dao.store(old_m)
    return self.dao.to_json(old_m)
