"""Base class for API handlers."""

import flask

from flask import request
from flask.ext.restful import Resource
from werkzeug.exceptions import BadRequest, NotFound

class BaseApi(Resource):
  """Base class for API handlers.

  Provides a generic implementation for an API handler which is backed by a
  BaseDao and supports POST and GET. 
  
  For APIs that support PATCH requests as well, extend from UpdatableApi instead.
  
  When extending this class, prefer to use the method_decorators class property
  for uniform authentication, e.g.:
    method_decorators = [api_util.auth_required_cron]
  """
  def __init__(self, dao):
    self.dao = dao    

  def get(self, id=None):
    """Handle a GET request.

    Args:
      id: If provided this is the id of the object to fetch.  If this is not
        present, this is assumed to be a "list" request, and the list() function
        will be called.
    """
    if not id:
      return self.list()
    obj = self.dao.get(id)
    if not obj:
      raise NotFound("%s with ID %s not found" % (self.dao.model_type.__name__, id))    
    return self._make_response(obj)

  def _make_response(self, obj):
    return obj.to_json()

  def _get_model_to_insert(self, resource, a_id=None):
    if a_id:
      return self.dao.model_type.from_json(resource, a_id=a_id)
    else:
      return self.dao.model_type.from_json(resource)
      
  def post(self, a_id=None):
    """Handles a POST (insert) request.

    Args:
      a_id: The ancestor id.
    """
    resource = request.get_json(force=True)
    m = self._get_model_to_insert(resource, a_id)    
    self.dao.insert(m)
    return self._make_response(m)

  def list(self):
    """Handles a list request, as the default behavior when a GET has no id provided.

    Subclasses should pull the query parameters from the request with
    request.args.get().
    """
    raise BadRequest('List not implemented, provide GET with an ID.')


class UpdatableApi(BaseApi):
  """Base class for API handlers that support PATCH requests."""
  
  def _make_etag(self, version):
    return 'W/"%d"' % version

  def _parse_etag(self, etag):
    if etag.startswith('W/"') and etag.endswith('"'):
      version_str = etag.split('"')[1]
      try:
        return int(version_str)
      except ValueError:
        raise BadRequest("Invalid version: %s" % version_str)
    raise BadRequest("Invalid ETag: %s" % etag)
    
  def _get_model_to_update(self, resource, id, expected_version, a_id=None):
    if a_id:
      return self.dao.model_type.from_json(resource, a_id=a_id, id=id, 
                                           expected_version=expected_version)
    else:
      return self.dao.model_type.from_json(resource, id=id, expected_version=expected_version)
  
  def patch(self, id, a_id=None):
    """Handles a PATCH (update) request.

    Args:
      id: The id of the object to update.
      a_id: The ancestor id (if applicable).
    """
    resource = request.get_json(force=True)
    expected_version = None
    etag = request.headers.get('If-Match')
    if not etag:
      raise BadRequest("If-Match is missing for PATCH request")
    expected_version = self._parse_etag(etag)    
    m = self._get_model_to_update(resource, id, expected_version, a_id)
    self.dao.update(m)
    return self._make_response(m)
