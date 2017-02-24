"""Base class for API handlers."""

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

  def get(self, id_=None):
    """Handle a GET request.

    Args:
      id: If provided this is the id of the object to fetch.  If this is not
        present, this is assumed to be a "list" request, and the list() function
        will be called.
    """    
    if id_ is None:
      return self.list()
    obj = self.dao.get(id_)
    if not obj:
      raise NotFound("%s with ID %s not found" % (self.dao.model_type.__name__, id_))    
    return self._make_response(obj)

  def _make_response(self, obj):
    return obj.to_client_json()

  def _get_model_to_insert(self, resource, participant_id=None):
    if participant_id:
      return self.dao.model_type.from_client_json(resource, participant_id=participant_id)
    else:
      return self.dao.model_type.from_client_json(resource)
      
  def post(self, participant_id=None):
    """Handles a POST (insert) request.

    Args:
      participant_id: The ancestor id.
    """
    resource = request.get_json(force=True)
    m = self._get_model_to_insert(resource, participant_id)    
    self.dao.insert(m)
    return self._make_response(m)

  def list(self):
    """Handles a list request, as the default behavior when a GET has no id provided.

    Subclasses should pull the query parameters from the request with
    request.args.get().
    """
    raise BadRequest('List not implemented, provide GET with an ID.')


class UpdatableApi(BaseApi):
  """Base class for API handlers that support PUT requests.
  
  To be used with UpdatableDao for model objects with a version field.
  """    
  def _get_model_to_update(self, resource, id_, expected_version, participant_id=None):
    if participant_id:
      return self.dao.model_type.from_client_json(resource, participant_id=participant_id, id=id_, 
                                                  expected_version=expected_version)
    else:
      return self.dao.model_type.from_client_json(resource, id=id_, 
                                                  expected_version=expected_version)
  
  def _make_response(self, obj):    
    result = super(UpdatableApi, self)._make_response(obj)
    etag = _make_etag(obj.version)
    result['meta'] = {'versionId': etag}
    return result, 200, {'ETag': etag}
  
  def put(self, id_, participant_id=None):
    """Handles a PUT (replace) request; the current object must exist, and will be replaced 
    completely.
      
    Args:
      id: The id of the object to update.
      participant_id: The ancestor id (if applicable).
    """
    resource = request.get_json(force=True)
    expected_version = None
    etag = request.headers.get('If-Match')
    if not etag:
      raise BadRequest("If-Match is missing for PATCH request")
    expected_version = _parse_etag(etag)    
    m = self._get_model_to_update(resource, id_, expected_version, participant_id)
    self.dao.update(m)
    return self._make_response(m)

def _make_etag(self, version):
  return 'W/"%d"' % version

def _parse_etag(etag):
  if etag.startswith('W/"') and etag.endswith('"'):
    version_str = etag.split('"')[1]
    try:
      return int(version_str)
    except ValueError:
      raise BadRequest("Invalid version: %s" % version_str)
  raise BadRequest("Invalid ETag: %s" % etag)
