"""Base class for API handlers."""
import api_util

from query import OrderBy, Query
from flask import request
from flask.ext.restful import Resource
from werkzeug.exceptions import BadRequest, NotFound

DEFAULT_MAX_RESULTS = 100
MAX_MAX_RESULTS = 10000

class BaseApi(Resource):
  """Base class for API handlers.

  Provides a generic implementation for an API handler which is backed by a
  BaseDao and supports POST and GET.

  For APIs that support PATCH requests as well, extend from UpdatableApi instead.

  When extending this class, prefer to use the method_decorators class property
  for uniform authentication, e.g.:
    method_decorators = [api_util.auth_required_cron]
  """
  def __init__(self, dao, get_returns_children=False):
    self.dao = dao
    self._get_returns_children = get_returns_children

  def get(self, id_=None):
    """Handle a GET request.

    Args:
      id: If provided this is the id of the object to fetch.  If this is not
        present, this is assumed to be a "list" request, and the list() function
        will be called.
    """
    if id_ is None:
      return self.list()
    obj = self.dao.get_with_children(id_) if self._get_returns_children else self.dao.get(id_)
    if not obj:
      raise NotFound("%s with ID %s not found" % (self.dao.model_type.__name__, id_))
    return self._make_response(obj)

  def _make_response(self, obj):
    return obj.to_client_json()

  def _get_model_to_insert(self, resource, participant_id=None):
    # Children of participants accept a participant_id parameter to from_client_json; others don't.
    if participant_id is not None:
      return self.dao.model_type.from_client_json(resource, participant_id=participant_id,
                                                  client_id=api_util.get_oauth_id())
    else:
      return self.dao.model_type.from_client_json(resource, client_id=api_util.get_oauth_id())

  def _do_insert(self, m):
    self.dao.insert(m)

  def post(self, participant_id=None):
    """Handles a POST (insert) request.

    Args:
      participant_id: The ancestor id.
    """
    resource = request.get_json(force=True)
    m = self._get_model_to_insert(resource, participant_id)
    self._do_insert(m)
    return self._make_response(m)

  def list(self):
    """Handles a list request, as the default behavior when a GET has no id provided.

    Subclasses should pull the query parameters from the request with
    request.args.get().
    """
    raise BadRequest('List not implemented, provide GET with an ID.')

  def _query(self, id_field, participant_id=None):
    """Run a query against the DAO.
    Extracts query parameters from request using FHIR conventions.

    Returns an FHIR Bundle containing entries for each item in the
    results, with a "next" link if there are more results to fetch. An empty Bundle
    will be returned if no results match the query.

    Args:
      id_field: name of the field containing the ID used when constructing resource URLs for results
      participant_id: the participant ID under which to perform this query, if appropriate
    """
    query = self._make_query(participant_id)
    results = self.dao.query(query)
    return self._make_bundle(results, id_field, participant_id)

  def _make_query(self, participant_id=None):
    field_filters = []
    max_results = DEFAULT_MAX_RESULTS
    pagination_token = None
    order_by = None
    for key, value in request.args.iteritems():
      if key == '_count':
        max_results = int(request.args['_count'])
        if max_results < 1:
          raise BadRequest("_count < 1")
        if max_results > MAX_MAX_RESULTS:
          raise BadRequest("_count exceeds {}".format(MAX_MAX_RESULTS))
      elif key == '_token':
        pagination_token = value
      elif key == '_sort' or key == '_sort:asc':
        order_by = OrderBy(value, True)
      elif key == '_sort:desc':
        order_by = OrderBy(value, False)
      else:
        field_filter = self.dao.make_query_filter(key, value)
        if field_filter:
          field_filters.append(field_filter)
    return Query(field_filters, order_by, max_results, pagination_token, participant_id)

  def _make_bundle(self, results, id_field, participant_id):
    import main
    bundle_dict = {"resourceType": "Bundle", "type": "searchset"}
    if results.pagination_token:
      query_params = request.args.copy()
      query_params['_token'] = results.pagination_token
      next_url = main.api.url_for(self.__class__, _external=True, **query_params)
      bundle_dict['link'] = [{"relation": "next", "url": next_url}]
    entries = []
    for item in results.items:
      json = item.to_client_json()
      if participant_id:
        full_url = main.api.url_for(self.__class__,
                                    id_=json[id_field],
                                    p_id=participant_id,
                                    _external=True)
      else:
        full_url = main.api.url_for(self.__class__,
                                    p_id=json[id_field],
                                    _external=True)
      entries.append({"fullUrl": full_url,
                     "resource": json})
    bundle_dict['entry'] = entries
    return bundle_dict


class UpdatableApi(BaseApi):
  """Base class for API handlers that support PUT requests.

  To be used with UpdatableDao for model objects with a version field.
  """
  def _get_model_to_update(self, resource, id_, expected_version, participant_id=None):
    # Children of participants accept a participant_id parameter to from_client_json; others don't.
    if participant_id is not None:
      return self.dao.model_type.from_client_json(resource, participant_id=participant_id, id_=id_,
                                                  expected_version=expected_version,
                                                  client_id=api_util.get_oauth_id())
    else:
      return self.dao.model_type.from_client_json(resource, id_=id_,
                                                  expected_version=expected_version,
                                                  client_id=api_util.get_oauth_id())

  def _make_response(self, obj):
    result = super(UpdatableApi, self)._make_response(obj)
    etag = _make_etag(obj.version)
    result['meta'] = {'versionId': etag}
    return result, 200, {'ETag': etag}

  def _do_update(self, m):
    self.dao.update(m)

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
    self._do_update(m)
    return self._make_response(m)

def _make_etag(version):
  return 'W/"%d"' % version

def _parse_etag(etag):
  if etag.startswith('W/"') and etag.endswith('"'):
    version_str = etag.split('"')[1]
    try:
      return int(version_str)
    except ValueError:
      raise BadRequest("Invalid version: %s" % version_str)
  raise BadRequest("Invalid ETag: %s" % etag)
