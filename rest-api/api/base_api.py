import json
import logging
import re

import app_util
from flask import request, jsonify, url_for
from flask_restful import Resource
from model.utils import to_client_participant_id
from model.requests_log import RequestsLog
from dao.base_dao import deferred_save_raw_request
from dao.bq_participant_summary_dao import deferred_bq_participant_summary_update
from query import OrderBy, Query
from werkzeug.exceptions import BadRequest, NotFound
from google.appengine.ext import deferred
from sqlalchemy import inspect
from sqlalchemy.exc import NoInspectionAvailable

DEFAULT_MAX_RESULTS = 100
MAX_MAX_RESULTS = 10000


def log_api_request(model_obj=None):
  """ Create deferred task to save the request payload and possibly link it to a table record """
  log = RequestsLog()

  log.endpoint = request.endpoint
  log.method = request.method
  log.url = request.url
  log.user = app_util.get_oauth_id()
  if request.method in ['POST', 'PUT', 'PATCH']:
    try:
      # We don't want to use request.json or request.get_json here.
      log.resource = json.loads(request.data)
    except ValueError:
      log.resource = request.data
  log.version = int(request.url.split('/')[4][1:])

  request.logged = True

  # See if we can get the participant id and a foreign key id out of the url.
  if request.view_args and isinstance(request.view_args, dict):
    for k, v in request.view_args.items():
      if k == 'p_id':
        log.participantId = int(v)
      else:
        if isinstance(v, int) or str(v).strip().isdigit():
          log.fpk_id = int(v)
        else:
          log.fpk_alt_id = str(v).strip()

  if model_obj:
    try:
      if hasattr(model_obj, '__table__'):
        log.fpk_table = model_obj.__table__.name
      if hasattr(model_obj, 'participantId'):
        log.participantId = int(model_obj.participantId)

      insp = inspect(model_obj)
      if hasattr(insp, 'mapper'):
        if insp.mapper._primary_key_propkeys and len(insp.mapper._primary_key_propkeys) == 1:
          log.fpk_column = str(max(insp.mapper._primary_key_propkeys))
      if insp.identity is None:
        if log.fpk_column and log.fpk_column == 'participant_id' and log.participantId:
          log.fpk_id = int(log.participantId)
      else:
        if isinstance(insp.identity[0], int) or str(insp.identity[0]).strip().isdigit():
          log.fpk_id = int(insp.identity[0])
        else:
          log.fpk_alt_id = str(insp.identity[0])

    except NoInspectionAvailable:
      pass
    except Exception:  # pylint: disable=broad-except
      pass
    deferred.defer(deferred_save_raw_request, log)


class BaseApi(Resource):
  """Base class for API handlers.

  Provides a generic implementation for an API handler which is backed by a
  BaseDao and supports POST and GET.

  For APIs that support PUT requests as well, extend from UpdatableApi instead.

  When extending this class, prefer to use the method_decorators class property
  for uniform authentication, e.g.:
    method_decorators = [app_util.auth_required_cron]
  """
  def __init__(self, dao, get_returns_children=False):
    self.dao = dao
    self._get_returns_children = get_returns_children

  def _get_request_arg_bool(self, key, default=False):
    """
    Return boolean value for the given request parameter key.
    :param key: key name
    :param default: default to use when key does not exist in arguments.
    :return: if key exists return value, otherwise return default.
    """
    if isinstance(default, bool) is False:
      raise ValueError('invalid value for default parameter')

    if key not in request.args:
      return default
    return request.args.get(key).lower() == 'true'

  def get(self, id_=None, participant_id=None):
    """Handle a GET request.

    Args:
      id: If provided this is the id of the object to fetch.  If this is not
        present, this is assumed to be a "list" request, and the list() function
        will be called.
    """
    if id_ is None:
      return self.list(participant_id)
    obj = self.dao.get_with_children(id_) if self._get_returns_children else self.dao.get(id_)
    if not obj:
      raise NotFound("%s with ID %s not found" % (self.dao.model_type.__name__, id_))
    if participant_id:
      if participant_id != obj.participantId:
        raise NotFound("%s with ID %s is not for participant with ID %s" %
                       (self.dao.model_type.__name__, id_, participant_id))
    log_api_request(obj)
    return self._make_response(obj)

  def _make_response(self, obj):
    return self.dao.to_client_json(obj)

  def _get_model_to_insert(self, resource, participant_id=None):
    # Children of participants accept a participant_id parameter to from_client_json; others don't.
    if participant_id is not None:
      return self.dao.from_client_json(
          resource, participant_id=participant_id, client_id=app_util.get_oauth_id())
    else:
      return self.dao.from_client_json(resource, client_id=app_util.get_oauth_id())

  def _do_insert(self, m):
    return self.dao.insert(m)

  def post(self, participant_id=None):
    """Handles a POST (insert) request.

    Args:
      participant_id: The ancestor id.
    """
    resource = request.get_json(force=True)
    m = self._get_model_to_insert(resource, participant_id)
    result = self._do_insert(m)
    if participant_id:
      deferred.defer(deferred_bq_participant_summary_update, participant_id)
    log_api_request(result)
    return self._make_response(result)

  def list(self, participant_id=None):
    """Handles a list request, as the default behavior when a GET has no id provided.

    Subclasses should pull the query parameters from the request with
    request.args.get().
    """
    #pylint: disable=unused-argument
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
    logging.info('Preparing query for %s.', self.dao.model_type)
    query = self._make_query()
    results = self.dao.query(query)
    logging.info('Query complete, bundling results.')
    response = self._make_bundle(results, id_field, participant_id)
    logging.info('Returning response.')
    return response

  def _make_query(self):
    field_filters = []
    max_results = DEFAULT_MAX_RESULTS
    pagination_token = None
    order_by = None
    missing_id_list = ['awardee', 'organization', 'site']
    include_total = request.args.get('_includeTotal', False)
    offset = request.args.get('_offset', False)

    for key, value in request.args.iteritems(multi=True):
      if value in missing_id_list:
        if 'awardee' in value:
          value = 'hpoId'
        else:
          value = value + 'Id'
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
    return Query(field_filters, order_by, max_results, pagination_token,
                 include_total=include_total, offset=offset)

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
      json = self._make_response(item)
      full_url = self._make_resource_url(json, id_field, participant_id)
      entries.append({"fullUrl": full_url,
                     "resource": json})
    bundle_dict['entry'] = entries
    if results.total is not None:
      bundle_dict['total'] = results.total
    return bundle_dict

  def _make_resource_url(self, json, id_field, participant_id):
    import main
    if participant_id:
      return main.api.url_for(self.__class__,
                              id_=json[id_field],
                              p_id=to_client_participant_id(participant_id),
                              _external=True)
    else:
      return main.api.url_for(self.__class__, p_id=json[id_field],
                              _external=True)

class UpdatableApi(BaseApi):
  """Base class for API handlers that support PUT requests.

  To be used with UpdatableDao for model objects with a version field.
  """
  def _get_model_to_update(self, resource, id_, expected_version, participant_id=None):
    # Children of participants accept a participant_id parameter to from_client_json; others don't.
    if participant_id is not None:
      return self.dao.from_client_json(
          resource, participant_id=participant_id, id_=id_, expected_version=expected_version,
          client_id=app_util.get_oauth_id())
    else:
      return self.dao.from_client_json(
          resource, id_=id_, expected_version=expected_version, client_id=app_util.get_oauth_id())

  def _make_response(self, obj):
    result = super(UpdatableApi, self)._make_response(obj)
    etag = _make_etag(obj.version)
    result['meta'] = {'versionId': etag}
    return result, 200, {'ETag': etag}

  def _do_update(self, m):
    self.dao.update(m)

  def put(self, id_, participant_id=None, skip_etag=False, resource=None):
    """
    Replace resource with new values. if skip_etag, version is not required in request.
    If resource, do not fetch original request.
    :param id_:
    :param participant_id:
    :param skip_etag:
    :param resource:
    :return: make_response
    """
    if not resource:
      resource = request.get_json(force=True)
    if skip_etag:
      expected_version = self.dao.get_etag(id_, participant_id)
    else:
      etag = request.headers.get('If-Match')
      if not etag:
        raise BadRequest("If-Match is missing for PUT request")
      expected_version = _parse_etag(etag)
    m = self._get_model_to_update(resource, id_, expected_version, participant_id)
    self._do_update(m)
    if participant_id:
      deferred.defer(deferred_bq_participant_summary_update, participant_id)
    log_api_request(m)
    return self._make_response(m)

  def patch(self, id_):
    """Handles a PATCH request; the current object must exist, and will be amended

    Args:
      :param id_: The id of the object to update.
    """
    resource = request.get_json(force=True)
    etag = request.headers.get('If-Match')
    if not etag:
      raise BadRequest("If-Match is missing for PATCH request")
    expected_version = _parse_etag(etag)
    order = self.dao.update_with_patch(id_, resource, expected_version)
    log_api_request(order)
    return self._make_response(order)

  def update_with_patch(self, id_, resource, expected_version):
    #pylint: disable=unused-argument
    raise NotImplementedError("update_with_patch not implemented in % s " % self.__class__)


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


def get_sync_results_for_request(dao, max_results):
  token = request.args.get('_token')
  count_str = request.args.get('_count')
  count = int(count_str) if count_str else max_results

  results = dao.query(Query([], OrderBy('logPositionId', True),
                            count, token, always_return_token=True))
  return make_sync_results_for_request(dao, results)


def make_sync_results_for_request(dao, results):
  bundle_dict = {'resourceType': 'Bundle', 'type': 'history'}
  if results.pagination_token:
    query_params = request.args.copy()
    query_params['_token'] = results.pagination_token
    link_type = 'next' if results.more_available else 'sync'
    next_url = url_for(request.url_rule.endpoint, _external=True, **query_params)
    bundle_dict['link'] = [{'relation': link_type, 'url': next_url}]
  entries = []
  for item in results.items:
    entries.append({'resource': dao.to_client_json(item)})
  bundle_dict['entry'] = entries
  return jsonify(bundle_dict)
