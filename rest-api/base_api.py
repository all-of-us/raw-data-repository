"""Base class for API handlers."""

import api_util
import config

from data_access_object import PROPERTY_TYPE_MAP
from query import Query, OrderBy, FieldFilter, Results, Operator, PropertyType
from flask import request
from flask.ext.restful import Resource
from werkzeug.exceptions import BadRequest
from werkzeug.urls import url_encode

DEFAULT_MAX_RESULTS = 100
MAX_MAX_RESULTS = 10000

class BaseApi(Resource):
  """Base class for API handlers.

  Provides a generic implementation for an API handler which is backed by a
  DataAccessObject.

  Subclasses should override the list() method; they can use the query() method
  to return an FHIR bundle containing the results.  The generic implementations
  of get(), post() and patch() should be sufficient for most subclasses.

  When extending this class, prefer to use the method_decorators class property
  for uniform authentication, e.g.:
    method_decorators = [api_util.auth_required_cron]

  If include_meta is True (the default), meta will be returned to clients in
  resources when populated; if False, it will be ignored.

  meta.versionId will be used to populate an ETag header on resource responses;
  an If-Match header must be sent on patch requests that matches the current ETag
  value

  """
  def __init__(self, dao, include_meta=True):
    self.dao = dao
    self.include_meta = include_meta

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
    result = self.dao.to_json(self.dao.load(id_, a_id))
    return self.make_response_for_resource(result)

  def list(self, a_id=None):
    """Handle a list request.
    Subclasses should pull the query parameters from the request with
    request.args.get().
    Args:
      a_id: The ancestor id.
    """
    pass
          
  def query(self, id_field):    
    """Run a query against the DAO. 
    Extracts query parameters from request using FHIR conventions.
    
    Returns an FHIR Bundle containing entries for each item in the 
    results, with a "next" link if there are more results to fetch. An empty Bundle
    will be returned if no results match the query.
    
    Args:
      id_field: the name of the field containing the ID used when constructing resource URLs for results
    """     
    query = self.make_query()
    results = self.dao.query(query)
    return self.make_bundle(results, query.max_results, id_field)

  def validate_object(self, obj, a_id=None):
    """Override this function to validate the passed object.

    This function should raise an exception if the object doesn't pass
    validation.
    """
    pass

  def post(self, a_id=None):
    """Handles a POST request.

    Args:
      a_id: The ancestor id.
    """
    resource = request.get_json(force=True)
    m = self.dao.from_json(resource, a_id, self.dao.allocate_id())
    self.validate_object(m, a_id)
    self.dao.insert(m, date=consider_fake_date(),
                    client_id=api_util.get_oauth_id())
    return self.make_response_for_resource(self.dao.to_json(m))

  def patch(self, id_, a_id=None):
    """Handles a PATCH (update) request.

    Args:
      id_: The id of the object to update.
      a_id: The ancestor id.
    """
    old_m = self.dao.load(id_, a_id)
    new_m = self.dao.from_json(request.get_json(force=True), a_id, id_)
    self.validate_object(new_m, a_id)
    api_util.update_model(old_model=old_m, new_model=new_m)
    self.dao.update(old_m, request.headers.get('If-Match'),
                    date=consider_fake_date(),
                    client_id=api_util.get_oauth_id())
    return self.make_response_for_resource(self.dao.to_json(old_m))

  def put(self, id_, a_id=None):
    """Handles a PUT (replace) request.

    Args:
      id_: The id of the object to replace.
      a_id: The ancestor id.
    """
    m = self.dao.from_json(request.get_json(force=True), a_id, id_)
    self.validate_object(m, a_id)
    self.dao.replace(m, date=consider_fake_date(), client_id=api_util.get_oauth_id())
    return self.make_response_for_resource(self.dao.to_json(m))

  def make_response_for_resource(self, result):
    meta = result.get('meta')
    if meta:
      if not self.include_meta:
        result['meta'] = None
      version_id = meta.get('versionId')
      if version_id:
        return result, 200, {'ETag': version_id}
    return result 
  
  def make_query(self):
    field_filters = []
    order_by = None
    max_results = DEFAULT_MAX_RESULTS
    pagination_token = None
    inequality_field = None
    for key, value in request.args.iteritems():
      if key == '_count':
        max_results = int(request.args['_count'])
        if max_results < 1:
          raise BadRequest("_count < 1")
        if max_results > MAX_MAX_RESULTS:
          raise BadRequest("_count exceeds {}".format(MAX_MAX_RESULTS))
      elif key == '_sort' or key == '_sort:asc':
        order_by = OrderBy(value, True)
      elif key == '_sort:desc':
        order_by = OrderBy(value, False)
      elif key == '_token':
        pagination_token = value
      else:
        property = getattr(self.dao.model_type, key, None)
        if property:
          property_type = PROPERTY_TYPE_MAP.get(property.__class__.__name__)
          if not property_type:
            raise BadRequest("Unsupported query for field {}".format(key))
          elif property_type == PropertyType.DATE:
            if value.startswith("lt"):
              operator = Operator.LESS_THAN
            elif value.startswith("gt"):
              operator = Operator.GREATER_THAN
            elif value.startswith("le"):
              operator = Operator.LESS_THAN_OR_EQUALS
            elif value.startswith("ge"):
              operator = Operator.GREATER_THAN_OR_EQUALS
            else:
              operator = Operator.EQUALS
            date_str = value
            if operator != Operator.EQUALS:
              if inequality_field:
                raise BadRequest("Can't have multiple inequality expressions in a single query")
              date_str = value[2:]
              inequality_field = key
            date_val = api_util.parse_date(date_str)
            field_filters.append(FieldFilter(key, operator, date_val))   
          elif property_type == PropertyType.ENUM:
            field_filters.append(FieldFilter(key, Operator.EQUALS, property._enum_type(value)))
          else:
            field_filters.append(FieldFilter(key, Operator.EQUALS, value))
    if inequality_field and order_by and inequality_field != order_by.field_name:
      raise BadRequest("Can't combine inequality expression with sort on a different property")
    return Query(field_filters, order_by, max_results, pagination_token)        

  def make_bundle(self, results, max_results, id_field):
    bundle_dict = { "resourceType" : "Bundle", "type": "searchset" }
    import main
    if results.pagination_token:
      query_params = request.args.copy()
      query_params['_token'] = results.pagination_token
      next_url = main.api.url_for(self.__class__, _external=True, **query_params)
      bundle_dict['link'] = [{ "relation": "next", "url": next_url}]      
    entries = []
    for item in results.items:      
      json = self.dao.to_json(item)
      entries.append({"fullUrl": main.api.url_for(self.__class__,
                                                 id_=json[id_field],
                                                 _external=True),
                     "resource": json })
    bundle_dict['entry'] = entries
    return bundle_dict  

def consider_fake_date():
  if "True" == config.getSetting(config.ALLOW_FAKE_HISTORY_DATES, None):
    date = request.headers.get('x-pretend-date', None)
    if date:
      return api_util.parse_date(date)
  return None
