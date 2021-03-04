#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import importlib
import json
import os
import re

from flask import request
from flask_restful import Resource
from sqlalchemy.sql.functions import max as _max, coalesce as _coalesce
from werkzeug.exceptions import NotFound, BadRequest

from rdr_service.api_util import RESOURCE
from rdr_service.app_util import auth_required
from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.model.resource_data import ResourceData
from rdr_service.model.resource_schema import ResourceSchema
from rdr_service.model.resource_type import ResourceType
from rdr_service.services.flask import RESOURCE_PREFIX

# Import all of the available resource schema objects.
# TODO: If the Resource Schema class name is not the same as the Resource Schema's Meta.resource_uri
#  value (minus Schema suffix) we break things.  Loop through each of the classes and build the list
#  by instantiating each resource schema class and getting the Meta.resource_uri value for the final list.
#  Remember to add a 'Schema' suffix to each Meta.resource_uri value.  IE: Participant URI = ParticipantSchema.
RESOURCE_SCHEMAS = importlib.import_module("rdr_service.resource.schemas.__init__")

# TODO: Fill out all the possible FHIR operands.
# TODO: Is this mapping really needed?
FIHR_TO_SQLACHEMY_OPERATOR_MAP = {
    'eq': 'eq',
    'ne': 'ne',
    'gt': 'gt',
    'lt': 'lt',
    'ge': 'ge',
    'le': 'le'
}

class RequestResource(object):

    path = None  # Full request URI path.
    path_components = None  # List of URI path components.
    uri = None  # Parsed Resource URI path.
    resource_uri = None  # The requested resource URI.
    schema_name = None  # Name of Resource Schema object
    schema = None  # Resource Schema object
    suffix = None  # Resource operation suffix if found in URI.
    pk_id = None  # Integer Primary Key value.
    pk_alt_id = None  # String Primary Key value.
    batch_ids = None  # List of resource id values.
    uri_args = None  # URI arguments
    uri_args_filter = None  # URI argument SQLAlchemy filter tuples.

    def __init__(self):

        self._parse_uri_path()

    def _parse_uri_path(self):
        """
        Parse the resource URI path
        """
        # Remove trailing slash if present.
        self.path = request.path[:-1] if request.path.endswith('/') else request.path
        # Validate url path
        if self.path.count('/') < 2:
            raise NotFound('Invalid Resource path.')

        # Remove the resource prefix
        self.uri = self.path.replace(RESOURCE_PREFIX, '')

        # Find suffix if in URI.
        if os.path.basename(self.uri).lower() in ['_meta', '_schema', '_batch', '_search']:
            self.suffix = os.path.basename(self.uri).lower()
            self.uri = self.uri[:-(len(self.suffix) + 1)]  # remove suffix from URI path.

        # Get and validate the schema, we start from the right most components of the URI path.
        uri_components = self.uri.split('/')
        if hasattr(RESOURCE_SCHEMAS, f'{uri_components[-1]}Schema'):  # IE: "resource/Participant?..."
            self.resource_uri = uri_components[-1]
            self.schema_name = f'{uri_components[-1]}Schema'
        elif hasattr(RESOURCE_SCHEMAS, f'{uri_components[-2]}Schema'):  # IE: "resource/Participant/P123456789?..."
            self.resource_uri = uri_components[-2]
            self.schema_name = f'{uri_components[-2]}Schema'
            self.pk_alt_id = uri_components[-1]
            # See if we can convert the Resource id to an integer.
            _tmp_pk = re.sub('\D', '', self.pk_alt_id)
            if _tmp_pk:
                self.pk_id = int(_tmp_pk)
        else:
            raise NotFound(f'Resource not found in URI.')

        # Get response body and URI arguments.
        if self.suffix and self.suffix == '_batch' and request.data:
            if self.pk_id or self.pk_alt_id:
                raise BadRequest('A batch request may not be used together with resource id')
            try:
                _data = json.loads(request.data.decode('utf-8'))
                if not isinstance(_data, list):
                    raise BadRequest('Request body must be list of resource ids.')
                # Convert request body to list of resource ids.
                self.batch_ids = list()
                for _id in _data:
                    if isinstance(_id, str) or isinstance(_id, int):
                        self.batch_ids.append(_id)
                if not self.batch_ids:
                    raise BadRequest('Invalid batch request data.')
            except json.JSONDecodeError:
                raise BadRequest('Invalid batch request data.')

        self.uri_args = request.values
        if self.uri_args:
            self.uri_args_filter = list()
            for k, v in self.uri_args.items():
                op = v[:2] if v[:2] in FIHR_TO_SQLACHEMY_OPERATOR_MAP else None
                if op:
                    self.uri_args_filter.append((k, FIHR_TO_SQLACHEMY_OPERATOR_MAP[op], v[2:]))
                else:
                    self.uri_args_filter.append((k, FIHR_TO_SQLACHEMY_OPERATOR_MAP['eq'], v))

        mod = getattr(RESOURCE_SCHEMAS, self.schema_name)
        self.schema = mod()


class ResourceRequestApi(Resource):
    """
    Resource Request API
    Resource Request Syntax:
        METHOD /[prefix]/[resource]/{[suffix]|/?[argument{:modifiers}={prefix}value,...]}
    """
    dao = ResourceDataDao()

    @auth_required(RESOURCE)
    def get(self, path):  # pylint: disable=unused-argument
        """
        Handle GET requests
        :param path: URI request path
        :return: response
        """
        resource = RequestResource()

        if resource.suffix == '_schema':
            resp = self._get_schema(resource)
        elif resource.suffix == '_meta':
            resp = self._get_meta(resource)
        elif resource.suffix == '_search':
            raise BadRequest('Not Implemented Yet')
        elif resource.suffix == '_batch':
            BadRequest('Batch requests are not allowed with GET method.')
        else:
            resp = self._get_resource(resource)

        # TODO: Handle request argument modifiers, IE: _sort, _id.
        # TODO: Handle awardee filtering (probably very last todo item).

        return resp

    @auth_required(RESOURCE)
    def post(self, path):  # pylint: disable=unused-argument
        """
        Handle POST batch requests.
        :param path: URI request path
        :return: batch response.
        """
        resource = RequestResource()
        if resource.suffix == '_batch':
            resp = self._get_batch(resource)
            return resp

        raise BadRequest('Resource POST requests other than batch requests are not allowed.')

    def _add_arg_filters(self, resource, query):
        """
        Add any argument filters to query.
        :param resource: RequestResource object
        :param query: SQLAlchemy Query object
        :return: SQLAlchemy Query object
        """
        if not resource.uri_args_filter:
            return query

        # https://stackoverflow.com/questions/14845196/dynamically-constructing-filters-in-sqlalchemy
        for field_name, op, value in resource.uri_args_filter:
            column = getattr(ResourceData, field_name, None)
            if column:
                try:
                    attr = list(filter(lambda e: hasattr(column, e % op), ['%s', '%s_', '__%s__']))[0] % op
                    query = query.filter(getattr(column, attr)(value))
                except IndexError:
                    raise BadRequest(f'Invalid filter operator ({op})')
            else:
                # TODO: implement filtering by resource JSON here.
                continue

        return query

    def _get_resource(self, resource):
        """
        Return the requested resource.
        :param resource: RequestResource object
        :return: Meta data json.
        """
        if not resource.pk_id and not resource.pk_alt_id:
            raise BadRequest('Resource request without Resource id.')

        with self.dao.session() as session:

            # Return the meta data for the requested Resource Data record.
            for _id in [resource.pk_id, resource.pk_alt_id]:
                query = session.query(ResourceData.id, ResourceData.created, ResourceData.modified,
                                      ResourceData.resource)
                if isinstance(_id, int):
                    query = query.filter(ResourceData.resourcePKID == _id)
                elif isinstance(_id, str):
                    query = query.filter(ResourceData.resourcePKAltID == _id)
                else:
                    raise ValueError(f'Invalid resource id: {_id}.')
                # sql = self.dao.query_to_text(query)
                if '_count' in resource.uri_args:
                    return {"count": query.count()}
                rec = query.first()
                if rec:
                    return rec.resource

        raise NotFound(f'Resource record not found for resource "{resource.resource_uri}".')

    def _get_batch(self, resource):
        """
        Retreive a batch of resource records.
        :param resource:
        :return:
        """
        if not resource.batch_ids:
            return []

        pk_id_lookup = True
        resource.batch_ids = resource.batch_ids[:1000]  # Restrict maximum number of resource ids in batch to 1,000.
        with self.dao.session() as session:

            # Determine if we are going to use ResourceData.pk_id or Resource.pk_alt_id for lookups.
            # See if we can convert a resource id to an integer.
            if isinstance(resource.batch_ids[0], str):
                pk_id_lookup = False
                _tmp_pk = re.sub('\D', '', resource.batch_ids[0])
                if _tmp_pk:
                    query = session.query(ResourceData.id).filter(ResourceData.resourcePKID == int(_tmp_pk))
                    pk_id_lookup = query.first() is not None
                # If we are using pk_id, convert all resource ids to int.
                if pk_id_lookup:
                    resource.batch_ids = [int(re.sub('\D', '', x)) for x in resource.batch_ids]

            query = session.query(ResourceData.id, ResourceData.created, ResourceData.modified, ResourceData.resource)
            if pk_id_lookup:
                query = query.filter(ResourceData.resourcePKID.in_(resource.batch_ids))
            else:
                query = query.filter(ResourceData.resourcePKAltID.in_(resource.batch_ids))
            # sql = self.dao.query_to_text(query)
            if '_count' in resource.uri_args:
                return {"count": query.count()}
            recs = query.all()
            result = [self.dao.to_dict(rec) for rec in recs]
            return result

    def _get_meta(self, resource):
        """
        Return the Meta data for the Resource Data record.
        :param resource: RequestResource object
        :return: Meta data json.
        """
        with self.dao.session() as session:

            # If no specific resource record requested, return all meta data records.
            if not resource.pk_id and not resource.pk_alt_id:
                query = session.query(ResourceData.id, ResourceData.created, ResourceData.modified,
                            ResourceData.resourceSchemaID.label('schema_id'),
                            ResourceData.resourceTypeID.label('type_id'),
                            _coalesce(ResourceData.resourcePKAltID, ResourceData.resourcePKID).label('resource_pk')).\
                        join(ResourceType, ResourceType.id == ResourceData.resourceTypeID).\
                        filter(ResourceType.resourceURI == resource.resource_uri)
                query = self._add_arg_filters(resource, query)
                query = query.order_by(ResourceData.id)

                # sql = self.dao.query_to_text(query)
                if '_count' in resource.uri_args:
                    return {"count": query.count()}
                results = list()
                records = query.all()
                for record in records:
                    results.append(self.dao.to_dict(record))
                return results

            # Return the meta data for the requested Resource Data record.
            for _id in [resource.pk_id, resource.pk_alt_id]:
                query = session.query(ResourceData.id, ResourceData.created, ResourceData.modified,
                        ResourceData.resourceSchemaID.label('schema_id'),
                        ResourceData.resourceTypeID.label('type_id'),
                        _coalesce(ResourceData.resourcePKAltID, ResourceData.resourcePKID).label('resource_pk'))
                if isinstance(_id, int):
                    query = query.filter(ResourceData.resourcePKID == _id)
                elif isinstance(_id, str):
                    query = query.filter(ResourceData.resourcePKAltID == _id)
                else:
                    raise ValueError(f'Invalid resource id: {_id}.')
                # sql = self.dao.query_to_text(query)
                if '_count' in resource.uri_args:
                    return {"count": query.count()}
                rec = query.first()
                if rec:
                    return self.dao.to_dict(rec)

        raise NotFound(f'Resource meta not found for resource "{resource.resource_uri}".')

    def _get_schema(self, resource):
        """
        Return the requested schema model.
        :param resource: RequestResource object
        :return: schema json
        """
        # Example: dump the schema from the current model schema object.
        # import JSONSchema
        # js = JSONSchema()
        # return js.dump(resource.schema)

        with self.dao.session() as session:

            # If no specific resource record requested, return the most recent version of the schema.
            if not resource.pk_id and not resource.pk_alt_id:
                sub_query = session.query(_max(ResourceSchema.id).label('max_schema_id'))\
                                .join(ResourceType, ResourceSchema.resourceTypeID == ResourceType.id)\
                                .filter(ResourceType.resourceURI == resource.resource_uri).subquery()
                query = session.query(ResourceSchema.id, ResourceSchema.schema)\
                            .filter(ResourceSchema.id == sub_query.c.max_schema_id)
                # sql = self.dao.query_to_text(query)
                if '_count' in resource.uri_args:
                    return {"count": query.count()}
                rec = query.first()
                if rec:
                    _schema = rec.schema
                    _schema['id'] = rec.id
                    return _schema

                raise NotFound(f'Resource schema not found for requested resource "{resource.resource_uri}".')

            # Return the schema for the requested Resource Data record.
            for _id in [resource.pk_id, resource.pk_alt_id]:
                query = session.query(ResourceSchema.id, ResourceSchema.schema)\
                    .join(ResourceData, ResourceSchema.id == ResourceData.resourceSchemaID)
                if isinstance(_id, int):
                    query = query.filter(ResourceData.resourcePKID == _id)
                elif isinstance(_id, str):
                    query = query.filter(ResourceData.resourcePKAltID == _id)
                else:
                    raise ValueError(f'Invalid resource id: {_id}.')
                # sql = self.dao.query_to_text(query)
                if '_count' in resource.uri_args:
                    return {"count": query.count()}
                rec = query.first()
                if rec:
                    _schema = rec.schema
                    _schema['id'] = rec.id
                    return _schema

        raise NotFound(f'Resource schema not found for resource "{resource.resource_uri}".')
