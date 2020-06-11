#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import Schema as MarshmallowSchema

from rdr_service.resource import fields


class SchemaMeta(object):
    """
    Resource schema meta information about storing the schema in the database.  Used in Schema class Meta.
    """
    _resource_uri = None
    _resource_pk_field = None
    _type_name = None
    _type_uid = 0

    nested_schemas = []

    def __init__(self, type_uid, type_name, resource_uri, resource_pk_field, nested_schemas=None):
        """
        :param type_uid: Type unique identifier
        :param type_name: Simple type name. IE: "ParticipantSummary", "BiobankSamples", ...
        :param resource_uri: Resource URI. IE: “ParticipantSummary”, “BiobankOrder/Samples”, ...
        :param resource_pk_field: Field name of primary key in resource_data record
        :param nested_schemas: List of nested resources present in the parent resource.
        """
        if not resource_uri or not type_name or not type_uid:
            raise ValueError('Missing schema type information')
        if not isinstance(type_uid, int):
            raise ValueError('Schema type uid must be a unique integer value between 1 and 16384')
        if not isinstance(resource_uri, str):
            raise ValueError('Schema resource URI must be a string')
        if not isinstance(resource_pk_field, str):
            raise ValueError('Schema resource PK field must be a string')
        if not isinstance(type_name, str):
            raise ValueError('Schema type name must be a string')

        self._resource_uri = resource_uri
        self._resource_pk_field = resource_pk_field
        self._type_name = type_name
        self._type_uid = type_uid

        if isinstance(nested_schemas, list):
            self.nested_schemas = nested_schemas

    def resource_uri(self):
        """ Return resource uri """
        if not self._resource_uri:
            raise NotImplementedError('Schema meta information has not been set.')
        return self._resource_uri

    def resource_pk_field(self):
        """ Return resource pk field name """
        if not self._resource_pk_field:
            raise NotImplementedError('Schema meta information has not been set.')
        return self._resource_pk_field

    def name(self):
        """ Return type name """
        if not self._type_name:
            raise NotImplementedError('Schema meta information has not been set.')
        return self._type_name

    def uid(self):
        """ Return type unique identifier """
        if self._type_uid == 0:
            raise NotImplementedError('Schema meta information has not been set.')
        return self._type_uid


class Schema(MarshmallowSchema):
    """ Required fields for all resource schemas """
    id = fields.Int64(required=True)
    created = fields.DateTime(required=True)
    modified = fields.DateTime(required=True)

    @classmethod
    def get_field(cls, name):
        """
        Return the field object for the given field name.
        :param name: string
        :return: field object
        """
        return cls._declared_fields.get(name, None)

    class Meta:
        ordered = True