#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import json

from marshmallow import Schema as MarshmallowSchema
from marshmallow_jsonschema import JSONSchema

from rdr_service.resource import fields
from rdr_service.resource.constants import SchemaID


class SchemaMeta(object):
    """
    Resource schema meta information about storing the schema in the database.  Used in Schema class.
    """
    _resource_uri = None
    _resource_pk_field = None
    _schema_id = None
    _nested_fields = []

    def __init__(self, **kwargs):
        """
        :param schema_id: SchemaID Enum object.
        :param resource_uri: Resource URI. IE: "ParticipantSummary", "BiobankOrder/Samples", ...
        :param resource_pk_field: Field name of primary key in resource_data record
        :param nested_fields: List of nested fields present in the resource schema.
        """
        if 'resource_uri' not in kwargs or 'schema_id' not in kwargs:
            raise ValueError('Missing schema type information')
        if not isinstance(kwargs['resource_uri'], str):
            raise ValueError('Schema resource URI must be a string')
        if not isinstance(kwargs['schema_id'], SchemaID):
            raise ValueError(f'Schema ID has not been set on Schema object {kwargs["resource_uri"]}.')
        # if not isinstance(kwargs['resource_pk_field'], str):
        #     raise ValueError('Schema resource PK field must be a string')

        self._resource_uri = kwargs['resource_uri']
        self._resource_pk_field = kwargs['resource_pk_field'] if kwargs['resource_pk_field'] else 'auto'
        self._schema_id = kwargs['schema_id']

        if hasattr(kwargs, 'nested_fields') and isinstance(kwargs['nested_fields'], list):
            self._nested_fields = kwargs['nested_fields']

    def resource_uri(self):
        """ Return resource uri """
        return self._resource_uri

    def resource_pk_field(self):
        """ Return resource pk field name """
        return self._resource_pk_field

    def schema_id(self):
        """ Return SchemaID Enum value """
        return self._schema_id

    def nested_fields(self):
        """ Return nested fields list """
        return self._nested_fields

    def name(self):
        """ Return type name """
        return self._schema_id.name

    def uid(self):
        """ Return type unique identifier """
        return self._schema_id.value


class Schema(MarshmallowSchema):
    """ Base resource schema class """

    def __init__(self, *args, **kwargs):
        super(Schema, self).__init__(*args, **kwargs)
        # Turn our simple Meta class into a SchemaMeta object.
        props = ['schema_id', 'resource_uri', 'resource_pk_field']
        kwargs = dict(zip(props, [getattr(self.Meta, k, None) for k in props]))
        # Find all fields defined as Nested.
        kwargs['nested_fields'] = self.nested_fields()
        self.Meta = SchemaMeta(**kwargs)

    @classmethod
    def get_field(cls, name):
        """
        Return the field object for the given field name.
        :param name: string
        :return: field object
        """
        return cls._declared_fields.get(name, None)

    @classmethod
    def nested_fields(cls):
        """
        Return a list of nested schema fields in this schema.
        :return list
        """
        results = list()
        for name, field in cls._declared_fields.items():
            if isinstance(field, fields.Nested):
                results.append((name, field))
        return results

    def to_json(self, pretty=False):
        """
        Return our schema in JSON format.
        :param pretty: Format JSON with indentation and linefeeds.
        :return: str
        """
        return json.dumps(JSONSchema().dump(self), indent=(2 if pretty else None))

    def to_dict(self):
        """
        Return our schema as a dict.
        :return: dict
        """
        return JSONSchema().dump(self)

    def __repr__(self):
        return self.Meta.name()


    class Meta:
        """
        schema_meta info declares how the schema and data is stored and organized in the Resource database tables.
        """
        # SchemaID Enum object
        schema_id = None
        # A URI string, may reference parent resource primary key id fields.
        # Example: '/Participant/{participant_id}/BiobankOrder/{order_id}/
        resource_uri = None
        # The name of the resource schema's primary key field.
        resource_pk_field = None
