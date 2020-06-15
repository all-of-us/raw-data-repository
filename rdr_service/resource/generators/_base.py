#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import hashlib
import json
import re

from marshmallow_jsonschema import JSONSchema
from sqlalchemy import and_

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.model.code import Code
from rdr_service.model.resource_data import ResourceData
from rdr_service.model.resource_schema import ResourceSchema
from rdr_service.model.resource_type import ResourceType
from rdr_service.model.site import Site


class ResourceRecordSet(object):
    """

    """
    _schema = None
    _data = None
    _resource = None
    _meta = None

    def __init__(self, schema, data=None):
        """
        :param schema: Schema model object for for this object
        :param data: Data record dict
        """
        if not hasattr(schema.Meta, 'schema_meta'):
            raise AttributeError('SchemaMeta object not found in schema.Meta.')
        self._schema = schema()
        self._meta = self._schema.Meta.schema_meta
        self._data = data

        if data:
            self._resource = self._schema.dump(data)

    def _get_or_create_type_record(self, dao, schema_meta):
        """
        Get or create the ResourceType record.
        :param dao: DAO object
        :param schema_meta: Schema model SchemaMeta object
        :return: ResourceType object
        """
        type_uid = schema_meta.uid()

        with dao.session() as session:
            rec = session.query(ResourceType).filter(ResourceType.typeUID == type_uid).first()
            if rec:
                return rec

            rec = ResourceType()
            rec.resourceURI = schema_meta.resource_uri()
            rec.resourcePKField = schema_meta.resource_pk_field()
            rec.typeName = schema_meta.name()
            rec.typeUID = type_uid

            session.add(rec)
            session.commit()

        return rec

    def _get_or_create_schema_record(self, dao, type_rec, schema):
        """
        Get or create the ResourceSchema record.
        :param dao: DAO object
        :param type_rec: ResourceType object
        :param schema: Schema model object.
        :return: ResourceSchema object
        """
        schema_dict = JSONSchema().dump(schema)
        # Hash the schema. sort_keys=True is required so the hash remains the same for the same schema.
        hash_value = hashlib.sha256(json.dumps(schema_dict, sort_keys=True).encode('utf-8')).hexdigest()

        with dao.session() as session:
            rec = session.query(ResourceSchema).filter(and_(
                ResourceSchema.resourceTypeID == type_rec.id, ResourceSchema.schemaHash == hash_value)
            ).first()
            if rec:
                return rec

            rec = ResourceSchema()
            rec.schema = schema_dict
            rec.schemaHash = hash_value
            rec.resourceTypeID = type_rec.id

            session.add(rec)
            session.commit()

        return rec

    def _save(self, resources, schema, schema_meta, parent_resource=None):
        """
        Recursive save resource function
        :param resources: List of Resource data dicts.
        :param schema: Resource schema object
        :param schema_meta: Resource SchemaMeta object
        :param parent_resource: Parent resource data dict
        """
        if parent_resource:
            for k, v in schema.declared_fields.items():  # pylint: disable=unused-variable
                if k not in resources[0] and k in parent_resource:
                    for row in resources:
                        row[k] = parent_resource[k]

        dao = ResourceDataDao()
        # Retrieve the schema type record, create if needed.
        type_rec = self._get_or_create_type_record(dao, schema_meta)
        # Retrieve the schema record, create if needed.
        schema_rec = self._get_or_create_schema_record(dao, type_rec, schema)

        pk_fld = type_rec.resourcePKField

        with dao.session() as session:
            for resource in resources:

                hpo_id = resource['hpo_id'] if 'hpo_id' in resource else None
                # TODO: Populate parent resource values in URI in recursive calls.
                res_uri = type_rec.resourceURI + '/' + resource[type_rec.resourcePKField]

                # Look for existing resource record.
                rec = session.query(ResourceData).filter(ResourceData.uri == res_uri).first()
                if not rec:
                    rec = ResourceData()

                rec.resourceTypeID = type_rec.id
                rec.resourceSchemaID = schema_rec.id
                rec.uri = res_uri
                rec.hpoId = hpo_id
                rec.resourcePKID = resource[pk_fld] if isinstance(resource[pk_fld], int) else None
                rec.resource_PKAltID = str(resource[pk_fld]) if isinstance(resource[pk_fld], str) else None
                rec.resource = resource

                if pk_fld in ['participant_id']:
                    rec.resourcePKID = int(re.sub('[^0-9]', '', rec.resource_PKAltID))

                # TODO: Populate rec.parent_id and rec.parent_type_id in recursive calls.

                session.add(rec)
                session.commit()

                # TODO: Fix recursive calls in the future.
                # # Recursively save nested resources.
                # if schema_meta.nested_schemas:
                #     for nested_id, nested_schema in schema_meta.nested_schemas:
                #         nested_schema = nested_schema()
                #         if nested_id in resource:
                #             self._save(resource[nested_id], nested_schema, nested_schema.Meta.schema_meta,
                #                        parent_resource if parent_resource else resource)

        return rec

    def save(self):
        """
        Save resource to database.
        """
        return self._save([self._resource], self._schema, self._meta)

    def get_schema(self):
        """ Return data schema """
        return self._schema

    def get_resource(self):
        """ Return resource """
        return self._resource

    def get_data(self):
        """ Return data dict """
        return self._data


class BaseGenerator(object):
    """
    Base class for generating Resource data JSON.
    """

    def _merge_schema_dicts(self, dict1, dict2):
        """
        Safely merge dict2 schema into dict1 schema
        :param dict1: dict object
        :param dict2: dict object
        :return: dict
        """
        lists = {key: val for key, val in dict1.items()}
        dict1.update(dict2)
        for key, val in lists.items():  # pylint: disable=unused-variable
            if key in dict2:
                # This assumes all sub-tables are set to repeated (multi-row) type.
                dict1[key] = lists[key] + dict2[key]

        return dict1

    def _lookup_code_value(self, code_id, session):
        """
        Return the code id string value from the code table.
        :param code_id: codeId from code table
        :param session: DAO session object
        :return: string
        """
        if code_id is None:
            return None
        result = session.query(Code.value).filter(Code.codeId == int(code_id)).first()
        if not result:
            return None
        return result.value

    def _lookup_code_id(self, code, ro_session):
        """
        Return the code id for the given code value string.
        :param code: code value string
        :param ro_session: ReadOnly DAO session object
        :return: int
        """
        if code is None:
            return None
        result = ro_session.query(Code.codeId).filter(Code.value == code).first()
        if not result:
            return None
        return result.codeId

    def _lookup_site_name(self, site_id, ro_session):
        """
        Look up the site name
        :param site_id: site id integer
        :param ro_session: Readonly DAO session object
        :return: string
        """
        site = ro_session.query(Site.googleGroup).filter(Site.siteId == site_id).first()
        if not site:
            return None
        return site.googleGroup
