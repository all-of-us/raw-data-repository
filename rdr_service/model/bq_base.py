#
# BigQuery table schema support
# API Docs: https://cloud.google.com/bigquery/docs/reference/rest/
#
import collections
import datetime
import importlib
import inspect
import itertools
import json
import operator
import re
from enum import Enum, EnumMeta
from dateutil import parser

from rdr_service.dao.base_dao import json_serial


# BigQuery exceptions
class BQException(Exception):
    """ Base class for BQ exceptions """
    pass


class BQInvalidModeException(BQException):
    """
    BigQuery error in update operation: Provided Schema does not match Table
      - Cannot add required columns to an existing schema
    """
    pass


class BQModeChangedException(BQException):
    """
    BigQuery error in update operation: Provided Schema does not match Table
      - Field [FIELD] has changed mode from REPEATED to NULLABLE
    """
    pass


class BQSchemaStructureException(BQException):
    """
    BigQuery error in update operation
      - Precondition Failed
    Note: This occurs if new fields are not added to the end of the schema.
    """
    pass


class BQInvalidSchemaException(BQException):
    """
    Error while reading data, error message
      - parsing error in row starting at position [INT] - No such field: [FIELD]
    """
    pass


class BQDuplicateFieldException(BQException):
    """
    BigQuery error in update operation
      - Field new_field already exists in schema
    """
    pass


class BQFieldTypeEnum(Enum):
    """
    BigQuery column types
    """
    STRING = 1
    BYTES = 2
    INTEGER = 3
    FLOAT = 4
    # BOOLEAN = 5  # Use INTEGER instead.
    TIMESTAMP = 6
    DATE = 7
    TIME = 8
    DATETIME = 9
    ARRAY = 10
    RECORD = 11


class BQFieldModeEnum(Enum):
    """
    BigQuery mode types
    """
    NULLABLE = 1
    REQUIRED = 2
    REPEATED = 3


class BQSchema(object):
    """
    A BigQuery dataset object schema.
    Note: Field properties of python object derived from this should the underscore naming
          convention. Property names must exactly match BigQuery field names.
    """

    def __init__(self, *args):

        if args is not None and len(args) is not 0 and args[0] is not None:
            if isinstance(args[0], str):
                self._add_fields(self, json.loads(args[0]))
            else:
                self._add_fields(self, args[0])

    def _cmp_schema(self, o1, o2):
        """
        Recursively compare schemas. This is normally used to compare a local schema with
        a remote BQ schema.  For this to succeed, schema property names for field object
        must exactly match BQ field names.
        # TODO: Future: exclude 'enum' and 'description' dict values.
        # We really want to only compare the core field info.
        :param o1: BQSchema object
        :param o2: BQSchema object
        :return:
        """
        if len(dir(o1)) != len(dir(o2)):
            return False
        pairs = zip(o1.get_fields(), o2.get_fields())
        for l1, l2 in pairs:
            if isinstance(l1, BQRecordField):
                if not self._cmp_schema(l1.get_schema(), l2.get_schema()):
                    return False
            else:
                if l1.to_dict() != l2.to_dict():
                    return False
        return True

    def __eq__(self, other):
        return self._cmp_schema(self, other)

    def __ne__(self, other):
        return not self._cmp_schema(self, other)

    def __getitem__(self, item):
        return getattr(self, item, None)

    def _add_fields(self, obj, fields):
        """
        Recursively add fields to object
        :param obj: object to add fields to.
        :param fields: list of field dicts
        :return: object
        """
        for field in fields:
            fld_name = field['name']
            fld_type = BQFieldTypeEnum[field['type']]
            fld_mode = BQFieldModeEnum[field['mode']]
            fld_descr = field['description'] if 'description' in field else None
            fld_enum = None

            if fld_descr and fld_type != BQFieldTypeEnum.RECORD:
                try:
                    mod_path, mod_name = fld_descr.rsplit('.', 1)
                    fld_enum = getattr(importlib.import_module(mod_path, mod_name), mod_name)
                    fld_descr = None
                except AttributeError:
                    pass
                except ValueError:
                    pass

            if fld_type == BQFieldTypeEnum.RECORD:
                # Note: instantiating the subclass in descr here causes local and remote
                #       comparisons to fail.  This is because the instantiated subclass
                #       will always have any new fields as properties, regardless to
                #       what fields the remote BQ server has.
                schema = self._add_fields(BQSchema(), field['fields'])
                rec_obj = BQRecordField(fld_name, schema, fld_descr=fld_descr)
                obj.__dict__[fld_name] = rec_obj
            else:
                obj.__dict__[fld_name] = BQField(fld_name, fld_type, fld_mode, fld_enum, fld_descr)

        return obj

    def get_fields(self):
        """
        Return a list of schema fields
        :return: list
        """
        fields = list()
        for key in dir(self):
            field = getattr(self, key)
            if isinstance(field, BQField):
                fields.append(field)

        # sort the BQField objects in the original order they are defined.
        fields.sort(key=operator.attrgetter('_count'))
        return fields

    @classmethod
    def get_sql_field_names(cls, exclude_fields=None):
        """
        Return a list of schema field names ready for a SQL statement.
        :param exclude_fields: list of fields to exclude.
        :return: formatted string
        """
        if exclude_fields is None:
            exclude_fields = []
        fields = list()
        for key in dir(cls):
            field = getattr(cls, key)
            if isinstance(field, BQField):
                name = field['name']
                if name in exclude_fields:
                    continue
                fields.append(field)

        # sort the BQField objects in the original order they are defined.
        fields.sort(key=operator.attrgetter('_count'))

        return ', '.join([fld['name'] for fld in fields])

    def to_json(self):
        """
        Return the json representation of the schema.
        :return: json string
        """
        return json.dumps(self.get_fields(), indent=2, default=BQField.serialize)

    # def __repr__(self):
    #   self.to_json()

    @staticmethod
    def make_bq_field_name(name, alt_name=None):
        """
        Validate/convert the provided name into a field name that meets BigQuery table field naming requirements.
        The field name must start with letter or underscore, must not exceed 128 chars, and must only contain
        alphanumeric characters and underscores.  Invalid chars (whitespace, /, etc.) will be converted to an
        underscore
        :param name: string to validate/convert  (generally corresponds to a code.value column value)
        :param alt_name:  alternate string to validate/convert (generally corresponds to code.short_value)
        :return: field name string (or None if one cannot be successfully constructed), message string
        """

        bq_field_name = None
        # Only fall back to using the alternate name parameter if the primary name parameter is too long
        for field_name in [name, alt_name]:
            if isinstance(field_name, str) and len(field_name) <= 128:
                bq_field_name = field_name
                break

        if not bq_field_name:
            return None, f'{name} (alt: {alt_name}) is not a valid string less than 128 characters'

        # Replace BigQuery illegal field name characters with an underscore
        bq_field_name = re.sub('\W+', '_', bq_field_name)

        # Make sure the field name starts with a character or underscore.  Prefix w/ underscore otherwise
        # (provided we're still within the length limit)
        if re.match("\A[^a-zA-Z_]", bq_field_name):
            bq_field_name = '_' + bq_field_name
            if len(bq_field_name) > 128:
                return None, f'Resulting field name {bq_field_name} exceeds 128 characters'

        return bq_field_name, ''


class BQField(object):
    """
    A BigQuery table schema field
    """
    _fld_name = None
    _fld_type = None
    _fld_mode = None
    _fld_enum = None
    _fld_descr = None
    _counter = itertools.count()

    def __init__(self, fld_name, fld_type, fld_mode=BQFieldModeEnum.REQUIRED, fld_enum=None, fld_descr=None):
        # https://stackoverflow.com/questions/350799/how-does-django-know-the-order-to-render-form-fields
        self._count = next(BQField._counter)

        if not isinstance(fld_name, str):
            raise TypeError('field name must be a string')
        if not isinstance(fld_type, Enum):
            raise TypeError('field type must be a BQFieldTypeEnum value')
        if not isinstance(fld_mode, Enum):
            raise TypeError('field mode must be a BQFieldModeEnum value')
        if fld_enum:
            if not isinstance(fld_enum, EnumMeta):
                raise TypeError('field enum must be an instance of Enum')
            pass
        self._fld_name = fld_name
        self._fld_type = fld_type
        self._fld_mode = fld_mode
        self._fld_enum = fld_enum
        self._fld_descr = fld_descr

    def __getitem__(self, item):
        return self.to_dict().get(item, None)

    def to_dict(self):
        """
        Return a dictionary of the field information.
        :return: dict
        """
        data = collections.OrderedDict()
        data['name'] = self._fld_name
        data['type'] = getattr(self._fld_type, 'name')
        data['mode'] = getattr(self._fld_mode, 'name')
        if self._fld_enum:
            data['enum'] = True
            data['description'] = '{0}.{1}'.format(self._fld_enum.__module__, self._fld_enum.__name__)
        elif self._fld_descr:
            data['enum'] = False
            data['description'] = self._fld_descr
        return data

    @staticmethod
    def serialize(o):
        """
        serialize object for converting to json
        :param o: object
        :return: dict
        """
        return o.to_dict()

    def __repr__(self):
        fields = self.to_dict()
        return json.dumps(fields, default=BQField.serialize)


class BQRecordField(BQField):
    """
    A BigQuery Record field which holds a set of BQFields.
    """

    class __schema__(BQSchema):
        pass

    def __init__(self, fld_name, schema, fld_descr=None):
        """
        :param fld_name: field name
        :param schema: BQSchema object
        """
        super(BQRecordField, self). \
            __init__(fld_name, BQFieldTypeEnum.RECORD, BQFieldModeEnum.REPEATED, fld_descr=fld_descr)
        self.__schema__ = schema

    def get_schema(self):
        """
        If self.__schema__ is a class then try to instantiate it, otherwise return the schema object.
        """
        try:
            return self.__schema__()
        except TypeError:
            pass
        return self.__schema__

    def del_prop(self, name):

        if hasattr(self, name):
            setattr(self, name, None)

    def to_dict(self):
        """
        Return a dictionary of the field information.
        :return: dict
        """
        # get our field information
        data = super(BQRecordField, self).to_dict()
        schema = self.get_schema()
        data['description'] = '{0}.{1}'.format(schema.__module__, schema.__class__.__name__)
        # get list of BQField objects in correct order.
        fields = self.get_schema().get_fields()
        if len(fields) > 0:
            data['fields'] = fields

        return data

    def get_fields(self):
        """
        Return a list of field object for this record object
        :return: list
        """
        return self.get_schema().get_fields()


class BQTable(object):
    """
    https://cloud.google.com/bigquery/docs/managing-table-schemas
    Rules for changing the table structure:
      1) New fields added to an existing table must be set to "NULLABLE".
      2) New fields may only be added to the bottom of the field list.
      3) Existing fields can not be removed.
    """
    __tablename__ = None

    class __schema__(BQSchema):
        pass

    __dataset__ = 'rdr_ops_data_view'
    # GCP project and dataset mapping. Allows redirecting table data into other GCP projects and/or datasets.
    # The value for this would be a list of tuples with source project id and one or more
    # destination project id and dataset names.
    # Format: [ ( src project_id, (dest project_id, dataset), (dest project_id, dataset), ...)), ]
    # IE: [
    #       ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view'), ('all-of-us-prod', 'rdr_other_ds')),
    #     ]
    # To prevent data from going to a project, set the destination dataset id to None. This will disable
    # creating a table/view in the destination project.
    # IE: [
    #       ('all-of-us-rdr-prod', ('aou-pdr-data-prod', None)),
    #     ]
    __project_map__ = [
        ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
        ('all-of-us-rdr-stable', ('aou-pdr-data-stable', 'rdr_ops_data_view')),
        ('pmi-drc-api-test', ('aou-pdr-data-test', 'rdr_ops_data_view')),
    ]

    def get_name(self):
        return self.__tablename__

    def get_schema(self):
        return self.__schema__()

    @classmethod
    def get_project_map(cls, project_id):
        """
        Return a list of mapped project ids, datasets and table names.
        :param project_id: source project id
        :return: list of tuples containing (project id, dataset name, table name)
        """
        if not project_id:
            raise ValueError('Invalid project id.')
        results = list()

        if isinstance(cls.__project_map__, list):
            for project in cls.__project_map__:
                if project_id == project[0]:
                    for x in range(1, len(project)):
                        results.append((project[x][0], project[x][1], cls.__tablename__))

        # if there was no mapping found, just return the project_id with default values.
        if len(results) == 0:
            results.append((project_id, cls.__dataset__, cls.__tablename__))

        return results


class BQView(object):
    __viewname__ = None  # type: str
    __viewdescr__ = None  # type: str
    __table__ = None  # type: BQTable
    __pk_id__ = 'id'  # type: (str, list)
    __sql__ = None  # type: str

    def __init__(self):

        if not self.__sql__ and self.__table__:
            tbl = self.__table__()
            fields = tbl.get_schema().get_fields()
            pk = ', '.join(self.__pk_id__) if isinstance(self.__pk_id__, list) else str(self.__pk_id__)

            self.__sql__ = """
                SELECT {fields}
              """.format(fields=', '.join([f['name'] for f in fields]))

            self.__sql__ += """
                FROM (
                  SELECT *,
                      ROW_NUMBER() OVER (PARTITION BY %%pk_id%% ORDER BY modified desc) AS rn
                    FROM `{project}`.{dataset}.%%table%%
                ) t
                WHERE t.rn = 1
              """.replace('%%table%%', tbl.get_name()).replace('%%pk_id%%', pk)

    def get_table(self):
        return self.__table__

    def get_name(self):
        return self.__viewname__

    def get_descr(self):
        return self.__viewdescr__

    def get_sql(self):
        return self.__sql__


# class BQSession(object):
#
#   def __init__(self, project_id, dataset_id):
#     self._project_id = project_id
#     pass
#
#   def execute(self, query, page_size=1000):
#     """
#
#     :param query: bigquery sql string
#     :param project_id: gcp project id
#     :param dataset_id: bigquery dataset id
#     :param page_size: maximum number of records per page.
#     :return: BQRecordSet object
#     """
#     pass


class BQRecordSet(object):
    """
    Represents a BigQuery data set.
    """

    _bq_job = None

    def __int__(self, bigquery_job):
        """
        :param bigquery_job: A BigQueryJob object.
        """
        self._bq_job = bigquery_job
        pass


class BQRecord(object):
    """
    Represents a single BigQuery data record.
    """
    __schema__ = None
    __fields__ = None
    _convert_to_enum = True

    def __init__(self, schema=None, data=None, convert_to_enum=True):
        """
        :param schema: BQSchema type, BQSchema instance or schema json string.
        :param data: initial dict of data for object
        :param convert_to_enums: If schema field description includes Enum class info, convert value to Enum.
        """
        self._convert_to_enum = convert_to_enum

        if schema:
            # if schema is a json string, convert it to a BQSchema object.
            if isinstance(schema, str):
                self.__schema__ = BQSchema(schema)
            else:
                # See if schema is a Class or an object Instance.
                try:
                    self.__fields__ = schema.get_fields()
                    self.__schema__ = schema
                except TypeError:
                    self.__schema__ = schema()
            self.__fields__ = self.__schema__.get_fields()

        if data:
            self.update_values(data)

    def __getitem__(self, item):
        """
        Lookup item data value
        :param item: string
        :return: value
        """
        return getattr(self, item)

    def update_values(self, data):
        """
        Update this object with the given dict values, validate against BQSchema if available.
        :param data: dict of data values to add/update.
        """

        def update(dest, src, schema):
            """
            recursive function to add values from one dict to another and validate keys against schema
            :param dest: destination dict object
            :param src: source dict object
            :param schema: schema object
            :return: dict
            """
            for key, val in src.items():
                # validate key against schema if needed
                if schema and not getattr(schema, key, None):
                    # raise KeyError('{0} key not in schema'.format(key))
                    continue  # just ignore keys not in schema.
                # TODO: Future: Validate value against schema BQField type and constraints here.
                if schema and isinstance(schema, BQSchema):
                    try:
                        fld_type = getattr(schema, key)._fld_type
                        if val and fld_type == BQFieldTypeEnum.DATETIME and isinstance(val, str):
                            val = parser.parse(val)
                        elif val and fld_type == BQFieldTypeEnum.DATE and isinstance(val, str):
                            val = parser.parse(val).date()
                    except AttributeError:
                        pass
                # check for Enum32 object, if it is set the value to the enum value
                if self._convert_to_enum and schema and schema[key]['description'] and schema[key]['enum'] is True:
                    try:
                        mod_path, mod_name = schema[key]['description'].rsplit('.', 1)
                        fld_enum = getattr(importlib.import_module(mod_path, mod_name), mod_name)
                        if isinstance(val, int):
                            dest[key] = fld_enum(val)
                        else:
                            dest[key] = fld_enum[val]
                    except AttributeError:
                        dest[key] = val
                    except ValueError:
                        dest[key] = val
                elif isinstance(val, collections.abc.Mapping):
                    dest[key] = update(dest.get(key, {}), val, schema.__dict__[key] if schema else None)
                elif isinstance(val, list):
                    # TODO: Future: Do we want to instantiate a new BQRecord for nested data here, instead of
                    # TODO:         just adding a list of dicts to 'dest'?  We can get the nested BQSchema by
                    # TODO:         testing: if schema[key]['description'] and schema[key]['enum'] is False.
                    dest[key] = list()
                    for d2 in val:
                        dest[key].append(update(dict(), d2, getattr(schema, key).get_schema() if schema else None))
                else:
                    dest[key] = val
            return dest

        update(self.__dict__, data, self.__schema__)

    def get_fields(self):
        return self.__fields__

    def get_schema(self):
        return self.__schema__

    def _serialize_dict(self, data):
        """
        Recursively loop through dict and encode dates to string
        :param data: dict object
        :return: dict object
        """
        for key, value in data.items():
            if isinstance(value, list):
                for x in range(len(value)):
                    value[x] = self._serialize_dict(value[x])
            if isinstance(value, dict):
                data[key] = self._serialize_dict(value)
            if isinstance(value, (datetime.datetime, datetime.date)):
                data[key] = value.isoformat()

        return data

    def to_dict(self, serialize=False, full_schema=False):  # pylint: disable=unused-argument
        """
        convert properties to a dict
        :param serialize: If True, convert dates to string.
        :param full_schema: If True, add missing schema properties.
        """
        data = collections.OrderedDict()
        for key in dir(self):
            if key.startswith('_'):
                continue
            value = getattr(self, key)
            if inspect.ismethod(value):
                continue
            data[key] = value

        if serialize:
            data = self._serialize_dict(data)
        # TODO: future (maybe), add in missing data keys found in schema and exclude non-schema properties.
        return data

    def to_json(self, full_schema=False):
        return json.dumps(self.to_dict(full_schema), default=json_serial)

    def get(self, attr: str, default: any = None):
        result = getattr(self, attr, default)
        return result if result else default
