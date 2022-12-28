import collections
import datetime
import json
import logging
import random
import re

from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections import OrderedDict
from contextlib import closing

import sqlparse

from rdr_service.lib_fhir.fhirclient_1_0_6.models.domainresource import DomainResource
from rdr_service.lib_fhir.fhirclient_1_0_6.models.fhirabstractbase import FHIRValidationError
from protorpc import messages
from sqlalchemy import and_, inspect, or_
from sqlalchemy.dialects import mysql
from sqlalchemy.engine.result import ResultProxy
from sqlalchemy.exc import IntegrityError, OperationalError
from werkzeug.exceptions import BadRequest, NotFound, PreconditionFailed, ServiceUnavailable

from rdr_service import api_util
from rdr_service.code_constants import ORIGINATING_SOURCES, UNSET
from rdr_service.dao import database_factory
from rdr_service.model.participant import Participant
from rdr_service.model.requests_log import RequestsLog
from rdr_service.model.utils import get_property_type
from rdr_service.query import FieldFilter, Operator, PropertyType, Results
# Maximum number of times we will attempt to insert an entity with a random ID before
# giving up.

MAX_INSERT_ATTEMPTS = 50

# Range of possible values for random IDs.
_MIN_ID = 100000000
_MAX_ID = 999999999

_MIN_RESEARCH_ID = 1000000
_MAX_RESEARCH_ID = 9999999

_COMPARABLE_PROPERTY_TYPES = [PropertyType.DATE, PropertyType.DATETIME, PropertyType.INTEGER]

_OPERATOR_PREFIX_MAP = {
    "lt": Operator.LESS_THAN,
    "le": Operator.LESS_THAN_OR_EQUALS,
    "gt": Operator.GREATER_THAN,
    "ge": Operator.GREATER_THAN_OR_EQUALS,
    "ne": Operator.NOT_EQUALS,
}


class BaseDao(object):
    """A data access object base class; defines common methods for inserting and retrieving
  objects using SQLAlchemy.

  Extend directly from BaseDao if entities cannot be updated after being
  inserted; extend from UpdatableDao if they can be updated.

  order_by_ending is a list of field names to always order by (in ascending order, possibly after
  another sort field) when query() is invoked. It should always end in the primary key.
  If not specified, query() is not supported.
  """

    def __init__(self, model_type, backup=False, alembic=False, read_uncommitted=False, order_by_ending=None, db=None):
        self.model_type = model_type
        if not db:
            if alembic:
                db = database_factory.get_database_with_alembic_user()
            elif backup:
                db = database_factory.get_backup_database()
            elif read_uncommitted:
                db = database_factory.get_read_uncommitted_database()
            else:
                db = database_factory.get_database()
        self._database = db
        self.order_by_ending = order_by_ending

    def session(self):
        return self._database.session()

    def raw_connection(self):
        """
    Return a raw connection object. Used for calling stored procedures.
    https://docs.sqlalchemy.org/en/13/core/connections.html#calling-stored-procedures
    :return: raw connection object from engine.
    """
        return self._database.raw_connection()

    def call_proc(self, proc, args=None, filters=None, skip_null=False):
        """
    Call a Stored Procedure with parameters. Always returns last set if query
    returns multiple data sets.
    :param proc: stored procedure name
    :param args: list of argument values
    :param filters: string of columns to return
    :param skip_null: do not return columns with null values
    :return: list of dicts objects
    """
        if not proc or not isinstance(proc, str):
            raise ValueError("stored procedure name is invalid")

        if args is None:
            args = list()
        if not isinstance(args, list):
            raise ValueError("args value is invalid")

        sets = list()
        data = list()
        conn = self.raw_connection()

        try:
            with closing(conn.cursor()) as cursor:
                cursor.callproc(proc, args)
                # capture the result set fields and data. cursor.fetchall() needs to be first.
                sets.append({"data": list(cursor.fetchall()), "fields": list(cursor.description)})

                # if multiple sets are returned, capture them as well.
                while cursor.nextset():
                    try:
                        sets.append({"data": list(cursor.fetchall()), "fields": list(cursor.description)})
                    except TypeError:
                        pass
        except:
            raise
        finally:
            conn.close()

        # reverse the set list, return the first set.
        sets.reverse()
        for item in sets:

            if not item["fields"] or len(item["fields"]) == 0:
                continue

            fields = item["fields"]
            results = item["data"]

            for row in results:
                od = collections.OrderedDict()

                for x in range(0, len(fields)):
                    field = fields[x][0]
                    value = row[x]

                    if skip_null is True and value is None:
                        continue
                    if filters and field.lower() not in filters.lower():
                        continue

                    if isinstance(value, (datetime.datetime, datetime.date)):
                        od[field] = value.isoformat()
                    elif field == "participant_id":
                        od[field] = "P{0}".format(value)
                    else:
                        od[field] = value

                data.append(od)

            # skip other sets.
            break

        return data

    def _validate_model(self, session, obj):
        """Override to validate a model before any db write (insert or update)."""
        pass

    def _validate_insert(self, session, obj):
        """Override to validate a new model before inserting it (not applied to updates)."""
        self._validate_model(session, obj)

    def validate_origin(self, obj):
        from rdr_service import app_util
        pid = obj.participantId
        email = app_util.get_oauth_id()
        user_info = app_util.lookup_user_info(email)
        base_name = user_info.get('clientId')
        if email == api_util.DEV_MAIL and base_name is None:
            base_name = 'example'  # account for temp configs that dont create the key
        with self.session() as session:
            result = session.query(Participant.participantOrigin).filter(
                Participant.participantId == pid).first()
            if result:
                result = result[0]
        if base_name in ORIGINATING_SOURCES and base_name != result:
            raise BadRequest(f"{base_name} can not submit questionnaire response for participant with an origin from "
                             f"{result}")

    def insert_with_session(self, session, obj):
        """Adds the object into the session to be inserted."""
        self._validate_insert(session, obj)
        session.add(obj)
        return obj

    def insert(self, obj):
        """Inserts an object into the database. The calling object may be mutated
    in the process."""
        with self.session() as session:
            return self.insert_with_session(session, obj)

    def get_id(self, obj):
        """Returns the ID (for single primary key column tables) or a list of IDs (for multiple
    primary key column tables). Must be overridden by subclasses."""
        raise NotImplementedError

    def get_with_session(self, session, obj_id, for_update=False, options=None):
        """Gets an object by ID for this type using the specified session. Returns None if not found."""
        query = session.query(self.model_type)
        if for_update:
            query = query.with_for_update()
        if options:
            query = query.options(options)
        return query.get(obj_id)

    def get(self, obj_id):
        """Gets an object with the specified ID for this type from the database.

    Returns None if not found.
    """
        with self.session() as session:
            return self.get_with_session(session, obj_id)

    def get_with_children(self, obj_id):
        """Subclasses may override this to eagerly loads any child objects (using subqueryload)."""
        return self.get(obj_id)

    def get_all(self):
        """Fetches all entities from the database. For use on small tables."""
        with self.session() as session:
            return session.query(self.model_type).all()

    def get_or_create(self, insert_if_created=False, default_values=None, **properties):
        """
    Creates a new session and executes get or create queries.

    See get_or_create_with_session() docstring.
    """
        with self.session() as session:
            return self.get_or_create_with_session(
                session, insert_if_created=insert_if_created, default_values=default_values, **properties
            )

    def get_or_create_with_session(self, session, insert_if_created=False, default_values=None, **properties):
        """
    Given a set of properties, get an item that matches or create a new one. Optionally inserting
    the new item.

    :param session: the session to execute the queries
    :param insert_if_created: If creating a new instance, insert or just return?
    :type insert_if_created: bool
    :param default_values: If creating a new instance, also fill in these values
    :type default_values: dict
    :param properties: The values to search for
    :return: The instance, was it created, was it inserted
    :rtype: tuple[object, bool, bool]
    """
        filter_args = [getattr(self.model_type, key) == value for key, value in list(properties.items())]
        existing_results = session.query(self.model_type).filter(*filter_args).limit(2).all()
        if len(existing_results) > 1:
            raise ValueError("More than one row matched the given parameters for get_or_create")
        elif len(existing_results) == 1:
            instance = existing_results[0]
            created = False
        else:
            instance_kwargs = dict(properties, **(default_values or {}))
            instance = self.model_type(**instance_kwargs)
            created = True
        if created and insert_if_created:
            self.insert_with_session(session, instance)
            inserted = True
        else:
            inserted = False
        return instance, created, inserted

    def make_query_filter(self, field_name, value):
        """Attempts to make a query filter for the model property with the specified name, matching
    the specified value. If no such property exists, None is returned.
    """
        alias_map = self.get_aliased_field_map()
        if field_name in alias_map.keys():
            field_name = alias_map[field_name]

        prop = getattr(self.model_type, field_name, None)
        if prop:
            property_type = get_property_type(prop)
            filter_value = None
            operator = Operator.EQUALS
            if property_type == PropertyType.ENUM and value == UNSET:
                operator = Operator.EQUALS_OR_NONE
            # If we're dealing with a comparable property type, look for a prefix that indicates an
            # operator other than EQUALS and strip it off
            if property_type in _COMPARABLE_PROPERTY_TYPES:
                for prefix, op in list(_OPERATOR_PREFIX_MAP.items()):
                    if isinstance(value, str) and value.startswith(prefix):
                        operator = op
                        value = value[len(prefix) :]
                        break
            filter_value = self._parse_value(prop, property_type, value)
            return FieldFilter(field_name, operator, filter_value)
        else:
            return None

    @staticmethod
    def _parse_value(prop, property_type, value):
        if value is None:
            return None
        try:
            if property_type == PropertyType.DATE:
                return api_util.parse_date(value).date()
            elif property_type == PropertyType.DATETIME:
                return api_util.parse_date(value)
            elif property_type == PropertyType.ENUM:
                enum_cls = prop.property.columns[0].type.enum_type
                try:
                    return enum_cls(value)
                except (KeyError, TypeError):
                    raise BadRequest(f'Invalid {prop} parameter: "{value}". '
                                     f'must be one of {list(enum_cls.to_dict().keys())}')
            elif property_type == PropertyType.INTEGER:
                return int(value)
            else:
                return value
        except ValueError:
            raise BadRequest(f"Invalid value for property of type {property_type}: {value}.")

    def _from_json_value(self, prop, value):
        property_type = get_property_type(prop)
        result = self._parse_value(prop, property_type, value)
        return result

    def query(self, query_definition):
        if query_definition.invalid_filters and not query_definition.field_filters:
            raise BadRequest("No valid fields were provided")

        if not self.order_by_ending:
            raise BadRequest(f"Can't query on type {self.model_type} -- no order by ending specified")

        with self.session() as session:
            total = None

            query, field_names = self._make_query(session, query_definition)
            items = query.with_session(session).all()

            if query_definition.include_total:
                total = self._count_query(session, query_definition)

            if not items:
                return Results([], total=total)

        if len(items) > query_definition.max_results:
            # Items, pagination token, and more are available
            page = items[0 : query_definition.max_results]
            token = self._make_pagination_token(items[query_definition.max_results - 1].asdict(), field_names)
            return Results(page, token, more_available=True, total=total)
        else:
            token = (
                self._make_pagination_token(items[-1].asdict(),
                                            field_names) if query_definition.always_return_token else None
            )
            return Results(items, token, more_available=False, total=total)

    @staticmethod
    def _make_pagination_token(item_dict, field_names):
        vals = [item_dict.get(field_name) for field_name in field_names]
        vals_json = json.dumps(vals, default=json_serial)
        return urlsafe_b64encode(str.encode(vals_json))

    def _initialize_query(self, session, query_def):
        """Creates the initial query, before the filters, order by, and limit portions are added
    from the query definition. Clients can subclass to manipulate the initial query criteria
    or validate the query definition."""
        # pylint: disable=unused-argument
        return session.query(self.model_type)

    def _count_query(self, session, query_def):
        query = self._initialize_query(session, query_def)
        query = self._set_filters(query, query_def.field_filters)
        return query.count()

    def _make_query(self, session, query_definition):
        query = self._initialize_query(session, query_definition)
        query = self._set_filters(query, query_definition.field_filters)
        order_by_field_names = []
        order_by_fields = []
        first_descending = False
        if query_definition.order_by:
            query = self._add_order_by(query, query_definition.order_by, order_by_field_names, order_by_fields)
            first_descending = not query_definition.order_by.ascending
        query = self._add_order_by_ending(query, order_by_field_names, order_by_fields)
        if query_definition.pagination_token:
            # Add a query filter based on the pagination token.
            query = self._add_pagination_filter(query, query_definition, order_by_fields, first_descending)
        # Return one more than max_results, so that we know if there are more results.
        query = query.limit(query_definition.max_results + 1)
        if query_definition.offset:
            query = query.offset(query_definition.offset)
        if query_definition.options:
            query = query.options(query_definition.options)
        return query, order_by_field_names

    def _set_filters(self, query, filters):
        for field_filter in filters:
            try:
                f = getattr(self.model_type, field_filter.field_name)
            except AttributeError:
                raise BadRequest(f"No field named {field_filter.field_name} found on {self.model_type}.")
            query = self._add_filter(query, field_filter, f)
        return query

    @staticmethod
    def _add_filter(query, field_filter, f):
        try:
            return field_filter.add_to_sqlalchemy_query(query, f)
        except ValueError as e:
            raise BadRequest(str(e))

    def _add_pagination_filter(self, query, query_def, fields, first_descending):
        """Adds a pagination filter for the decoded values in the pagination token based on
    the sort order."""
        decoded_vals = self._decode_token(query_def, fields)
        # SQLite does not support tuple comparisons, so make an or-of-ands statements that is
        # equivalent.
        or_clauses = []
        if first_descending:
            if decoded_vals[0] is not None:
                or_clauses.append(fields[0] < decoded_vals[0])
                or_clauses.append(fields[0].is_(None))
        else:
            if decoded_vals[0] is None:
                or_clauses.append(fields[0].isnot(None))
            else:
                or_clauses.append(fields[0] > decoded_vals[0])
        for i in range(1, len(fields)):
            and_clauses = []
            for j in range(0, i):
                and_clauses.append(fields[j] == decoded_vals[j])
            if decoded_vals[i] is None:
                and_clauses.append(fields[i].isnot(None))
            else:
                and_clauses.append(fields[i] > decoded_vals[i])
            or_clauses.append(and_(*and_clauses))
        return query.filter(or_(*or_clauses))

    def _unpack_page_token(self, token):
        try:
            return json.loads(urlsafe_b64decode(token))
        except:
            raise BadRequest(f"Invalid pagination token: {token}.")

    def _decode_token(self, query_def, fields):
        pagination_token = query_def.pagination_token
        decoded_vals = self._unpack_page_token(pagination_token)
        return self._parse_pagination_data(decoded_vals, fields)

    def _parse_pagination_data(self, pagination_json_data, field_list):
        if not isinstance(pagination_json_data, list) or len(pagination_json_data) != len(field_list):
            raise BadRequest("Pagination token does not match url fields.")
        return [
            self._from_json_value(field_name, pagination_json_data[index])
            for index, field_name in enumerate(field_list)
        ]

    def _add_order_by(self, query, order_by, field_names, fields):
        """Adds a single order by field, as the primary sort order."""
        field_name = order_by.field_name

        alias_map = self.get_aliased_field_map()
        if field_name in alias_map.keys():
            field_name = alias_map[field_name]

        try:
            f = getattr(self.model_type, field_name)
        except AttributeError:
            raise BadRequest(f"No field named {field_name} found on {self.model_type}.")
        field_names.append(field_name)
        fields.append(f)
        if order_by.ascending:
            return query.order_by(f)
        else:
            return query.order_by(f.desc())

    def _get_order_by_ending(self, query):
        # pylint: disable=unused-argument
        return self.order_by_ending

    def _add_order_by_ending(self, query, field_names, fields):
        """Adds the order by ending."""
        for order_by_field in self.order_by_ending:
            if order_by_field in field_names:
                continue
            try:
                f = getattr(self.model_type, order_by_field)
            except AttributeError:
                raise BadRequest(f"No field named {order_by_field} found on {self.model_type}.")
            field_names.append(order_by_field)
            fields.append(f)
            query = query.order_by(f)
        return query

    @staticmethod
    def get_random_id():
        return random.randint(_MIN_ID, _MAX_ID)

    @staticmethod
    def _get_random_research_id():
        return random.randint(_MIN_RESEARCH_ID, _MAX_RESEARCH_ID)

    def _insert_with_random_id(self, obj, fields, insert_fun=None):
        """Attempts to insert an entity with randomly assigned ID(s) repeatedly until success
    or a maximum number of attempts are performed."""
        all_tried_ids = []
        for attempt_number in range(0, MAX_INSERT_ATTEMPTS):
            tried_ids = {}
            for field in fields:
                if field == 'researchId':
                    rand_id = self._get_random_research_id()
                else:
                    rand_id = self.get_random_id()
                tried_ids[field] = rand_id
                setattr(obj, field, rand_id)
            all_tried_ids.append(tried_ids)
            try:
                with self.session() as session:
                    if not insert_fun:
                        return self.insert_with_session(session, obj)
                    else:
                        return insert_fun(session, obj)
            except IntegrityError as e:
                result = self.handle_integrity_error(tried_ids, e, obj)
                if result:
                    return result
            except OperationalError:
                logging.warning('Failed insert with operational error', exc_info=True)
                if attempt_number == MAX_INSERT_ATTEMPTS - 1:
                    # Raise the error out if we're on the last retry attempt
                    raise
        # We were unable to insert a participant (unlucky). Throw an error.
        logging.warning(f"Giving up after {MAX_INSERT_ATTEMPTS} insert attempts, tried {all_tried_ids}.")
        raise ServiceUnavailable(f"Giving up after {MAX_INSERT_ATTEMPTS} insert attempts.")

    @staticmethod
    def validate_str_lengths(obj):
        _obj = dict(obj)
        invalid = False
        message = None
        for key, val in _obj.items():
            _type = getattr(obj.__class__, str(key))
            _type = _type.expression.type
            if _type.__class__.__name__.lower() == 'string' and val:
                if len(val) > _type.length:
                    invalid = True
                    message = f'Value for {key} cannot exceed char limit of {_type.length}'
                    break

        return invalid, message

    def handle_integrity_error(self, tried_ids, e, _):
        logging.warning(f"Failed insert with {tried_ids}: {str(e)}")

    def count(self):
        with self.session() as session:
            return session.query(self.model_type).count()

    def to_client_json(self, model):
        # pylint: disable=unused-argument
        """Converts the given model to a JSON object to be returned to API clients.

    Subclasses must implement this unless their model store a model.resource attribute.
    """
        try:
            return json.loads(model.resource)
        except AttributeError:
            raise NotImplementedError()

    def from_client_json(self):
        """Subclasses must implement this to parse API request bodies into model objects.

    Subclass args:
      resource: JSON object.
      participant_id: For subclasses which are children of participants only, the numeric ID.
      client_id: An informative string ID of the caller (who is creating/modifying the resource).
      id_: For updates, ID of the model to modify.
      expected_version: For updates, require this to match the existing model's version.
    """
        raise NotImplementedError()

    def get_model_obj_from_items(self, data_items):
        """
        Parses items from dictionary to check for all
        items that are key matches to attributes in a model
        data_items['my_key'] => myModel.my_key
        :param data_items: dict_items (dictionary) or odict_items (ordered dictionary)
        :return: obj (model object)
        """
        acceptable_types = ['dict_items', 'odict_items']
        if data_items.__class__.__name__.lower() in acceptable_types:
            insert_data = {key: value for key, value in data_items if key in
                           self.model_type.__table__.columns.keys()}
            return self.model_type(**insert_data)
        else:
            raise TypeError(f"Items passed in parameter are required to be {', '.join(acceptable_types)}")

    @staticmethod
    def literal_sql_from_query(query):
        """
        Returns actual Raw SQL with translated MySQL dialects, with literal
        value bindings in string
        :param query: sqlalchemy query object
        :return: string
        """
        return str(query.statement.compile(
            compile_kwargs={"literal_binds": True},
            dialect=mysql.dialect()
        ))

    @staticmethod
    def query_to_text(query, reindent=True):
        """
    Return the SQL statement text from a sqlalchemy query object.
    :param query: sqlalchemy query object
    :param reindent: True if pretty format
    :return: string
    """
        return sqlparse.format(str(query), reindent=reindent)

    @staticmethod
    def camel_to_snake(string_value):
        return re.sub(r'(?<!^)(?=[A-Z])', '_', string_value).lower()

    @staticmethod
    def snake_to_camel(string_value):
        mod_string = string_value.split('_')
        return mod_string[0] + ''.join(x.title() for x in mod_string[1:])

    def to_dict(self, obj, result_proxy=None):
        """
    Dump a sqlalchemy model or query result object to python dict. Converts
    Enums columns from integer to enum value string.
    Note: See patient_status_dao.py for example usage.
    :param obj: Model object, Query Result object or Row Proxy object.
    :param result_proxy: ResultProxy object if obj=RowProxy object.
    :return: ordered dict
    """
        if not obj:
            return None

        data = OrderedDict()

        # Get the list of columns returned in the query.
        if result_proxy and isinstance(result_proxy, ResultProxy):  # this is a ResultProxy object
            columns = list()
            for column in result_proxy.cursor.description:
                columns.append(column[0])
        elif hasattr(obj, "_fields"):  # This is a custom query result object.
            columns = obj._fields
        elif hasattr(obj, '_keymap'):  # RowProxy
            columns = obj._keymap
        else:
            mapper = inspect(obj)  # Simple model object
            columns = mapper.attrs

        for column in columns:
            key = str(column.key) if hasattr(column, "key") else column
            if not isinstance(key, str):
                # logging.warning('bad column key value [{0}], unable to lookup result column value.'.format(column))
                continue
            value = getattr(obj, key)

            if isinstance(value, (datetime.datetime, datetime.date)):
                data[key] = value.isoformat()
            # Check for Enum
            elif hasattr(value, "name"):
                data[key] = value.name
            else:
                data[key] = value

        return data

    @staticmethod
    def get_aliased_field_map():
        """
        Return a dictionary mapping new attribute names to the field they should copy.
        This allows the API to work with field names that aren't coming directly from the database.

        Example of return value:
        {
            'newAttributeName': 'existingAttributeItShouldCopy'
        }
        :return: Dictionary mapping alias names to their values.
        """
        return {}


class UpsertableDao(BaseDao):
    """A DAO that allows upserts of its entities (without any checking to see if the
  entities already exist or have a particular expected version.
  """

    def _validate_upsert(self, session, obj):
        """Override to validate a new model before upserting it (not applied to inserts)."""
        self._validate_model(session, obj)

    def _do_upsert(self, session, obj):
        """Perform the upsert of the specified object. Subclasses can override to alter things."""
        return session.merge(obj)

    def upsert_with_session(self, session, obj):
        """Upserts the object in the database with the specified session."""
        self._validate_upsert(session, obj)
        return self._do_upsert(session, obj)

    def upsert(self, obj):
        """Upserts the object in the database (creating the object if it does not exist, and replacing
    it if it does.)"""
        with self.session() as session:
            return self.upsert_with_session(session, obj)


class UpdatableDao(BaseDao):
    """A DAO that allows updates to entities.

  Extend from UpdatableDao if entities can be updated after being inserted.

  All model objects using this DAO should define a "version" field in order to allow version
  checking during update validation.

  To bypass version checking in a subclass, you must override the `validate_version_match`
  subclass property.
  """

    validate_version_match = True

    def _validate_update(self, session, obj, existing_obj):
        """Validates that an update is OK before performing it. (Not applied on insert.)

    By default, validates that the object already exists, and if an expected version ID is provided,
    that it matches.
    """
        if not existing_obj:
            raise NotFound(f"{self.model_type.__name__} with id {id} does not exist")
        if self.validate_version_match and existing_obj.version != obj.version:
            raise PreconditionFailed(
                f"Expected version was {obj.version}; stored version was {existing_obj.version}"
            )
        self._validate_model(session, obj)

    # pylint: disable=unused-argument
    def _do_update(self, session, obj, existing_obj):
        """Perform the update of the specified object. Subclasses can override to alter things."""
        session.merge(obj)

    def get_for_update(self, session, obj_id):
        return self.get_with_session(session, obj_id, for_update=True)

    def update_with_session(self, session, obj):
        """Updates the object in the database with the specified session. Will fail if the object
    doesn't exist already, or if obj.version does not match the version of the existing object."""
        existing_obj = self.get_for_update(session, self.get_id(obj))
        self._validate_update(session, obj, existing_obj)
        self._do_update(session, obj, existing_obj)

    def update(self, obj):
        """Updates the object in the database. Will fail if the object doesn't exist already, or
    if obj.version does not match the version of the existing object.
    May modify the passed in object."""
        with self.session() as session:
            return self.update_with_session(session, obj)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime.datetime) or isinstance(obj, datetime.date):
        return obj.isoformat()
    if isinstance(obj, messages.Enum):
        return str(obj)
    if hasattr(obj, "name"):
        return obj.name
    raise TypeError("Type not serializable")


_FhirProperty = collections.namedtuple(
    "FhirProperty", ("name", "json_name", "fhir_type", "is_list", "of_many", "not_optional")
)


def FhirProperty(name, fhir_type, json_name=None, is_list=False, required=False):
    """Helper for declaring FHIR propertly tuples which fills in common default values.

  By default, JSON name is the camelCase version of the Python snake_case name.

  The tuples are documented in FHIRAbstractBase as:
  ("name", "json_name", type, is_list, "of_many", not_optional)
  """
    if json_name is None:
        components = name.split("_")
        json_name = components[0] + "".join(c.capitalize() for c in components[1:])
    of_many = None  # never used?
    return _FhirProperty(name, json_name, fhir_type, is_list, of_many, required)


class FhirMixin(object):
    """Derive from this to simplify declaring custom FHIR resource or element classes.

  This aids in (de)serialization of JSON, including validation of field presence and types.

  Subclasses should derive from DomainResource or (for nested fields) BackboneElement, and fill in
  two class-level fields: resource_name (an arbitrary string) and _PROPERTIES.
  """

    _PROPERTIES = None  # Subclasses declar a list of calls to FP (producing tuples).

    def __init__(self, jsondict=None):
        for proplist in self._PROPERTIES:
            setattr(self, proplist[0], None)
        try:
            super(FhirMixin, self).__init__(jsondict=jsondict, strict=True)
        except FHIRValidationError as e:
            if isinstance(self, DomainResource):
                # Only convert FHIR exceptions to BadError at the top level. For nested objects, FHIR
                # repackages exceptions itself.
                raise BadRequest(str(e))
            else:
                raise

    def __str__(self):
        """Returns an object description to be used in validation error messages."""
        return self.resource_name

    def elementProperties(self):
        js = super(FhirMixin, self).elementProperties()
        js.extend(self._PROPERTIES)
        return js


def save_raw_request_record(log: RequestsLog):
    """
    Save the request payload and possibly link it to a table record
    :param log: RequestsLog dao object
    """
    # Don't try to save values greater than the max value for a signed 32-bit integer.
    if isinstance(log.participantId, int) and log.participantId > 0x7FFFFFFF:
        log.participantId = 0
    if isinstance(log.fpk_id, int) and log.fpk_id > 0x7FFFFFFF:
        log.fpk_id = 0

    _dao = BaseDao(RequestsLog)
    with _dao.session() as session:
        session.add(log)
        try:
            session.flush()
        except OperationalError:
            logging.error('Failed to save requests_log record, trying again without resource column data. ')
            session.rollback()
            log.resource = None
            session.add(log)
            session.flush()

        return log
