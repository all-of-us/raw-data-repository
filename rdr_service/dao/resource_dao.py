#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from collections import OrderedDict

from sqlalchemy import inspect
from sqlalchemy.engine import ResultProxy

from rdr_service.dao.base_dao import UpsertableDao
from rdr_service.model.resource_data import ResourceData
from rdr_service.resource.fields import EnumString, EnumInteger


class ResourceDataDao(UpsertableDao):

    def __init__(self, backup=False):
        """
        :param backup: Use backup readonly database connection.
        """
        super().__init__(ResourceData, backup=backup)

    def to_resource_dict(self, obj, schema=None, result_proxy=None):
        """
        Dump a sqlalchemy model or query result object to python dict.
        :param obj: SqlAlchemy Query Result object or Row Proxy object.
        :param schema: Resource schema object.
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

            # TODO:  This section may need reworking to correctly process different Enum classes (e.g., Python base
            # TODO:  Enum classes from Python 3.4 and protopc messages.Enum).  The to_resource_dict() method is not
            # TODO:  currently called anywhere except by a few resource generators not yet in use
            if schema:
                # Check for Enum column type and convert to Enum if needed.
                _field = schema.get_field(key)
                if type(_field) in (EnumString, EnumInteger):
                    value = _field.enum(value)
                    # check for a (key + '_id') field to support for both EnumString and EnumInteger columns in schema.
                    _field = schema.get_field(key + '_id')
                    if _field and type(_field) == EnumInteger:
                        data[key + '_id'] = int(_field.enum(value))

            data[key] = value

        return data
