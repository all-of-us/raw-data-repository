#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# A light wrapper of Marshmallow field types to extend the field types to be
# more descriptive when exporting to a JSON Schema.
#
from enum import EnumMeta

from marshmallow import validate, fields, exceptions

#
# Integer Marshmallow validators
#
validateBoolean = validate.Range(min=0, max=1)
validateInt8 = validate.Range(min=-2 ** (8 - 1), max=2 ** (8 - 1) - 1)
validateUInt8 = validate.Range(0, max=(2 ** 8) - 1)
validateInt16 = validate.Range(min=-2 ** (16 - 1), max=2 ** (16 - 1) - 1)
validateUInt16 = validate.Range(min=0, max=(2 ** 16) - 1)
validateInt32 = validate.Range(min=-2 ** (32 - 1), max=2 ** (32 - 1) - 1)
validateUInt32 = validate.Range(min=0, max=(2 ** 32) - 1)
validateInt64 = validate.Range(min=-2 ** (64 - 1), max=2 ** (64 - 1) - 1)
validateUInt64 = validate.Range(min=0, max=(2 ** 64) - 1)


class _ValidateEnum(object):
    """
    Validate Enumerator Class values.
    """
    enum_class = None

    def __init__(self, enum_class):
        self.enum_class = enum_class

    def enum_keys(self):
        return [i.name for i in self.enum_class]

    def enum_values(self):
        # Support old Python 2 protorpc.messages Enum class.
        return [i.value if hasattr(i, 'value') else i.number for i in self.enum_class]

    def keys(self):
        return validate.OneOf(self.enum_keys(), labels=self.enum_values())

    def values(self):
        return validate.OneOf(self.enum_values(), labels=self.enum_keys())


validateEnum = _ValidateEnum

#
# Nest Schemas
#
class Nested(fields.Nested):
    pass

#
# String type fields
#
class String(fields.String):
    pass


class Text(fields.String):
    """ Represents a Text field """
    format = 'text'
    validator = validate.Length(max=(2 ** 16) - 1)

    def __init__(self, *args, **kwargs):
        kwargs.update({
            'format': self.format,
            'validate': (self.validator if 'validate' not in kwargs else kwargs['validate'])
        })
        super().__init__(*args, **kwargs)


class MediumText(fields.String):
    """ Represents a Medium Text field """
    format = 'medium-text'
    validator = validate.Length(max=(2 ** 24) - 1)

    def __init__(self, *args, **kwargs):
        kwargs.update({
            'format': self.format,
            'validate': (self.validator if 'validate' not in kwargs else kwargs['validate'])
        })
        super().__init__(*args, **kwargs)


class LongText(fields.String):
    """ Represents a Long Text field """
    format = 'medium-text'
    validator = validate.Length(max=(2 ** 32) - 1)

    def __init__(self, *args, **kwargs):
        kwargs.update({
            'format': self.format,
            'validate': (self.validator if 'validate' not in kwargs else kwargs['validate'])
        })
        super().__init__(*args, **kwargs)


class JSON(fields.String):
    """ Represents a JSON field """
    format = 'json'

    def __init__(self, *args, **kwargs):
        kwargs.update({'format': self.format})
        super().__init__(*args, **kwargs)


class Binary(fields.String):
    """ Represents a Binary data field """
    format = 'binary'

    def __init__(self, *args, **kwargs):
        kwargs.update({'format': self.format})
        super().__init__(*args, **kwargs)


class Email(fields.Email):
    pass


#
# Date and Time fields
#
class Date(fields.Date):
    pass


class DateTime(fields.DateTime):
    pass


class AwareDateTime(fields.AwareDateTime):
    pass


class NativeDateTime(fields.NaiveDateTime):
    pass


class Decimal(fields.Decimal):
    pass


class Float(fields.Float):
    pass


class _BaseInteger(fields.Integer):
    """
    Base class for all Integer types.
    """
    format = ''
    validator = None

    def __init__(self, *args, **kwargs):
        kwargs.update({
            'format': self.format,
            'multipleOf': '1.0',
            'validate': (self.validator if 'validate' not in kwargs else kwargs['validate'])
        })
        super().__init__(*args, **kwargs)


class Boolean(fields.Boolean):
    pass


class Int8(_BaseInteger):
    """ Tiny 8-bit signed integer. Range: -128 to 127 """
    format = 'int8'
    validator = validateInt8


class UInt8(_BaseInteger):
    """ Tiny 8-bit unsigned integer. Range: 0 to 255 """
    format = 'uint8'
    validator = validateUInt8


class Int16(_BaseInteger):
    """ Short 16-bit signed integer. Range: -32,768 to 32,767 """
    format = 'int16'
    validator = validateInt16


class UInt16(_BaseInteger):
    """ 16-bit unsigned integer. Range: 0 to 65,535 """
    format = 'uint16'
    validator = validateUInt16


class Int32(_BaseInteger):
    """ 32-bit signed integer. Range: -2,147,483,648 to 2,147,483,647 """
    format = 'int32'
    validator = validateInt32


class UInt32(_BaseInteger):
    """ 32-bit unsigned integer. Range: 0 to 4,294,967,295 """
    format = 'uint32'
    validator = validateUInt32


class Int64(_BaseInteger):
    """ Big 64-bit signed integer. Range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 """
    format = 'int64'
    validator = validateInt64


class UInt64(_BaseInteger):
    """ Big 64-bit unsigned integer. Range: 0 to 18,446,744,073,709,551,615 """
    format = 'uint64'
    validator = validateUInt64


class EnumString(fields.String):
    """ Enum field class that enumerates the string keys of the enum object. """
    validator = None

    def __init__(self, enum, *args, **kwargs):
        self.enum = enum
        # Support old Python 2 protorpc.messages Enum class.
        import protorpc
        if not enum or (type(enum) != EnumMeta and type(enum) != protorpc.messages._EnumClass):
            raise exceptions.ValidationError(f'Invalid enum argument, expected type Enum.')

        self.validator = validateEnum(enum).keys()

        kwargs.update({
            'validate': (self.validator if 'validate' not in kwargs else kwargs['validate'])
        })
        super().__init__(*args, **kwargs)


class EnumInteger(fields.Integer):
    """ Enum field class that enumerates the integer id values of the enum object. """
    validator = None

    def __init__(self, enum, *args, **kwargs):
        self.enum = enum
        # Support old Python 2 protorpc.messages Enum class.
        import protorpc
        if not enum or (type(enum) != EnumMeta and type(enum) != protorpc.messages._EnumClass):
            raise exceptions.ValidationError(f'Invalid enum argument, expected type Enum.')

        self.validator = validateEnum(enum).values()

        kwargs.update({
            'validate': (self.validator if 'validate' not in kwargs else kwargs['validate'])
        })
        super().__init__(*args, **kwargs)
