import sqlalchemy.types as types


class BlobUTF8(types.TypeDecorator):
    '''Prefixes Unicode values with "PREFIX:" on the way in and
    strips it off on the way out.
    '''

    impl = types.BLOB

    def process_bind_param(self, value, _):
        if not value:
            return None
        return bytes(value, 'utf-8')

    def process_result_value(self, value, _):
        if not value:
            return None
        return value.decode('utf-8')

#    def copy(self, **kw):
#       return BlobUTF8(self.impl.length)
