"""Model utility functions."""

from sqlalchemy.types import SmallInteger, TypeDecorator
  
class Enum(TypeDecorator):
    """A type for a SQLAlchemy column based on a protomsg Enum provided in the constructor"""
    impl = SmallInteger

    def __init__(self, enum_type):
      super(Enum, self).__init__()
      self.enum_type = enum_type
    
    def __repr__(self):
      return "Enum(%s)" % self.enum_type.__name__
          
    def process_bind_param(self, value, dialect):        
        return int(value) if value else None
        
    def process_result_value(self, value, dialect):
        return self.enum_type(value) if value else None