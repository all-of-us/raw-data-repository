from protorpc import messages
from model.base import Base
from model.utils import Enum
from sqlalchemy import Column, Integer, String, Text, Boolean, UniqueConstraint
from sqlalchemy import ForeignKey, ForeignKeyConstraint, DateTime
from sqlalchemy.orm import backref, relationship
from sqlalchemy.ext.declarative import declared_attr

class CodeType(messages.Enum):
  """A type of code"""
  MODULE = 1
  QUESTION = 2
  ANSWER = 3

class CodeBook(Base):
  """A book of codes. Each import of a code book gets a new ID."""
  __tablename__ = 'code_book'
  codeBookId = Column('code_book_id', Integer, primary_key=True)
  created = Column('created', DateTime, nullable=False)

class CodeBase(object):
  """Mixin with shared columns for Code and CodeHistory"""
  codeId = Column('code_id', Integer, primary_key=True)
  system = Column('system', String(255), nullable=False)
  value = Column('value', String(80), nullable=False)
  display = Column('display', Text)
  topic = Column('topic', Text)
  type = Column('type', Enum(CodeType), nullable=False)
  mapped = Column('mapped', Boolean, nullable=False)

  @declared_attr
  def codeBookId(cls):
    return Column('code_book_id', Integer, ForeignKey('code_book.code_book_id'))

class Code(CodeBase, Base):
  """A code for a module, question, or answer.

  Questions have modules for parents, and answers have questions for parents.


  """
  __tablename__ = 'code'

  @declared_attr
  def parentId(cls):
    return Column('parent_id', Integer, ForeignKey('code.code_id'))

  @declared_attr
  def children(cls):
    return relationship(
        cls.__name__,
        backref=backref(
            'parent',
            remote_side='Code.codeId'
        ),
        cascade='all, delete-orphan'
    )

  __table_args__ = (
    UniqueConstraint('value'),
  )

class CodeHistory(CodeBase, Base):
  """A version of a code.

  New versions are inserted every time a code book is imported.
  """
  __tablename__ = 'code_history'

  parentId = Column('parent_id', Integer)

  @declared_attr
  def codeBookId(cls):
    return Column('code_book_id', Integer, ForeignKey('code_book.code_book_id'),
                  primary_key=True)

  __table_args__ = (
    UniqueConstraint('code_book_id', 'value'),
  )
