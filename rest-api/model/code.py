from protorpc import messages
from model.base import Base
from model.utils import Enum
from sqlalchemy import Column, Integer, String, Text, UniqueConstraint, ForeignKey
from sqlalchemy.orm import relationship

class CodeType(messages.Enum):
  """A type of code"""
  MODULE = 1
  QUESTION = 2
  ANSWER = 3

class Code(Base):
  __tablename__ = 'code'
  """A code for a module, question, or answer.

  Questions have modules for parents, and answers have questions for parents.
  """
  codeId = Column('code_id', Integer, primary_key=True)
  parentId = Column('parent_id', Integer, ForeignKey('code.code_id'))
  system = Column('system', String(255), nullable=False)
  value = Column('value', String(80), nullable=False)
  display = Column('display', Text, nullable=False)
  topic = Column('topic', Text, nullable=False)
  type = Column('type', Enum(CodeType))

  parent = relationship("Code", remote_side=[codeId])
  children = relationship("Code")

  __table_args__ = (
    UniqueConstraint('value'),
  )
