import logging

from dao.base_dao import BaseDao
from model.code import CodeBook, Code, CodeHistory

class CodeBookDao(BaseDao):
  def __init__(self):
    super(CodeBookDao, self).__init__(CodeBook)

  def get_id(self, obj):
    return obj.codeBookId

class CodeDao(BaseDao):
  def __init__(self):
    super(CodeDao, self).__init__(Code)

  def insert(self, obj):
    result = super(CodeDao, self).insert(obj)
    return result

  def get_id(self, obj):
    return obj.codeId

  def get_code_with_session(self, session, system, value):
    return (session.query(Code)
            .filter(Code.system == system)
            .filter(Code.value == value)
            .one_or_none())

  def get_or_add_codes(self, code_map):
    """Accepts a map of (system, value) -> (display, code_type) for codes found in a questionnaire.

    Returns a map of (system, value) -> codeId.

    Adds new unmapped codes for anything that is missing.
    """
    result_map = {}
    with self.session() as session:
      for (system, value) in code_map.keys():
        existing_code = self.get_code_with_session(session, system, value)
        if existing_code:
          result_map[(system, value)] = existing_code.codeId
        else:
          (display, code_type) = code_map[(system, value)]
          code = Code(system=system, value=value, display=display,
                      type=code_type, mapped=False)
          logging.warn("Adding unmapped code: %s" % code)
          self.insert_with_session(session, code)
          session.flush()
          result_map[(system, value)] = code.codeId
    return result_map

class CodeHistoryDao(BaseDao):
  def __init__(self):
    super(CodeHistoryDao, self).__init__(CodeHistory)

  def get_id(self, obj):
    return [obj.codeId, obj.codeBookId]