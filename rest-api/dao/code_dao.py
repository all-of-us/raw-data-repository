import clock
import logging

from dao.base_dao import BaseDao, UpdatableDao
from model.code import CodeBook, Code, CodeHistory
from werkzeug.exceptions import BadRequest

class CodeBookDao(BaseDao):
  def __init__(self):
    super(CodeBookDao, self).__init__(CodeBook)

  def insert_with_session(self, session, obj):
    obj.created = clock.CLOCK.now()
    obj.latest = True
    old_latest = self.get_latest_with_session(session)
    if old_latest:
      old_latest.latest = False
      session.merge(old_latest)
    super(CodeBookDao, self).insert_with_session(session, obj)
    return obj

  def get_latest_with_session(self, session):
    return session.query(CodeBook).filter(CodeBook.latest == True).one_or_none()

  def get_id(self, obj):
    return obj.codeBookId
    
  def import_codebook(self, codebook_json):
    pass

class CodeDao(UpdatableDao):
  def __init__(self):
    super(CodeDao, self).__init__(Code)

  def _add_history(self, session, obj):
    history = CodeHistory()
    history.fromdict(obj.asdict(), allow_pk=True)
    session.add(history)

  def insert_with_session(self, session, obj):
    obj.created = clock.CLOCK.now()
    super(CodeDao, self).insert_with_session(session, obj)
    # Flush the insert so that the code's ID gets assigned and can be copied to history.
    session.flush()
    self._add_history(session, obj)
    return obj

  def _validate_update(self, session, obj, existing_obj):
    if obj.codeBookId is None or existing_obj.codeBookId == obj.codeBookId:
      raise BadRequest("codeBookId must be set to a new value when updating a code")

  def _do_update(self, session, obj, existing_obj):
    obj.created = existing_obj.created
    super(CodeDao, self)._do_update(session, obj, existing_obj)
    self._add_history(session, obj)

  def get_id(self, obj):
    return obj.codeId

  def get_code_with_session(self, session, system, value):
    return (session.query(Code)
            .filter(Code.system == system)
            .filter(Code.value == value)
            .one_or_none())

  def get_or_add_codes(self, code_map):
    """Accepts a map of (system, value) -> (display, code_type, parent_id) for codes found in a
    questionnaire or questionnaire response.

    Returns a map of (system, value) -> codeId for new and existing codes.

    Adds new unmapped codes for anything that is missing.
    """
    result_map = {}
    with self.session() as session:
      for system, value in code_map.keys():
        existing_code = self.get_code_with_session(session, system, value)
        if existing_code:
          result_map[(system, value)] = existing_code.codeId
        else:
          display, code_type, parent_id = code_map[(system, value)]
          code = Code(system=system, value=value, display=display,
                      codeType=code_type, mapped=False, parentId=parent_id)
          logging.warn("Adding unmapped code: %s" % code)
          self.insert_with_session(session, code)
          session.flush()
          result_map[(system, value)] = code.codeId
    return result_map

class CodeHistoryDao(BaseDao):
  def __init__(self):
    super(CodeHistoryDao, self).__init__(CodeHistory)

  def get_id(self, obj):
    return obj.codeHistoryId