import clock
import logging
import traceback

from dao.base_dao import BaseDao
from dao.cache_all_dao import CacheAllDao
from model.code import CodeBook, Code, CodeHistory, CodeType
from werkzeug.exceptions import BadRequest

_CODE_TYPE_MAP = {
  "Module Name": CodeType.MODULE,
  "Topic": CodeType.TOPIC,
  "Question": CodeType.QUESTION,
  "Answer": CodeType.ANSWER
}

class CodeBookDao(BaseDao):
  def __init__(self):
    super(CodeBookDao, self).__init__(CodeBook)
    self.code_dao = CodeDao()

  def insert_with_session(self, session, obj):
    obj.created = clock.CLOCK.now()
    obj.latest = True
    old_latest = self.get_latest_with_session(session, obj.system)
    if old_latest:
      if old_latest.version == obj.version:
        raise BadRequest("Codebook with system %s, version %s already exists" %
                         (obj.system, obj.version))
      old_latest.latest = False
      session.merge(old_latest)
    super(CodeBookDao, self).insert_with_session(session, obj)
    return obj

  def get_latest_with_session(self, session, system):
    return (session.query(CodeBook)
        .filter(CodeBook.latest == True)
        .filter(CodeBook.system == system)
        .one_or_none())

  def get_id(self, obj):
    return obj.codeBookId

  def _import_concept(self, session, concept, system, code_book_id, parent_id):
    """Recursively imports a concept and its descendants as codes.

    Existing codes will be updated; codes that weren't there before will be inserted. Codes that
    are in the database but not in the codebook will be left untouched.
    """
    property_dict = {p['code']: p['valueCode'] for p in concept['property']}
    topic = property_dict['concept-topic']
    value = concept['code']
    display = concept['display']
    code_type = _CODE_TYPE_MAP.get(property_dict['concept-type'])
    if code_type is None:
      logging.warning("Unrecognized concept type: %s, value: %s; ignoring." %
                      (property_dict['concept-type'], value))
      return 0
    code = Code(system=system, codeBookId=code_book_id, value=value, display=display, topic=topic,
                codeType=code_type, mapped=True, parentId=parent_id)
    existing_code = self.code_dao._get_code_with_session(session, system, value)
    if existing_code:
      code.codeId = existing_code.codeId
      self.code_dao._do_update(session, code, existing_code)
    else:
      self.code_dao.insert_with_session(session, code)
    child_concepts = concept.get('concept')
    code_count = 1
    if child_concepts:
      session.flush()
      for child_concept in child_concepts:
        code_count += self._import_concept(session, child_concept, system, code_book_id,
                                           code.codeId)
    return code_count

  def import_codebook(self, codebook_json):
    """Imports a codebook and all codes inside it."""
    logging.info("Importing codes...")
    system = codebook_json['url']
    codebook = CodeBook(name=codebook_json['name'], version=codebook_json['version'],
                        system=system)
    code_count = 0
    with self.session() as session:
      self.insert_with_session(session, codebook)
      session.flush()
      for concept in codebook_json['concept']:
        code_count += self._import_concept(session, concept, system, codebook.codeBookId, None)
    logging.info("%d codes imported.", code_count)

SYSTEM_AND_VALUE = ('system', 'value')

class CodeDao(CacheAllDao):
  def __init__(self):
    super(CodeDao, self).__init__(Code, cache_ttl_seconds=600,
                                  index_field_keys=[SYSTEM_AND_VALUE])

  def _load_cache(self, key):
    result = super(CodeDao, self)._load_cache(key)
    for code in result.id_to_entity.values():
      if code.parentId is not None:
        parent = result.id_to_entity.get(code.parentId)
        if parent:
          parent.children.append(code)
          code.parent = parent
    return result

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
    #pylint: disable=unused-argument
    if obj.codeBookId is None or existing_obj.codeBookId == obj.codeBookId:
      raise BadRequest("codeBookId must be set to a new value when updating a code")

  def _do_update(self, session, obj, existing_obj):
    obj.created = existing_obj.created
    super(CodeDao, self)._do_update(session, obj, existing_obj)
    self._add_history(session, obj)

  def get_id(self, obj):
    return obj.codeId

  def _get_code_with_session(self, session, system, value):
    # In the context of an import, where this is called, don't use the cache.
    return (session.query(Code)
            .filter(Code.system == system)
            .filter(Code.value == value)
            .one_or_none())

  def get_code(self, system, value):
    return self._get_cache().index_maps[SYSTEM_AND_VALUE].get((system, value))

  def find_ancestor_of_type(self, code, code_type):
    if code.codeType == code_type:
      return code
    if code.parentId:
      return self.find_ancestor_of_type(code.parent, code_type)
    return None

  def get_or_add_codes(self, code_map, add_codes_if_missing=True):
    """Accepts a map of (system, value) -> (display, code_type, parent_id) for codes found in a
    questionnaire or questionnaire response.

    Returns a map of (system, value) -> codeId for new and existing codes.

    Adds new unmapped codes for anything that is missing.
    """
    # First get whatever is already in the cache.
    result_map = {}
    for system, value in code_map.keys():
      code = self.get_code(system, value)
      if code:
        result_map[(system, value)] = code.codeId
    if len(result_map) == len(code_map):
      return result_map
    with self.session() as session:
      for system, value in code_map.keys():
        existing_code = result_map.get((system, value))
        if not existing_code:
          # Check to see if it's in the database. (Normally it won't be.)
          existing_code = self._get_code_with_session(session, system, value)
          if existing_code:
            result_map[(system, value)] = code.codeId
            continue

          if not add_codes_if_missing:
            raise BadRequest("Couldn't find code: system = %s, value = %s" % (system, value))
          # If it's not in the database, add it.
          display, code_type, parent_id = code_map[(system, value)]
          code = Code(system=system, value=value, display=display,
                      codeType=code_type, mapped=False, parentId=parent_id)
          # Log the traceback so that stackdriver error reporting reports on it.
          logging.error("Adding unmapped code: system = %s, value = %s: %s",
                        code.system, code.value, traceback.format_exc())
          self.insert_with_session(session, code)
          session.flush()
          result_map[(system, value)] = code.codeId
    return result_map

class CodeHistoryDao(BaseDao):
  def __init__(self):
    super(CodeHistoryDao, self).__init__(CodeHistory)

  def get_id(self, obj):
    return obj.codeHistoryId