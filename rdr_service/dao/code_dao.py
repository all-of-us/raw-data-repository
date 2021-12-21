import logging
from werkzeug.exceptions import BadRequest

from rdr_service import clock
from rdr_service.dao.base_dao import BaseDao
from rdr_service.dao.cache_all_dao import CacheAllDao
from rdr_service.model.code import Code, CodeBook, CodeHistory, CodeType
from rdr_service.singletons import CODE_CACHE_INDEX

_CODE_TYPE_MAP = {
    "Module Name": CodeType.MODULE,
    "Topic": CodeType.TOPIC,
    "Question": CodeType.QUESTION,
    "Answer": CodeType.ANSWER,
}


class CodeMap(object):
    """Stores code object ids by the value and system"""

    def __init__(self):
        self.codes = {}

    def add(self, code: Code):
        self.codes[(code.system, code.value.lower())] = code.codeId

    def get(self, system, value):
        return self.codes.get((system, value.lower()))

    def __len__(self):
        return len(self.codes)


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
                raise BadRequest(f"Codebook with system {obj.system}, version {obj.version} already exists")
            old_latest.latest = False
            session.merge(old_latest)
        super(CodeBookDao, self).insert_with_session(session, obj)
        return obj

    def get_latest_with_session(self, session, system):
        return session.query(CodeBook).filter(CodeBook.latest == True).filter(CodeBook.system == system).one_or_none()

    def get_id(self, obj):
        return obj.codeBookId

    def _import_concept(self, session, existing_codes, concept, system, code_book_id, parent_id):
        """Recursively imports a concept and its descendants as codes.

    Existing codes will be updated; codes that weren't there before will be inserted. Codes that
    are in the database but not in the codebook will be left untouched.
    """
        property_dict = {p["code"]: p["valueCode"] for p in concept["property"]}
        topic = property_dict["concept-topic"]
        value = concept["code"]
        short_value = property_dict.get("short-code") or value[:50]
        display = concept["display"]
        code_type = _CODE_TYPE_MAP.get(property_dict["concept-type"])
        if code_type is None:
            logging.warning(
                f"Unrecognized concept type: {property_dict['concept-type']}, value: {value}; ignoring."
            )
            return 0
        code = Code(
            system=system,
            codeBookId=code_book_id,
            value=value,
            shortValue=short_value,
            display=display,
            topic=topic,
            codeType=code_type,
            mapped=True,
            parentId=parent_id,
        )
        existing_code = existing_codes.get((system, value))
        if existing_code:
            code.codeId = existing_code.codeId
            self.code_dao._do_update(session, code, existing_code)
        else:
            self.code_dao.insert_with_session(session, code)
        child_concepts = concept.get("concept")
        code_count = 1
        if child_concepts:
            session.flush()
            for child_concept in child_concepts:
                code_count += self._import_concept(
                    session, existing_codes, child_concept, system, code_book_id, code.codeId
                )
        return code_count

    def import_codebook(self, codebook_json):
        """Imports a codebook and all codes inside it. Returns (new_codebook, imported_code_count)."""
        version = codebook_json["version"]
        num_concepts = len(codebook_json["concept"])
        logging.info(f"Importing {num_concepts} concepts into new CodeBook version {version}...")
        system = codebook_json["url"]
        codebook = CodeBook(name=codebook_json["name"], version=version, system=system)
        code_count = 0
        with self.session() as session:
            # Pre-fetch all Codes. This avoids any potential race conditions, and keeps a persistent
            # cache even though updates below invalidate the cache repeatedly.
            # Fetch within the session so later merges are faster.
            existing_codes = {
                (code.system, code.value): code for code in session.query(self.code_dao.model_type).all()
            }
            self.insert_with_session(session, codebook)
            session.flush()
            for i, concept in enumerate(codebook_json["concept"], start=1):
                logging.info(f"Importing root concept {i} of {num_concepts} ({concept.get('display')}).")
                code_count += self._import_concept(session, existing_codes, concept, system, codebook.codeBookId, None)
        logging.info(f"Finished, {code_count} codes imported.")
        return codebook, code_count


SYSTEM_AND_VALUE = ("system", "value")


class CodeDao(CacheAllDao):
    def __init__(self, silent=False, use_cache=True):
        super(CodeDao, self).__init__(
            Code, cache_index=CODE_CACHE_INDEX, cache_ttl_seconds=600, index_field_keys=[SYSTEM_AND_VALUE]
        )
        self.silent = silent
        self.use_cache = use_cache

    def _load_cache(self):
        result = super(CodeDao, self)._load_cache()
        for code in list(result.id_to_entity.values()):
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
        # pylint: disable=unused-argument
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
        return session.query(Code).filter(Code.system == system).filter(Code.value == value).one_or_none()

    def get_code(self, system, value):
        if self.use_cache:
            return self._get_cache().index_maps[SYSTEM_AND_VALUE].get((system, value))
        else:
            with self.session() as session:
                print('looking for sys:', system, ', code:', value)
                return session.query(Code).filter(
                    Code.system == system,
                    Code.value == value
                ).one_or_none()

    def find_ancestor_of_type(self, code, code_type):
        if code.codeType == code_type:
            return code
        if code.parentId:
            return self.find_ancestor_of_type(code.parent, code_type)
        return None

    def get_internal_id_code_map(self, code_map):
        """Accepts a map of (system, value) -> (display, code_type, parent_id) for codes found in a
    questionnaire or questionnaire response.

    Returns a map of (system, value) -> codeId for existing codes.
    """
        # First get whatever is already in the cache.
        result_map = CodeMap()
        for system, value in list(code_map.keys()):
            code = self.get_code(system, value)
            if code:
                result_map.add(code)
        if len(result_map) == len(code_map):
            return result_map

        missing_codes = []
        with self.session() as session:
            for system, value in list(code_map.keys()):
                existing_code = result_map.get(system, value)
                if not existing_code:
                    # Check to see if it's in the database. (Normally it won't be.)
                    existing_code = self._get_code_with_session(session, system, value)
                    if existing_code:
                        result_map.add(existing_code)
                    else:
                        missing_codes.append(f'{value} (system: {system})')

        if missing_codes:
            raise BadRequest(
                f"The following code values were unrecognized: {', '.join(missing_codes)}"
            )
        else:
            return result_map


class CodeHistoryDao(BaseDao):
    def __init__(self):
        super(CodeHistoryDao, self).__init__(CodeHistory)

    def get_id(self, obj):
        return obj.codeHistoryId
