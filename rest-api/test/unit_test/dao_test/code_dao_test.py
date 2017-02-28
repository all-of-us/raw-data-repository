import datetime

from clock import FakeClock
from unit_test_util import SqlTestBase
from dao.code_dao import CodeDao, CodeBookDao, CodeHistoryDao
from model.code import Code, CodeBook, CodeHistory, CodeType
from werkzeug.exceptions import BadRequest

TIME = datetime.datetime(2016, 1, 1, 10, 0)
TIME_2 = datetime.datetime(2016, 1, 2, 10, 0)
TIME_3 = datetime.datetime(2016, 1, 3, 10, 0)
TIME_4 = datetime.datetime(2016, 1, 4, 10, 0)

class CodeDaoTest(SqlTestBase):

  def setUp(self):
    super(CodeDaoTest, self).setUp()
    self.code_book_dao = CodeBookDao()
    self.code_dao = CodeDao()
    self.code_history_dao = CodeHistoryDao()

  def test_get_before_insert(self):
    self.assertIsNone(self.code_book_dao.get(1))
    self.assertIsNone(self.code_dao.get(1))
    self.assertIsNone(self.code_history_dao.get(1))

  def test_insert_without_codebook_or_parent(self):
    code = Code(system="a", value="b", display=u"c", topic=u"d",
                codeType=CodeType.MODULE, mapped=True)
    with FakeClock(TIME):
      self.code_dao.insert(code)

    expected_code = Code(codeId=1, system="a", value="b", display=u"c", topic=u"d",
                         codeType=CodeType.MODULE, mapped=True, created=TIME)
    self.assertEquals(expected_code.asdict(), self.code_dao.get(1).asdict())

    expected_code_history = CodeHistory(codeHistoryId=1, codeId=1, system="a", value="b",
                                        display=u"c", topic=u"d", codeType=CodeType.MODULE,
                                        mapped=True, created=TIME)
    self.assertEquals(expected_code_history.asdict(), self.code_history_dao.get(1).asdict())

  def test_insert_with_codebook_and_parent(self):
    code_book_1 = CodeBook(name="pmi", version="v1")
    with FakeClock(TIME):
      self.code_book_dao.insert(code_book_1)
    expected_code_book = CodeBook(codeBookId=1, latest=True, created=TIME, name="pmi", version="v1")
    self.assertEquals(expected_code_book.asdict(), self.code_book_dao.get(1).asdict())

    code_1 = Code(codeBookId=1, system="a", value="b", display=u"c", topic=u"d",
                  codeType=CodeType.MODULE, mapped=True)
    with FakeClock(TIME_2):
      self.code_dao.insert(code_1)

    expected_code = Code(codeBookId=1, codeId=1, system="a", value="b", display=u"c", topic=u"d",
                         codeType=CodeType.MODULE, mapped=True, created=TIME_2)
    self.assertEquals(expected_code.asdict(), self.code_dao.get(1).asdict())

    expected_code_history = CodeHistory(codeBookId=1, codeHistoryId=1, codeId=1, system="a",
                                        value=u"b", display=u"c", topic=u"d", 
                                        codeType=CodeType.MODULE, mapped=True, created=TIME_2)
    self.assertEquals(expected_code_history.asdict(), self.code_history_dao.get(1).asdict())

    code_2 = Code(codeBookId=1, system="x", value="y", display=u"z", topic=u"q",
                  codeType=CodeType.QUESTION, mapped=False, parentId=1)
    with FakeClock(TIME_3):
      self.code_dao.insert(code_2)

    expected_code_2 = Code(codeBookId=1, codeId=2, system="x", value="y", display=u"z", topic=u"q",
                           codeType=CodeType.QUESTION, mapped=False, created=TIME_3, parentId=1)
    self.assertEquals(expected_code_2.asdict(), self.code_dao.get(2).asdict())

  def test_insert_second_codebook(self):
    code_book_1 = CodeBook(name="pmi", version="v1")
    with FakeClock(TIME):
      self.code_book_dao.insert(code_book_1)

    code_book_2 = CodeBook(name="pmi", version="v2")
    with FakeClock(TIME_2):
      self.code_book_dao.insert(code_book_2)

    expected_code_book = CodeBook(codeBookId=1, latest=False, created=TIME, name="pmi", 
                                  version="v1")
    self.assertEquals(expected_code_book.asdict(), self.code_book_dao.get(1).asdict())

    expected_code_book_2 = CodeBook(codeBookId=2, latest=True, created=TIME_2, name="pmi",
                                    version="v2")
    self.assertEquals(expected_code_book_2.asdict(), self.code_book_dao.get(2).asdict())

  def test_update_codes_no_codebook_id(self):
    code_book_1 = CodeBook(name="pmi", version="v1")
    with FakeClock(TIME):
      self.code_book_dao.insert(code_book_1)
    code_1 = Code(codeBookId=1, system="a", value="b", display=u"c", topic=u"d",
                  codeType=CodeType.MODULE, mapped=True)
    with FakeClock(TIME_2):
      self.code_dao.insert(code_1)

    new_code_1 = Code(codeId=1, system="x", value="b", display=u"c", topic=u"d",
                      codeType=CodeType.MODULE, mapped=True)
    with self.assertRaises(BadRequest):
      self.code_dao.update(new_code_1)

  def test_update_codes_same_codebook_id(self):
    code_book_1 = CodeBook(name="pmi", version="v1")
    with FakeClock(TIME):
      self.code_book_dao.insert(code_book_1)
    code_1 = Code(codeBookId=1, system="a", value="b", display=u"c", topic=u"d",
                  codeType=CodeType.MODULE, mapped=True)
    with FakeClock(TIME_2):
      self.code_dao.insert(code_1)

    new_code_1 = Code(codeBookId=1, codeId=1, system="x", value="b", display=u"c", topic=u"d",
                      codeType=CodeType.MODULE, mapped=True)
    with self.assertRaises(BadRequest):
      self.code_dao.update(new_code_1)

  def test_update_codes_new_codebook_id(self):
    code_book_1 = CodeBook(name="pmi", version="v1")
    with FakeClock(TIME):
      self.code_book_dao.insert(code_book_1)
    code_1 = Code(codeBookId=1, system="a", value="b", display=u"c", topic=u"d",
                  codeType=CodeType.MODULE, mapped=True)
    with FakeClock(TIME_2):
      self.code_dao.insert(code_1)

    code_book_2 = CodeBook(name="pmi", version="v2")
    with FakeClock(TIME_3):
      self.code_book_dao.insert(code_book_2)

    new_code_1 = Code(codeBookId=2, codeId=1, system="x", value="b", display=u"c", topic=u"d",
                      codeType=CodeType.MODULE, mapped=True)
    with FakeClock(TIME_4):
      self.code_dao.update(new_code_1)

    expected_code = Code(codeBookId=2, codeId=1, system="x", value="b", display=u"c", topic=u"d",
                         codeType=CodeType.MODULE, mapped=True, created=TIME_2)
    self.assertEquals(expected_code.asdict(), self.code_dao.get(1).asdict())

    expected_code_history = CodeHistory(codeBookId=1, codeHistoryId=1, codeId=1, system="a",
                                        value="b", display=u"c", topic=u"d", 
                                        codeType=CodeType.MODULE, mapped=True, created=TIME_2)
    self.assertEquals(expected_code_history.asdict(), self.code_history_dao.get(1).asdict())

    expected_code_history_2 = CodeHistory(codeHistoryId=2, codeBookId=2, codeId=1, system="x",
                                          value="b", display=u"c", topic=u"d", 
                                          codeType=CodeType.MODULE, mapped=True, created=TIME_2)
    self.assertEquals(expected_code_history_2.asdict(), self.code_history_dao.get(2).asdict())

  def _make_concept(self, concept_topic, concept_type, code, display, child_concepts=None):
    concept = { 'property': [{ 'code': 'concept-topic', 'valueCode': concept_topic },
                             { 'code': 'concept-type', 'valueCode': concept_type } ],
                'code': code,
                'display': display }
    if child_concepts:
      concept['concept'] = child_concepts
    return concept

  def test_import_codebook(self):
    answer_1 = _make_concept("t1", "Answer", "c1", "d1") 
    answer_2 = _make_concept("t2", "Answer", "c2", "d2")
    answer_3 = _make_concept("t2", "Answer", "c3", "d3")
    
    
    
    codebook = { 'name': 'pmi', 'version': 'v1', 'system': 'sys' }
    
    
