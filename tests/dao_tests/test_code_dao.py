import datetime

from werkzeug.exceptions import BadRequest

from rdr_service.clock import FakeClock
from rdr_service.dao.code_dao import CodeBookDao, CodeDao, CodeHistoryDao
from rdr_service.model.code import Code, CodeBook, CodeHistory, CodeType
from tests.helpers.unittest_base import BaseTestCase

TIME = datetime.datetime(2016, 1, 1, 10, 0)
TIME_2 = datetime.datetime(2016, 1, 2, 10, 0)
TIME_3 = datetime.datetime(2016, 1, 3, 10, 0)
TIME_4 = datetime.datetime(2016, 1, 4, 10, 0)


class CodeDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.code_book_dao = CodeBookDao()
        self.code_dao = CodeDao()
        self.code_history_dao = CodeHistoryDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.code_book_dao.get(1))
        self.assertIsNone(self.code_dao.get(1))
        self.assertIsNone(self.code_history_dao.get(1))

    def test_insert_without_codebook_or_parent(self):
        code = Code(system="a", value="b", display="c", topic="d", codeType=CodeType.MODULE, mapped=True)
        with FakeClock(TIME):
            self.code_dao.insert(code)

        expected_code = Code(
            codeId=1,
            system="a",
            value="b",
            display="c",
            topic="d",
            codeType=CodeType.MODULE,
            mapped=True,
            created=TIME,
        )
        self.assertEqual(expected_code.asdict(), self.code_dao.get(1).asdict())

        expected_code_history = CodeHistory(
            codeHistoryId=1,
            codeId=1,
            system="a",
            value="b",
            display="c",
            topic="d",
            codeType=CodeType.MODULE,
            mapped=True,
            created=TIME,
        )
        self.assertEqual(expected_code_history.asdict(), self.code_history_dao.get(1).asdict())

    def test_insert_with_codebook_and_parent(self):
        code_book_1 = CodeBook(name="pmi", version="v1", system="a")
        with FakeClock(TIME):
            self.code_book_dao.insert(code_book_1)
        expected_code_book = CodeBook(codeBookId=1, latest=True, created=TIME, name="pmi", version="v1", system="a")
        self.assertEqual(expected_code_book.asdict(), self.code_book_dao.get(1).asdict())

        code_1 = Code(
            codeBookId=1, system="a", value="b", display="c", topic="d", codeType=CodeType.MODULE, mapped=True
        )
        with FakeClock(TIME_2):
            self.code_dao.insert(code_1)

        expected_code = Code(
            codeBookId=1,
            codeId=1,
            system="a",
            value="b",
            display="c",
            topic="d",
            codeType=CodeType.MODULE,
            mapped=True,
            created=TIME_2,
        )
        self.assertEqual(expected_code.asdict(), self.code_dao.get(1).asdict())

        expected_code_history = CodeHistory(
            codeBookId=1,
            codeHistoryId=1,
            codeId=1,
            system="a",
            value="b",
            display="c",
            topic="d",
            codeType=CodeType.MODULE,
            mapped=True,
            created=TIME_2,
        )
        self.assertEqual(expected_code_history.asdict(), self.code_history_dao.get(1).asdict())

        code_2 = Code(
            codeBookId=1,
            system="x",
            value="y",
            display="z",
            topic="q",
            codeType=CodeType.QUESTION,
            mapped=False,
            parentId=1,
        )
        with FakeClock(TIME_3):
            self.code_dao.insert(code_2)

        expected_code_2 = Code(
            codeBookId=1,
            codeId=2,
            system="x",
            value="y",
            display="z",
            topic="q",
            codeType=CodeType.QUESTION,
            mapped=False,
            created=TIME_3,
            parentId=1,
        )
        self.assertEqual(expected_code_2.asdict(), self.code_dao.get(2).asdict())

    def test_insert_second_codebook_same_system(self):
        code_book_1 = CodeBook(name="pmi", version="v1", system="a")
        with FakeClock(TIME):
            self.code_book_dao.insert(code_book_1)

        code_book_2 = CodeBook(name="pmi", version="v2", system="a")
        with FakeClock(TIME_2):
            self.code_book_dao.insert(code_book_2)

        expected_code_book = CodeBook(codeBookId=1, latest=False, created=TIME, name="pmi", version="v1", system="a")
        self.assertEqual(expected_code_book.asdict(), self.code_book_dao.get(1).asdict())

        expected_code_book_2 = CodeBook(
            codeBookId=2, latest=True, created=TIME_2, name="pmi", version="v2", system="a"
        )
        self.assertEqual(expected_code_book_2.asdict(), self.code_book_dao.get(2).asdict())

    def test_insert_second_codebook_different_system(self):
        code_book_1 = CodeBook(name="pmi", version="v1", system="a")
        with FakeClock(TIME):
            self.code_book_dao.insert(code_book_1)

        code_book_2 = CodeBook(name="pmi", version="v2", system="b")
        with FakeClock(TIME_2):
            self.code_book_dao.insert(code_book_2)

        expected_code_book = CodeBook(codeBookId=1, latest=True, created=TIME, name="pmi", version="v1", system="a")
        self.assertEqual(expected_code_book.asdict(), self.code_book_dao.get(1).asdict())

        expected_code_book_2 = CodeBook(
            codeBookId=2, latest=True, created=TIME_2, name="pmi", version="v2", system="b"
        )
        self.assertEqual(expected_code_book_2.asdict(), self.code_book_dao.get(2).asdict())

    def test_insert_second_codebook_same_system_same_version(self):
        code_book_1 = CodeBook(name="pmi", version="v1", system="a")
        self.code_book_dao.insert(code_book_1)

        code_book_2 = CodeBook(name="pmi", version="v1", system="a")
        with self.assertRaises(BadRequest):
            self.code_book_dao.insert(code_book_2)

    def test_update_codes_no_codebook_id(self):
        code_book_1 = CodeBook(name="pmi", version="v1", system="c")
        with FakeClock(TIME):
            self.code_book_dao.insert(code_book_1)
        code_1 = Code(
            codeBookId=1, system="a", value="b", display="c", topic="d", codeType=CodeType.MODULE, mapped=True
        )
        with FakeClock(TIME_2):
            self.code_dao.insert(code_1)

        new_code_1 = Code(
            codeId=1, system="x", value="b", display="c", topic="d", codeType=CodeType.MODULE, mapped=True
        )
        with self.assertRaises(BadRequest):
            self.code_dao.update(new_code_1)

    def test_update_codes_same_codebook_id(self):
        code_book_1 = CodeBook(name="pmi", version="v1", system="c")
        with FakeClock(TIME):
            self.code_book_dao.insert(code_book_1)
        code_1 = Code(
            codeBookId=1, system="a", value="b", display="c", topic="d", codeType=CodeType.MODULE, mapped=True
        )
        with FakeClock(TIME_2):
            self.code_dao.insert(code_1)

        new_code_1 = Code(
            codeBookId=1,
            codeId=1,
            system="x",
            value="b",
            display="c",
            topic="d",
            codeType=CodeType.MODULE,
            mapped=True,
        )
        with self.assertRaises(BadRequest):
            self.code_dao.update(new_code_1)

    def test_update_codes_new_codebook_id(self):
        code_book_1 = CodeBook(name="pmi", version="v1", system="a")
        with FakeClock(TIME):
            self.code_book_dao.insert(code_book_1)
        code_1 = Code(
            codeBookId=1, system="a", value="b", display="c", topic="d", codeType=CodeType.MODULE, mapped=True
        )
        with FakeClock(TIME_2):
            self.code_dao.insert(code_1)

        code_book_2 = CodeBook(name="pmi", version="v2", system="a")
        with FakeClock(TIME_3):
            self.code_book_dao.insert(code_book_2)

        new_code_1 = Code(
            codeBookId=2,
            codeId=1,
            system="x",
            value="b",
            display="c",
            topic="d",
            codeType=CodeType.MODULE,
            mapped=True,
        )
        with FakeClock(TIME_4):
            self.code_dao.update(new_code_1)

        expected_code = Code(
            codeBookId=2,
            codeId=1,
            system="x",
            value="b",
            display="c",
            topic="d",
            codeType=CodeType.MODULE,
            mapped=True,
            created=TIME_2,
        )
        self.assertEqual(expected_code.asdict(), self.code_dao.get(1).asdict())

        expected_code_history = CodeHistory(
            codeBookId=1,
            codeHistoryId=1,
            codeId=1,
            system="a",
            value="b",
            display="c",
            topic="d",
            codeType=CodeType.MODULE,
            mapped=True,
            created=TIME_2,
        )
        self.assertEqual(expected_code_history.asdict(), self.code_history_dao.get(1).asdict())

        expected_code_history_2 = CodeHistory(
            codeHistoryId=2,
            codeBookId=2,
            codeId=1,
            system="x",
            value="b",
            display="c",
            topic="d",
            codeType=CodeType.MODULE,
            mapped=True,
            created=TIME_2,
        )
        self.assertEqual(expected_code_history_2.asdict(), self.code_history_dao.get(2).asdict())

    def test_import_codebook(self):
        answer_1 = _make_concept("t1", "Answer", "c1", "d1")
        answer_2 = _make_concept("t2", "Answer", "c2", "d2")
        answer_3 = _make_concept("t2", "Answer", "c3", "d3")
        question_1 = _make_concept("t1", "Question", "q1", "d4", [answer_1])
        question_2 = _make_concept("t2", "Question", "q2", "d5", [answer_2, answer_3])
        topic_1 = _make_concept("t1", "Topic", "t1", "d6", [question_1])
        module_1 = _make_concept("mt1", "Module Name", "m1", "d7", [topic_1])
        module_2 = _make_concept("mt2", "Module Name", "m2", "d8", [question_2])
        system = "http://blah/foo"
        codebook = {"name": "pmi", "version": "v1", "url": system, "concept": [module_1, module_2]}
        with FakeClock(TIME):
            self.code_book_dao.import_codebook(codebook)

        expectedCodeBook = CodeBook(codeBookId=1, latest=True, created=TIME, name="pmi", version="v1", system=system)
        self.assertEqual(expectedCodeBook.asdict(), self.code_book_dao.get(1).asdict())

        expectedModule1 = Code(
            codeBookId=1,
            codeId=1,
            system=system,
            value="m1",
            shortValue="m1",
            display="d7",
            topic="mt1",
            codeType=CodeType.MODULE,
            mapped=True,
            created=TIME,
        )
        self.assertEqual(expectedModule1.asdict(), self.code_dao.get(1).asdict())

        expectedModuleHistory1 = CodeHistory(
            codeHistoryId=1,
            codeBookId=1,
            codeId=1,
            system=system,
            value="m1",
            shortValue="m1",
            display="d7",
            topic="mt1",
            codeType=CodeType.MODULE,
            mapped=True,
            created=TIME,
        )
        self.assertEqual(expectedModuleHistory1.asdict(), self.code_history_dao.get(1).asdict())

        expectedTopic1 = Code(
            codeBookId=1,
            codeId=2,
            system=system,
            value="t1",
            shortValue="t1",
            display="d6",
            topic="t1",
            codeType=CodeType.TOPIC,
            mapped=True,
            created=TIME,
            parentId=1,
        )
        self.assertEqual(expectedTopic1.asdict(), self.code_dao.get(2).asdict())

        expectedQuestion1 = Code(
            codeBookId=1,
            codeId=3,
            system=system,
            value="q1",
            shortValue="q1",
            display="d4",
            topic="t1",
            codeType=CodeType.QUESTION,
            mapped=True,
            created=TIME,
            parentId=2,
        )
        self.assertEqual(expectedQuestion1.asdict(), self.code_dao.get(3).asdict())

        expectedAnswer1 = Code(
            codeBookId=1,
            codeId=4,
            system=system,
            value="c1",
            shortValue="c1",
            display="d1",
            topic="t1",
            codeType=CodeType.ANSWER,
            mapped=True,
            created=TIME,
            parentId=3,
        )
        self.assertEqual(expectedAnswer1.asdict(), self.code_dao.get(4).asdict())


def _make_concept(concept_topic, concept_type, code, display, child_concepts=None):
    concept = {
        "property": [
            {"code": "concept-topic", "valueCode": concept_topic},
            {"code": "concept-type", "valueCode": concept_type},
        ],
        "code": code,
        "display": display,
    }
    if child_concepts:
        concept["concept"] = child_concepts
    return concept
