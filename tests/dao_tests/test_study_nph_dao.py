from datetime import datetime, timedelta
from uuid import uuid4
from typing import Dict, Any, Tuple

from rdr_service.dao.study_nph_dao import (
    NphParticipantDao,
    NphStudyCategoryDao,
    NphSiteDao,
    NphOrderDao,
    NphOrderedSampleDao,
    NphSampleUpdateDao,
    NphBiobankFileExportDao,
    NphSampleExportDao
)
from rdr_service.clock import FakeClock
from rdr_service.model.study_nph import (
    Participant,
    StudyCategory,
    Site,
    Order,
    OrderedSample,
    # SampleUpdate,
    # BiobankFileExport,
    # SampleExport
)
from tests.helpers.unittest_base import BaseTestCase


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)


class NphParticipantDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.nph_participant_dao = NphParticipantDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.nph_participant_dao.get(1))

    def test_insert_participant(self):
        nph_participant_params = {
            "id": 1,
            "ignore_flag": 0,
            "disable_flag": 0,
            "disable_reason": "N/A",
            "biobank_id": 1E7,
            "research_id": 1E7
        }
        nph_participant = Participant(**nph_participant_params)
        with FakeClock(TIME):
            self.nph_participant_dao.insert(nph_participant)

        expected_nph_participant = {
            "id": 1,
            "created": TIME,
            "modified": TIME,
            "ignore_flag": 0,
            "disable_flag": 0,
            "disable_reason": "N/A",
            "biobank_id": int(1E7),
            "research_id": int(1E7),
        }
        expected_nph_participant_ = Participant(**expected_nph_participant)
        participant_obj = self.nph_participant_dao.get(1)
        self.assertEqual(self.nph_participant_dao.get_id(participant_obj), 1)
        self.assertEqual(expected_nph_participant_.asdict(), participant_obj.asdict())

    def tearDown(self):
        self.clear_table_after_test("nph.participant")


class NphStudyCategoryTest(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.nph_study_category_dao = NphStudyCategoryDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.nph_study_category_dao.get(1))

    def _create_study_category(self, study_category_obj: Dict[str, Any]) -> StudyCategory:
        nph_study_category = StudyCategory(**study_category_obj)
        with FakeClock(TIME):
            return self.nph_study_category_dao.insert(nph_study_category)

    def test_insert_parent_study_category(self):
        parent_study_category = {
            "name": "Parent Study Category",
            "type_label": "PARENT",
            "parent_id": None
        }
        _parent_study_category = self._create_study_category(parent_study_category)
        expected_parent_study_category = {
            "id": 1,
            "created": TIME,
            "name": "Parent Study Category",
            "type_label": "PARENT",
            "parent_id": None
        }
        expected_parent_study_category_ = StudyCategory(**expected_parent_study_category)
        self.assertEqual(
            expected_parent_study_category_.asdict(),
            _parent_study_category.asdict()
        )

    def test_insert_child_study_category(self):
        parent_study_category_obj = {
            "name": "Parent Study Category",
            "type_label": "PARENT",
            "parent_id": None
        }
        parent_study_category = self._create_study_category(parent_study_category_obj)

        child_study_category_obj = {
            "name": "Child Study Category",
            "type_label": "CHILD",
            "parent_id": parent_study_category.id
        }
        child_study_category = self._create_study_category(child_study_category_obj)
        expected_child_study_category = {
            "id": 2,
            "created": TIME,
            "name": "Child Study Category",
            "type_label": "CHILD",
            "parent_id": parent_study_category.id
        }
        expected_child_study_category_ = StudyCategory(**expected_child_study_category)
        self.assertEqual(
            expected_child_study_category_.asdict(),
            child_study_category.asdict()
        )

    def tearDown(self):
        self.clear_table_after_test("nph.study_category")


class NphSiteDaoTest(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.nph_site_dao = NphSiteDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.nph_site_dao.get(1))

    def test_insert_site(self):
        _time = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)
        site_external_id = str(uuid4())
        awardee_external_id = str(uuid4())
        site_mapping_params = {
            "created": _time,
            "modified": _time,
            "external_id": site_external_id,
            "name": "Site 1",
            "awardee_external_id": awardee_external_id
        }
        nph_site = Site(**site_mapping_params)
        with FakeClock(_time):
            self.nph_site_dao.insert(nph_site)

        expected_site_mapping = {
            "id": 1,
            "created": _time,
            "modified": _time,
            "external_id": site_external_id,
            "name": "Site 1",
            "awardee_external_id": awardee_external_id
        }
        expected_nph_site = Site(**expected_site_mapping)
        self.assertEqual(
            expected_nph_site.asdict(), self.nph_site_dao.get(1).asdict()
        )

    def tearDown(self):
        self.clear_table_after_test("nph.site")


class NphOrderDaoTest(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.nph_participant_dao = NphParticipantDao()
        self.nph_study_category_dao = NphStudyCategoryDao()
        self.nph_site_dao = NphSiteDao()
        self.nph_order_dao = NphOrderDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.nph_order_dao.get(1))

    def _create_study_category(self, study_category_obj: Dict[str, Any]) -> StudyCategory:
        nph_study_category = StudyCategory(**study_category_obj)
        with FakeClock(TIME):
            return self.nph_study_category_dao.insert(nph_study_category)

    def _create_parent_and_child_study_categories(
        self,
        parent_sc_name: str,
        parent_sc_type_label: str,
        child_sc_name: str,
        child_sc_type_label: str
    ) -> Tuple[StudyCategory, StudyCategory]:
        parent_study_category_params = {
            "name": parent_sc_name,
            "type_label": parent_sc_type_label,
            "parent_id": None
        }
        parent_sc = self._create_study_category(parent_study_category_params)

        child_study_category_params = {
            "name": child_sc_name,
            "type_label": child_sc_type_label,
            "parent_id": parent_sc.id
        }
        child_sc = self._create_study_category(child_study_category_params)
        return parent_sc, child_sc

    def _create_nph_participant(self, participant_obj: Dict[str, Any]) -> Participant:
        nph_participant = Participant(**participant_obj)
        with FakeClock(TIME):
            return self.nph_participant_dao.insert(nph_participant)

    def _create_site(self, name: str, site_external_id: str, awardee_external_id: str) -> Site:
        site_mapping_params = {
            "external_id": site_external_id,
            "name": name,
            "awardee_external_id": awardee_external_id
        }
        nph_site = Site(**site_mapping_params)
        with FakeClock(TIME):
            return self.nph_site_dao.insert(nph_site)

    def _create_order(self, order_params: Dict[str, Any], ts: str) -> Order:
        nph_order = Order(**order_params)
        with FakeClock(ts):
            return self.nph_order_dao.insert(nph_order)

    def test_insert_order(self):

        _, study_category = self._create_parent_and_child_study_categories(
            parent_sc_name="Parent Study Category",
            parent_sc_type_label="PARENT",
            child_sc_name="Child Study Category",
            child_sc_type_label="CHILD"
        )

        participant_obj_params = {
            "ignore_flag": 0,
            "disable_flag": 0,
            "disable_reason": "N/A",
            "biobank_id": 1E7,
            "research_id": 1E7
        }
        nph_participant = self._create_nph_participant(participant_obj_params)

        created_site_external_id = str(uuid4())
        created_awardee_external_id = str(uuid4())
        created_site = self._create_site(
            name="Created Site",
            site_external_id=created_site_external_id,
            awardee_external_id=created_awardee_external_id
        )
        created_author = "created@foobar.com"

        collected_site_external_id = str(uuid4())
        collected_awardee_external_id = str(uuid4())
        collected_site = self._create_site(
            name="Collected Site",
            site_external_id=collected_site_external_id,
            awardee_external_id=collected_awardee_external_id
        )
        collected_author = "collected@foobar.com"

        amended_site_external_id = str(uuid4())
        amended_awardee_external_id = str(uuid4())
        amended_site = self._create_site(
            name="Amended Site",
            site_external_id=amended_site_external_id,
            awardee_external_id=amended_awardee_external_id
        )
        amended_author = "amended@foobar.com"

        finalized_site_external_id = str(uuid4())
        finalized_awardee_external_id = str(uuid4())
        finalized_site = self._create_site(
            name="Finalized Site",
            site_external_id=finalized_site_external_id,
            awardee_external_id=finalized_awardee_external_id
        )
        finalized_author = "finalized@foobar.com"

        order_notes = {
            "NOTE": "DO NOT PROCESS THIS ORDER"
        }
        order_created_ts = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)
        _time = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)
        nph_order_id = str(uuid4())
        nph_order_params = {
            "nph_order_id": nph_order_id,
            "order_created": order_created_ts,
            "category_id": study_category.id,
            "participant_id": nph_participant.id,
            "created_site": created_site.id,
            "created_author": created_author,
            "collected_site": collected_site.id,
            "collected_author": collected_author,
            "finalized_site": finalized_site.id,
            "finalized_author": finalized_author,
            "amended_site": amended_site.id,
            "amended_author": amended_author,
            "amended_reason": "",
            "notes": order_notes,
            "status": "PROCESSING"
        }
        nph_order = self._create_order(nph_order_params, ts=_time)
        self.assertEqual(self.nph_order_dao.get(1).asdict(), nph_order.asdict())

    def tearDown(self):
        self.clear_table_after_test("nph.order")
        self.clear_table_after_test("nph.site")
        self.clear_table_after_test("nph.study_category")
        self.clear_table_after_test("nph.participant")


class NphOrderedSampleDaoTest(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.nph_participant_dao = NphParticipantDao()
        self.nph_study_category_dao = NphStudyCategoryDao()
        self.nph_site_dao = NphSiteDao()
        self.nph_order_dao = NphOrderDao()
        self.nph_ordered_sample_dao = NphOrderedSampleDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.nph_ordered_sample_dao.get(1))

    def _create_study_category(self, study_category_obj: Dict[str, Any]) -> StudyCategory:
        nph_study_category = StudyCategory(**study_category_obj)
        with FakeClock(TIME):
            return self.nph_study_category_dao.insert(nph_study_category)

    def _create_parent_and_child_study_categories(
        self,
        parent_sc_name: str,
        parent_sc_type_label: str,
        child_sc_name: str,
        child_sc_type_label: str
    ) -> Tuple[StudyCategory, StudyCategory]:
        parent_study_category_params = {
            "name": parent_sc_name,
            "type_label": parent_sc_type_label,
            "parent_id": None
        }
        parent_sc = self._create_study_category(parent_study_category_params)

        child_study_category_params = {
            "name": child_sc_name,
            "type_label": child_sc_type_label,
            "parent_id": parent_sc.id
        }
        child_sc = self._create_study_category(child_study_category_params)
        return parent_sc, child_sc

    def _create_nph_participant(self, participant_obj: Dict[str, Any]) -> Participant:
        nph_participant = Participant(**participant_obj)
        with FakeClock(TIME):
            return self.nph_participant_dao.insert(nph_participant)

    def _create_site(self, name: str, site_external_id: str, awardee_external_id: str) -> Site:
        site_mapping_params = {
            "external_id": site_external_id,
            "name": name,
            "awardee_external_id": awardee_external_id
        }
        nph_site = Site(**site_mapping_params)
        with FakeClock(TIME):
            return self.nph_site_dao.insert(nph_site)

    def _create_test_order(self) -> Order:
        _, study_category = self._create_parent_and_child_study_categories(
            parent_sc_name="Parent Study Category",
            parent_sc_type_label="PARENT",
            child_sc_name="Child Study Category",
            child_sc_type_label="CHILD"
        )

        participant_obj_params = {
            "ignore_flag": 0,
            "disable_flag": 0,
            "disable_reason": "N/A",
            "biobank_id": 1E7,
            "research_id": 1E7
        }
        nph_participant = self._create_nph_participant(participant_obj_params)

        created_site_external_id = str(uuid4())
        created_awardee_external_id = str(uuid4())
        created_site = self._create_site(
            name="Created Site",
            site_external_id=created_site_external_id,
            awardee_external_id=created_awardee_external_id
        )
        created_author = "created@foobar.com"

        collected_site_external_id = str(uuid4())
        collected_awardee_external_id = str(uuid4())
        collected_site = self._create_site(
            name="Collected Site",
            site_external_id=collected_site_external_id,
            awardee_external_id=collected_awardee_external_id
        )
        collected_author = "collected@foobar.com"

        amended_site_external_id = str(uuid4())
        amended_awardee_external_id = str(uuid4())
        amended_site = self._create_site(
            name="Amended Site",
            site_external_id=amended_site_external_id,
            awardee_external_id=amended_awardee_external_id
        )
        amended_author = "amended@foobar.com"

        finalized_site_external_id = str(uuid4())
        finalized_awardee_external_id = str(uuid4())
        finalized_site = self._create_site(
            name="Finalized Site",
            site_external_id=finalized_site_external_id,
            awardee_external_id=finalized_awardee_external_id
        )
        finalized_author = "finalized@foobar.com"

        order_notes = {
            "NOTE": "DO NOT PROCESS THIS ORDER"
        }
        order_created_ts = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)
        _time = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)
        nph_order_id = str(uuid4())
        nph_order_params = {
            "nph_order_id": nph_order_id,
            "order_created": order_created_ts,
            "category_id": study_category.id,
            "participant_id": nph_participant.id,
            "created_site": created_site.id,
            "created_author": created_author,
            "collected_site": collected_site.id,
            "collected_author": collected_author,
            "finalized_site": finalized_site.id,
            "finalized_author": finalized_author,
            "amended_site": amended_site.id,
            "amended_author": amended_author,
            "amended_reason": "",
            "notes": order_notes,
            "status": "PROCESSING"
        }
        nph_order = Order(**nph_order_params)
        with FakeClock(_time):
            return self.nph_order_dao.insert(nph_order)

    def _create_ordered_sample(self, ordered_sample_params: Dict[str, Any]) -> OrderedSample:
        nph_ordered_sample = OrderedSample(**ordered_sample_params)
        with FakeClock(TIME):
            return self.nph_ordered_sample_dao.insert(nph_ordered_sample)

    def test_insert_ordered_sample(self):
        test_order = self._create_test_order()
        nph_sample_id = str(uuid4())
        collected_ts = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)
        finalized_ts = datetime.strptime((datetime.now() + timedelta(hours=3)).strftime(DATETIME_FORMAT), DATETIME_FORMAT)
        ordered_sample_params = {
            "nph_sample_id": nph_sample_id,
            "order_id": test_order.id,
            "parent_sample_id": None,
            "test": "test",
            "description": "ordered sample",
            "collected": collected_ts,
            "finalized": finalized_ts,
            "aliquot_id": str(uuid4()),
            "container": "container 1",
            "volume": "volume 2",
            "status": "2 aliquots restored",
            "supplemental_fields": None
        }
        ordered_sample = self._create_ordered_sample(ordered_sample_params)
        self.assertIsNotNone(ordered_sample)
        ordered_sample_params["id"] = 1
        self.assertEqual(ordered_sample.asdict(), ordered_sample_params)

    def tearDown(self):
        self.clear_table_after_test("nph.ordered_sample")
        self.clear_table_after_test("nph.order")
        self.clear_table_after_test("nph.site")
        self.clear_table_after_test("nph.study_category")
        self.clear_table_after_test("nph.participant")


class NphSampleUpdateDaoTest(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.nph_sample_update_dao = NphSampleUpdateDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.nph_sample_update_dao.get(1))


class NphBiobankFileExportDaoTest(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.nph_biobank_file_export_dao = NphBiobankFileExportDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.nph_biobank_file_export_dao.get(1))


class NphSampleExportDaoTest(BaseTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.nph_sample_export_dao = NphSampleExportDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.nph_sample_export_dao.get(1))
