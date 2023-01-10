from datetime import datetime
# from datetime import  timedelta
# from zlib import crc32
# from uuid import uuid4
# from typing import Dict, Any, Tuple
from unittest import TestCase
from unittest.mock import MagicMock, patch
import json
from types import SimpleNamespace as Namespace
from itertools import zip_longest
from werkzeug.exceptions import BadRequest, NotFound

from rdr_service.dao.study_nph_dao import (
    NphParticipantDao,
    NphStudyCategoryDao,
    NphSiteDao,
    NphOrderDao,
    NphOrderedSampleDao,
    # NphSampleUpdateDao,
    # NphBiobankFileExportDao,
    # NphSampleExportDao
)
# from rdr_service.clock import FakeClock
from rdr_service.model.study_nph import (
    # Participant,
    StudyCategory,
    # Site,
    Order,
    OrderedSample,
    # SampleUpdate,
    # BiobankFileExport,
    # SampleExport
)
from tests.helpers.unittest_base import BaseTestCase


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)

TEST_SAMPLE = {
    "subject": "Patient/P124820391",
    "identifier": [{
        "system": "http://www.pmi-ops.org/order-id",
        "value": "nph-order-id-123"
    }, {
        "system": "http://www.pmi-ops.org/sample-id",
        "value": "nph-sample-id-456"
    }],
    "createdInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "nph-site-testa"
        }
    },
    "collectedInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "nph-site-testa"
        }
    },
    "finalizedInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "hnphpo-site-testa"
        }
    },
    "created": "2022-11-03T09:40:21Z",
    "notes": {
        "collected": "Test notes 1",
        "finalized": "Test notes 2"
    }
}

TEST_URINE_SAMPLE = {
    "subject": "Patient/P124820391",
    "identifier": [{
        "system": "http://www.pmi-ops.org/order-id",
        "value": "nph-order-id-123"
    }, {
        "system": "http://www.pmi-ops.org/sample-id",
        "value": "nph-sample-id-456"
    }],
    "createdInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "nph-site-testa"
        }
    },
    "collectedInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "nph-site-testa"
        }
    },
    "finalizedInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "hnphpo-site-testa"
        }
    },
    "created": "2022-11-03T09:40:21Z",
    "module": "1",
    "visitType": "LMT",
    "timepoint": "preLMT",
    "sample": {
        "test": "UrineS",
        "description": "Spot Urine",
        "collected": "2022-11-03T09:45:49Z",
        "finalized": "2022-11-03T10:55:41Z",
        "color": "Color 1",
        "clarity": "Clean",
    },
    "aliquots": [{
        "id": "123",
        "identifier": "RU1",
        "container": "1.4mL Matrix Tube (1000 uL)",
        "volume": "970uL",
        "description": "1.4 mL matrix tubes",
        "collected": "2022-11-03T09:45:49Z"
    }, {
        "id": "456",
        "identifier": "RU2",
        "container": "6mL Matrix Tube (5 mL)",
        "volume": "3mL",
        "description": "1.4 mL matrix tubes",
        "collected": "2022-11-03T09:45:49Z"
    }, {
        "id": "789",
        "identifier": "RU2",
        "container": "6mL Matrix Tube (5 mL)",
        "volume": "3mL",
        "description": "1.4 mL matrix tubes",
        "collected": "2022-11-03T09:45:49Z"
    }, ],
    "notes": {
        "collected": "Test notes 1",
        "finalized": "Test notes 2"
    }
}


class NphParticipantDaoTest(TestCase):

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_get_id(self, query_filter):
        response = json.loads(json.dumps({"id": 1}), object_hook=lambda d: Namespace(**d))
        session = MagicMock()
        query_filter.return_value.first.return_value = response
        test_participant_dao = NphParticipantDao()
        participant_id = test_participant_dao.get_id(session, "10001")
        self.assertEqual(1, participant_id)

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_get_participant(self, query_filter):
        test_data = {"id": 1, "name": "test_participant"}
        response = json.loads(json.dumps(test_data), object_hook=lambda d: Namespace(**d))
        session = MagicMock()
        query_filter.return_value.first.return_value = response
        test_participant_dao = NphParticipantDao()
        participant = test_participant_dao.get_participant("10001", session)
        self.assertEqual(test_data.get("id"), participant.id)
        self.assertEqual(test_data.get("name"), participant.name)

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_check_participant_exist(self, query_filter):
        test_data = {"id": 1, "name": "test_participant"}
        response = json.loads(json.dumps(test_data), object_hook=lambda d: Namespace(**d))
        session = MagicMock()
        query_filter.return_value.first.return_value = response
        test_participant_dao = NphParticipantDao()
        exist = test_participant_dao.check_participant_exist("10001", session)
        self.assertTrue(exist)
        query_filter.return_value.first.return_value = None
        not_exist = test_participant_dao.check_participant_exist("10001", session)
        self.assertFalse(not_exist)

    def test_convert_id(self):
        test_participant_dao = NphParticipantDao()
        participant_id = test_participant_dao.convert_id("10001")
        self.assertEqual(1, participant_id)

    # def setUp(self):
    #     super().setUp()
    #     self.nph_participant_dao = NphParticipantDao()

    # def test_get_before_insert(self):
    #     self.assertIsNone(self.nph_participant_dao.get(1))

    # def test_insert_participant(self):
    #     nph_participant_params = {
    #         "id": 10001,
    #         "ignore_flag": 0,
    #         "disable_flag": 0,
    #         "disable_reason": "N/A",
    #         "biobank_id": 1E7,
    #         "research_id": 1E7
    #     }
    #     nph_participant = Participant(**nph_participant_params)
    #     with FakeClock(TIME):
    #         self.nph_participant_dao.insert(nph_participant)
    #
    #     expected_nph_participant = {
    #         "id": 1,
    #         "created": TIME,
    #         "modified": TIME,
    #         "ignore_flag": 0,
    #         "disable_flag": 0,
    #         "disable_reason": "N/A",
    #         "biobank_id": int(1E7),
    #         "research_id": int(1E7),
    #     }
    #     expected_nph_participant_ = Participant(**expected_nph_participant)
    #     participant_obj = self.nph_participant_dao.get_id(self.session, "1001")
    #     self.assertEqual(self.nph_participant_dao.convert_id("1001"), 1)
    #     self.assertEqual(expected_nph_participant_.__dict__, participant_obj.asdict())

    # def tearDown(self):
    #     self.clear_table_after_test("nph.participant")


class NphStudyCategoryTest(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.nph_study_category_dao = NphStudyCategoryDao()

    TEST_DATA = {"module": "1", "visitType": "LMT", "timepoint": "15min"}

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_insert_with_session(self, query_filter):
        query_filter.return_value.first.return_value = StudyCategory()
        session = MagicMock()
        request = json.loads(json.dumps(TEST_URINE_SAMPLE), object_hook=lambda d: Namespace(**d))
        self.nph_study_category_dao.insert_with_session(request, session)
        self.assertEqual(TEST_URINE_SAMPLE.get("timepoint"), session.method_calls[0][1][0].children[0].name)
        self.assertEqual("timepoint", session.method_calls[0][1][0].children[0].type_label)

    def test_no_module(self):
        test_data = {"visitType": "LMT", "timepoint": "15min"}
        request = json.loads(json.dumps(test_data), object_hook=lambda d: Namespace(**d))
        with self.assertRaises(BadRequest) as module_err:
            self.nph_study_category_dao.validate_model(request)
        self.assertEqual("400 Bad Request: Module is missing", str(module_err.exception))

    def test_no_time_point(self):
        test_data = {"visitType": "LMT", "module": "1"}
        request = json.loads(json.dumps(test_data), object_hook=lambda d: Namespace(**d))
        with self.assertRaises(BadRequest) as time_point_error:
            self.nph_study_category_dao.validate_model(request)
        self.assertEqual("400 Bad Request: Time Point ID is missing", str(time_point_error.exception))

    def test_no_value_type(self):
        test_data = {"module": "1", "timepoint": "15min"}
        request = json.loads(json.dumps(test_data), object_hook=lambda d: Namespace(**d))
        with self.assertRaises(BadRequest) as visit_type_error:
            self.nph_study_category_dao.validate_model(request)
        self.assertEqual("400 Bad Request: Visit Type is missing", str(visit_type_error.exception))

    def test_get_before_insert(self):
        session = MagicMock()
        self.assertIsNone(self.nph_study_category_dao.get_study_category_sample(1, session)[0])
    #
    # def _create_study_category(self, study_category_obj: Namespace) -> StudyCategory:
    #     with FakeClock(TIME):
    #         return self.nph_study_category_dao.insert(study_category_obj)
    #
    # def test_insert_parent_study_category(self):
    #     parent_study_category = {
    #         "timepoint": "Test_timePoint",
    #         "module": "PARENT",
    #         "visitType": "Test_visitType"
    #     }
    #     request = json.loads(json.dumps(parent_study_category), object_hook=lambda d: Namespace(**d))
    #     session = MagicMock()
    #     _parent_study_category = self.nph_study_category_dao.insert_with_session(request, session)
    #     expected_parent_study_category = {
    #         "id": 1,
    #         "created": TIME,
    #         "name": "Parent Study Category",
    #         "type_label": "PARENT",
    #         "parent_id": None
    #     }
    #     expected_parent_study_category_ = StudyCategory(**expected_parent_study_category)
    #     self.assertEqual(
    #         expected_parent_study_category_.asdict(),
    #         _parent_study_category.asdict()
    #     )
    #
    # def test_insert_child_study_category(self):
    #     parent_study_category_obj = {
    #         "name": "Parent Study Category",
    #         "type_label": "PARENT",
    #         "parent_id": None
    #     }
    #     parent_study_category = self._create_study_category(parent_study_category_obj)
    #
    #     child_study_category_obj = {
    #         "name": "Child Study Category",
    #         "type_label": "CHILD",
    #         "parent_id": parent_study_category.id
    #     }
    #     child_study_category = self._create_study_category(child_study_category_obj)
    #     expected_child_study_category = {
    #         "id": 2,
    #         "created": TIME,
    #         "name": "Child Study Category",
    #         "type_label": "CHILD",
    #         "parent_id": parent_study_category.id
    #     }
    #     expected_child_study_category_ = StudyCategory(**expected_child_study_category)
    #     self.assertEqual(
    #         expected_child_study_category_.asdict(),
    #         child_study_category.asdict()
    #     )

    def tearDown(self):
        self.clear_table_after_test("nph.study_category")


class NphSiteDaoTest(TestCase):

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_get_good_id(self, query_filter):
        response = json.loads(json.dumps({"id": 1}), object_hook=lambda d: Namespace(**d))
        session = MagicMock()
        site_dao = NphSiteDao()
        query_filter.return_value.first.return_value = response
        response = site_dao._fetch_site_id(session, "test_site")
        self.assertEqual(1, response)

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_get_bad_id(self, query_filter):
        session = MagicMock()
        site_dao = NphSiteDao()
        query_filter.return_value.first.return_value = None
        with self.assertRaises(NotFound) as bad_id_error:
            site_dao._fetch_site_id(session, "test_site")
        self.assertEqual("404 Not Found: Site is not found -- test_site", str(bad_id_error.exception))

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_site_exist(self, query_filter):
        session = MagicMock()
        site_dao = NphSiteDao()
        query_filter.return_value.first.return_value = 1
        exist = site_dao.site_exist(session, "test_site")
        self.assertTrue(exist)

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_site_not_exist(self, query_filter):
        session = MagicMock()
        site_dao = NphSiteDao()
        query_filter.return_value.first.return_value = None
        not_exist = site_dao.site_exist(session, "test_site")
        self.assertFalse(not_exist)

    # def setUp(self):
    #     super().setUp()
    #     self.nph_site_dao = NphSiteDao()
    #
    # def test_get_before_insert(self):
    #     self.assertIsNone(self.nph_site_dao.get(1))
    #
    # def test_insert_site(self):
    #     _time = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)
    #     site_external_id = str(uuid4())
    #     awardee_external_id = str(uuid4())
    #     site_mapping_params = {
    #         "created": _time,
    #         "modified": _time,
    #         "external_id": site_external_id,
    #         "name": "Site 1",
    #         "awardee_external_id": awardee_external_id
    #     }
    #     nph_site = Site(**site_mapping_params)
    #     with FakeClock(_time):
    #         self.nph_site_dao.insert(nph_site)
    #
    #     expected_site_mapping = {
    #         "id": 1,
    #         "created": _time,
    #         "modified": _time,
    #         "external_id": site_external_id,
    #         "name": "Site 1",
    #         "awardee_external_id": awardee_external_id
    #     }
    #     expected_nph_site = Site(**expected_site_mapping)
    #     self.assertEqual(
    #         expected_nph_site.asdict(), self.nph_site_dao.get(1).asdict()
    #     )
    #
    # def tearDown(self):
    #     self.clear_table_after_test("nph.site")


class NphOrderDaoTest(TestCase):

    RESTORED_PAYLOAD = {
                        "status": "restored",
                        "amendedReason": "ORDER_RESTORE_WRONG_PARTICIPANT",
                        "restoredInfo": {
                              "author": {"system": "https://www.pmi-ops.org/nph-username", "value": "test@pmi-ops.org"
                                         },
                              "site": {"system": "https://www.pmi-ops.org/site-id", "value": "nph-site-testa"
                                       }
                        }
                    }

    CANCEL_PAYLOAD = {
        "status": "canceled",
        "amendedReason": "ORDER_RESTORE_WRONG_PARTICIPANT",
        "cancelledInfo": {
            "author": {"system": "https://www.pmi-ops.org/nph-username", "value": "test@pmi-ops.org"
                       },
            "site": {"system": "https://www.pmi-ops.org/site-id", "value": "nph-site-testa"
                     }
        }
    }

    @patch('rdr_service.dao.study_nph_dao.NphOrderDao.get_order')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_patch_restored_update(self, site_id, order):
        session = MagicMock()
        request = json.loads(json.dumps(self.RESTORED_PAYLOAD), object_hook=lambda d: Namespace(**d))
        site_id.return_value = 1
        order.return_value = Order(id=1, participant_id=1)
        order_dao = NphOrderDao()
        result = order_dao.patch_update(request, 1, "10001", session)
        self.assertEqual(1, result.amended_site)
        self.assertEqual(request.restoredInfo.author.value, result.amended_author)
        self.assertEqual(request.amendedReason, result.amended_reason)
        self.assertEqual("RESTORED", result.status.upper())

    @patch('rdr_service.dao.study_nph_dao.NphOrderDao.get_order')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_patch_cancel_update(self, site_id, order):
        session = MagicMock()
        request = json.loads(json.dumps(self.CANCEL_PAYLOAD), object_hook=lambda d: Namespace(**d))
        site_id.return_value = 1
        order.return_value = Order(id=1, participant_id=1)
        order_dao = NphOrderDao()
        result = order_dao.patch_update(request, 1, "10001", session)
        self.assertEqual(1, result.amended_site)
        self.assertEqual(request.cancelledInfo.author.value, result.amended_author)
        self.assertEqual(request.amendedReason, result.amended_reason)
        self.assertEqual("CANCELED", result.status.upper())

    @patch('rdr_service.dao.study_nph_dao.NphParticipantDao.get_participant')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_from_client_json(self, site_id, p_id):
        session = MagicMock()
        p_id.return_value = 1
        site_id.return_value = 1
        order_dao = NphOrderDao()
        order_dao.set_order_cls(json.dumps(TEST_SAMPLE))
        result = order_dao.from_client_json(session, "10001", 1)
        self.assertEqual("nph-order-id-123", result.nph_order_id)
        self.assertEqual(result.order_created, TEST_SAMPLE.get("created"))
        self.assertEqual(1, result.category_id)
        self.assertEqual(1, result.participant_id)
        self.assertEqual(1, result.created_site)
        self.assertEqual(1, result.collected_site)
        self.assertEqual(1, result.finalized_site)
        self.assertEqual(TEST_SAMPLE.get("createdInfo").get("author").get("value"), result.created_author)
        self.assertEqual(TEST_SAMPLE.get("collectedInfo").get("author").get("value"), result.collected_author)
        self.assertEqual(TEST_SAMPLE.get("finalizedInfo").get("author").get("value"), result.finalized_author)
        self.assertEqual(TEST_SAMPLE.get("notes"), result.notes)

    @patch('rdr_service.dao.study_nph_dao.NphOrderDao.get_order')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_update(self, site_id, order):
        session = MagicMock()
        site_id.return_value = 1
        order_dao = NphOrderDao()
        order.return_value = Order(id=1, participant_id=1)
        order_dao.set_order_cls(json.dumps(TEST_SAMPLE))
        result = order_dao.update_order(1, "10001", session)
        self.assertEqual("nph-order-id-123", result.nph_order_id)
        self.assertEqual(1, result.created_site)
        self.assertEqual(1, result.collected_site)
        self.assertEqual(1, result.finalized_site)
        self.assertEqual(TEST_SAMPLE.get("createdInfo").get("author").get("value"), result.created_author)
        self.assertEqual(TEST_SAMPLE.get("collectedInfo").get("author").get("value"), result.collected_author)
        self.assertEqual(TEST_SAMPLE.get("finalizedInfo").get("author").get("value"), result.finalized_author)
        self.assertEqual(TEST_SAMPLE.get("notes"), result.notes)

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_get_good_order(self, query_filter):
        test_data = {"id": 1}
        response = json.loads(json.dumps(test_data), object_hook=lambda d: Namespace(**d))
        session = MagicMock()
        query_filter.return_value.first.return_value = response
        order_dao = NphOrderDao()
        result = order_dao.get_order(1, session)
        self.assertEqual(test_data, result.__dict__)

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_get_bad_order(self, query_filter):
        session = MagicMock()
        query_filter.return_value.first.return_value = None
        order_dao = NphOrderDao()
        with self.assertRaises(NotFound) as bad_id_error:
            order_dao.get_order(1, session)
        self.assertEqual("404 Not Found: Order Id does not exist -- 1.", str(bad_id_error.exception))

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_get_good_order_exit(self, query_filter):
        test_data = {"id": 1}
        response = json.loads(json.dumps(test_data), object_hook=lambda d: Namespace(**d))
        session = MagicMock()
        query_filter.return_value.first.return_value = response
        order_dao = NphOrderDao()
        exist, result = order_dao.check_order_exist(1, session)
        self.assertTrue(exist)
        self.assertEqual(test_data, result.__dict__)

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_get_bad_order_exit(self, query_filter):
        session = MagicMock()
        query_filter.return_value.first.return_value = None
        order_dao = NphOrderDao()
        not_exist, result = order_dao.check_order_exist(1, session)
        self.assertFalse(not_exist)
        self.assertIsNone(result)

    def test_set_order_cls(self):
        request = json.loads(json.dumps(TEST_SAMPLE), object_hook=lambda d: Namespace(**d))
        order_dao = NphOrderDao()
        order_dao.set_order_cls(json.dumps(TEST_SAMPLE))
        order = order_dao.order_cls
        self.assertEqual(request, order)

    @patch('rdr_service.dao.study_nph_dao.NphParticipantDao.get_participant')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_validate_model(self, site_id, p_id):
        session = MagicMock()
        p_id.return_value =1
        site_id.return_value = 1
        order_dao = NphOrderDao()
        order_dao.set_order_cls(json.dumps(TEST_SAMPLE))
        order = order_dao.from_client_json(session, "10001", 1)
        order_dao._validate_model(order)


class NphOrderedSampleDaoTest(TestCase):

    def test_from_client_json(self):
        request = json.loads(json.dumps(TEST_URINE_SAMPLE), object_hook=lambda d: Namespace(**d))
        order_sample_dao = NphOrderedSampleDao()
        os = order_sample_dao.from_client_json(request, 1, "10001")
        self.assertEqual(os.nph_sample_id, "10001")
        self.assertEqual(os.order_id, 1)
        self.assertEqual(os.test, request.sample.test)
        self.assertEqual(os.description, request.sample.description)
        self.assertEqual(os.collected, request.sample.collected)
        self.assertEqual(os.finalized, request.sample.finalized)
        self.assertEqual(os.supplemental_fields, {'clarity': 'Clean', 'color': 'Color 1'})

    def test_from_aliquot_client_json(self):
        request = json.loads(json.dumps(TEST_URINE_SAMPLE), object_hook=lambda d: Namespace(**d))
        order_sample_dao = NphOrderedSampleDao()
        aliquot = request.aliquots[0]
        aos = order_sample_dao.from_aliquot_client_json(aliquot, 1, "10001")
        self.assertEqual(aos.nph_sample_id, "10001")
        self.assertEqual(aos.order_id, 1)
        self.assertEqual(aos.aliquot_id, aliquot.id)
        self.assertEqual(aos.identifier, aliquot.identifier)
        self.assertEqual(aos.collected, aliquot.collected)
        self.assertEqual(aos.container, aliquot.container)
        self.assertEqual(aos.volume, aliquot.volume)

    def test_fetch_supplemental_fields(self):
        request = json.loads(json.dumps(TEST_URINE_SAMPLE), object_hook=lambda d: Namespace(**d))
        order_sample_dao = NphOrderedSampleDao()
        os = order_sample_dao.from_client_json(request, 1, "10001")
        supplemental_field = order_sample_dao._fetch_supplemental_fields(request)
        self.assertEqual(os.supplemental_fields, supplemental_field)

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_get_parent_os(self, query_filter):
        session = MagicMock()
        query_filter.return_value.first.return_value = 1
        os_dao = NphOrderedSampleDao()
        os = os_dao._get_parent_order_sample(1, session)
        self.assertEqual(1, os)

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_get_parent_no_os(self, query_filter):
        session = MagicMock()
        query_filter.return_value.first.return_value = None
        os_dao = NphOrderedSampleDao()
        with self.assertRaises(NotFound) as bad_id_error:
            os_dao._get_parent_order_sample(1, session)
        self.assertEqual("404 Not Found: Order sample not found", str(bad_id_error.exception))

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    def test_get_child_os(self, query_filter):
        session = MagicMock()
        query_filter.return_value.all.return_value = [1, 2, 3]
        os_dao = NphOrderedSampleDao()
        result = os_dao._get_child_order_sample(1, session)
        self.assertEqual([1, 2, 3], result)

    def test_insert_os(self):
        request = json.loads(json.dumps(TEST_URINE_SAMPLE), object_hook=lambda d: Namespace(**d))
        order_sample_dao = NphOrderedSampleDao()
        os = order_sample_dao.from_client_json(request, 1, 'nph-sample-id-456')
        session = MagicMock()
        order_sample_dao._insert_order_sample(request, 1, session)
        parent_os = session.method_calls[0][1][0]
        self.assertEqual(os.nph_sample_id, parent_os.nph_sample_id)
        self.assertEqual(os.collected, parent_os.collected)
        self.assertEqual(os.description, parent_os.description)
        self.assertEqual(os.finalized, parent_os.finalized)
        self.assertEqual(os.test, parent_os.test)
        self.assertEqual(os.volume, parent_os.volume)
        self.assertEqual(os.supplemental_fields, parent_os.supplemental_fields)
        children = parent_os.children
        cos_list = []
        for each in request.aliquots:
            cos = order_sample_dao.from_aliquot_client_json(each, 1, 'nph-sample-id-456')
            cos_list.append(cos)
        for (each, cos) in zip_longest(children, cos_list):
            self.assertEqual(each.collected, cos.collected)
            self.assertEqual(each.container, cos.container)
            self.assertEqual(each.description, cos.description)
            self.assertEqual(each.aliquot_id, cos.aliquot_id)
            self.assertEqual(each.identifier, cos.identifier)
            self.assertEqual(each.volume, cos.volume)

    def test_update_po(self):
        request = json.loads(json.dumps(TEST_URINE_SAMPLE), object_hook=lambda d: Namespace(**d))
        order_sample_dao = NphOrderedSampleDao()
        os = order_sample_dao.from_client_json(request, 1, 'nph-sample-id-456')
        old_os = OrderedSample(nph_sample_id="1", order_id=1, parent_sample_id=1, test="", description="test")
        new_os = order_sample_dao._update_parent_order(request, old_os)
        self.assertEqual(os.nph_sample_id, new_os.nph_sample_id)
        self.assertEqual(os.test, new_os.test)
        self.assertEqual(os.description, new_os.description)
        self.assertEqual(os.collected, new_os.collected)
        self.assertEqual(os.finalized, new_os.finalized)
        self.assertEqual(os.supplemental_fields, new_os.supplemental_fields)

    def test_canceled_co(self):
        request = json.loads(json.dumps(TEST_URINE_SAMPLE), object_hook=lambda d: Namespace(**d))
        order_sample_dao = NphOrderedSampleDao()
        os = order_sample_dao.from_client_json(request, 1, 'nph-sample-id-456')
        os = order_sample_dao._update_canceled_child_order(os)
        self.assertEqual("canceled", os.status)

    def test_restored_co(self):
        request = json.loads(json.dumps(TEST_URINE_SAMPLE), object_hook=lambda d: Namespace(**d))
        order_sample_dao = NphOrderedSampleDao()
        child = OrderedSample()
        new_os = order_sample_dao._update_restored_child_order(request.aliquots[0], child, 'nph-sample-id-456')
        self.assertEqual("restored", new_os.status)
        self.assertEqual(request.aliquots[0].identifier, new_os.identifier)
        self.assertEqual(request.aliquots[0].collected, new_os.collected)
        self.assertEqual(request.aliquots[0].container, new_os.container)
        self.assertEqual(request.aliquots[0].description, new_os.description)
        self.assertEqual(request.aliquots[0].volume, new_os.volume)
