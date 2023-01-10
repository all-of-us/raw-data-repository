# Sample ID = NP124820391
import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

from rdr_service.main import app
from rdr_service.model.study_nph import (
    StudyCategory, Order, OrderedSample
)

BLOOD_SAMPLE = {
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
            "value": "hpo-site-testa"
        }
    },
    "created": "2022-11-03T09:40:21Z",
    "module": "1",
    "visitType": "LMT",
    "timepoint": "15min",
    "sample": {
        "test": "PST8",
        "description": "8 mL PST",
        "collected": "2022-11-03T09:45:49Z",
        "finalized": "2022-11-03T10:55:41Z"
    },
    "aliquots": [{
        "id": "123",
        "identifier": "LHPSTP1",
        "container": "1.4mL Matrix Tube (500 uL)",
        "volume": "450uL",
        "description": "1.4 mL matrix tubes",
        "collected": "2022-11-03T09:45:49Z"
    }, {
        "id": "456",
        "identifier": "LHPSTP1",
        "container": "1.4mL Matrix Tube (1000 uL)",
        "volume": "970uL",
        "description": "1.4 mL matrix tubes",
        "collected": "2022-11-03T09:45:49Z"
    }, {
        "id": "789",
        "identifier": "LHPSTP1",
        "container": "1.4mL Matrix Tube (1000 uL)",
        "volume": "970uL",
        "description": "1.4 mL matrix tubes",
        "collected": "2022-11-03T09:45:49Z"
    }, ],
    "notes": {
        "collected": "Test notes 1",
        "finalized": "Test notes 2"
    }
}

PATCH_SAMPLE = {
                "status": "restored",
                "amendedReason": "ORDER_RESTORE_WRONG_PARTICIPANT",
                "restoredInfo": {
                      "author": {
                                    "system": "https://www.pmi-ops.org/nph-username",
                                    "value": "test@pmi-ops.org"
                      },
                      "site": {
                                "system": "https://www.pmi-ops.org/site-id",
                                "value": "nph-site-testa"
                       }
                }
}


class TestNPHParticipantOrderAPI(TestCase):

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    @patch('rdr_service.api.nph_participant_biobank_order_api.database_factory')
    @patch('rdr_service.dao.study_nph_dao.NphParticipantDao.get_participant')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_post(self, site_id, pid, database_factor, query_filter):
        query_filter.return_value.first.return_value = StudyCategory()
        database_factor.return_value.session.return_value = MagicMock()
        pid.return_value = 124820391
        site_id.return_value = 1
        queries = [BLOOD_SAMPLE]
        for query in queries:
            executed = app.test_client().post('rdr/v1/api/v1/nph/Participant/1000124820391/BiobankOrder', json=query)
            result = json.loads(executed.data.decode('utf-8'))
            for k, _ in result.items():
                if k.upper() != "ID":
                    self.assertEqual(query.get(k), result.get(k))

    @patch('rdr_service.dao.study_nph_dao.NphOrderDao.get_order')
    @patch('rdr_service.api.nph_participant_biobank_order_api.database_factory')
    @patch('rdr_service.dao.study_nph_dao.NphParticipantDao.get_participant')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_patch(self, site_id, pid, database_factor, order_id):
        order_id.return_value = Order(id=1, participant_id=124820391)
        database_factor.return_value.session.return_value = MagicMock()
        pid.return_value = 124820391
        site_id.return_value = 1
        queries = [PATCH_SAMPLE]
        for query in queries:
            executed = app.test_client().patch('rdr/v1/api/v1/nph/Participant/1000124820391/BiobankOrder/1', json=query)
            result = json.loads(executed.data.decode('utf-8'))
            for k, _ in result.items():
                if k.upper() != "ID":
                    self.assertEqual(query.get(k), result.get(k))

    @patch('rdr_service.dao.study_nph_dao.NphOrderedSampleDao._get_child_order_sample')
    @patch('rdr_service.dao.study_nph_dao.NphOrderedSampleDao._get_parent_order_sample')
    @patch('rdr_service.dao.study_nph_dao.NphStudyCategoryDao.get_study_category_sample')
    @patch('rdr_service.dao.study_nph_dao.NphOrderDao.check_order_exist')
    @patch('rdr_service.dao.study_nph_dao.NphOrderDao.get_order')
    @patch('rdr_service.api.nph_participant_biobank_order_api.database_factory')
    @patch('rdr_service.dao.study_nph_dao.NphParticipantDao.check_participant_exist')
    @patch('rdr_service.dao.study_nph_dao.NphParticipantDao.get_participant')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.site_exist')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_put(self, site_id, site_exist, pid, p_exist, database_factor, order_id, order_exist,
                 sc_exist, parent_os, child_os):
        child_os.return_value = []
        parent_os.return_value = OrderedSample()
        sc_exist.return_value = StudyCategory(name="15min"), StudyCategory(name="LMT"), StudyCategory(name="1")
        p_exist.return_value = True
        order_exist.return_value = True, Order(id=1, participant_id=124820391)
        order_id.return_value = Order(id=1, participant_id=124820391)
        database_factor.return_value.session.return_value = MagicMock()
        pid.return_value = 124820391
        site_id.return_value = 1
        site_exist.return_value = True
        queries = [BLOOD_SAMPLE]
        for query in queries:
            executed = app.test_client().put('rdr/v1/api/v1/nph/Participant/1000124820391/BiobankOrder/1', json=query)
            result = json.loads(executed.data.decode('utf-8'))
            for k, _ in result.items():
                if k.upper() != "ID":
                    self.assertEqual(query.get(k), result.get(k))
