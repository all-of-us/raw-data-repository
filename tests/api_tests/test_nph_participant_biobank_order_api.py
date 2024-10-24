# Sample ID = NP124820391
from datetime import datetime
import json
from sqlalchemy.orm import Query
from unittest.mock import MagicMock, patch

from rdr_service.dao.rex_dao import RexStudyDao
from rdr_service.dao.study_nph_dao import NphOrderedSampleDao
from rdr_service.offline.study_nph_biobank_file_export import get_processing_timestamp
from rdr_service.data_gen.generators.nph import NphDataGenerator, NphSmsDataGenerator
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.dao import database_factory
from rdr_service.main import app
from rdr_service.model.study_nph import (
    StudyCategory, Order, OrderedSample, Participant, SampleUpdate, Site, DlwDosage
)
from tests.workflow_tests.test_data.test_biobank_order_payloads import (SALIVA_DIET_SAMPLE, URINE_DIET_SAMPLE,
                                                                        STOOL_DIET_SAMPLE)

BLOOD_SAMPLE = {
    "subject": "Patient/P124820391",
    "identifier": [{
        "system": "http://www.pmi-ops.org/order-id",
        "value": "nph-order-id-123"
    }, {
        "system": "http://www.pmi-ops.org/sample-id",
        "value": "nph-sample-id-456"
    },  {
            "system": "https://www.pmi-ops.org/client-id",
            "value": "7042688"
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
        "volume": "450",
        "units": "uL",
        "description": "1.4 mL matrix tubes",
        "collected": "2022-11-03T09:45:49Z"
    }, {
        "id": "456",
        "identifier": "LHPSTP1",
        "container": "1.4mL Matrix Tube (1000 uL)",
        "volume": "970",
        "units": "uL",
        "description": "1.4 mL matrix tubes",
        "collected": "2022-11-03T09:45:49Z"
    }, {
        "id": "789",
        "identifier": "LHPSTP1",
        "container": "1.4mL Matrix Tube (1000 uL)",
        "volume": "970",
        "units": "uL",
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

PATCH_CANCEL_SAMPLE = {
                "status": "cancelled",
                "amendedReason": "CANCEL_ERROR",
                "cancelledInfo": {
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


class TestNPHParticipantOrderAPI(BaseTestCase):

    def setUp(self, *args, **kwargs) -> None:
        super().setUp(*args, **kwargs)
        self.nph_datagen = NphDataGenerator()
        self.sms_datagen = NphSmsDataGenerator()

    @staticmethod
    def _create_initial_study_data():
        study_dao = RexStudyDao()
        aou = study_dao.model_type(schema_name='rdr')
        nph = study_dao.model_type(schema_name='nph')
        study_dao.insert(aou)
        study_dao.insert(nph)

    def setup_backend_for_diet_orders(self):
        self._create_initial_study_data()
        self.nph_datagen.create_database_site(
            external_id="test-site-1",
            name="Test Site 1",
            awardee_external_id="test-hpo-1",
            organization_external_id="test-org-1"
        )
        self.sms_datagen.create_database_study_category(
            name="3",
            type_label="module"
        )
        self.sms_datagen.create_database_study_category(
            name="Diet",
            type_label="visitType",
            parent_id=1,
        )
        self.sms_datagen.create_database_study_category(
            name="Day 0",
            type_label="timepoint",
            parent_id=2,
        )
        self.nph_datagen.create_database_participant(
            id=100001,
            biobank_id=11110000101
        )
        aou_participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(participant=aou_participant)
        self.nph_datagen.create_database_rex_participant_mapping(
            primary_participant_id=aou_participant.participantId,
            ancillary_participant_id=100001,
        )

    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    @patch('rdr_service.api.nph_participant_biobank_order_api.database_factory')
    @patch('rdr_service.dao.study_nph_dao.NphParticipantDao.get_participant_by_id')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_post(self, site_id, pid, database_factor, query_filter):
        query_filter.return_value.first.return_value = StudyCategory()
        database_factor.return_value.session.return_value = MagicMock()
        pid.return_value = Participant(id=124820391)
        site_id.return_value = 1
        queries = [BLOOD_SAMPLE]
        for query in queries:
            executed = app.test_client().post('rdr/v1/api/v1/nph/Participant/1000124820391/BiobankOrder', json=query)
            result = json.loads(executed.data.decode('utf-8'))
            for k, _ in result.items():
                if k.upper() != "ID":
                    self.assertEqual(query.get(k), result.get(k))
        with database_factory.get_database().session() as session:
            query = Query(SampleUpdate)
            query.session = session
            sample_update_result = query.all()
            for each in sample_update_result:
                self.assertIsNotNone(each.ordered_sample_json)

    @patch('rdr_service.dao.study_nph_dao.NphOrderDao.get_order')
    @patch('rdr_service.api.nph_participant_biobank_order_api.database_factory')
    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_patch_update(self, site_id, query_filter, database_factor, order_id):
        order_id.return_value = Order(id=1, participant_id=124820391)
        database_factor.return_value.session.return_value = MagicMock()
        query_filter.return_value.first.return_value = Participant(id=124820391)
        site_id.return_value = 1
        queries = [PATCH_SAMPLE]
        for query in queries:
            executed = app.test_client().patch('rdr/v1/api/v1/nph/Participant/1000124820391/BiobankOrder/1', json=query)
            result = json.loads(executed.data.decode('utf-8'))
            for k, _ in result.items():
                if k.upper() != "ID":
                    self.assertEqual(query.get(k), result.get(k))

    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_patch_cancel(self, site_id):
        participant = Participant(id=12345, biobank_id=12345)
        site = Site(id=1)
        self.session.add(participant)
        self.session.add(site)
        self.session.commit()
        self.session.add(
            Order(
                id=1,
                participant_id=participant.id,
                notes={},
                samples=[
                    OrderedSample(id=1, collected=datetime.utcnow()),
                    OrderedSample(id=2)
                ]
            )
        )
        self.session.commit()

        site_id.return_value = 1
        patch_json = PATCH_CANCEL_SAMPLE

        response = self.send_patch(f'api/v1/nph/Participant/{participant.id}/BiobankOrder/1', patch_json)

        del response['id']
        self.assertDictEqual(patch_json, response)

        sample_update_list = self.session.query(SampleUpdate).all()
        self.assertListEqual([1, 2], [sample_update.rdr_ordered_sample_id for sample_update in sample_update_list])

    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_put_cancel(self, site_id):
        participant = Participant(id=12345, biobank_id=12345)
        site = Site(id=1, external_id='nph-site-testa')
        self.session.add(participant)
        self.session.add(site)

        timepoint = StudyCategory(
            name='15min',
            type_label='timepoint',
            parent=StudyCategory(
                name='LMT',
                type_label='visitType',
                parent=StudyCategory(
                    name='1',
                    type_label='module'
                )
            )
        )
        self.session.add(timepoint)
        self.session.commit()
        self.session.add(
            Order(
                id=1,
                participant_id=participant.id,
                notes={},
                category_id=timepoint.id,
                samples=[
                    OrderedSample(
                        id=1,
                        collected=datetime.utcnow(),
                        description='parent sample',
                        children=[
                            OrderedSample(
                                aliquot_id='a123',
                                order_id=1
                            )
                        ]
                    )
                ]
            )
        )
        self.session.commit()

        site_id.return_value = 1
        patch_json = {
            "subject": f"Patient/P{participant.id}",
            "identifier": [
                {
                    "system": "http://www.pmi-ops.org/order-id",
                    "value": "nph-order-id-123"
                }, {
                    "system": "http://www.pmi-ops.org/sample-id",
                    "value": "nph-sample-id-456"
                },  {
                        "system": "https://www.pmi-ops.org/client-id",
                        "value": "7042688"
                }
            ],
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
                    "value": "nph-site-testa"
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
            "aliquots": [
                {
                    "id": "a123",
                    "identifier": "a123",
                    "container": "1.4mL Matrix Tube (500 uL)",
                    "volume": "450",
                    "units": "uL",
                    "description": "1.4 mL matrix tubes",
                    "collected": "2022-11-03T09:45:49Z",
                    "status": "cancel"
                }
            ],
            "notes": {}
        }

        self.send_put(
            f'api/v1/nph/Participant/{participant.id}/BiobankOrder/1',
            patch_json,
            expected_status=201
        )
        aliquot = self.session.query(OrderedSample).filter(OrderedSample.aliquot_id == 'a123').one()
        self.assertEqual('cancelled', aliquot.status)

    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_patch_aliquot_update(self, site_id):
        participant = Participant(id=12345, biobank_id=12345)
        site = Site(id=1)
        self.session.add(participant)
        self.session.add(site)
        self.session.commit()
        test_order = Order(
            id=1,
            participant_id=participant.id,
            notes={},
            samples=[
                OrderedSample(
                    id=1,
                    description='update this',
                    children=[
                        OrderedSample(aliquot_id='a12', volume='error'),
                        OrderedSample(aliquot_id='c34', description='to be cancelled')
                    ]
                )
            ]
        )
        self.session.add(test_order)
        self.session.commit()

        site_id.return_value = 1

        self.send_patch(
            f'api/v1/nph/Participant/{participant.id}/BiobankOrder/1',
            {
                'status': 'amended',
                "amendedInfo": {
                    "author": {
                        "system": "https://www.pmi-ops.org\/nph-username",
                        "value": "test@example.com"
                    },
                    "site": {
                        "system": "https://www.pmi-ops.org\/site-id",
                        "value": "nph-site-testa"
                    }
                },
                'sample': {
                    'description': 'updated'
                },
                "aliquots": [
                    {
                        "id": "a12",
                        "volume": 450
                    }, {
                        "id": "456",
                        "identifier": "new1",
                        "container": "matrix tube",
                        "volume": "970",
                        "units": "uL",
                        "description": "1.4 mL matrix tubes",
                        "collected": "2022-11-03T09:45:49Z"
                    }
                ]
            }
        )

        self.session.expire_all()  # Force the order to be refresh
        db_order: Order = self.session.query(Order).filter(Order.id == 1).one()

        # check updates on parent sample
        parent_sample = db_order.samples[0]
        self.assertEqual('updated', parent_sample.description)

        for aliquot in parent_sample.children:
            if aliquot.aliquot_id == 'a12':  # check that the volume updated
                self.assertEqual('450', aliquot.volume)
            elif aliquot.aliquot_id == 'c34':  # check that the aliquot got cancelled
                self.assertEqual('cancelled', aliquot.status)

    @patch('rdr_service.dao.study_nph_dao.NphOrderedSampleDao._get_child_order_sample')
    @patch('rdr_service.dao.study_nph_dao.NphOrderedSampleDao._get_parent_order_sample')
    @patch('rdr_service.dao.study_nph_dao.NphStudyCategoryDao.get_study_category_sample')
    @patch('rdr_service.dao.study_nph_dao.NphOrderDao.check_order_exist')
    @patch('rdr_service.dao.study_nph_dao.NphOrderDao.get_order')
    @patch('rdr_service.api.nph_participant_biobank_order_api.database_factory')
    @patch('rdr_service.dao.study_nph_dao.NphParticipantDao.get_participant_by_id')
    @patch('rdr_service.dao.study_nph_dao.Query.filter')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.site_exist')
    @patch('rdr_service.dao.study_nph_dao.NphSiteDao.get_id')
    def test_put(self, site_id, site_exist, query_filter, p_exist, database_factor, order_id, order_exist,
                 sc_exist, parent_os, child_os):
        child_os.return_value = []
        parent_os.return_value = OrderedSample()
        sc_exist.return_value = StudyCategory(name="15min"), StudyCategory(name="LMT"), StudyCategory(name="1")
        p_exist.return_value = True
        order_exist.return_value = True, Order(id=1, participant_id=124820391)
        order_id.return_value = Order(id=1, participant_id=124820391)
        database_factor.return_value.session.return_value = MagicMock()
        query_filter.return_value.first.return_value = Participant(id=124820391)
        site_id.return_value = 1
        site_exist.return_value = True
        queries = [BLOOD_SAMPLE]
        for query in queries:
            executed = app.test_client().put('rdr/v1/api/v1/nph/Participant/1000124820391/BiobankOrder/1', json=query)
            result = json.loads(executed.data.decode('utf-8'))
            for k, _ in result.items():
                if k.upper() != "ID":
                    self.assertEqual(query.get(k), result.get(k))
        with database_factory.get_database().session() as session:
            query = Query(SampleUpdate)
            query.session = session
            result = query.all()
            for each in result:
                self.assertIsNotNone(each.ordered_sample_json)

    def test_diet_saliva_order(self):
        self.setup_backend_for_diet_orders()
        app.test_client().post('rdr/v1/api/v1/nph/Participant/100001/BiobankOrder', json=SALIVA_DIET_SAMPLE)
        dao = NphOrderedSampleDao()
        aliquot = dao.get_from_aliquot_id('456')[0]
        expected_supplement = {"glycerolAdditiveVolume": {"units": "uL", "volume": 1000}}
        self.assertEqual(expected_supplement, aliquot.supplemental_fields)

    def test_diet_urine_order(self):
        self.setup_backend_for_diet_orders()
        app.test_client().post('rdr/v1/api/v1/nph/Participant/100001/BiobankOrder', json=URINE_DIET_SAMPLE)
        dao = NphOrderedSampleDao()
        sample = dao.get(1)
        expected_supplement = {
            "dlwdose":
                {
                    "dose": "84",
                    "batchid": "NPHDLW9172397",
                    "dosetime": "2022-11-03T08:45:49Z",
                    "calculateddose": "84.57",
                    "participantweight": "56.38"
                }
        }
        self.assertEqual(expected_supplement, sample.supplemental_fields)

    def test_freeze_stool_order(self):
        self.setup_backend_for_diet_orders()
        app.test_client().post('rdr/v1/api/v1/nph/Participant/100001/BiobankOrder', json=STOOL_DIET_SAMPLE)
        dao = NphOrderedSampleDao()
        sample = dao.get(1)
        sample_freeze_date = datetime.strptime(sample.supplemental_fields["freezed"], "%Y-%m-%dT%H:%M:%SZ")
        freezeDateUTC = datetime.strptime("2022-11-03 10:30:49", "%Y-%m-%d %H:%M:%S")
        self.assertEqual(freezeDateUTC, sample_freeze_date)
        processingDateUTC = get_processing_timestamp(sample)
        self.assertEqual(freezeDateUTC, processingDateUTC)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("nph.ordered_sample")
        self.clear_table_after_test("nph.order")
        self.clear_table_after_test("nph.site")
        self.clear_table_after_test("nph.study_category")
        self.clear_table_after_test("nph.participant")
        self.clear_table_after_test("nph.sample_update")
        self.clear_table_after_test("rex.participant_mapping")
        self.clear_table_after_test("rex.study")


class DLWDosageApiTest(BaseTestCase):

    def setUp(self, *args, **kwargs) -> None:
        super().setUp(*args, **kwargs)
        self.nph_datagen = NphDataGenerator()
        self.nph_pid = 10000
        self.nph_datagen.create_database_participant(id=self.nph_pid)

    def test_post(self):
        payload = {
            "module": "3",
            "visitperiod": "Period1DLW",
            "batchid": "NPHDLW9172397",
            "participantweight": "56.38",
            "dose": "84",
            "calculateddose": "84.57",
            "dosetime": "2022-11-03T08:45:49Z",
        }
        response = app.test_client().post(
            f"rdr/v1/api/v1/nph/Participant/{self.nph_pid}/DlwDosage", json=payload
        )
        self.assertEqual(200, response.status_code)

        dlw_dosage = self.session.query(DlwDosage).filter(DlwDosage.participant_id == self.nph_pid).all()

        self.assertEqual(dlw_dosage[0].id, response.get_json())
        self.assertEqual("3", str(dlw_dosage[0].module.number))
        self.assertEqual("PERIOD1DLW", dlw_dosage[0].visit_period.name)
        self.assertEqual("NPHDLW9172397", dlw_dosage[0].batch_id)
        self.assertEqual("56.38", dlw_dosage[0].participant_weight)
        self.assertEqual("84", dlw_dosage[0].dose)
        self.assertEqual("84.57", dlw_dosage[0].calculated_dose)
        self.assertEqual("2022-11-03T08:45:49Z", dlw_dosage[0].dose_time.strftime('%Y-%m-%dT%H:%M:%SZ'))

    def test_post_with_invalid_module_and_visit_period(self):
        payload_with_invalid_module = {
            "module": "6",
            "visitperiod": "Period1DLW",
            "batchid": "NPHDLW9172397",
            "participantweight": "56.38",
            "dose": "84",
            "calculateddose": "84.57",
            "dosetime": "2022-11-03T08:45:49Z",
        }
        response1 = app.test_client().post(
            f"rdr/v1/api/v1/nph/Participant/{self.nph_pid}/DlwDosage", json=payload_with_invalid_module
        )
        self.assertEqual(400, response1.status_code)

        payload_with_invalid_visit_period = {
            "module": "2",
            "visitperiod": "Period6DLW",
            "batchid": "NPHDLW9172397",
            "participantweight": "56.38",
            "dose": "84",
            "calculateddose": "84.57",
            "dosetime": "2022-11-03T08:45:49Z",
        }
        response2 = app.test_client().post(
            f"rdr/v1/api/v1/nph/Participant/{self.nph_pid}/DlwDosage", json=payload_with_invalid_visit_period
        )
        self.assertEqual(400, response2.status_code)

    def test_post_with_missing_values(self):
        invalid_payload = {
            "module": "2",
            "visitperiod": "",
            "batchid": "NPHDLW9172397",
            "participantweight": "56.38",
            "dose": "84",
            "calculateddose": "84.57",
            "dosetime": "2022-11-03T08:45:49Z",
        }
        response = app.test_client().post(
            f"rdr/v1/api/v1/nph/Participant/{self.nph_pid}/DlwDosage", json=invalid_payload
        )
        self.assertEqual(400, response.status_code)

    def test_put(self):
        payload = {
            "module": "3",
            "visitperiod": "Period1DLW",
            "batchid": "NPHDLW9172397",
            "participantweight": "56.38",
            "dose": "84",
            "calculateddose": "84.57",
            "dosetime": "2022-11-03T08:45:49Z",
        }
        response_post = app.test_client().post(
            f"rdr/v1/api/v1/nph/Participant/{self.nph_pid}/DlwDosage", json=payload
        )
        self.assertEqual(200, response_post.status_code)

        # Update "dose" to test if PUT worked
        payload["dose"] = 90
        response_put = app.test_client().put(
            f"rdr/v1/api/v1/nph/Participant/{self.nph_pid}/DlwDosage/1", json=payload
        )

        self.assertEqual(200, response_put.status_code)

        dlw_dosage = self.session.query(DlwDosage).filter(DlwDosage.participant_id == self.nph_pid).all()

        self.assertEqual("3", str(dlw_dosage[0].module.number))
        self.assertEqual("PERIOD1DLW", dlw_dosage[0].visit_period.name)
        self.assertEqual("NPHDLW9172397", dlw_dosage[0].batch_id)
        self.assertEqual("56.38", dlw_dosage[0].participant_weight)
        self.assertEqual("90", dlw_dosage[0].dose)
        self.assertEqual("84.57", dlw_dosage[0].calculated_dose)
        self.assertEqual("2022-11-03T08:45:49Z", dlw_dosage[0].dose_time.strftime('%Y-%m-%dT%H:%M:%SZ'))

    def test_put_with_incorrect_id(self):
        payload = {
            "module": "3",
            "visitperiod": "Period1DLW",
            "batchid": "NPHDLW9172397",
            "participantweight": "56.38",
            "dose": "84",
            "calculateddose": "84.57",
            "dosetime": "2022-11-03T08:45:49Z",
        }
        response_post = app.test_client().post(
            f"rdr/v1/api/v1/nph/Participant/{self.nph_pid}/DlwDosage", json=payload
        )
        self.assertEqual(200, response_post.status_code)

        invalid_id = 9
        response_put = app.test_client().put(
            f"rdr/v1/api/v1/nph/Participant/{self.nph_pid}/DlwDosage/{invalid_id}", json=payload
        )
        self.assertEqual(404, response_put.status_code)
        self.assertIn(f"{invalid_id} does not exist", response_put.get_json())

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("nph.participant")
        self.clear_table_after_test("nph.dlw_dosage")
