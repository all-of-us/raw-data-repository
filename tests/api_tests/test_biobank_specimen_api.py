import datetime
import http.client

from rdr_service import clock
from rdr_service.dao.biobank_specimen_dao import BiobankSpecimen, BiobankSpecimenDao, BiobankSpecimenAttributeDao,\
    BiobankAliquotDatasetItemDao, BiobankAliquotDao, BiobankAliquotDatasetDao
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model import config_utils
from rdr_service.model.participant import Participant
from rdr_service.model.biobank_order import BiobankOrderIdentifier, BiobankOrderedSample, BiobankOrder, BiobankAliquot
from tests.helpers.unittest_base import BaseTestCase

TIME_1 = datetime.datetime(2020, 4, 1)
TIME_2 = datetime.datetime(2020, 4, 2)


class BiobankOrderApiTest(BaseTestCase):
    def setUp(self):
        print('setting up test')
        super().setUp()
        print('finished parent setup')
        self.participant = Participant(participantId=123, biobankId=555)
        self.participant_dao = ParticipantDao()
        self.participant_dao.insert(self.participant)
        self.summary_dao = ParticipantSummaryDao()
        self.dao = BiobankSpecimenDao()
        self.bo_dao = BiobankOrderDao()

        ParticipantSummaryDao().insert(self.participant_summary(self.participant))
        self.bio_order = self.bo_dao.insert(self._make_biobank_order(participantId=self.participant.participantId))

    def _make_biobank_order(self, **kwargs):
        """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

        Kwargs pass through to BiobankOrder constructor, overriding defaults.
        """
        for k, default_value in (
            ("biobankOrderId", "1"),
            ("created", clock.CLOCK.now()),
            ("participantId", self.participant.participantId),
            ("sourceSiteId", 1),
            ("sourceUsername", "fred@pmi-ops.org"),
            ("collectedSiteId", 1),
            ("collectedUsername", "joe@pmi-ops.org"),
            ("processedSiteId", 1),
            ("processedUsername", "sue@pmi-ops.org"),
            ("finalizedSiteId", 2),
            ("finalizedUsername", "bob@pmi-ops.org"),
            ("identifiers", [BiobankOrderIdentifier(system="a", value="c")]),
            (
                "samples",
                [
                    BiobankOrderedSample(
                        biobankOrderId="1",
                        test='2SST8',
                        finalized=TIME_2,
                        description="description",
                        processingRequired=True,
                    )
                ],
            ),
        ):
            if k not in kwargs:
                kwargs[k] = default_value
        return BiobankOrder(**kwargs)

    def put_specimen(self, payload):
        rlims_id = payload['rlimsID']
        return self.send_put(f'Biobank/specimens/{rlims_id}', request_data=payload)

    def get_minimal_specimen_json(self, rlims_id='sabrina'):
        return {
            'rlimsID': rlims_id,
            'orderID': self.bio_order.biobankOrderId,
            'participantID': config_utils.to_client_biobank_id(self.participant.biobankId),
            'testcode': 'test 1234567'
        }

    @staticmethod
    def is_matching_json(actual_json, expected_json):
        for key in expected_json:
            if expected_json[key] != actual_json[key]:
                return False

        return True

    def is_matching_dataset(self, actual_dataset, expected_dataset):
        if 'datasetItems' in expected_dataset:
            for expected_item in expected_dataset['datasetItems']:
                if not any(self.is_matching_json(actual_item, expected_item)
                           for actual_item in actual_dataset['datasetItems']):
                    return False
            del expected_dataset['datasetItems']

        return BiobankOrderApiTest.is_matching_json(actual_dataset, expected_dataset)

    def is_matching_aliquot(self, actual_aliquot, expected_aliquot):
        if 'status' in expected_aliquot:
            if not self.is_matching_json(actual_aliquot['status'], expected_aliquot['status']):
                return False
            del expected_aliquot['status']

        if 'disposalStatus' in expected_aliquot:
            if not self.is_matching_json(actual_aliquot['disposalStatus'], expected_aliquot['disposalStatus']):
                return False
            del expected_aliquot['disposalStatus']

        if 'datasets' in expected_aliquot:
            for expected_dataset in expected_aliquot['datasets']:
                if not any(self.is_matching_dataset(actual_dataset, expected_dataset)
                           for actual_dataset in actual_aliquot['datasets']):
                    return False
            del expected_aliquot['datasets']

        if 'aliquots' in expected_aliquot:
            self.assertCollectionsMatch(actual_aliquot['aliquots'], expected_aliquot['aliquots'],
                                        self.is_matching_aliquot, 'Expected nested aliquots to match')
            del expected_aliquot['aliquots']

        return self.is_matching_json(actual_aliquot, expected_aliquot)

    def assertCollectionsMatch(self, actual_list, expected_list, comparator, message):
        for expected_item in expected_list:
            if not any(comparator(actual_item, expected_item) for actual_item in actual_list):
                self.fail(message)

    def assertSpecimenJsonMatches(self, specimen_json, test_json):
        for top_level_field in ['rlimsID', 'orderID', 'participantID', 'testcode', 'repositoryID', 'studyID',
                                'cohortID', 'sampleType', 'collectionDate', 'confirmationDate']:
            if top_level_field in test_json:
                self.assertEqual(test_json[top_level_field], specimen_json[top_level_field])

        if 'status' in test_json:
            for status_field in ['status', 'freezeThawCount', 'location', 'quantity', 'quantityUnits',
                                 'processingCompleteDate', 'deviations']:
                if status_field in test_json:
                    self.assertEqual(test_json['status'][status_field], specimen_json['status'][status_field])

        if 'disposalStatus' in test_json:
            for disposal_field in ['reason', 'disposalDate']:
                if disposal_field in test_json['disposalStatus']:
                    self.assertEqual(test_json['disposalStatus'][disposal_field],
                                     specimen_json['disposalStatus'][disposal_field])

        if 'attributes' in test_json:
            self.assertCollectionsMatch(specimen_json['attributes'], test_json['attributes'], self.is_matching_json,
                                        'Expected attributes to match')

        if 'aliquots' in test_json:
            self.assertCollectionsMatch(specimen_json['aliquots'], test_json['aliquots'], self.is_matching_aliquot,
                                        'Expected aliquots to match')

    def get_specimen_from_dao(self, _id=None, rlims_id=None):
        with self.dao.session() as session:
            if rlims_id is not None:
                filter_expr = BiobankSpecimen.rlimsId == rlims_id
            else:
                filter_expr = BiobankSpecimen.id == _id
            specimen = session.query(BiobankSpecimen).filter(filter_expr).one()
        return specimen

    def retrieve_specimen_json(self, specimen_id):
        specimen = self.get_specimen_from_dao(_id=specimen_id)
        json = self.dao.to_client_json(specimen)
        return json

    @staticmethod
    def get_only_item_from_dao(dao):
        return dao.get_all()[0]

    def test_put_new_specimen_minimal_data(self):
        payload = self.get_minimal_specimen_json()
        rlims_id = payload['rlimsID']
        result = self.send_put(f'Biobank/specimens/{rlims_id}', request_data=payload)

        saved_specimen_client_json = self.retrieve_specimen_json(result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

    def test_put_new_specimen_all_data(self):
        payload = self.get_minimal_specimen_json()
        payload.update({
            'repositoryID': 'repo id',
            'studyID': 'study id',
            'cohortID': 'cohort id',
            'sampleType': 'sample',
            'status': {
                'status': 'good',
                'freezeThawCount': 1,
                'location': 'Greendale',
                'quantity': '1',
                'quantityUnits': 'some units',
                'processingCompleteDate': TIME_2.isoformat(),
                'deviations': 'no deviation'
            },
            'disposalStatus': {
                'reason': 'contaminated',
                'disposalDate': TIME_2.isoformat()
            },
            'attributes': [
                {
                    'name': 'attr_one',
                    'value': '1'
                },
                {
                    'name': 'attr_two',
                    'value': 'two'
                }
            ],
            'collectionDate': TIME_1.isoformat(),
            'confirmationDate': TIME_2.isoformat()
        })
        result = self.put_specimen(payload)

        saved_specimen_client_json = self.retrieve_specimen_json(result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

    def test_put_specimen_exists(self):
        payload = self.get_minimal_specimen_json()
        initial_result = self.put_specimen(payload)

        payload['testcode'] = 'updated testcode'
        self.put_specimen(payload)

        updated_specimen_json = self.retrieve_specimen_json(initial_result['id'])
        self.assertSpecimenJsonMatches(updated_specimen_json, payload)

    def test_optional_args_not_cleared(self):
        initial_payload = self.get_minimal_specimen_json()
        initial_payload['sampleType'] = 'test type'
        initial_result = self.put_specimen(initial_payload)

        # Make a new request without the optional sampleType field
        new_payload = self.get_minimal_specimen_json()
        self.put_specimen(new_payload)

        # Make sure sampleType is still set on specimen
        updated_specimen_json = self.retrieve_specimen_json(initial_result['id'])
        self.assertSpecimenJsonMatches(updated_specimen_json, initial_payload)
        self.assertEqual(updated_specimen_json['sampleType'], 'test type')

    def test_add_attribute_to_existing_specimen(self):
        payload = self.get_minimal_specimen_json()
        initial_result = self.put_specimen(payload)

        payload['attributes'] = [{
            'name': 'test',
            'value': '123'
        }]
        self.put_specimen(payload)

        saved_specimen_client_json = self.retrieve_specimen_json(initial_result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

        attribute = self.get_only_item_from_dao(BiobankSpecimenAttributeDao())
        self.assertEqual(attribute.specimen_rlims_id, 'sabrina')

    def test_replacing_attributes(self):
        payload = self.get_minimal_specimen_json()
        payload['attributes'] = [
            {
                'name': 'attr_one',
                'value': '1'
            },
            {
                'name': 'attr_two',
                'value': 'two'
            }
        ]
        initial_result = self.put_specimen(payload)

        payload['attributes'] = [{
            'name': 'test',
            'value': '123'
        }]
        self.put_specimen(payload)

        saved_specimen_client_json = self.retrieve_specimen_json(initial_result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

    def test_update_attribute(self):
        payload = self.get_minimal_specimen_json()
        payload['attributes'] = [
            {
                'name': 'attr_one',
                'value': '1'
            }
        ]
        self.put_specimen(payload)

        attribute_dao = BiobankSpecimenAttributeDao()
        initial_attribute = self.get_only_item_from_dao(attribute_dao)

        payload['attributes'] = [{
            'name': 'attr_one',
            'value': '123'
        }]
        self.put_specimen(payload)

        final_attribute = self.get_only_item_from_dao(attribute_dao)
        self.assertEqual(initial_attribute.id, final_attribute.id)
        self.assertEqual(final_attribute.value, '123')

    def test_put_minimal_aliquot_data(self):
        payload = self.get_minimal_specimen_json()
        payload['aliquots'] = [
            {
                "rlimsID": "aliquot_one"
            },
            {
                "rlimsID": "second"
            }
        ]
        result = self.put_specimen(payload)

        saved_specimen_client_json = self.retrieve_specimen_json(result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

    def test_put_simple_aliquot(self):
        payload = self.get_minimal_specimen_json()
        payload['aliquots'] = [
            {
                "rlimsID": "other",
                "sampleType": "test",
                "status": {
                    "status": "frozen",
                    "freezeThawCount": 3,
                    "location": "biobank",
                    "quantity": "5",
                    "quantityUnits": "tube",
                    "processingCompleteDate": TIME_1.isoformat(),
                    "deviations": "no deviations"
                },
                "disposalStatus": {
                    "reason": "garbage",
                    "disposalDate": TIME_2.isoformat()
                },
                "childPlanService": "feed",
                "initialTreatment": "pill",
                "containerTypeID": "tubular",
            }
        ]
        result = self.put_specimen(payload)

        saved_specimen_client_json = self.retrieve_specimen_json(result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

        aliquot = self.get_only_item_from_dao(BiobankAliquotDao())
        self.assertEqual(aliquot.specimen_rlims_id, 'sabrina')

    def test_update_aliquot(self):
        payload = self.get_minimal_specimen_json()
        payload['aliquots'] = [
            {
                "rlimsID": "other",
                "sampleType": "test",
                "childPlanService": "feed"
            }
        ]
        self.put_specimen(payload)

        aliquot_dao = BiobankAliquotDao()
        initial_aliquot = self.get_only_item_from_dao(aliquot_dao)

        payload['aliquots'][0]['sampleType'] = 'check'
        self.put_specimen(payload)

        final_aliquot = self.get_only_item_from_dao(aliquot_dao)
        self.assertEqual(initial_aliquot.id, final_aliquot.id)
        self.assertEqual(final_aliquot.sampleType, 'check')

    def test_put_simple_aliquot_dataset(self):
        payload = self.get_minimal_specimen_json()
        payload['aliquots'] = [
            {
                'rlimsID': 'other',
                'datasets': [
                    {
                        'rlimsID': 'data_id',
                        'name': 'test set',
                        'status': 'nested'
                    }
                ]
            }
        ]
        result = self.put_specimen(payload)

        saved_specimen_client_json = self.retrieve_specimen_json(result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

        dataset = self.get_only_item_from_dao(BiobankAliquotDatasetDao())
        self.assertEqual(dataset.aliquot_rlims_id, 'other')

    def test_update_aliquot_dataset(self):
        payload = self.get_minimal_specimen_json()
        payload['aliquots'] = [
            {
                'rlimsID': 'other',
                'datasets': [
                    {
                        'rlimsID': 'first_data_set',
                        'name': 'placeholder',
                        'status': 'nested'
                    },
                    {
                        'rlimsID': 'data_id',
                        'name': 'test set',
                        'status': 'nested'
                    }
                ]
            }
        ]
        self.put_specimen(payload)

        dataset_dao = BiobankAliquotDatasetDao()
        initial_dataset = self.get_only_item_from_dao(dataset_dao)

        payload['aliquots'][0]['datasets'][0]['status'] = 'updated'
        self.put_specimen(payload)

        final_dataset = self.get_only_item_from_dao(dataset_dao)
        self.assertEqual(initial_dataset.id, final_dataset.id)
        self.assertEqual(final_dataset.status, 'updated')

    def test_put_simple_aliquot_dataset_items(self):
        payload = self.get_minimal_specimen_json()
        payload['aliquots'] = [
            {
                'rlimsID': 'other',
                'datasets': [
                    {
                        'rlimsID': 'data_id',
                        'datasetItems': [
                            {
                                'paramID': 'param1',
                                'displayValue': 'One',
                                'displayUnits': 'param'
                            }
                        ]
                    }
                ]
            }
        ]
        result = self.put_specimen(payload)

        saved_specimen_client_json = self.retrieve_specimen_json(result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

        dataset_item = self.get_only_item_from_dao(BiobankAliquotDatasetItemDao())
        self.assertEqual(dataset_item.dataset_rlims_id, 'data_id')

    def test_update_aliquot_dataset_item(self):
        payload = self.get_minimal_specimen_json()
        payload['aliquots'] = [
            {
                'rlimsID': 'other',
                'datasets': [
                    {
                        'rlimsID': 'data_id',
                        'datasetItems': [
                            {
                                'paramID': 'param1',
                                'displayValue': 'One',
                                'displayUnits': 'param'
                            }
                        ]
                    }
                ]
            }
        ]
        self.put_specimen(payload)

        dataset_item_dao = BiobankAliquotDatasetItemDao()
        initial_dataset_item = self.get_only_item_from_dao(dataset_item_dao)

        payload['aliquots'][0]['datasets'][0]['datasetItems'][0]['displayUnits'] = 'params'
        self.put_specimen(payload)

        final_dataset_item = self.get_only_item_from_dao(dataset_item_dao)
        self.assertEqual(initial_dataset_item.id, final_dataset_item.id)
        self.assertEqual(final_dataset_item.displayUnits, 'params')

    def test_put_nested_aliquots(self):
        payload = self.get_minimal_specimen_json()
        payload['aliquots'] = [
            {
                'rlimsID': 'grandparent',
                'aliquots': [
                    {
                        'rlimsID': 'parent',
                        'aliquots': [
                            {
                                'rlimsID': 'child'
                            }
                        ]
                    }
                ]
            }
        ]
        result = self.put_specimen(payload)

        saved_specimen_client_json = self.retrieve_specimen_json(result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

        aliquot_dao = BiobankAliquotDao()
        with aliquot_dao.session() as session:
            grand_child_aliquot = session.query(BiobankAliquot).filter(BiobankAliquot.rlimsId == 'child').one()
            self.assertEqual(grand_child_aliquot.specimen_rlims_id, 'sabrina')
            self.assertEqual(grand_child_aliquot.parent_aliquot_rlims_id, 'parent')

    def test_deeply_nested_aliquots(self):
        payload = self.get_minimal_specimen_json()
        aliquot = {
            'rlimsID': 'root'
        }
        payload['aliquots'] = [aliquot]
        for level in range(20):
            new_aliquot = {
                'rlimsID': f'aliquot_descendant_{level}'
            }
            aliquot['aliquots'] = [new_aliquot]
            aliquot = new_aliquot

        self.put_specimen(payload)

        aliquot_dao = BiobankAliquotDao()
        with aliquot_dao.session() as session:
            descendant_aliquot = session.query(BiobankAliquot).filter(
                BiobankAliquot.rlimsId == 'aliquot_descendant_19').one()
            self.assertEqual(descendant_aliquot.parent_aliquot_rlims_id, 'aliquot_descendant_18')

    def test_put_multiple_specimen(self):
        specimens = [self.get_minimal_specimen_json(rlims_id) for rlims_id in ['sabrina', 'salem']]
        specimens[0]['testcode'] = 'migration'
        specimens[1]['testcode'] = 'checking'

        result = self.send_put(f"Biobank/specimens", request_data=specimens)
        self.assertJsonResponseMatches(result, {
            'summary': 'Added 2 of 2 specimen'
        })

    def test_update_multiple_specimen(self):
        specimens = [self.get_minimal_specimen_json(rlims_id) for rlims_id in ['one', 'two', 'three', 'four', 'five']]
        inital_test_code = specimens[0]['testcode']
        result = self.send_put(f"Biobank/specimens", request_data=specimens)
        self.assertJsonResponseMatches(result, {
            'summary': 'Added 5 of 5 specimen'
        })

        third = self.get_specimen_from_dao(rlims_id='three')
        self.assertEqual(third.testCode, inital_test_code)
        fifth = self.get_specimen_from_dao(rlims_id='five')
        self.assertEqual(fifth.testCode, inital_test_code)

        specimens[2]['testcode'] = 'third test code'
        specimens[4]['testcode'] = 'checking last too'
        self.send_put(f"Biobank/specimens", request_data=specimens)

        third = self.get_specimen_from_dao(_id=third.id)
        self.assertEqual(third.testCode, 'third test code')
        fifth = self.get_specimen_from_dao(_id=fifth.id)
        self.assertEqual(fifth.testCode, 'checking last too')

    def test_error_missing_fields_specimen_migration(self):
        specimens = [self.get_minimal_specimen_json(rlims_id) for rlims_id in ['sabrina', 'two', 'salem', 'bob']]
        del specimens[0]['testcode']
        del specimens[0]['orderID']
        del specimens[1]['rlimsID']
        del specimens[1]['orderID']
        del specimens[2]['testcode']

        result = self.send_put(f"Biobank/specimens", request_data=specimens)
        self.assertJsonResponseMatches(result, {
            'summary': 'Added 1 of 4 specimen',
            'errors': [
                '[sabrina] Missing fields: orderID, testcode',
                '[specimen #2] Missing fields: rlimsID, orderID',
                '[salem] Missing fields: testcode'
            ]
        })

    def _create_minimal_specimen(self, rlims_id='sabrina'):
        return self.put_specimen(self.get_minimal_specimen_json(rlims_id))

    def test_parent_status_updated_all_fields(self):
        self._create_minimal_specimen()
        specimen = self.get_specimen_from_dao(rlims_id='sabrina')

        self.send_put(f"Biobank/specimens/sabrina/status", {
            'status': 'new',
            'freezeThawCount': 8,
            'location': 'Washington',
            'quantity': '3',
            'quantityUnits': 'some units',
            'processingCompleteDate': TIME_2.isoformat(),
            'deviations': 'no deviation'
        })

        specimen = self.get_specimen_from_dao(_id=specimen.id)
        self.assertEqual('new', specimen.status)
        self.assertEqual(8, specimen.freezeThawCount)
        self.assertEqual('Washington', specimen.location)
        self.assertEqual('3', specimen.quantity)
        self.assertEqual('some units', specimen.quantityUnits)
        self.assertEqual(TIME_2, specimen.processingCompleteDate)
        self.assertEqual('no deviation', specimen.deviations)

    def test_parent_status_updated_required_fields(self):
        self._create_minimal_specimen()
        specimen = self.get_specimen_from_dao(rlims_id='sabrina')
        self.assertIsNone(specimen.status)

        self.send_put(f"Biobank/specimens/sabrina/status", {
            'status': 'updated'
        })

        specimen = self.get_specimen_from_dao(_id=specimen.id)
        self.assertEqual('updated', specimen.status)

    def test_parent_status_update_not_found(self):
        self.send_put(f"Biobank/specimens/sabrina/status", {
            'status': 'updated'
        }, expected_status=http.client.NOT_FOUND)

    def test_parent_disposed_all_fields(self):
        self._create_minimal_specimen()
        specimen = self.get_specimen_from_dao(rlims_id='sabrina')

        self.send_put(f"Biobank/specimens/sabrina/disposalStatus", {
            'reason': 'contaminated',
            'disposalDate': TIME_2.isoformat()
        })

        specimen = self.get_specimen_from_dao(_id=specimen.id)
        self.assertEqual('contaminated', specimen.disposalReason)
        self.assertEqual(TIME_2, specimen.disposalDate)

    def test_parent_disposed_optional_fields_not_cleared(self):
        payload = self.get_minimal_specimen_json()
        payload['disposalStatus'] = {
            'reason': 'contaminated',
            'disposalDate': TIME_2.isoformat()
        }
        self.put_specimen(payload)
        specimen = self.get_specimen_from_dao(rlims_id='sabrina')

        self.send_put(f"Biobank/specimens/sabrina/disposalStatus", {
            'disposalDate': TIME_1.isoformat()
        })

        specimen = self.get_specimen_from_dao(_id=specimen.id)
        self.assertEqual('contaminated', specimen.disposalReason)
        self.assertEqual(TIME_1, specimen.disposalDate)

    def test_parent_disposed_required_fields(self):
        self._create_minimal_specimen()
        specimen = self.get_specimen_from_dao(rlims_id='sabrina')
        self.assertIsNone(specimen.disposalDate)

        self.send_put(f"Biobank/specimens/sabrina/disposalStatus", {
            'disposalDate': TIME_1.isoformat()
        })

        specimen = self.get_specimen_from_dao(_id=specimen.id)
        self.assertEqual(TIME_1, specimen.disposalDate)

    def test_parent_disposed_not_found(self):
        self.send_put(f"Biobank/specimens/sabrina/status", {
            'disposalDate': TIME_1.isoformat()
        }, expected_status=http.client.NOT_FOUND)

    def test_parent_attribute_created(self):
        result = self._create_minimal_specimen()
        specimen = self.retrieve_specimen_json(result['id'])
        self.assertIsNone(specimen['attributes'])

        self.send_put(f"Biobank/specimens/sabrina/attributes/attr1", {
            'value': 'test attribute'
        })

        specimen = self.retrieve_specimen_json(specimen['id'])
        attribute = specimen['attributes'][0]
        self.assertEqual('attr1', attribute['name'])
        self.assertEqual('test attribute', attribute['value'])

    def test_parent_attribute_update(self):
        payload = self.get_minimal_specimen_json()
        payload['attributes'] = [
            {
                'name': 'attr_one',
                'value': '1'
            },
            {
                'name': 'attr_two',
                'value': 'two'
            }
        ]
        initial_result = self.put_specimen(payload)

        self.send_put(f"Biobank/specimens/sabrina/attributes/attr_one", {
            'value': 'updated'
        })

        specimen = self.retrieve_specimen_json(initial_result['id'])
        attribute = specimen['attributes'][0]
        self.assertEqual('updated', attribute['value'])

    def test_parent_attribute_not_found(self):
        self.send_put(f"Biobank/specimens/sabrina/attributes/attr1", {
            'disposalDate': TIME_1.isoformat()
        }, expected_status=http.client.NOT_FOUND)

    def test_parent_aliquot_created(self):
        result = self._create_minimal_specimen()
        specimen = self.retrieve_specimen_json(result['id'])
        self.assertIsNone(specimen['aliquots'])

        self.send_put(f"Biobank/specimens/sabrina/aliquots/first", {
            'sampleType': 'first sample',
            'containerTypeID': 'tube'
        })

        specimen = self.retrieve_specimen_json(specimen['id'])
        aliquot = specimen['aliquots'][0]
        self.assertEqual('first sample', aliquot['sampleType'])
        self.assertEqual('tube', aliquot['containerTypeID'])

    def test_parent_aliquot_update(self):
        payload = self.get_minimal_specimen_json()
        payload['aliquots'] = [
            {
                'rlimsID': 'salem',
                'sampleType': 'first sample',
                'containerTypeID': 'tube'
            }
        ]
        initial_result = self.put_specimen(payload)

        self.send_put(f"Biobank/specimens/sabrina/aliquots/salem", {
            'sampleType': 'updated'
        })

        specimen = self.retrieve_specimen_json(initial_result['id'])
        aliquot = specimen['aliquots'][0]
        self.assertEqual('updated', aliquot['sampleType'])
        self.assertEqual('tube', aliquot['containerTypeID'])

    def _create_minimal_specimen_with_aliquot(self, rlims_id='sabrina', aliquot_rlims_id='salem'):
        payload = self.get_minimal_specimen_json(rlims_id)
        payload['aliquots'] = [{
            'rlimsID': aliquot_rlims_id
        }]
        return self.put_specimen(payload)

    def test_aliquot_status_updated_all_fields(self):
        result = self._create_minimal_specimen_with_aliquot()

        self.send_put(f"Biobank/aliquots/salem/status", {
            'status': 'new',
            'freezeThawCount': 8,
            'location': 'Washington',
            'quantity': '3',
            'quantityUnits': 'some units',
            'processingCompleteDate': TIME_2.isoformat(),
            'deviations': 'no deviation'
        })

        specimen = self.retrieve_specimen_json(result['id'])
        aliquot_status = specimen['aliquots'][0]['status']
        self.assertEqual('new', aliquot_status['status'])
        self.assertEqual(8, aliquot_status['freezeThawCount'])
        self.assertEqual('Washington', aliquot_status['location'])
        self.assertEqual('3', aliquot_status['quantity'])
        self.assertEqual('some units', aliquot_status['quantityUnits'])
        self.assertEqual(TIME_2.isoformat(), aliquot_status['processingCompleteDate'])
        self.assertEqual('no deviation', aliquot_status['deviations'])

    def test_aliquot_status_updated_required_fields(self):
        result = self._create_minimal_specimen_with_aliquot()
        specimen = self.retrieve_specimen_json(result['id'])
        self.assertIsNone(specimen['aliquots'][0]['status']['status'])

        self.send_put(f'Biobank/aliquots/salem/status', {
            'status': 'updated'
        })

        specimen = self.retrieve_specimen_json(specimen['id'])
        self.assertEqual('updated', specimen['aliquots'][0]['status']['status'])

    def test_aliquot_disposed_all_fields(self):
        result = self._create_minimal_specimen_with_aliquot()

        self.send_put(f'Biobank/aliquots/salem/disposalStatus', {
            'reason': 'contaminated',
            'disposalDate': TIME_2.isoformat()
        })

        specimen = self.retrieve_specimen_json(result['id'])
        aliquot_disposal_status = specimen['aliquots'][0]['disposalStatus']
        self.assertEqual('contaminated', aliquot_disposal_status['reason'])
        self.assertEqual(TIME_2.isoformat(), aliquot_disposal_status['disposalDate'])

    def test_aliquot_dataset_created(self):
        result = self._create_minimal_specimen_with_aliquot()
        specimen = self.retrieve_specimen_json(result['id'])
        self.assertIsNone(specimen['aliquots'][0]['datasets'])

        self.send_put(f'Biobank/aliquots/salem/datasets/data1', {
            'status': 'created',
            'datasetItems': [
                {
                    'paramID': 'param1',
                    'displayValue': 'One',
                    'displayUnits': 'param'
                }
            ]
        })

        specimen = self.retrieve_specimen_json(specimen['id'])
        dataset = specimen['aliquots'][0]['datasets'][0]
        self.assertEqual('created', dataset['status'])
        self.assertEqual('One', dataset['datasetItems'][0]['displayValue'])

    def test_aliquot_dataset_update(self):
        payload = self.get_minimal_specimen_json()
        payload['aliquots'] = [
            {
                'rlimsID': 'salem',
                'datasets': [
                    {
                        'rlimsID': 'data_one',
                        'datasetItems': [
                            {
                                'paramID': 'param_one'
                            }
                        ]
                    },
                    {
                        'rlimsID': 'data_two',
                        'datasetItems': [
                            {
                                'paramID': 'param_one'
                            }
                        ]
                    }
                ]
            }
        ]
        result = self.put_specimen(payload)

        self.send_put(f'Biobank/aliquots/salem/datasets/data_two', {
            'rlimsID': 'data_two',
            'status': 'updated',
            'datasetItems': [
                {
                    'paramID': 'param_one',
                    'displayValue': 'foobar'
                }
            ]
        })

        specimen = self.retrieve_specimen_json(result['id'])
        dataset = specimen['aliquots'][0]['datasets'][1]
        self.assertEqual('updated', dataset['status'])
        self.assertEqual('foobar', dataset['datasetItems'][0]['displayValue'])
