import datetime

from rdr_service import clock
from rdr_service.dao.biobank_specimen_dao import BiobankSpecimenDao, BiobankSpecimenAttributeDao,\
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
        super().setUp()
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
        return self.send_put(f"Biobank/specimens/{rlims_id}", request_data=payload)

    def get_minimal_specimen_json(self):
        return {
            'rlimsID': 'sabrina',
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
                                        self.is_matching_aliquot, "Expected nested aliquots to match")
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
                                        "Expected attributes to match")

        if 'aliquots' in test_json:
            self.assertCollectionsMatch(specimen_json['aliquots'], test_json['aliquots'], self.is_matching_aliquot,
                                        "Expected aliquots to match")

    def retrieve_specimen_json(self, specimen_id):
        specimen = self.dao.get(specimen_id)
        json = self.dao.to_client_json(specimen)
        return json

    @staticmethod
    def get_only_item_from_dao(dao):
        return dao.get_all()[0]

    def test_put_new_specimen_minimal_data(self):
        payload = self.get_minimal_specimen_json()
        rlims_id = payload['rlimsID']
        result = self.send_put(f"Biobank/specimens/{rlims_id}", request_data=payload)

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
            "name": "test",
            "value": "123"
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
