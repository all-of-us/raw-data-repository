import datetime

from rdr_service import clock
from rdr_service.dao.biobank_specimen_dao import BiobankSpecimenDao
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model import config_utils
from rdr_service.model.participant import Participant
from rdr_service.model.biobank_order import BiobankOrderIdentifier, BiobankOrderedSample, BiobankOrder
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

    def putSpecimen(self, payload):
        rlims_id = payload['rlimsID']
        return self.send_put(f"Biobank/specimens/{rlims_id}", request_data=payload)

    @staticmethod
    def isMatchingAttribute(specimen_attribute, test_attribute):
        return specimen_attribute['name'] == test_attribute['name'] and\
               specimen_attribute['value'] == test_attribute['value']

    def assertAttributesMatch(self, specimen_attributes, test_attributes):
        for test_attribute in test_attributes:
            if not any(self.isMatchingAttribute(specimen_attribute, test_attribute) for
                       specimen_attribute in specimen_attributes):
                self.fail("Attribute not found on specimen")

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
            self.assertAttributesMatch(specimen_json['attributes'], test_json['attributes'])

    def retrieve_specimen_json(self, specimen_id):
        specimen = self.dao.get(specimen_id)
        json = self.dao.to_client_json(specimen)
        return json

    def test_put_new_specimen_minimal_data(self):
        payload = {
            'rlimsID': 'sabrina',
            'orderID': self.bio_order.biobankOrderId,
            'participantID': config_utils.to_client_biobank_id(self.participant.biobankId),
            'testcode': 'test 1234567'
        }
        rlims_id = payload['rlimsID']
        result = self.send_put(f"Biobank/specimens/{rlims_id}", request_data=payload)

        saved_specimen_client_json = self.retrieve_specimen_json(result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

    def test_put_new_specimen_all_data(self):
        payload = {
            'rlimsID': 'sabrina',
            'orderID': self.bio_order.biobankOrderId,
            'participantID': config_utils.to_client_biobank_id(self.participant.biobankId),
            'testcode': 'test 1234567',
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
        }
        rlims_id = payload['rlimsID']
        result = self.send_put(f"Biobank/specimens/{rlims_id}", request_data=payload)

        saved_specimen_client_json = self.retrieve_specimen_json(result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

    def test_put_specimen_exists(self):
        payload = {
            'rlimsID': 'sabrina',
            'orderID': self.bio_order.biobankOrderId,
            'participantID': config_utils.to_client_biobank_id(self.participant.biobankId),
            'testcode': 'test 1234567'
        }
        rlims_id = payload['rlimsID']
        initial_result = self.send_put(f"Biobank/specimens/{rlims_id}", request_data=payload)

        new_payload = payload
        new_payload['testcode'] = 'updated testcode'
        self.send_put(f"Biobank/specimens/{rlims_id}", request_data=new_payload)

        updated_specimen_json = self.retrieve_specimen_json(initial_result['id'])
        self.assertSpecimenJsonMatches(updated_specimen_json, new_payload)

    def test_optional_args_not_cleared(self):
        initial_payload = {
            'rlimsID': 'sabrina',
            'orderID': self.bio_order.biobankOrderId,
            'participantID': config_utils.to_client_biobank_id(self.participant.biobankId),
            'testcode': 'test 1234567',
            'sampleType': 'test type'
        }
        rlims_id = initial_payload['rlimsID']
        initial_result = self.send_put(f"Biobank/specimens/{rlims_id}", request_data=initial_payload)

        # Make a new request without the optional sampleType field
        new_payload = {
            'rlimsID': 'sabrina',
            'orderID': self.bio_order.biobankOrderId,
            'participantID': config_utils.to_client_biobank_id(self.participant.biobankId),
            'testcode': 'test 1234567'
        }
        self.send_put(f"Biobank/specimens/{rlims_id}", request_data=new_payload)

        # Make sure sampleType is still set on specimen
        updated_specimen_json = self.retrieve_specimen_json(initial_result['id'])
        self.assertSpecimenJsonMatches(updated_specimen_json, initial_payload)

    def test_add_attribute_to_existing_specimen(self):
        payload = {
            'rlimsID': 'sabrina',
            'orderID': self.bio_order.biobankOrderId,
            'participantID': config_utils.to_client_biobank_id(self.participant.biobankId),
            'testcode': 'test 1234567'
        }
        initial_result = self.putSpecimen(payload)

        payload['attributes'] = [{
            "name": "test",
            "value": "123"
        }]
        self.putSpecimen(payload)

        saved_specimen_client_json = self.retrieve_specimen_json(initial_result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)

    def test_replacing_attributes_on_existing_specimen(self):
        payload = {
            'rlimsID': 'sabrina',
            'orderID': self.bio_order.biobankOrderId,
            'participantID': config_utils.to_client_biobank_id(self.participant.biobankId),
            'testcode': 'test 1234567',
            'attributes': [
                {
                    'name': 'attr_one',
                    'value': '1'
                },
                {
                    'name': 'attr_two',
                    'value': 'two'
                }
            ]
        }
        initial_result = self.putSpecimen(payload)

        payload['attributes'] = [{
            'name': 'test',
            'value': '123'
        }]
        self.putSpecimen(payload)

        saved_specimen_client_json = self.retrieve_specimen_json(initial_result['id'])
        self.assertSpecimenJsonMatches(saved_specimen_client_json, payload)
