import unittest
# from testlib import testutil

from rdr_service import config  # pylint: disable=unused-import
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.utils import from_client_participant_id
from rdr_service.offline.biobank_samples_pipeline import upsert_from_latest_csv
from rdr_service.participant_enums import SampleStatus
from tests.test_data import load_biobank_order_json
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.model.hpo import HPO
from rdr_service.participant_enums import (
    OrganizationType,
    TEST_HPO_ID,
    TEST_HPO_NAME
)


def _callthrough(fn, *args, **kwargs):
    fn(*args, **kwargs)


class DataGenApiTest(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)
        self.hpo_dao = HPODao()
        self.hpo_dao.insert(
            HPO(hpoId=TEST_HPO_ID, name=TEST_HPO_NAME, displayName="Test", organizationType=OrganizationType.UNSET)
        )

    @unittest.skip("DA-471, Only tests fake data generator, test is flaky.")
    #@mock.patch("google.appengine.ext.deferred.defer", new=_callthrough)
    def test_generate_samples(self):
        participant_id = self.send_post("Participant", {})["participantId"]
        self.send_consent(participant_id)
        self.send_post(
            "Participant/%s/BiobankOrder" % participant_id,
            load_biobank_order_json(from_client_participant_id(participant_id)),
        )

        # Sanity check that the orders were created correctly.
        bo_dao = BiobankOrderDao()
        self.assertEqual(1, bo_dao.count())
        order = bo_dao.get_all()[0]
        self.assertEqual(16, len(bo_dao.get_with_children(order.biobankOrderId).samples))

        self.send_post("DataGen", {"create_biobank_samples": True, "samples_missing_fraction": 0.0})
        upsert_from_latest_csv()  # Run the (usually offline) Biobank CSV import job.

        self.assertEqual(16, BiobankStoredSampleDao().count())
        ps = ParticipantSummaryDao().get(from_client_participant_id(participant_id))
        self.assertEqual(SampleStatus.RECEIVED, ps.samplesToIsolateDNA)
        self.assertEqual(13, ps.numBaselineSamplesArrived)

    def test_generate_participant(self):
        data = {'api': 'Participant',
                'data': {'providerLink': [{'primary': True, 'organization': {'reference': 'Organization/TEST'}}]},
                'timestamp': '2020-04-24T04:28:53.862157'}
        url = 'SpecDataGen'
        resp = self.send_post(url, data)

        participant_id = resp['participantId']
        biobank_id = resp['biobankId']

        headers = {'If-Match': resp['meta']['versionId']}
        put_data = {'api': 'Participant/'+participant_id,
                    'data': {'participantId': participant_id, 'externalId': None, 'hpoId': 'TEST', 'awardee': 'TEST',
                             'organization': 'UNSET', 'biobankId': biobank_id, 'lastModified': '2018-10-12T13:29:57',
                             'signUpTime': '2018-10-12T13:29:57',
                             'providerLink': [{'primary': True, 'organization': {'reference': 'Organization/TEST'}}],
                             'withdrawalStatus': 'NOT_WITHDRAWN', 'withdrawalReason': 'UNSET',
                             'withdrawalReasonJustification': None, 'withdrawalAuthored': None,
                             'suspensionStatus': 'NOT_SUSPENDED', 'site': 'hpo-site-a', 'meta': {'versionId': 'W/"1"'}},
                    'timestamp': '2018-10-12T13:29:57.119160', 'method': 'PUT'}
        resp2 = self.send_request('POST', url, request_data=put_data, headers=headers)
        self.assertEqual(resp['participantId'], resp2['participantId'])
