
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
