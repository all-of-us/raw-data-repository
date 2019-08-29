import copy
import http.client
import json
import os

from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant import Participant
from tests.helpers.unittest_base import BaseTestCase


class DvOrderApiTestBase(BaseTestCase):
    mayolink_response = None

    def setUp(self):
        super().setUp()

        self.test_data = {
            "subject": "Patient/P123456789",
            "awardee": "PITT",
            "organization": "PITT_BANNER_HEALTH",
            "patient_status": "YES",
            "user": "john.doe@pmi-ops.org",
            "site": "hpo-site-monroeville",
            "authored": "2019-04-26T12:11:41",
            "comment": "This is comment",
        }

        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()
        self.hpo_dao = HPODao()

        self.hpo = self.hpo_dao.get_by_name("PITT")

        self.participant = Participant(hpoId=self.hpo.hpoId, participantId=123456789, biobankId=7)
        self.participant_dao.insert(self.participant)
        self.summary = self.participant_summary(self.participant)
        self.summary_dao.insert(self.summary)

    def test_patient_status_created(self):
        data = copy.copy(self.test_data)

        # insert patient status
        url = os.path.join(
            "PatientStatus", "P{0}".format(self.participant.participantId), "Organization", "PITT_BANNER_HEALTH"
        )
        resp = self.send_post(url, data, expected_status=http.client.CREATED)

        # test that our test_data dict is in the resp.response dict.
        resp_data = json.loads(resp.response[0])
        self.assertDictContainsSubset(data, resp_data)

        # attempt to insert again, should fail with duplicate.
        self.send_post(url, data, expected_status=http.client.CONFLICT)

        # Get record and test that our test_data dict is in the resp.response dict.
        resp = self.send_get(url)
        self.assertDictContainsSubset(data, resp)

    def test_patient_status_udpated(self):
        data = copy.copy(self.test_data)

        # insert patient status
        url = os.path.join(
            "PatientStatus", "P{0}".format(self.participant.participantId), "Organization", "PITT_BANNER_HEALTH"
        )
        resp = self.send_post(url, data, expected_status=http.client.CREATED)

        data["authored"] = "2019-04-27T16:32:01Z"
        data["comment"] = "saw patient at new site"
        data["site"] = "hpo-site-bannerphoenix"

        resp = self.send_put(url, data, expected_status=http.client.OK)
        data["authored"] = data["authored"].strip("Z")
        self.assertDictContainsSubset(data, resp)

        # Get record and test that our test_data dict is in the resp.response dict.
        resp = self.send_get(url)
        self.assertDictContainsSubset(data, resp)

        # TODO: When new style history tables and triggers have been added to unit tests, test history URL.
        # # make call for participant patient status history
        # url = os.path.join('PatientStatus', 'P{0}'.format(self.participant.participantId), 'Organization',
        #                    'PITT_BANNER_HEALTH', 'History')
        # resp = self.send_get(url)
        # self.assertEqual(2, len(resp))
