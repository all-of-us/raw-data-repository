import http.client

from rdr_service.model.participant import Participant
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.participant_dao import ParticipantDao
from tests.helpers.unittest_base import BaseTestCase


class ParticipantApiTest(BaseTestCase):
    def setUp(self):
        super(ParticipantApiTest, self).setUp()
        provider_link = {"primary": False, "organization": {"reference": "columbia"}}
        self.participant = {"providerLink": [provider_link]}
        self.participant_2 = {"externalId": 12345}
        self.provider_link_2 = {"primary": True, "organization": {"reference": "Organization/PITT"}}
        self.summary_dao = ParticipantSummaryDao()
        self.dao = ParticipantDao()

    def tearDown(self):
        BaseTestCase.switch_auth_user('example@example.com', 'example')

    def test_one_partner_cannot_see_others_participant(self):
        vibrent_participant_list = []
        carevo_participant_list = []

        BaseTestCase.switch_auth_user('example@spellman.com', 'vibrent')
        response = self.participant_generator(3)
        for i in response:
            vibrent_participant_list.append(i['participantId'])

        BaseTestCase.switch_auth_user('example@care.com', 'careevolution')

        response = self.participant_generator(3)
        for i in response:
            carevo_participant_list.append(i['participantId'])

        # care evolution can not retrieve vibrent participants
        for i in vibrent_participant_list:
            print('getting participant...')
            self.send_get("Participant/{}".format(i), expected_status=http.client.BAD_REQUEST)

        BaseTestCase.switch_auth_user('example@spellman.com', 'vibrent')
        # vibrent can not retrieve care evolution participants
        for i in carevo_participant_list:
            self.send_get("Participant/{}".format(i), expected_status=http.client.BAD_REQUEST)

        participant = self.send_get("Participant/{}".format(vibrent_participant_list[0]))

        BaseTestCase.switch_auth_user('example@care.com', 'careevolution')
        # Change the provider link for a vibrent participant
        participant_id = participant['participantId']
        path = "Participant/%s" % participant_id
        # carevo can not update
        self.send_put(path, participant, headers={"If-Match": 'W/"1"'}, expected_status=http.client.BAD_REQUEST)

        # get carevo participant
        participant_id = carevo_participant_list[0]
        path = "Participant/%s" % participant_id
        # carevo can update
        self.send_put(path, participant, headers={"If-Match": 'W/"1"'})

        # test hpro can get all participants
        BaseTestCase.switch_auth_user('example@hpro.com', 'healthpro')
        vibrent_participant_list.extend(carevo_participant_list)
        self.assertEqual(len(vibrent_participant_list), 6)
        for i in vibrent_participant_list:
            self.send_get("Participant/{}".format(i))


    def test_cannot_get_summary(self):
        BaseTestCase.switch_auth_user('example@spellman.com', 'vibrent')
        participant_id = 22
        self.dao.insert(Participant(participantId=participant_id, biobankId=2))
        refetched = self.dao.get(participant_id)
        self.summary_dao.insert(self.participant_summary(refetched))

        BaseTestCase.switch_auth_user('example@care.com', 'careevolution')
        # we dont return bad request for participant summary, we filter by participant origin
        response = self.send_get("ParticipantSummary?participantId={}".format(participant_id))
        self.assertEqual(len(response['entry']), 0)

        BaseTestCase.switch_auth_user('example@spellman.com', 'vibrent')
        response = self.send_get("ParticipantSummary?participantId={}".format(participant_id))
        self.assertEqual(len(response['entry']), 1)

    def test_update_hpro_can_edit(self):
        BaseTestCase.switch_auth_user('example@care.com', 'careevolution')
        response = self.send_post("Participant", self.participant)

        # Change the provider link for the participant
        participant_id = response["participantId"]
        response["providerLink"] = [self.provider_link_2]
        path = "Participant/%s" % participant_id
        BaseTestCase.switch_auth_user('example@spellman.com', 'hpro')
        self.send_put(path, response, headers={"If-Match": 'W/"1"'})

    def participant_generator(self, num):
        for _ in range(num):
            res = self.send_post("Participant", self.participant)
            yield res
