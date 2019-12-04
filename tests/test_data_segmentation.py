import http.client
from tests.helpers.unittest_base import BaseTestCase


class ParticipantApiTest(BaseTestCase):
    def setUp(self):
        super(ParticipantApiTest, self).setUp()
        provider_link = {"primary": False, "organization": {"reference": "columbia"}}
        self.participant = {"providerLink": [provider_link]}
        self.provider_link_2 = {"primary": True, "organization": {"reference": "Organization/PITT"}}

    def tearDown(self):
        BaseTestCase.switch_auth_user('example@example.com', 'example')

    def test_one_partner_cannot_see_other(self):
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
