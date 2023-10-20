#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from datetime import datetime

from rdr_service import clock
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.hpo import HPO
from rdr_service.model.site import Site
from rdr_service.resource.generators.participant import ParticipantSummaryGenerator
from tests.helpers.unittest_base import BaseTestCase


class MultipleQuestionnaireResponsesTest(BaseTestCase):
    """
    Test that multiple full and partial questionnaire response submissions work properly
    in the Participant resource generator.
    """
    TIME_1 = datetime(2018, 9, 20, 5, 49, 11)
    TIME_2 = datetime(2018, 9, 24, 14, 21, 1)
    TIME_3 = datetime(2018, 9, 26, 10, 0, 0)

    site = None
    hpo = None

    qn_thebasics_id = None

    def setUp(self):
        super().setUp(with_consent_codes=True)

        self.dao = ParticipantDao()

        with self.dao.session() as session:
            self.site = session.query(Site).filter(Site.googleGroup == 'hpo-site-monroeville').first()
            self.hpo = session.query(HPO).filter(HPO.name == 'PITT').first()
            self.provider_link = {
                "primary": True, "organization": {"display": None, "reference": "Organization/PITT"}}

        with clock.FakeClock(self.TIME_1):
            self.participant = self.create_participant(self.provider_link)
            self.participant_id = int(self.participant['participantId'].replace('P', ''))
            self.biobank_id = int(self.participant['biobankId'].replace('Z', ''))

    def create_participant(self, provider_link=None):
        if provider_link:
            provider_link = {"providerLink": [provider_link]}
        else:
            provider_link = {}
        response = self.send_post("Participant", provider_link)
        return response

    def test_multiple_survey_submissions(self):
        """
        Test that multiple survey module response submissions update the initial submission.
        """
        with clock.FakeClock(self.TIME_2):
            self.send_consent(self.participant_id)

            gen = ParticipantSummaryGenerator()
            ps_data = gen.make_resource(self.participant_id).get_data()

            first_name = ps_data['first_name']
            last_name = ps_data['last_name']

            self.assertIsNotNone(ps_data)
            self.assertEqual(ps_data['addresses'][0]['addr_street_address_1'], '1234 Main Street')
            self.assertEqual(ps_data['addresses'][0]['addr_street_address_2'], 'APT C')
            self.assertIsNone(ps_data['login_phone_number'])
            self.assertIsNone(ps_data['date_of_birth'])

        # Submit a new ConsentPII response with only new values, no existing values.
        with clock.FakeClock(self.TIME_3):

            values = list()
            values.append(('loginPhoneNumber', '(555)-555-5555'))
            values.append(('dateOfBirth', '1960-10-01'))

            self.send_consent(self.participant_id, string_answers=values)

            ps_data = gen.make_resource(self.participant_id).get_data()

            # Verify data from original response submission is unchanged.
            self.assertEqual(ps_data['first_name'], first_name)
            self.assertEqual(ps_data['last_name'], last_name)
            self.assertEqual(ps_data['addresses'][0]['addr_street_address_1'], '1234 Main Street')
            self.assertEqual(ps_data['addresses'][0]['addr_street_address_2'], 'APT C')

            # Verify data from second submission has been added.
            self.assertEqual(ps_data['login_phone_number'], '(555)-555-5555')
            # Date of birth is now pulled from the 'participant_summary' table in the generator. Since DOB
            # was NULL in the first submission, we would expect DOB to stay NULL when an additional ConsentPII
            # response are sent because 'participant_summary' is only updated on the first submission.
            self.assertIsNone(ps_data['date_of_birth'])
