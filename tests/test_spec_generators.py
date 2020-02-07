#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#

from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.spec_participant_mixin import SpecParticipantMixin


class TestSpecParticipant(BaseTestCase, SpecParticipantMixin):
    """
    The spec_data dict defines specific values for everything about a participant. Keys that
    start with an underscore define everything that is not associated with a questionnaire module.
    Keys that do not start with an underscore are Codebook question IDs.  Questions may be answered
    using other Codebook question answer codes, comma delimit multiple choice code answers.
    """
    spec_data = {
        '_HPOSite': 'hpo-site-monroeville',
        '_PM': 'yes',
        '_PPIModule': [
            'ConsentPII',
            'TheBasics'
        ],
        '_BIOOrder': [
            '1SAL2',
            '1ED04',
        ],
        'PIIName_First': 'Jane',
        'PIIName_Last': 'Doe',
        'PIIBirthInformation_BirthDate': '1933-03-03',
        'Gender_GenderIdentity': 'GenderIdentity_Woman',
        'StreetAddress_PIIState': 'PIIState_DC',
        'StreetAddress_PIIZIP': 20001,
        'WhatRaceEthnicity_Asian': 'WhatRaceEthnicity_NHPI,WhatRaceEthnicity_Asian'
    }

    def test_create_spec_participant(self):
        """
        Create a basic spec participant
        """
        participant = self.spec_participant(self.spec_data)

        self.assertEqual(participant.site, self.spec_data['_HPOSite'])
        self.assertRegex(participant.participantId, '^P(\d{9})$')

        # retrieve participant from API.
        response = self.send_get(f"Participant/{participant.participantId}")

        self.assertEqual(participant.participantId, response['participantId'])
        self.assertEqual(participant.hpoId, response['hpoId'])
        self.assertEqual(participant.site, response['site'])
