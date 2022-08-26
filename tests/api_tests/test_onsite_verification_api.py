from datetime import datetime

from tests.helpers.unittest_base import BaseTestCase

from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.participant_enums import OnSiteVerificationType, OnSiteVerificationVisitType


class OnsiteVerificationApiTest(BaseTestCase):
    def setUp(self):
        super(OnsiteVerificationApiTest, self).setUp()
        self.hpo = self.data_generator.create_database_hpo()
        self.org = self.data_generator.create_database_organization(hpoId=self.hpo.hpoId)
        self.site = self.data_generator.create_database_site(hpoId=self.hpo.hpoId,
                                                             organizationId=self.org.organizationId,
                                                             googleGroup='test-group')
        self.p = self.data_generator.create_database_participant(hpoId=self.hpo.hpoId)
        self.p2 = self.data_generator.create_database_participant(hpoId=self.hpo.hpoId)
        self.p3 = self.data_generator.create_database_participant(hpoId=self.hpo.hpoId)
        self.data_generator.create_database_participant_summary(participant=self.p)
        self.data_generator.create_database_participant_summary(participant=self.p2)
        self.data_generator.create_database_participant_summary(participant=self.p3)

        self.ps_dao = ParticipantSummaryDao()

    def test_onsite_verification(self):
        path = 'Onsite/Id/Verification'
        payload_1 = {
          "participantId": 'P' + str(self.p.participantId),
          "userEmail": "test@mail.com",
          "verifiedTime": "2022-03-22T06:07:08Z",
          "siteGoogleGroup": self.site.googleGroup,
          "verificationType": "PHOTO_AND_ONE_OF_PII",
          "visitType": "PMB_INITIAL_VISIT"
        }
        payload_2 = {
            "participantId": 'P' + str(self.p.participantId),
            "userEmail": "test@mail.com",
            "verifiedTime": "2022-02-22T06:07:08Z",
            "siteGoogleGroup": self.site.googleGroup,
            "verificationType": "TWO_OF_PII",
            "visitType": "PHYSICAL_MEASUREMENTS_ONLY"
        }
        payload_3 = {
            "participantId": 'P' + str(self.p2.participantId),
            "userEmail": "test2@mail.com",
            "verifiedTime": "2022-01-22T06:07:08Z",
            "siteGoogleGroup": self.site.googleGroup,
            "verificationType": "TWO_OF_PII",
            "visitType": "BIOSPECIMEN_COLLECTION_ONLY"
        }
        payload_4 = {
            "participantId": 'P' + str(self.p3.participantId),
            "verifiedTime": "2022-02-03T04:05:06Z",
        }

        response1 = self.send_post(path, payload_1)
        response2 = self.send_post(path, payload_2)
        response3 = self.send_post(path, payload_3)
        response4 = self.send_post(path, payload_4)
        self.assertEqual(response1,
                         {'participantId': 'P' + str(self.p.participantId),
                          'verifiedTime': '2022-03-22T06:07:08',
                          'userEmail': 'test@mail.com',
                          'siteGoogleGroup': self.site.googleGroup,
                          'siteName': self.site.siteName,
                          'verificationType': 'PHOTO_AND_ONE_OF_PII',
                          'visitType': 'PMB_INITIAL_VISIT'}
                         )
        self.assertEqual(response2,
                         {'participantId': 'P' + str(self.p.participantId),
                          'verifiedTime': '2022-02-22T06:07:08',
                          'userEmail': 'test@mail.com',
                          'siteGoogleGroup': self.site.googleGroup,
                          'siteName': self.site.siteName,
                          'verificationType': 'TWO_OF_PII',
                          'visitType': 'PHYSICAL_MEASUREMENTS_ONLY'}
                         )
        self.assertEqual(response3,
                         {'participantId': 'P' + str(self.p2.participantId),
                          'verifiedTime': '2022-01-22T06:07:08',
                          'userEmail': 'test2@mail.com',
                          'siteGoogleGroup': self.site.googleGroup,
                          'siteName': self.site.siteName,
                          'verificationType': 'TWO_OF_PII',
                          'visitType': 'BIOSPECIMEN_COLLECTION_ONLY'}
                         )
        self.assertEqual(response4,
                         {'participantId': 'P' + str(self.p3.participantId),
                          'verifiedTime': '2022-02-03T04:05:06',
                          'userEmail': None,
                          'siteGoogleGroup': None,
                          'siteName': None,
                          'verificationType': 'UNSET',
                          'visitType': 'UNSET'}
                         )

        get_path = 'Onsite/Id/Verification/P' + str(self.p.participantId)
        result = self.send_get(get_path)
        self.assertEqual(result,
                         {'entry': [
                             {'participantId': 'P' + str(self.p.participantId),
                              'verifiedTime': '2022-03-22T06:07:08',
                              'userEmail': 'test@mail.com',
                              'siteGoogleGroup': self.site.googleGroup,
                              'siteName': self.site.siteName,
                              'verificationType': 'PHOTO_AND_ONE_OF_PII',
                              'visitType': 'PMB_INITIAL_VISIT'},
                             {'participantId': 'P' + str(self.p.participantId),
                              'verifiedTime': '2022-02-22T06:07:08',
                              'userEmail': 'test@mail.com',
                              'siteGoogleGroup': self.site.googleGroup,
                              'siteName': self.site.siteName,
                              'verificationType': 'TWO_OF_PII',
                              'visitType': 'PHYSICAL_MEASUREMENTS_ONLY'}
                         ]})
        get_path = 'Onsite/Id/Verification/P' + str(self.p2.participantId)
        result = self.send_get(get_path)
        self.assertEqual(result,
                         {'entry': [
                             {'participantId': 'P' + str(self.p2.participantId),
                              'verifiedTime': '2022-01-22T06:07:08',
                              'userEmail': 'test2@mail.com',
                              'siteGoogleGroup': self.site.googleGroup,
                              'siteName': self.site.siteName,
                              'verificationType': 'TWO_OF_PII',
                              'visitType': 'BIOSPECIMEN_COLLECTION_ONLY'}
                         ]})
        get_path = 'Onsite/Id/Verification/P' + str(self.p3.participantId)
        result = self.send_get(get_path)
        self.assertEqual(result,
                         {'entry': [
                             {'participantId': 'P' + str(self.p3.participantId),
                              'verifiedTime': '2022-02-03T04:05:06',
                              'userEmail': None,
                              'siteGoogleGroup': None,
                              'siteName': None,
                              'verificationType': 'UNSET',
                              'visitType': 'UNSET'}
                         ]})

        participant_summary = self.ps_dao.get_by_participant_id(self.p.participantId)
        self.assertEqual(participant_summary.onsiteIdVerificationTime, datetime(2022, 2, 22, 6, 7, 8))
        self.assertEqual(participant_summary.onsiteIdVerificationType, OnSiteVerificationType.TWO_OF_PII)
        self.assertEqual(participant_summary.onsiteIdVerificationVisitType,
                         OnSiteVerificationVisitType.PHYSICAL_MEASUREMENTS_ONLY)
        self.assertEqual(participant_summary.onsiteIdVerificationUser, 'test@mail.com')
        self.assertEqual(participant_summary.onsiteIdVerificationSite, self.site.siteId)
