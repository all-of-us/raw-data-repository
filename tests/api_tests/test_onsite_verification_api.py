import mock
from datetime import datetime, date

from tests.helpers.unittest_base import BaseTestCase

from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.participant_enums import OnSiteVerificationType, OnSiteVerificationVisitType, IdVerificationOriginType


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

    def verify_pdr_resource_data(self, pdr_dict, test_payload):
        """
        Verify the data dict built by the PDR resource generator against the RDR onsite_id_verification test data
        """
        # Payloads may only have a subset of the usual values; set up expectations
        expected_verification_type = test_payload['verificationType'] if 'verificationType' in test_payload else 'UNSET'
        expected_visit_type = test_payload['visitType'] if 'visitType' in test_payload else 'UNSET'
        expected_site = test_payload['siteGoogleGroup'] if 'siteGoogleGroup' in test_payload else None
        expected_site_id = self.site.siteId if 'siteGoogleGroup' in test_payload else None
        # POST payloads prefix participant ids with 'P' and add a 'Z' to the end of the timestamp; adjust for comparison
        participant_id = int(test_payload['participantId'][1:])
        verified_time = test_payload['verifiedTime'][:-1]
        self.assertEqual(pdr_dict['participant_id'], participant_id)
        self.assertEqual(pdr_dict['verified_time'], verified_time)
        self.assertEqual(pdr_dict['verification_type'], str(OnSiteVerificationType(expected_verification_type)))
        self.assertEqual(pdr_dict['verification_type_id'], int(OnSiteVerificationType(expected_verification_type)))
        self.assertEqual(pdr_dict['visit_type'], str(OnSiteVerificationVisitType(expected_visit_type)))
        self.assertEqual(pdr_dict['visit_type_id'], int(OnSiteVerificationVisitType(expected_visit_type)))
        self.assertEqual(pdr_dict['site'], expected_site)
        self.assertEqual(pdr_dict['site_id'], expected_site_id)
        # Extra fields for the resource_data table record;  check for presence
        self.assertTrue(all(key in pdr_dict.keys() for key in ['id', 'created', 'modified']))

    @mock.patch('rdr_service.resource.generators.ResourceRecordSet')
    def test_onsite_verification(self, mock_pdr_resource_generator):
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
        self.assertEqual(participant_summary.everIdVerified, True)
        self.assertEqual(participant_summary.firstIdVerifiedOn, date(2022, 3, 22))
        self.assertEqual(participant_summary.idVerificationOrigin, IdVerificationOriginType.ON_SITE)

        # Verify the data dict arg from each mocked ResourceRecordSet(schema, data) created by the PDR generator,
        # triggered by POST /OnSite/Id/Verification requests
        self.assertEqual(mock_pdr_resource_generator.call_count, 4)
        payload_list = [payload_1, payload_2, payload_3, payload_4]
        for i in range(4):
            resource_dict = mock_pdr_resource_generator.call_args_list[i].args[1]
            self.verify_pdr_resource_data(resource_dict, payload_list[i])


