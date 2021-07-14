from tests.helpers.unittest_base import BaseTestCase
from rdr_service.clock import FakeClock
from rdr_service import clock
from datetime import timedelta


class ResearchProjectsDirectoryApiTest(BaseTestCase):
    def setUp(self):
        super().setUp(with_data=False)

    def test_get_research_projects_directory_end_to_end(self):
        # create researchers
        researchers_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "modifiedTime": "2019-11-26T21:21:13.056Z",
                "givenName": "given name 1",
                "familyName": "family name 1",
                "streetAddress1": "string",
                "streetAddress2": "string",
                "city": "string",
                "state": "string",
                "zipCode": "string",
                "country": "string",
                "ethnicity": "HISPANIC",
                "gender": ["MAN"],
                "race": ["AIAN"],
                "degree": ["PHD", "MPH"],
                "sexAtBirth": ["FEMALE"],
                "sexualOrientation": "BISEXUAL",
                "affiliations": [
                    {
                        "institution": "institution1",
                        "role": "institution role 1",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ],
                "verifiedInstitutionalAffiliation": {
                    "institutionDisplayName": "display name",
                    "institutionShortName": "verified institution",
                    "institutionalRole": "verified institution role 1",
                    "nonAcademicAffiliation": "INDUSTRY"
                }
            },
            {
                "userId": 1,
                "creationTime": "2019-11-27T21:21:13.056Z",
                "modifiedTime": "2019-11-27T21:21:13.056Z",
                "givenName": "given name 2",
                "familyName": "family name 2",
                "streetAddress1": "string2",
                "streetAddress2": "string2",
                "city": "string2",
                "state": "string2",
                "zipCode": "string2",
                "country": "string2",
                "ethnicity": "HISPANIC",
                "sexualOrientation": "BISEXUAL",
                "gender": ["MAN", "WOMAN"],
                "race": ["AIAN", "WHITE"],
                "degree": ["PHD", "MPH"],
                "affiliations": [
                    {
                        "institution": "institution2",
                        "role": "institution role 2"
                    },
                    {
                        "institution": "institution22",
                        "role": "institution role 22",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ],
                "verifiedInstitutionalAffiliation": {
                    "institutionShortName": "verified institution",
                    "institutionalRole": "verified institution role 1",
                    "nonAcademicAffiliation": "INDUSTRY"
                }
            }
        ]
        self.send_post('workbench/directory/researchers', request_data=researchers_json)

        # create workspace
        request_json = [
            {
                "workspaceId": 0,
                "name": "workspace name str",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-25T17:43:41.085Z",
                "status": "ACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 0,
                        "role": "OWNER",
                        "status": "ACTIVE"
                    },
                    {
                        "userId": 1,
                        "role": "OWNER",
                        "status": "ACTIVE"
                    }
                ],
                "creator": {
                    "userId": 1,
                    "givenName": "aaa",
                    "familyName": "bbb"
                },
                "excludeFromPublicDirectory": False,
                "ethicalLegalSocialImplications": True,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "disease focused research name str",
                "otherPurposeDetails": "other purpose details str",
                "methodsDevelopment": True,
                "controlSet": True,
                "ancestry": True,
                "accessTier": "REGISTERED",
                "socialBehavioral": True,
                "populationHealth": True,
                "drugDevelopment": True,
                "commercialPurpose": True,
                "educational": True,
                "otherPurpose": True,
                "scientificApproaches": 'reasonForInvestigation string',
                "intendToStudy": 'intendToStudy string',
                "findingsFromStudy": 'findingsFromStudy string',
                "focusOnUnderrepresentedPopulations": True,
                "workspaceDemographic": {
                    "raceEthnicity": ['AIAN', 'MENA'],
                    "age": ['AGE_0_11', 'AGE_65_74'],
                    "sexAtBirth": "UNSET",
                    "genderIdentity": "OTHER_THAN_MAN_WOMAN",
                    "sexualOrientation": "OTHER_THAN_STRAIGHT",
                    "geography": "RURAL",
                    "disabilityStatus": "DISABILITY",
                    "accessToCare": "NOT_EASILY_ACCESS_CARE",
                    "educationLevel": "LESS_THAN_HIGH_SCHOOL",
                    "incomeLevel": "BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT",
                    "others": "string"
                }
            },
            {
                "workspaceId": 1,
                "name": "workspace name str 2",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-25T17:43:41.085Z",
                "status": "INACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 0,
                        "role": "OWNER",
                        "status": "ACTIVE"
                    },
                    {
                        "userId": 1,
                        "role": "READER",
                        "status": "ACTIVE"
                    }
                ],
                "creator": {
                    "userId": 0,
                    "givenName": "aaa",
                    "familyName": "bbb"
                },
                "excludeFromPublicDirectory": False,
                "ethicalLegalSocialImplications": False,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "disease focused research name str 2",
                "otherPurposeDetails": "other purpose details str 2",
                "methodsDevelopment": False,
                "controlSet": False,
                "ancestry": False,
                "socialBehavioral": False,
                "populationHealth": False,
                "drugDevelopment": False,
                "commercialPurpose": False,
                "educational": False,
                "otherPurpose": False,
                "scientificApproaches": 'reasonForInvestigation string2',
                "intendToStudy": 'intendToStudy string2',
                "findingsFromStudy": 'findingsFromStudy string2'
            }
        ]
        now = clock.CLOCK.now()
        sequest_hours_ago = now - timedelta(hours=24)
        with FakeClock(sequest_hours_ago):
            self.send_post('workbench/directory/workspaces', request_data=request_json)
        # test get research projects directory before review
        result = self.send_get('researchHub/projectDirectory')
        self.assertEqual(len(result['data']), 2)
        self.assertIn({'workspaceId': 0, 'snapshotId': 1, 'name': 'workspace name str',
                       'creationTime': '2019-11-25T17:43:41.085000',
                       'modifiedTime': '2019-11-25T17:43:41.085000', 'status': 'ACTIVE',
                       'workspaceUsers': [
                           {'userId': 0, 'userName': 'given name 1 family name 1', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution1', 'role': 'institution role 1', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'},
                               {'institution': 'display name', 'role': 'verified institution role 1',
                                'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}]},
                           {'userId': 1, 'userName': 'given name 2 family name 2', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution2', 'role': 'institution role 2', 'isVerified': None,
                                'nonAcademicAffiliation': 'UNSET'},
                               {'institution': 'institution22', 'role': 'institution role 22', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'},
                               {'institution': 'verified institution', 'role': 'verified institution role 1',
                                'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}
                           ]}
                       ],
                       'workspaceOwner': [
                           {'userId': 0, 'userName': 'given name 1 family name 1',
                            'degree': ['PHD', 'MPH'],
                            'affiliations': [{'institution': 'institution1', 'role': 'institution role 1',
                                              'isVerified': None, 'nonAcademicAffiliation': 'INDUSTRY'},
                                             {'institution': 'display name', 'role': 'verified institution role 1',
                                              'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}]},
                           {'userId': 1, 'userName': 'given name 2 family name 2',
                            'degree': ['PHD', 'MPH'],
                            'affiliations': [{'institution': 'institution2', 'role': 'institution role 2',
                                              'isVerified': None, 'nonAcademicAffiliation': 'UNSET'},
                                             {'institution': 'institution22', 'role': 'institution role 22',
                                              'isVerified': None, 'nonAcademicAffiliation': 'INDUSTRY'},
                                             {'institution': 'verified institution',
                                              'role': 'verified institution role 1', 'isVerified': True,
                                              'nonAcademicAffiliation': 'UNSET'}]}],
                       'hasVerifiedInstitution': True,
                       'excludeFromPublicDirectory': False, 'ethicalLegalSocialImplications': True,
                       'reviewRequested': False, 'diseaseFocusedResearch': True,
                       'diseaseFocusedResearchName': 'disease focused research name str',
                       'otherPurposeDetails': 'other purpose details str', 'methodsDevelopment': True,
                       'controlSet': True, 'ancestry': True, 'socialBehavioral': True, 'populationHealth': True,
                       'drugDevelopment': True, 'commercialPurpose': True, 'educational': True, 'otherPurpose': True,
                       'scientificApproaches': 'reasonForInvestigation string',
                       'intendToStudy': 'intendToStudy string',
                       'findingsFromStudy': 'findingsFromStudy string',
                       'focusOnUnderrepresentedPopulations': True,
                       'accessTier': 'REGISTERED',
                       'workspaceDemographic': {
                           "raceEthnicity": ['AIAN', 'MENA'],
                           "age": ['AGE_0_11', 'AGE_65_74'],
                           "sexAtBirth": None,
                           "genderIdentity": "OTHER_THAN_MAN_WOMAN",
                           "sexualOrientation": "OTHER_THAN_STRAIGHT",
                           "geography": "RURAL",
                           "disabilityStatus": "DISABILITY",
                           "accessToCare": "NOT_EASILY_ACCESS_CARE",
                           "educationLevel": "LESS_THAN_HIGH_SCHOOL",
                           "incomeLevel": "BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT",
                           "others": "string"
                       },
                       'cdrVersion': None
                       },
                      result['data'])
        self.assertIn({'workspaceId': 1, 'snapshotId': 2, 'name': 'workspace name str 2',
                       'creationTime': '2019-11-25T17:43:41.085000',
                       'modifiedTime': '2019-11-25T17:43:41.085000', 'status': 'INACTIVE',
                       'workspaceUsers': [
                           {'userId': 0, 'userName': 'given name 1 family name 1', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution1', 'role': 'institution role 1', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'},
                               {'institution': 'display name', 'role': 'verified institution role 1',
                                'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}
                           ]},
                           {'userId': 1, 'userName': 'given name 2 family name 2', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution2', 'role': 'institution role 2', 'isVerified': None,
                                'nonAcademicAffiliation': 'UNSET'},
                               {'institution': 'institution22', 'role': 'institution role 22', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'},
                               {'institution': 'verified institution', 'role': 'verified institution role 1',
                                'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}
                           ]}
                       ],
                       'workspaceOwner': [{'userId': 0, 'userName': 'given name 1 family name 1',
                                           'degree': ['PHD', 'MPH'],
                                           'affiliations': [{'institution': 'institution1',
                                                             'role': 'institution role 1',
                                                             'isVerified': None,
                                                             'nonAcademicAffiliation': 'INDUSTRY'},
                                                            {'institution': 'display name',
                                                             'role': 'verified institution role 1',
                                                             'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}]}],
                       'hasVerifiedInstitution': True,
                       'excludeFromPublicDirectory': False, 'ethicalLegalSocialImplications': False,
                       'reviewRequested': False, 'diseaseFocusedResearch': True,
                       'diseaseFocusedResearchName': 'disease focused research name str 2',
                       'otherPurposeDetails': 'other purpose details str 2', 'methodsDevelopment': False,
                       'controlSet': False, 'ancestry': False, 'socialBehavioral': False, 'populationHealth': False,
                       'drugDevelopment': False, 'commercialPurpose': False, 'educational': False,
                       'otherPurpose': False, 'scientificApproaches': 'reasonForInvestigation string2',
                       'intendToStudy': 'intendToStudy string2',
                       'findingsFromStudy': 'findingsFromStudy string2',
                       'focusOnUnderrepresentedPopulations': None,
                       'accessTier': 'UNSET',
                       'workspaceDemographic': {
                           "raceEthnicity": None,
                           "age": None,
                           "sexAtBirth": None,
                           "genderIdentity": None,
                           "sexualOrientation": None,
                           "geography": None,
                           "disabilityStatus": None,
                           "accessToCare": None,
                           "educationLevel": None,
                           "incomeLevel": None,
                           "others": None
                       },
                       'cdrVersion': None
                       },
                      result['data'])
        # test audit review
        review_results = [
            {
                "snapshotId": 1,
                "auditorEmail": "auditor_email_1",
                "reviewType": "RAB",
                "displayDecision": "PUBLISH_TO_RESEARCHER_DIRECTORY",
                "accessDecision": None,
                "auditorNotes": "note1"
            },
            {
                "snapshotId": 2,
                "auditorEmail": "auditor_email_2",
                "reviewType": "RAB",
                "displayDecision": "EXCLUDE_FROM_RESEARCHER_DIRECTORY",
                "accessDecision": "DISABLE_WORKSPACE",
                "auditorNotes": "note2"
            }
        ]
        result = self.send_post('workbench/audit/workspace/results', review_results)
        self.assertIn({'snapshotId': 1, 'auditorEmail': 'auditor_email_1', 'reviewType': 'RAB',
                       'displayDecision': 'PUBLISH_TO_RESEARCHER_DIRECTORY', 'accessDecision': 'UNSET',
                       'auditorNotes': 'note1'}, result)
        self.assertIn({'snapshotId': 2, 'auditorEmail': 'auditor_email_2', 'reviewType': 'RAB',
                       'displayDecision': 'EXCLUDE_FROM_RESEARCHER_DIRECTORY',
                       'accessDecision': 'DISABLE_WORKSPACE', 'auditorNotes': 'note2'}, result)

        # test get research projects directory after review
        result = self.send_get('researchHub/projectDirectory')
        self.assertEqual(len(result['data']), 1)
        self.assertIn({'workspaceId': 0, 'snapshotId': 1, 'name': 'workspace name str',
                       'creationTime': '2019-11-25T17:43:41.085000',
                       'modifiedTime': '2019-11-25T17:43:41.085000', 'status': 'ACTIVE',
                       'workspaceUsers': [
                           {'userId': 0, 'userName': 'given name 1 family name 1', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution1', 'role': 'institution role 1', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'},
                               {'institution': 'display name', 'role': 'verified institution role 1',
                                'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}
                           ]},
                           {'userId': 1, 'userName': 'given name 2 family name 2', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution2', 'role': 'institution role 2', 'isVerified': None,
                                'nonAcademicAffiliation': 'UNSET'},
                               {'institution': 'institution22', 'role': 'institution role 22', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'},
                               {'institution': 'verified institution', 'role': 'verified institution role 1',
                                'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}
                           ]}
                       ],
                       'workspaceOwner': [
                           {'userId': 0, 'userName': 'given name 1 family name 1', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                                {'institution': 'institution1', 'role': 'institution role 1', 'isVerified': None,
                                 'nonAcademicAffiliation': 'INDUSTRY'},
                                {'institution': 'display name', 'role': 'verified institution role 1',
                                 'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}
                            ]},
                           {'userId': 1, 'userName': 'given name 2 family name 2', 'degree': ['PHD', 'MPH'],
                            'affiliations': [{'institution': 'institution2', 'role': 'institution role 2',
                                              'isVerified': None, 'nonAcademicAffiliation': 'UNSET'},
                                             {'institution': 'institution22', 'role': 'institution role 22',
                                              'isVerified': None, 'nonAcademicAffiliation': 'INDUSTRY'},
                                             {'institution': 'verified institution',
                                              'role': 'verified institution role 1', 'isVerified': True,
                                              'nonAcademicAffiliation': 'UNSET'}]}],
                       'hasVerifiedInstitution': True,
                       'excludeFromPublicDirectory': False, 'ethicalLegalSocialImplications': True,
                       'reviewRequested': False, 'diseaseFocusedResearch': True,
                       'diseaseFocusedResearchName': 'disease focused research name str',
                       'otherPurposeDetails': 'other purpose details str', 'methodsDevelopment': True,
                       'controlSet': True, 'ancestry': True, 'socialBehavioral': True, 'populationHealth': True,
                       'drugDevelopment': True, 'commercialPurpose': True, 'educational': True, 'otherPurpose': True,
                       'scientificApproaches': 'reasonForInvestigation string',
                       'intendToStudy': 'intendToStudy string',
                       'findingsFromStudy': 'findingsFromStudy string',
                       'focusOnUnderrepresentedPopulations': True,
                       'accessTier': 'REGISTERED',
                       'workspaceDemographic': {
                           "raceEthnicity": ['AIAN', 'MENA'],
                           "age": ['AGE_0_11', 'AGE_65_74'],
                           "sexAtBirth": None,
                           "genderIdentity": "OTHER_THAN_MAN_WOMAN",
                           "sexualOrientation": "OTHER_THAN_STRAIGHT",
                           "geography": "RURAL",
                           "disabilityStatus": "DISABILITY",
                           "accessToCare": "NOT_EASILY_ACCESS_CARE",
                           "educationLevel": "LESS_THAN_HIGH_SCHOOL",
                           "incomeLevel": "BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT",
                           "others": "string"
                       },
                       'cdrVersion': None
                       },
                      result['data'])

        # test get research projects directory with status
        result = self.send_get('researchHub/projectDirectory?status=ACTIVE')
        self.assertEqual(len(result['data']), 1)
        self.assertIn({'workspaceId': 0, 'snapshotId': 1, 'name': 'workspace name str',
                       'creationTime': '2019-11-25T17:43:41.085000',
                       'modifiedTime': '2019-11-25T17:43:41.085000', 'status': 'ACTIVE',
                       'workspaceUsers': [
                           {'userId': 0, 'userName': 'given name 1 family name 1', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution1', 'role': 'institution role 1', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'},
                               {'institution': 'display name', 'role': 'verified institution role 1',
                                'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}
                           ]},
                           {'userId': 1, 'userName': 'given name 2 family name 2', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution2', 'role': 'institution role 2', 'isVerified': None,
                                'nonAcademicAffiliation': 'UNSET'},
                               {'institution': 'institution22', 'role': 'institution role 22', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'},
                               {'institution': 'verified institution', 'role': 'verified institution role 1',
                                'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}
                           ]}
                       ],
                       'workspaceOwner': [
                           {'userId': 0, 'userName': 'given name 1 family name 1', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                                {'institution': 'institution1', 'role': 'institution role 1', 'isVerified': None,
                                 'nonAcademicAffiliation': 'INDUSTRY'},
                                {'institution': 'display name', 'role': 'verified institution role 1',
                                 'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}
                            ]},
                           {'userId': 1, 'userName': 'given name 2 family name 2', 'degree': ['PHD', 'MPH'],
                            'affiliations': [{'institution': 'institution2', 'role': 'institution role 2',
                                              'isVerified': None, 'nonAcademicAffiliation': 'UNSET'},
                                             {'institution': 'institution22', 'role': 'institution role 22',
                                              'isVerified': None, 'nonAcademicAffiliation': 'INDUSTRY'},
                                             {'institution': 'verified institution',
                                              'role': 'verified institution role 1', 'isVerified': True,
                                              'nonAcademicAffiliation': 'UNSET'}]}],
                       'hasVerifiedInstitution': True,
                       'excludeFromPublicDirectory': False, 'ethicalLegalSocialImplications': True,
                       'reviewRequested': False, 'diseaseFocusedResearch': True,
                       'diseaseFocusedResearchName': 'disease focused research name str',
                       'otherPurposeDetails': 'other purpose details str', 'methodsDevelopment': True,
                       'controlSet': True, 'ancestry': True, 'socialBehavioral': True, 'populationHealth': True,
                       'drugDevelopment': True, 'commercialPurpose': True, 'educational': True, 'otherPurpose': True,
                       'scientificApproaches': 'reasonForInvestigation string',
                       'intendToStudy': 'intendToStudy string',
                       'findingsFromStudy': 'findingsFromStudy string',
                       'focusOnUnderrepresentedPopulations': True,
                       'accessTier': 'REGISTERED',
                       'workspaceDemographic': {
                           "raceEthnicity": ['AIAN', 'MENA'],
                           "age": ['AGE_0_11', 'AGE_65_74'],
                           "sexAtBirth": None,
                           "genderIdentity": "OTHER_THAN_MAN_WOMAN",
                           "sexualOrientation": "OTHER_THAN_STRAIGHT",
                           "geography": "RURAL",
                           "disabilityStatus": "DISABILITY",
                           "accessToCare": "NOT_EASILY_ACCESS_CARE",
                           "educationLevel": "LESS_THAN_HIGH_SCHOOL",
                           "incomeLevel": "BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT",
                           "others": "string"
                       },
                       'cdrVersion': None
                       },
                      result['data'])

        # change audit review result
        review_results = [
            {
                "snapshotId": 1,
                "auditorEmail": "auditor_email_1",
                "reviewType": "RAB",
                "displayDecision": "EXCLUDE_FROM_RESEARCHER_DIRECTORY",
                "accessDecision": 'DISABLE_WORKSPACE',
                "auditorNotes": "note1"
            },
            {
                "snapshotId": 2,
                "auditorEmail": "auditor_email_2",
                "reviewType": "RAB",
                "displayDecision": "PUBLISH_TO_RESEARCHER_DIRECTORY",
                "accessDecision": None,
                "auditorNotes": "note2"
            }
        ]
        self.send_post('workbench/audit/workspace/results', review_results)
        result = self.send_get('researchHub/projectDirectory')
        self.assertEqual(len(result['data']), 1)
        self.assertIn({'workspaceId': 1, 'snapshotId': 2, 'name': 'workspace name str 2',
                       'creationTime': '2019-11-25T17:43:41.085000',
                       'modifiedTime': '2019-11-25T17:43:41.085000', 'status': 'INACTIVE',
                       'workspaceUsers': [
                           {'userId': 0, 'userName': 'given name 1 family name 1', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution1', 'role': 'institution role 1', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'},
                               {'institution': 'display name', 'role': 'verified institution role 1',
                                'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}
                           ]},
                           {'userId': 1, 'userName': 'given name 2 family name 2', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution2', 'role': 'institution role 2', 'isVerified': None,
                                'nonAcademicAffiliation': 'UNSET'},
                               {'institution': 'institution22', 'role': 'institution role 22', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'},
                               {'institution': 'verified institution', 'role': 'verified institution role 1',
                                'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}
                           ]}
                       ],
                       'workspaceOwner': [{'userId': 0, 'userName': 'given name 1 family name 1',
                                           'degree': ['PHD', 'MPH'],
                                           'affiliations': [{'institution': 'institution1',
                                                             'role': 'institution role 1',
                                                             'isVerified': None,
                                                             'nonAcademicAffiliation': 'INDUSTRY'},
                                                            {'institution': 'display name',
                                                             'role': 'verified institution role 1',
                                                             'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}]}],
                       'hasVerifiedInstitution': True,
                       'excludeFromPublicDirectory': False, 'ethicalLegalSocialImplications': False,
                       'reviewRequested': False, 'diseaseFocusedResearch': True,
                       'diseaseFocusedResearchName': 'disease focused research name str 2',
                       'otherPurposeDetails': 'other purpose details str 2', 'methodsDevelopment': False,
                       'controlSet': False, 'ancestry': False, 'socialBehavioral': False, 'populationHealth': False,
                       'drugDevelopment': False, 'commercialPurpose': False, 'educational': False,
                       'otherPurpose': False, 'scientificApproaches': 'reasonForInvestigation string2',
                       'intendToStudy': 'intendToStudy string2',
                       'findingsFromStudy': 'findingsFromStudy string2',
                       'focusOnUnderrepresentedPopulations': None,
                       'accessTier': 'UNSET',
                       'workspaceDemographic': {
                           "raceEthnicity": None,
                           "age": None,
                           "sexAtBirth": None,
                           "genderIdentity": None,
                           "sexualOrientation": None,
                           "geography": None,
                           "disabilityStatus": None,
                           "accessToCare": None,
                           "educationLevel": None,
                           "incomeLevel": None,
                           "others": None
                       },
                       'cdrVersion': None
                       },
                      result['data'])

    def test_get_research_projects_directory_search_and_filter(self):
        # create researchers
        researchers_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "modifiedTime": "2019-11-26T21:21:13.056Z",
                "givenName": "givenname1",
                "familyName": "familyname1",
                "streetAddress1": "string",
                "streetAddress2": "string",
                "city": "string",
                "state": "string",
                "zipCode": "string",
                "country": "string",
                "ethnicity": "HISPANIC",
                "gender": ["MAN"],
                "race": ["AIAN"],
                "degree": ["PHD", "MPH"],
                "sexAtBirth": ["FEMALE"],
                "sexualOrientation": "BISEXUAL",
                "affiliations": [
                    {
                        "institution": "institution1",
                        "role": "institution role 1",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ],
                "verifiedInstitutionalAffiliation": {
                    "institutionDisplayName": "display name",
                    "institutionShortName": "verified institution",
                    "institutionalRole": "verified institution role 1",
                    "nonAcademicAffiliation": "INDUSTRY"
                }
            },
            {
                "userId": 1,
                "creationTime": "2019-11-27T21:21:13.056Z",
                "modifiedTime": "2019-11-27T21:21:13.056Z",
                "givenName": "givenname2",
                "familyName": "familyname2",
                "streetAddress1": "string2",
                "streetAddress2": "string2",
                "city": "string2",
                "state": "string2",
                "zipCode": "string2",
                "country": "string2",
                "ethnicity": "HISPANIC",
                "sexualOrientation": "BISEXUAL",
                "gender": ["MAN", "WOMAN"],
                "race": ["AIAN", "WHITE"],
                "degree": ["PHD", "MPH"],
                "affiliations": [
                    {
                        "institution": "institution2",
                        "role": "institution role 2"
                    },
                    {
                        "institution": "institution22",
                        "role": "institution role 22",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ],
                "verifiedInstitutionalAffiliation": {
                    "institutionShortName": "verified institution",
                    "institutionalRole": "verified institution role 1",
                    "nonAcademicAffiliation": "INDUSTRY"
                }
            }
        ]
        self.send_post('workbench/directory/researchers', request_data=researchers_json)

        # create workspace
        request_json = [
            {
                "workspaceId": 0,
                "name": "workspace name str Search test",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-25T17:43:41.085Z",
                "status": "ACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 1,
                        "role": "OWNER",
                        "status": "ACTIVE"
                    }
                ],
                "creator": {
                    "userId": 1,
                    "givenName": "aaa",
                    "familyName": "bbb"
                },
                "excludeFromPublicDirectory": False,
                "ethicalLegalSocialImplications": True,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "disease focused research name str",
                "otherPurposeDetails": "other purpose details str",
                "methodsDevelopment": True,
                "controlSet": True,
                "ancestry": True,
                "socialBehavioral": False,
                "populationHealth": True,
                "drugDevelopment": True,
                "commercialPurpose": True,
                "educational": True,
                "otherPurpose": True,
                "scientificApproaches": 'reasonForInvestigation string',
                "intendToStudy": 'intendToStudy string',
                "findingsFromStudy": 'findingsFromStudy string',
                "focusOnUnderrepresentedPopulations": True,
                "workspaceDemographic": {
                    "raceEthnicity": ['AIAN', 'MENA'],
                    "age": ['AGE_0_11', 'AGE_65_74'],
                    "sexAtBirth": "UNSET",
                    "genderIdentity": "OTHER_THAN_MAN_WOMAN",
                    "sexualOrientation": "OTHER_THAN_STRAIGHT",
                    "geography": "RURAL",
                    "disabilityStatus": "DISABILITY",
                    "accessToCare": "NOT_EASILY_ACCESS_CARE",
                    "educationLevel": "LESS_THAN_HIGH_SCHOOL",
                    "incomeLevel": "BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT",
                    "others": "string"
                }
            },
            {
                "workspaceId": 1,
                "name": "workspace name str 2",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-25T17:43:41.085Z",
                "status": "INACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 0,
                        "role": "OWNER",
                        "status": "ACTIVE"
                    },
                    {
                        "userId": 1,
                        "role": "READER",
                        "status": "ACTIVE"
                    }
                ],
                "creator": {
                    "userId": 0,
                    "givenName": "aaa",
                    "familyName": "bbb"
                },
                "excludeFromPublicDirectory": False,
                "ethicalLegalSocialImplications": False,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "disease focused research name str 2",
                "otherPurposeDetails": "other purpose details str 2",
                "methodsDevelopment": False,
                "controlSet": False,
                "ancestry": False,
                "socialBehavioral": False,
                "populationHealth": False,
                "drugDevelopment": False,
                "commercialPurpose": False,
                "educational": False,
                "otherPurpose": False,
                "scientificApproaches": 'reasonForInvestigation string2',
                "intendToStudy": 'intendToStudy string2',
                "findingsFromStudy": 'findingsFromStudy string2'
            }
        ]
        now = clock.CLOCK.now()
        sequest_hours_ago = now - timedelta(hours=24)
        with FakeClock(sequest_hours_ago):
            self.send_post('workbench/directory/workspaces', request_data=request_json)
        result = self.send_get('researchHub/projectDirectory?status=ACTIVE')
        self.assertEqual(len(result['data']), 1)
        # test search by project purpose
        result = self.send_get('researchHub/projectDirectory?projectPurpose=controlSet')
        self.assertEqual(result['totalActiveProjects'], 1)
        self.assertEqual(result['totalMatchedRecords'], 1)
        self.assertEqual(len(result['data']), 1)
        # test search by multiple project purpose
        result = self.send_get('researchHub/projectDirectory?projectPurpose=controlSet,socialBehavioral')
        self.assertEqual(len(result['data']), 0)
        # test search by workspace name
        result = self.send_get('researchHub/projectDirectory?workspaceNameLike=Search%20test')
        self.assertEqual(len(result['data']), 1)
        # test search by workspace intendToStudy
        result = self.send_get('researchHub/projectDirectory?intendToStudyLike=string2')
        self.assertEqual(len(result['data']), 1)
        # test search by generalized parameter workspaceLike
        result = self.send_get('researchHub/projectDirectory?workspaceLike=str')
        self.assertEqual(result['totalActiveProjects'], 1)
        self.assertEqual(result['totalMatchedRecords'], 2)
        self.assertEqual(len(result['data']), 2)
        result = self.send_get('researchHub/projectDirectory?workspaceLike=string2')
        self.assertEqual(len(result['data']), 1)
        # test parameter "workspaceLike" will overwrite "intendToStudyLike"
        result = self.send_get('researchHub/projectDirectory?workspaceLike=str&intendToStudyLike=string2')
        self.assertEqual(len(result['data']), 2)
        # test search by owner given/family name
        result = self.send_get('researchHub/projectDirectory?givenName=givenname1')
        self.assertEqual(len(result['data']), 1)
        result = self.send_get('researchHub/projectDirectory?givenName=givenname2')
        self.assertEqual(len(result['data']), 1)
        result = self.send_get('researchHub/projectDirectory?familyName=familyname1')
        self.assertEqual(len(result['data']), 1)
        result = self.send_get('researchHub/projectDirectory?familyName=familyname2')
        self.assertEqual(len(result['data']), 1)
        # test search by owner full name
        result = self.send_get('researchHub/projectDirectory?ownerName=nname1%20fami')
        self.assertEqual(len(result['data']), 1)
        # test search by user id
        result = self.send_get('researchHub/projectDirectory?userId=1&userRole=owner')
        self.assertEqual(result['totalActiveProjects'], 1)
        self.assertEqual(result['totalMatchedRecords'], 1)
        self.assertEqual(len(result['data']), 1)
        result = self.send_get('researchHub/projectDirectory?userId=1&userRole=member')
        self.assertEqual(len(result['data']), 1)
        result = self.send_get('researchHub/projectDirectory?userId=1&userRole=all')
        self.assertEqual(len(result['data']), 2)
        # test page and page size
        result = self.send_get('researchHub/projectDirectory?page=1&pageSize=1')
        self.assertEqual(result['totalActiveProjects'], 1)
        self.assertEqual(result['totalMatchedRecords'], 2)
        self.assertEqual(len(result['data']), 1)
        result = self.send_get('researchHub/projectDirectory?page=2&pageSize=1')
        self.assertEqual(len(result['data']), 1)
        result = self.send_get('researchHub/projectDirectory?page=3&pageSize=1')
        self.assertEqual(len(result['data']), 0)
        result = self.send_get('researchHub/projectDirectory?page=1&pageSize=2')
        self.assertEqual(result['totalMatchedRecords'], 2)
        self.assertEqual(len(result['data']), 2)

    def test_get_research_projects_directory_less_than_23_hours(self):
        # create researchers
        researchers_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "modifiedTime": "2019-11-26T21:21:13.056Z",
                "givenName": "given name 1",
                "familyName": "family name 1",
                "streetAddress1": "string",
                "streetAddress2": "string",
                "city": "string",
                "state": "string",
                "zipCode": "string",
                "country": "string",
                "ethnicity": "HISPANIC",
                "gender": ["MAN"],
                "race": ["AIAN"],
                "sexAtBirth": ["FEMALE"],
                "sexualOrientation": "BISEXUAL",
                "affiliations": [
                    {
                        "institution": "institution1",
                        "role": "institution role 1",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ],
                "verifiedInstitutionalAffiliation": {
                    "institutionShortName": "verified institution",
                    "institutionalRole": "verified institution role 1",
                    "nonAcademicAffiliation": "INDUSTRY"
                }
            }
        ]
        self.send_post('workbench/directory/researchers', request_data=researchers_json)

        # create workspace
        request_json = [
            {
                "workspaceId": 0,
                "name": "workspace name str",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-25T17:43:41.085Z",
                "status": "ACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 0,
                        "role": "READER",
                        "status": "ACTIVE"
                    }
                ],
                "excludeFromPublicDirectory": False,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "disease focused research name str",
                "otherPurposeDetails": "other purpose details str",
                "methodsDevelopment": True,
                "controlSet": True,
                "ancestry": True,
                "socialBehavioral": True,
                "populationHealth": True,
                "drugDevelopment": True,
                "commercialPurpose": True,
                "educational": True,
                "otherPurpose": True,
                "scientificApproaches": 'reasonForInvestigation string',
                "intendToStudy": 'intendToStudy string',
                "findingsFromStudy": 'findingsFromStudy string',
                "focusOnUnderrepresentedPopulations": True,
                "workspaceDemographic": {
                    "raceEthnicity": ['AIAN', 'MENA'],
                    "age": ['AGE_0_11', 'AGE_65_74'],
                    "sexAtBirth": "UNSET",
                    "genderIdentity": "OTHER_THAN_MAN_WOMAN",
                    "sexualOrientation": "OTHER_THAN_STRAIGHT",
                    "geography": "RURAL",
                    "disabilityStatus": "DISABILITY",
                    "accessToCare": "NOT_EASILY_ACCESS_CARE",
                    "educationLevel": "LESS_THAN_HIGH_SCHOOL",
                    "incomeLevel": "BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT",
                    "others": "string"
                }
            }
        ]
        now = clock.CLOCK.now()
        sequest_hours_ago = now - timedelta(hours=22)
        with FakeClock(sequest_hours_ago):
            self.send_post('workbench/directory/workspaces', request_data=request_json)
        # test get research projects directory before review
        result = self.send_get('researchHub/projectDirectory')
        self.assertEqual(len(result['data']), 0)

    def test_workspace_audit_sync_api(self):
        # create researchers
        researchers_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "modifiedTime": "2019-11-26T21:21:13.056Z",
                "givenName": "given name 1",
                "familyName": "family name 1",
                "streetAddress1": "string",
                "streetAddress2": "string",
                "city": "string",
                "state": "string",
                "zipCode": "string",
                "country": "string",
                "ethnicity": "HISPANIC",
                "gender": ["MAN"],
                "race": ["AIAN"],
                "sexAtBirth": ["FEMALE"],
                "sexualOrientation": "BISEXUAL",
                "affiliations": [
                    {
                        "institution": "institution1",
                        "role": "institution role 1",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ]
            },
            {
                "userId": 1,
                "creationTime": "2019-11-27T21:21:13.056Z",
                "modifiedTime": "2019-11-27T21:21:13.056Z",
                "givenName": "given name 2",
                "familyName": "family name 2",
                "streetAddress1": "string2",
                "streetAddress2": "string2",
                "city": "string2",
                "state": "string2",
                "zipCode": "string2",
                "country": "string2",
                "ethnicity": "HISPANIC",
                "sexualOrientation": "BISEXUAL",
                "gender": ["MAN", "WOMAN"],
                "race": ["AIAN", "WHITE"],
                "affiliations": [
                    {
                        "institution": "institution2",
                        "role": "institution role 2"
                    },
                    {
                        "institution": "institution22",
                        "role": "institution role 22",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ]
            }
        ]
        self.send_post('workbench/directory/researchers', request_data=researchers_json)

        # create workspace
        cdr_version = 'irving'
        request_json = [
            {
                "workspaceId": 0,
                "name": "workspace name str",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-25T17:43:41.085Z",
                "status": "ACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 0,
                        "role": "READER",
                        "status": "ACTIVE"
                    },
                    {
                        "userId": 1,
                        "role": "OWNER",
                        "status": "ACTIVE"
                    }
                ],
                "creator": {
                    "userId": 0,
                    "givenName": "aaa",
                    "familyName": "bbb"
                },
                "excludeFromPublicDirectory": False,
                "ethicalLegalSocialImplications": True,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "disease focused research name str",
                "otherPurposeDetails": "other purpose details str",
                "methodsDevelopment": True,
                "controlSet": True,
                "ancestry": True,
                "socialBehavioral": True,
                "populationHealth": True,
                "drugDevelopment": True,
                "commercialPurpose": True,
                "educational": True,
                "otherPurpose": True,
                "scientificApproaches": 'reasonForInvestigation string',
                "intendToStudy": 'intendToStudy string',
                "findingsFromStudy": 'findingsFromStudy string',
                "focusOnUnderrepresentedPopulations": True,
                "workspaceDemographic": {
                    "raceEthnicity": ['AIAN', 'MENA'],
                    "age": ['AGE_0_11', 'AGE_65_74'],
                    "sexAtBirth": "UNSET",
                    "genderIdentity": "OTHER_THAN_MAN_WOMAN",
                    "sexualOrientation": "OTHER_THAN_STRAIGHT",
                    "geography": "RURAL",
                    "disabilityStatus": "DISABILITY",
                    "accessToCare": "NOT_EASILY_ACCESS_CARE",
                    "educationLevel": "LESS_THAN_HIGH_SCHOOL",
                    "incomeLevel": "BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT",
                    "others": "string"
                },
                "cdrVersionName": cdr_version
            },
            {
                "workspaceId": 1,
                "name": "workspace name str 2",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-25T17:43:41.085Z",
                "status": "INACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 0,
                        "role": "OWNER",
                        "status": "ACTIVE"
                    },
                    {
                        "userId": 1,
                        "role": "READER",
                        "status": "ACTIVE"
                    }
                ],
                "excludeFromPublicDirectory": False,
                "ethicalLegalSocialImplications": False,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "disease focused research name str 2",
                "otherPurposeDetails": "other purpose details str 2",
                "methodsDevelopment": False,
                "controlSet": False,
                "ancestry": False,
                "socialBehavioral": False,
                "populationHealth": False,
                "drugDevelopment": False,
                "commercialPurpose": False,
                "educational": False,
                "otherPurpose": False,
                "scientificApproaches": 'reasonForInvestigation string2',
                "intendToStudy": 'intendToStudy string2',
                "findingsFromStudy": 'findingsFromStudy string2',
                "cdrVersionName": cdr_version
            }
        ]
        self.send_post('workbench/directory/workspaces', request_data=request_json)
        # test workbench audit
        result = self.send_get('workbench/audit/workspace/snapshots')
        self.assertIn({'snapshotId': 1, 'workspaceId': 0, 'name': 'workspace name str',
                       'creationTime': '2019-11-25T17:43:41.085000', 'modifiedTime': '2019-11-25T17:43:41.085000',
                       'status': 'ACTIVE',
                       'workspaceUsers': [{'userId': 0, 'role': 'READER', 'status': 'ACTIVE', 'isCreator': True},
                                          {'userId': 1, 'role': 'OWNER', 'status': 'ACTIVE', 'isCreator': False}],
                       'workspaceResearchers': [
                           {'userId': 0, 'creationTime': '2019-11-26T21:21:13.056000',
                            'modifiedTime': '2019-11-26T21:21:13.056000', 'givenName': 'given name 1',
                            'familyName': 'family name 1', 'email': None, 'verifiedInstitutionalAffiliation': {},
                            'affiliations': [
                                {'institution': 'institution1', 'role': 'institution role 1', 'isVerified': None,
                                 'nonAcademicAffiliation': 'INDUSTRY'}
                            ]},
                           {'userId': 1, 'creationTime': '2019-11-27T21:21:13.056000',
                            'modifiedTime': '2019-11-27T21:21:13.056000', 'givenName': 'given name 2',
                            'familyName': 'family name 2', 'email': None, 'verifiedInstitutionalAffiliation': {},
                            'affiliations': [
                                {'institution': 'institution2', 'role': 'institution role 2',
                                 'isVerified': None, 'nonAcademicAffiliation': 'UNSET'},
                                {'institution': 'institution22', 'role': 'institution role 22', 'isVerified': None,
                                 'nonAcademicAffiliation': 'INDUSTRY'}
                            ]}],
                       'excludeFromPublicDirectory': False, 'ethicalLegalSocialImplications': True,
                       'reviewRequested': False, 'diseaseFocusedResearch': True,
                       'diseaseFocusedResearchName': 'disease focused research name str',
                       'otherPurposeDetails': 'other purpose details str', 'methodsDevelopment': True,
                       'controlSet': True, 'ancestry': True, 'socialBehavioral': True, 'populationHealth': True,
                       'drugDevelopment': True, 'commercialPurpose': True, 'educational': True, 'otherPurpose': True,
                       'scientificApproaches': 'reasonForInvestigation string', 'intendToStudy': 'intendToStudy string',
                       'findingsFromStudy': 'findingsFromStudy string', 'focusOnUnderrepresentedPopulations': True,
                       'accessTier': 'UNSET',
                       'workspaceDemographic': {
                           'raceEthnicity': ['AIAN', 'MENA'], 'age': ['AGE_0_11', 'AGE_65_74'],
                           'sexAtBirth': None, 'genderIdentity': 'OTHER_THAN_MAN_WOMAN',
                           'sexualOrientation': 'OTHER_THAN_STRAIGHT', 'geography': 'RURAL',
                           'disabilityStatus': 'DISABILITY', 'accessToCare': 'NOT_EASILY_ACCESS_CARE',
                           'educationLevel': 'LESS_THAN_HIGH_SCHOOL',
                           'incomeLevel': 'BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT',
                           'others': 'string'},
                       'cdrVersion': cdr_version
                       }, result)
        self.assertIn({'snapshotId': 2, 'workspaceId': 1, 'name': 'workspace name str 2',
                       'creationTime': '2019-11-25T17:43:41.085000', 'modifiedTime': '2019-11-25T17:43:41.085000',
                       'status': 'INACTIVE',
                       'workspaceUsers': [
                           {'userId': 0, 'role': 'OWNER', 'status': 'ACTIVE', 'isCreator': False},
                           {'userId': 1, 'role': 'READER', 'status': 'ACTIVE', 'isCreator': False}
                       ],
                       'workspaceResearchers': [
                           {'userId': 0, 'creationTime': '2019-11-26T21:21:13.056000',
                            'modifiedTime': '2019-11-26T21:21:13.056000', 'givenName': 'given name 1',
                            'familyName': 'family name 1', 'email': None, 'verifiedInstitutionalAffiliation': {},
                            'affiliations': [
                                {'institution': 'institution1', 'role': 'institution role 1', 'isVerified': None,
                                 'nonAcademicAffiliation': 'INDUSTRY'}
                            ]},
                           {'userId': 1, 'creationTime': '2019-11-27T21:21:13.056000',
                            'modifiedTime': '2019-11-27T21:21:13.056000', 'givenName': 'given name 2',
                            'familyName': 'family name 2', 'email': None, 'verifiedInstitutionalAffiliation': {},
                            'affiliations': [
                                {'institution': 'institution2', 'role': 'institution role 2', 'isVerified': None,
                                 'nonAcademicAffiliation': 'UNSET'},
                                {'institution': 'institution22', 'role': 'institution role 22', 'isVerified': None,
                                 'nonAcademicAffiliation': 'INDUSTRY'}
                            ]}],
                       'excludeFromPublicDirectory': False, 'ethicalLegalSocialImplications': False,
                       'reviewRequested': False, 'diseaseFocusedResearch': True,
                       'diseaseFocusedResearchName': 'disease focused research name str 2',
                       'otherPurposeDetails': 'other purpose details str 2', 'methodsDevelopment': False,
                       'controlSet': False, 'ancestry': False, 'socialBehavioral': False, 'populationHealth': False,
                       'drugDevelopment': False, 'commercialPurpose': False, 'educational': False,
                       'otherPurpose': False, 'scientificApproaches': 'reasonForInvestigation string2',
                       'intendToStudy': 'intendToStudy string2', 'findingsFromStudy': 'findingsFromStudy string2',
                       'focusOnUnderrepresentedPopulations': None, 'accessTier': 'UNSET',
                       'workspaceDemographic': {
                           'raceEthnicity': None, 'age': None, 'sexAtBirth': None, 'genderIdentity': None,
                           'sexualOrientation': None, 'geography': None, 'disabilityStatus': None,
                           'accessToCare': None, 'educationLevel': None, 'incomeLevel': None, 'others': None},
                       'cdrVersion': cdr_version
                       }, result)

        result = self.send_get('workbench/audit/workspace/snapshots?last_snapshot_id=1')
        self.assertEqual(len(result), 1)
        self.assertIn({'snapshotId': 2, 'workspaceId': 1, 'name': 'workspace name str 2',
                       'creationTime': '2019-11-25T17:43:41.085000', 'modifiedTime': '2019-11-25T17:43:41.085000',
                       'status': 'INACTIVE',
                       'workspaceUsers': [
                           {'userId': 0, 'role': 'OWNER', 'status': 'ACTIVE', 'isCreator': False},
                           {'userId': 1, 'role': 'READER', 'status': 'ACTIVE', 'isCreator': False}
                       ],
                       'workspaceResearchers': [
                           {'userId': 0, 'creationTime': '2019-11-26T21:21:13.056000',
                            'modifiedTime': '2019-11-26T21:21:13.056000', 'givenName': 'given name 1',
                            'familyName': 'family name 1', 'email': None, 'verifiedInstitutionalAffiliation': {},
                            'affiliations': [
                                {'institution': 'institution1', 'role': 'institution role 1', 'isVerified': None,
                                 'nonAcademicAffiliation': 'INDUSTRY'}
                            ]},
                           {'userId': 1, 'creationTime': '2019-11-27T21:21:13.056000',
                            'modifiedTime': '2019-11-27T21:21:13.056000', 'givenName': 'given name 2',
                            'familyName': 'family name 2', 'email': None, 'verifiedInstitutionalAffiliation': {},
                            'affiliations': [
                                {'institution': 'institution2', 'role': 'institution role 2', 'isVerified': None,
                                 'nonAcademicAffiliation': 'UNSET'},
                                {'institution': 'institution22', 'role': 'institution role 22', 'isVerified': None,
                                 'nonAcademicAffiliation': 'INDUSTRY'}
                            ]}],
                       'excludeFromPublicDirectory': False, 'ethicalLegalSocialImplications': False,
                       'reviewRequested': False, 'diseaseFocusedResearch': True,
                       'diseaseFocusedResearchName': 'disease focused research name str 2',
                       'otherPurposeDetails': 'other purpose details str 2', 'methodsDevelopment': False,
                       'controlSet': False, 'ancestry': False, 'socialBehavioral': False, 'populationHealth': False,
                       'drugDevelopment': False, 'commercialPurpose': False, 'educational': False,
                       'otherPurpose': False, 'scientificApproaches': 'reasonForInvestigation string2',
                       'intendToStudy': 'intendToStudy string2', 'findingsFromStudy': 'findingsFromStudy string2',
                       'focusOnUnderrepresentedPopulations': None, 'accessTier': 'UNSET',
                       'workspaceDemographic': {
                           'raceEthnicity': None, 'age': None, 'sexAtBirth': None, 'genderIdentity': None,
                           'sexualOrientation': None, 'geography': None, 'disabilityStatus': None,
                           'accessToCare': None, 'educationLevel': None, 'incomeLevel': None, 'others': None},
                       'cdrVersion': cdr_version
                       }, result)

        result = self.send_get('workbench/audit/workspace/snapshots?last_snapshot_id=2')
        self.assertEqual(len(result), 0)

        result = self.send_get('workbench/audit/workspace/snapshots?snapshot_id=1')
        self.assertEqual(len(result), 1)

        # test get latest snapshot by workspace id
        updated_request_json = [
            {
                "workspaceId": 1,
                "name": "workspace name str 3",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-26T17:43:41.085Z",
                "status": "INACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 0,
                        "role": "OWNER",
                        "status": "ACTIVE"
                    },
                    {
                        "userId": 1,
                        "role": "READER",
                        "status": "ACTIVE"
                    }
                ],
                "excludeFromPublicDirectory": False,
                "ethicalLegalSocialImplications": False,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "disease focused research name str 2",
                "otherPurposeDetails": "other purpose details str 2",
                "methodsDevelopment": False,
                "controlSet": False,
                "ancestry": False,
                "socialBehavioral": False,
                "populationHealth": False,
                "drugDevelopment": False,
                "commercialPurpose": False,
                "educational": False,
                "otherPurpose": False,
                "scientificApproaches": 'reasonForInvestigation string2',
                "intendToStudy": 'intendToStudy string2',
                "findingsFromStudy": 'findingsFromStudy string2',
                "cdrVersionName": cdr_version
            }
        ]
        self.send_post('workbench/directory/workspaces', request_data=updated_request_json)
        result = self.send_get('workbench/audit/workspace/snapshots')
        self.assertEqual(len(result), 3)
        result = self.send_get('workbench/audit/workspace/snapshots?workspace_id=1')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'workspace name str 3')

    def test_hide_workspace_without_verified_institution_from_RH(self):
        # create researchers
        researchers_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "modifiedTime": "2019-11-26T21:21:13.056Z",
                "givenName": "given name 1",
                "familyName": "family name 1",
                "streetAddress1": "string",
                "streetAddress2": "string",
                "city": "string",
                "state": "string",
                "zipCode": "string",
                "country": "string",
                "ethnicity": "HISPANIC",
                "gender": ["MAN"],
                "race": ["AIAN"],
                "sexAtBirth": ["FEMALE"],
                "degree": ["PHD", "MPH"],
                "sexualOrientation": "BISEXUAL",
                "affiliations": [
                    {
                        "institution": "institution1",
                        "role": "institution role 1",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ],
                "verifiedInstitutionalAffiliation": {
                    "institutionShortName": "verified institution",
                    "institutionalRole": "verified institution role 1",
                    "nonAcademicAffiliation": "INDUSTRY"
                }
            },
            {
                "userId": 1,
                "creationTime": "2019-11-27T21:21:13.056Z",
                "modifiedTime": "2019-11-27T21:21:13.056Z",
                "givenName": "given name 2",
                "familyName": "family name 2",
                "streetAddress1": "string2",
                "streetAddress2": "string2",
                "city": "string2",
                "state": "string2",
                "zipCode": "string2",
                "country": "string2",
                "ethnicity": "HISPANIC",
                "sexualOrientation": "BISEXUAL",
                "gender": ["MAN", "WOMAN"],
                "race": ["AIAN", "WHITE"],
                "degree": ["PHD", "MPH"],
                "affiliations": [
                    {
                        "institution": "institution2",
                        "role": "institution role 2"
                    },
                    {
                        "institution": "institution22",
                        "role": "institution role 22",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ]
            }
        ]
        self.send_post('workbench/directory/researchers', request_data=researchers_json)

        # create workspace
        request_json = [
            {
                "workspaceId": 0,
                "name": "workspace name str",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-25T17:43:41.085Z",
                "status": "ACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 1,
                        "role": "OWNER",
                        "status": "ACTIVE"
                    }
                ],
                "excludeFromPublicDirectory": False,
                "ethicalLegalSocialImplications": True,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "disease focused research name str",
                "otherPurposeDetails": "other purpose details str",
                "methodsDevelopment": True,
                "controlSet": True,
                "ancestry": True,
                "socialBehavioral": True,
                "populationHealth": True,
                "drugDevelopment": True,
                "commercialPurpose": True,
                "educational": True,
                "otherPurpose": True,
                "scientificApproaches": 'reasonForInvestigation string',
                "intendToStudy": 'intendToStudy string',
                "findingsFromStudy": 'findingsFromStudy string',
                "focusOnUnderrepresentedPopulations": True,
                "workspaceDemographic": {
                    "raceEthnicity": ['AIAN', 'MENA'],
                    "age": ['AGE_0_11', 'AGE_65_74'],
                    "sexAtBirth": "UNSET",
                    "genderIdentity": "OTHER_THAN_MAN_WOMAN",
                    "sexualOrientation": "OTHER_THAN_STRAIGHT",
                    "geography": "RURAL",
                    "disabilityStatus": "DISABILITY",
                    "accessToCare": "NOT_EASILY_ACCESS_CARE",
                    "educationLevel": "LESS_THAN_HIGH_SCHOOL",
                    "incomeLevel": "BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT",
                    "others": "string"
                }
            },
            {
                "workspaceId": 1,
                "name": "workspace name str 2",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-25T17:43:41.085Z",
                "status": "INACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 0,
                        "role": "OWNER",
                        "status": "ACTIVE"
                    },
                    {
                        "userId": 1,
                        "role": "READER",
                        "status": "ACTIVE"
                    }
                ],
                "creator": {
                    "userId": 0,
                    "givenName": "aaa",
                    "familyName": "bbb"
                },
                "excludeFromPublicDirectory": False,
                "ethicalLegalSocialImplications": False,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "disease focused research name str 2",
                "otherPurposeDetails": "other purpose details str 2",
                "methodsDevelopment": False,
                "controlSet": False,
                "ancestry": False,
                "socialBehavioral": False,
                "populationHealth": False,
                "drugDevelopment": False,
                "commercialPurpose": False,
                "educational": False,
                "otherPurpose": False,
                "scientificApproaches": 'reasonForInvestigation string2',
                "intendToStudy": 'intendToStudy string2',
                "findingsFromStudy": 'findingsFromStudy string2'
            }
        ]
        now = clock.CLOCK.now()
        sequest_hours_ago = now - timedelta(hours=24)
        with FakeClock(sequest_hours_ago):
            self.send_post('workbench/directory/workspaces', request_data=request_json)
        # test get research projects directory before review
        result = self.send_get('researchHub/projectDirectory')
        self.assertEqual(len(result['data']), 1)
        self.assertIn({'workspaceId': 1, 'snapshotId': 2, 'name': 'workspace name str 2',
                       'creationTime': '2019-11-25T17:43:41.085000',
                       'modifiedTime': '2019-11-25T17:43:41.085000', 'status': 'INACTIVE',
                       'workspaceUsers': [
                           {'userId': 0, 'userName': 'given name 1 family name 1', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution1', 'role': 'institution role 1', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'},
                           {'institution': 'verified institution', 'role': 'verified institution role 1',
                            'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}]},
                           {'userId': 1, 'userName': 'given name 2 family name 2', 'degree': ['PHD', 'MPH'],
                            'affiliations': [
                               {'institution': 'institution2', 'role': 'institution role 2', 'isVerified': None,
                                'nonAcademicAffiliation': 'UNSET'},
                               {'institution': 'institution22', 'role': 'institution role 22', 'isVerified': None,
                                'nonAcademicAffiliation': 'INDUSTRY'}]}
                       ],
                       'workspaceOwner': [{'userId': 0, 'userName': 'given name 1 family name 1',
                                           'degree': ['PHD', 'MPH'],
                                           'affiliations': [{'institution': 'institution1',
                                                             'role': 'institution role 1',
                                                             'isVerified': None,
                                                             'nonAcademicAffiliation': 'INDUSTRY'},
                                                            {'institution': 'verified institution',
                                                             'role': 'verified institution role 1',
                                                             'isVerified': True, 'nonAcademicAffiliation': 'UNSET'}]}],
                       'hasVerifiedInstitution': True,
                       'excludeFromPublicDirectory': False, 'ethicalLegalSocialImplications': False,
                       'reviewRequested': False, 'diseaseFocusedResearch': True,
                       'diseaseFocusedResearchName': 'disease focused research name str 2',
                       'otherPurposeDetails': 'other purpose details str 2', 'methodsDevelopment': False,
                       'controlSet': False, 'ancestry': False, 'socialBehavioral': False, 'populationHealth': False,
                       'drugDevelopment': False, 'commercialPurpose': False, 'educational': False,
                       'otherPurpose': False, 'scientificApproaches': 'reasonForInvestigation string2',
                       'intendToStudy': 'intendToStudy string2',
                       'findingsFromStudy': 'findingsFromStudy string2',
                       'focusOnUnderrepresentedPopulations': None,
                       'accessTier': 'UNSET',
                       'workspaceDemographic': {
                           "raceEthnicity": None,
                           "age": None,
                           "sexAtBirth": None,
                           "genderIdentity": None,
                           "sexualOrientation": None,
                           "geography": None,
                           "disabilityStatus": None,
                           "accessToCare": None,
                           "educationLevel": None,
                           "incomeLevel": None,
                           "others": None
                       },
                       'cdrVersion': None
                       },
                      result['data'])
        # update researcher to add verified institution
        researchers_json = [
            {
                "userId": 1,
                "creationTime": "2019-11-27T21:21:13.056Z",
                "modifiedTime": "2019-11-27T21:21:14.056Z",
                "givenName": "given name 2",
                "familyName": "family name 2",
                "streetAddress1": "string2",
                "streetAddress2": "string2",
                "city": "string2",
                "state": "string2",
                "zipCode": "string2",
                "country": "string2",
                "ethnicity": "HISPANIC",
                "sexualOrientation": "BISEXUAL",
                "gender": ["MAN", "WOMAN"],
                "race": ["AIAN", "WHITE"],
                "affiliations": [
                    {
                        "institution": "institution2",
                        "role": "institution role 2"
                    },
                    {
                        "institution": "institution22",
                        "role": "institution role 22",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ],
                "verifiedInstitutionalAffiliation": {
                    "institutionShortName": "verified institution",
                    "institutionalRole": "verified institution role 1",
                    "nonAcademicAffiliation": "INDUSTRY"
                }
            }
        ]
        self.send_post('workbench/directory/researchers', request_data=researchers_json)

        result = self.send_get('researchHub/projectDirectory')
        self.assertEqual(len(result['data']), 2)

    def test_get_audit_researchers_with_params(self):
        researchers_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "modifiedTime": "2019-11-26T21:21:13.056Z",
                "givenName": "given name 1",
                "familyName": "family name 1",
                "email": "tester@email.com",
                "streetAddress1": "string",
                "streetAddress2": "string",
                "city": "string",
                "state": "string",
                "zipCode": "string",
                "country": "string",
                "ethnicity": "HISPANIC",
                "gender": ["MAN"],
                "race": ["AIAN"],
                "degree": ["PHD", "MPH"],
                "sexAtBirth": ["FEMALE"],
                "sexualOrientation": "BISEXUAL",
                "affiliations": [
                    {
                        "institution": "institution1",
                        "role": "institution role 1",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ],
                "verifiedInstitutionalAffiliation": {
                    "institutionDisplayName": "display name",
                    "institutionShortName": "verified institution",
                    "institutionalRole": "verified institution role 1",
                    "nonAcademicAffiliation": "INDUSTRY"
                }
            },
            {
                "userId": 1,
                "creationTime": "2019-11-27T21:21:13.056Z",
                "modifiedTime": "2019-11-27T21:21:13.056Z",
                "givenName": "given name 2",
                "familyName": "family name 2",
                "streetAddress1": "string2",
                "streetAddress2": "string2",
                "city": "string2",
                "state": "string2",
                "zipCode": "string2",
                "country": "string2",
                "ethnicity": "HISPANIC",
                "sexualOrientation": "BISEXUAL",
                "gender": ["MAN", "WOMAN"],
                "race": ["AIAN", "WHITE"],
                "degree": ["PHD", "MPH"],
                "affiliations": [
                    {
                        "institution": "institution2",
                        "role": "institution role 2"
                    },
                    {
                        "institution": "institution22",
                        "role": "institution role 22",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ],
                "verifiedInstitutionalAffiliation": {
                    "institutionShortName": "verified institution",
                    "institutionalRole": "verified institution role 1",
                    "nonAcademicAffiliation": "INDUSTRY"
                }
            }
        ]
        self.send_post('workbench/directory/researchers', request_data=researchers_json)

        result = self.send_get('workbench/audit/researcher/snapshots')
        self.assertEqual(len(result), 2)
        self.assertIsNotNone(result[0]['givenName'])
        self.assertIsNotNone(result[0]['familyName'])
        self.assertIsNotNone(result[0]['email'])
        self.assertEqual(len(result[0]['affiliations']), 2)

        self.assertIsNotNone(result[1]['givenName'])
        self.assertIsNotNone(result[1]['familyName'])
        self.assertIsNone(result[1]['email'])
        self.assertEqual(len(result[1]['affiliations']), 3)

        result = self.send_get('workbench/audit/researcher/snapshots?snapshot_id=1')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['givenName'], 'given name 1')
        self.assertEqual(result[0]['familyName'], 'family name 1')
        self.assertEqual(result[0]['email'], 'tester@email.com')

        result = self.send_get('workbench/audit/researcher/snapshots?last_snapshot_id=2')
        self.assertEmpty(result)

        result = self.send_get('workbench/audit/researcher/snapshots?last_snapshot_id=1')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['givenName'], 'given name 2')
        self.assertEqual(result[0]['familyName'], 'family name 2')
        self.assertIsNone(result[0]['email'])

        result = self.send_get('workbench/audit/researcher/snapshots?user_source_id=1')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['givenName'], 'given name 2')
        self.assertEqual(result[0]['familyName'], 'family name 2')
        self.assertIsNone(result[0]['email'])

