import http.client
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.dao.workbench_dao import WorkbenchResearcherDao, WorkbenchResearcherHistoryDao, \
    WorkbenchWorkspaceDao, WorkbenchWorkspaceHistoryDao
from rdr_service.participant_enums import WorkbenchWorkspaceUserRole, WorkbenchInstitutionNonAcademic, \
    WorkbenchResearcherEducation, WorkbenchResearcherDisability, WorkbenchResearcherDegree, \
    WorkbenchWorkspaceSexAtBirth, WorkbenchWorkspaceGenderIdentity, WorkbenchWorkspaceSexualOrientation, \
    WorkbenchWorkspaceGeography, WorkbenchWorkspaceDisabilityStatus, WorkbenchWorkspaceAccessToCare, \
    WorkbenchWorkspaceEducationLevel, WorkbenchWorkspaceIncomeLevel, WorkbenchResearcherEthnicity


class WorkbenchApiTest(BaseTestCase):
    def setUp(self):
        super().setUp(with_data=False)

    def test_create_and_update_researchers(self):
        # test create new
        request_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "modifiedTime": "2019-11-26T21:21:13.056Z",
                "givenName": "string",
                "familyName": "string",
                "streetAddress1": "string",
                "streetAddress2": "string",
                "city": "string",
                "state": "string",
                "zipCode": "string",
                "country": "string",
                "ethnicity": "HISPANIC",
                "sexAtBirth": ["FEMALE", "INTERSEX"],
                "identifiesAsLgbtq": False,
                "lgbtqIdentity": "string",
                "gender": ["MAN", "WOMAN"],
                "race": ["AIAN", "WHITE"],
                "education": "COLLEGE_GRADUATE",
                "degree": ["PHD", "MBA"],
                "disability": "YES",
                "affiliations": [
                    {
                        "institution": "string",
                        "role": "string",
                        "nonAcademicAffiliation": "INDUSTRY"
                    }
                ]
            }
        ]

        self.send_post('workbench/directory/researchers', request_data=request_json)
        researcher_dao = WorkbenchResearcherDao()
        self.assertEqual(researcher_dao.count(), 1)
        results = researcher_dao.get_all_with_children()
        self.assertEqual(results[0].userSourceId, 0)
        self.assertEqual(results[0].givenName, 'string')
        self.assertEqual(results[0].gender, [1, 2])
        self.assertEqual(results[0].race, [1, 5])
        self.assertEqual(results[0].sexAtBirth, [1, 3])
        self.assertEqual(results[0].ethnicity, WorkbenchResearcherEthnicity('HISPANIC'))
        self.assertEqual(results[0].education, WorkbenchResearcherEducation('COLLEGE_GRADUATE'))
        self.assertEqual(results[0].degree, [1, 8])
        self.assertEqual(results[0].disability, WorkbenchResearcherDisability('YES'))
        self.assertEqual(results[0].identifiesAsLgbtq, False)
        self.assertEqual(results[0].lgbtqIdentity, None)
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].institution, 'string')
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].nonAcademicAffiliation,
                         WorkbenchInstitutionNonAcademic('INDUSTRY'))

        researcher_history_dao = WorkbenchResearcherHistoryDao()
        results = researcher_history_dao.get_all_with_children()
        self.assertEqual(researcher_history_dao.count(), 1)
        self.assertEqual(results[0].userSourceId, 0)
        self.assertEqual(results[0].givenName, 'string')
        self.assertEqual(results[0].gender, [1, 2])
        self.assertEqual(results[0].race, [1, 5])
        self.assertEqual(results[0].identifiesAsLgbtq, False)
        self.assertEqual(results[0].sexAtBirth, [1, 3])
        self.assertEqual(results[0].ethnicity, WorkbenchResearcherEthnicity('HISPANIC'))
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].institution, 'string')
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].nonAcademicAffiliation,
                         WorkbenchInstitutionNonAcademic('INDUSTRY'))

        # test update existing
        update_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "modifiedTime": "2019-11-26T21:21:13.056Z",
                "givenName": "string_modify",
                "familyName": "string_modify",
                "streetAddress1": "string",
                "streetAddress2": "string",
                "city": "string",
                "state": "string",
                "zipCode": "string",
                "country": "string",
                "ethnicity": "NOT_HISPANIC",
                "gender": ["WOMAN", "NONE_DESCRIBE_ME"],
                "race": ["NHOPI", "WHITE"],
                "sexAtBirth": ["INTERSEX"],
                "identifiesAsLgbtq": True,
                "lgbtqIdentity": "string",
                "affiliations": [
                    {
                        "institution": "string_modify",
                        "role": "string",
                        "nonAcademicAffiliation": "EDUCATIONAL_INSTITUTION"
                    }
                ]
            },
            {
                "userId": 1,
                # test creationTime can be NULL
                # "creationTime": "2019-11-27T21:21:13.056Z",
                "modifiedTime": "2019-11-27T21:21:13.056Z",
                "givenName": "string2",
                "familyName": "string2",
                "streetAddress1": "string2",
                "streetAddress2": "string2",
                "city": "string2",
                "state": "string2",
                "zipCode": "string2",
                "country": "string2",
                "ethnicity": "PREFER_NOT_TO_ANSWER",
                # "gender": ["MALE", "INTERSEX"], # test no gender in the payload, will store None in DB
                # "race": ["WHITE", "AA"], # test no race in the payload, will store None in DB
                "affiliations": [
                    {
                        "institution": "string2",
                        "role": "string2",
                        "nonAcademicAffiliation": "EDUCATIONAL_INSTITUTION"
                    },
                    {
                        "institution": "string22",
                        "role": "string22",
                        "nonAcademicAffiliation": "CITIZEN_SCIENTIST"
                    }
                ]
            }
        ]
        self.send_post('workbench/directory/researchers', request_data=update_json)

        researcher_dao = WorkbenchResearcherDao()
        self.assertEqual(researcher_dao.count(), 2)
        results = researcher_dao.get_all_with_children()
        self.assertEqual(results[0].userSourceId, 0)
        self.assertEqual(results[0].givenName, 'string_modify')
        self.assertEqual(results[0].gender, [2, 5])
        self.assertEqual(results[0].race, [4, 5])
        self.assertEqual(results[0].identifiesAsLgbtq, True)
        self.assertEqual(results[0].lgbtqIdentity, "string")
        self.assertEqual(results[0].sexAtBirth, [3])
        self.assertEqual(results[0].ethnicity, WorkbenchResearcherEthnicity('NOT_HISPANIC'))
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].institution, 'string_modify')
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].nonAcademicAffiliation,
                         WorkbenchInstitutionNonAcademic('EDUCATIONAL_INSTITUTION'))

        self.assertEqual(results[1].userSourceId, 1)
        self.assertEqual(results[1].givenName, 'string2')
        self.assertEqual(len(results[1].workbenchInstitutionalAffiliations), 2)
        self.assertEqual(results[1].gender, None)
        self.assertEqual(results[1].race, None)
        self.assertEqual(results[1].identifiesAsLgbtq, None)
        self.assertEqual(results[1].ethnicity, WorkbenchResearcherEthnicity('PREFER_NOT_TO_ANSWER'))

        researcher_history_dao = WorkbenchResearcherHistoryDao()
        self.assertEqual(researcher_history_dao.count(), 3)
        results = researcher_history_dao.get_all_with_children()
        self.assertEqual(results[0].userSourceId, 0)
        self.assertEqual(results[0].givenName, 'string')
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].institution, 'string')
        self.assertEqual(results[0].gender, [1, 2])
        self.assertEqual(results[0].race, [1, 5])
        self.assertEqual(results[0].sexAtBirth, [1, 3])
        self.assertEqual(results[0].ethnicity, WorkbenchResearcherEthnicity('HISPANIC'))
        self.assertEqual(results[0].education, WorkbenchResearcherEducation('COLLEGE_GRADUATE'))
        self.assertEqual(results[0].disability, WorkbenchResearcherDisability('YES'))

        self.assertEqual(results[1].userSourceId, 0)
        self.assertEqual(results[1].givenName, 'string_modify')
        self.assertEqual(results[1].workbenchInstitutionalAffiliations[0].institution, 'string_modify')
        self.assertEqual(results[1].gender, [2, 5])
        self.assertEqual(results[1].race, [4, 5])
        self.assertEqual(results[1].sexAtBirth, [3])
        self.assertEqual(results[1].ethnicity, WorkbenchResearcherEthnicity('NOT_HISPANIC'))

        self.assertEqual(results[2].userSourceId, 1)
        self.assertEqual(results[2].givenName, 'string2')
        self.assertEqual(len(results[2].workbenchInstitutionalAffiliations), 2)
        self.assertEqual(results[2].gender, None)
        self.assertEqual(results[2].race, None)
        self.assertEqual(results[2].ethnicity, WorkbenchResearcherEthnicity('PREFER_NOT_TO_ANSWER'))

    def test_invalid_input_for_researchers(self):
        request_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "givenName": "string",
                "familyName": "string",
                "streetAddress1": "string",
                "streetAddress2": "string",
                "city": "string",
                "state": "string",
                "zipCode": "string",
                "country": "string",
                "ethnicity": "NOT_HISPANIC",
                "gender": "string",
                "race": "string",
                "affiliations": [
                    {
                        "institution": "string",
                        "role": "string",
                        "nonAcademicAffiliation": "xxx"
                    }
                ]
            }
        ]

        self.send_post('workbench/directory/researchers', request_data=request_json,
                       expected_status=http.client.BAD_REQUEST)

    def test_create_and_update_workspace(self):
        # create researchers first
        researchers_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "modifiedTime": "2019-11-26T21:21:13.056Z",
                "givenName": "string_modify",
                "familyName": "string_modify",
                "streetAddress1": "string",
                "streetAddress2": "string",
                "city": "string",
                "state": "string",
                "zipCode": "string",
                "country": "string",
                "ethnicity": "HISPANIC",
                "gender": ["MAN"],
                "race": ["ASIAN"],
                "affiliations": [
                    {
                        "institution": "string_modify",
                        "role": "string",
                        "nonAcademicAffiliation": True
                    }
                ]
            },
            {
                "userId": 1,
                "creationTime": "2019-11-27T21:21:13.056Z",
                "modifiedTime": "2019-11-27T21:21:13.056Z",
                "givenName": "string2",
                "familyName": "string2",
                "streetAddress1": "string2",
                "streetAddress2": "string2",
                "city": "string2",
                "state": "string2",
                "zipCode": "string2",
                "country": "string2",
                "ethnicity": "NOT_HISPANIC",
                "gender": ["WOMAN"],
                "race": ["NHOPI"],
                "affiliations": [
                    {
                        "institution": "string2",
                        "role": "string2",
                        "nonAcademicAffiliation": False
                    },
                    {
                        "institution": "string22",
                        "role": "string22",
                        "nonAcademicAffiliation": True
                    }
                ]
            }
        ]
        self.send_post('workbench/directory/researchers', request_data=researchers_json)

        # test create workspace
        request_json = [
            {
                "workspaceId": 0,
                "name": "string",
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
                "excludeFromPublicDirectory": True,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "string",
                "otherPurposeDetails": "string",
                "methodsDevelopment": True,
                "controlSet": True,
                "ancestry": True,
                "socialBehavioral": True,
                "populationHealth": True,
                "drugDevelopment": True,
                "commercialPurpose": True,
                "educational": True,
                "otherPurpose": True,
                "scientificApproaches": 'string',
                "intendToStudy": 'string',
                "findingsFromStudy": 'string',
                "focusOnUnderrepresentedPopulations": True,
                "workspaceDemographic": {
                    "raceEthnicity": ['AIAN', 'MENA'],
                    "age": ['AGE_0_11', 'AGE_65_74'],
                    "sexAtBirth": "INTERSEX",
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

        self.send_post('workbench/directory/workspaces', request_data=request_json)

        workspace_dao = WorkbenchWorkspaceDao()
        self.assertEqual(workspace_dao.count(), 1)
        results = workspace_dao.get_all_with_children()
        self.assertEqual(results[0].workspaceSourceId, 0)
        self.assertEqual(results[0].name, 'string')
        self.assertEqual(results[0].scientificApproaches, 'string')
        self.assertEqual(results[0].intendToStudy, 'string')
        self.assertEqual(results[0].focusOnUnderrepresentedPopulations, True)
        self.assertEqual(results[0].sexAtBirth, WorkbenchWorkspaceSexAtBirth("INTERSEX"))
        self.assertEqual(results[0].genderIdentity, WorkbenchWorkspaceGenderIdentity("OTHER_THAN_MAN_WOMAN"))
        self.assertEqual(results[0].sexualOrientation, WorkbenchWorkspaceSexualOrientation("OTHER_THAN_STRAIGHT"))
        self.assertEqual(results[0].geography, WorkbenchWorkspaceGeography("RURAL"))
        self.assertEqual(results[0].disabilityStatus, WorkbenchWorkspaceDisabilityStatus("DISABILITY"))
        self.assertEqual(results[0].accessToCare, WorkbenchWorkspaceAccessToCare("NOT_EASILY_ACCESS_CARE"))
        self.assertEqual(results[0].educationLevel, WorkbenchWorkspaceEducationLevel("LESS_THAN_HIGH_SCHOOL"))
        self.assertEqual(results[0].incomeLevel,
                         WorkbenchWorkspaceIncomeLevel("BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT"))
        self.assertEqual(results[0].others, 'string')
        self.assertEqual(results[0].raceEthnicity, [1, 5])
        self.assertEqual(results[0].age, [1, 3])
        self.assertEqual(results[0].workbenchWorkspaceUser[0].userId, 0)

        workspace_history_dao = WorkbenchWorkspaceHistoryDao()
        results = workspace_history_dao.get_all_with_children()
        self.assertEqual(workspace_history_dao.count(), 1)
        self.assertEqual(results[0].workspaceSourceId, 0)
        self.assertEqual(results[0].name, 'string')
        self.assertEqual(results[0].scientificApproaches, 'string')
        self.assertEqual(results[0].intendToStudy, 'string')
        self.assertEqual(results[0].workbenchWorkspaceUser[0].userId, 0)

        # test update workspace
        update_json = [
            {
                "workspaceId": 0,
                "name": "string_modify",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-25T17:43:41.085Z",
                "status": "ACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 1,
                        "role": "READER",
                        "status": "ACTIVE"
                    }
                ],
                "excludeFromPublicDirectory": True,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "string",
                "otherPurposeDetails": "string",
                "methodsDevelopment": True,
                "controlSet": True,
                "ancestry": True,
                "socialBehavioral": True,
                "populationHealth": True,
                "drugDevelopment": True,
                "commercialPurpose": True,
                "educational": True,
                "otherPurpose": True,
                "scientificApproaches": 'string2',
                "intendToStudy": 'string2',
                "findingsFromStudy": 'string2',
                "focusOnUnderrepresentedPopulations": True,
                "workspaceDemographic": {

                }
            },
            {
                "workspaceId": 1,
                "name": "string2",
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
                        "role": "WRITER",
                        "status": "INACTIVE"
                    }
                ],
                "excludeFromPublicDirectory": True,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "string",
                "otherPurposeDetails": "string",
                "methodsDevelopment": True,
                "controlSet": True,
                "ancestry": True,
                "socialBehavioral": True,
                "populationHealth": True,
                "drugDevelopment": True,
                "commercialPurpose": True,
                "educational": True,
                "otherPurpose": True,
                "scientificApproaches": 'string2',
                "intendToStudy": 'string2',
                "findingsFromStudy": 'string2'
            }
        ]

        self.send_post('workbench/directory/workspaces', request_data=update_json)
        workspace_dao = WorkbenchWorkspaceDao()
        self.assertEqual(workspace_dao.count(), 2)
        results = workspace_dao.get_all_with_children()
        self.assertEqual(results[0].workspaceSourceId, 0)
        self.assertEqual(results[0].name, 'string_modify')
        self.assertEqual(results[0].scientificApproaches, 'string2')
        self.assertEqual(results[0].workbenchWorkspaceUser[0].userId, 1)
        self.assertEqual(results[0].focusOnUnderrepresentedPopulations, True)
        self.assertEqual(results[0].sexAtBirth, None)
        self.assertEqual(results[1].workspaceSourceId, 1)
        self.assertEqual(results[1].name, 'string2')
        if results[1].workbenchWorkspaceUser[0].userId == 0:
            self.assertEqual(results[1].workbenchWorkspaceUser[0].role, WorkbenchWorkspaceUserRole.READER)
            self.assertEqual(results[1].workbenchWorkspaceUser[1].role, WorkbenchWorkspaceUserRole.WRITER)
        else:
            self.assertEqual(results[1].workbenchWorkspaceUser[0].role, WorkbenchWorkspaceUserRole.WRITER)
            self.assertEqual(results[1].workbenchWorkspaceUser[1].role, WorkbenchWorkspaceUserRole.READER)

        workspace_history_dao = WorkbenchWorkspaceHistoryDao()
        results = workspace_history_dao.get_all_with_children()
        self.assertEqual(workspace_history_dao.count(), 3)
        self.assertEqual(results[0].workspaceSourceId, 0)
        self.assertEqual(results[0].name, 'string')
        self.assertEqual(results[0].scientificApproaches, 'string')
        self.assertEqual(results[0].workbenchWorkspaceUser[0].userId, 0)
        self.assertEqual(results[1].workspaceSourceId, 0)
        self.assertEqual(results[1].name, 'string_modify')
        self.assertEqual(results[1].scientificApproaches, 'string2')
        self.assertEqual(results[1].workbenchWorkspaceUser[0].userId, 1)
        self.assertEqual(results[2].workspaceSourceId, 1)
        self.assertEqual(results[2].name, 'string2')
        self.assertEqual(results[2].scientificApproaches, 'string2')
        if results[2].workbenchWorkspaceUser[0].userId == 0:
            self.assertEqual(results[2].workbenchWorkspaceUser[0].role, WorkbenchWorkspaceUserRole.READER)
            self.assertEqual(results[2].workbenchWorkspaceUser[1].role, WorkbenchWorkspaceUserRole.WRITER)
        else:
            self.assertEqual(results[2].workbenchWorkspaceUser[0].role, WorkbenchWorkspaceUserRole.WRITER)
            self.assertEqual(results[2].workbenchWorkspaceUser[1].role, WorkbenchWorkspaceUserRole.READER)

    def test_invalid_input_for_workspace(self):
        request_json = [
            {
                "workspaceId": 0,
                "name": "string",
                "creationTime": "2019-11-25T17:43:41.085Z",
                "modifiedTime": "2019-11-25T17:43:41.085Z",
                "status": "ACTIVE",
                "workspaceUsers": [
                    {
                        "userId": 0,
                        "role": "bad input",
                        "status": "ACTIVE"
                    }
                ],
                "excludeFromPublicDirectory": True,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "string",
                "otherPurposeDetails": "string",
                "methodsDevelopment": True,
                "controlSet": True,
                "ancestry": True,
                "socialBehavioral": True,
                "populationHealth": True,
                "drugDevelopment": True,
                "commercialPurpose": True,
                "educational": True,
                "otherPurpose": True
            }
        ]

        self.send_post('workbench/directory/workspaces', request_data=request_json,
                       expected_status=http.client.BAD_REQUEST)

    def test_update_researcher_in_use(self):
        # create researchers first
        researchers_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "modifiedTime": "2019-11-26T21:21:13.056Z",
                "givenName": "string",
                "familyName": "string",
                "streetAddress1": "string",
                "streetAddress2": "string",
                "city": "string",
                "state": "string",
                "zipCode": "string",
                "country": "string",
                "ethnicity": "HISPANIC",
                "gender": ["MAN"],
                "race": ["ASIAN"],
                "affiliations": [
                    {
                        "institution": "institution_string",
                        "role": "string",
                        "nonAcademicAffiliation": True
                    }
                ]
            }
        ]
        self.send_post('workbench/directory/researchers', request_data=researchers_json)

        # test create workspace
        request_json = [
            {
                "workspaceId": 0,
                "name": "string",
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
                "excludeFromPublicDirectory": True,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "string",
                "otherPurposeDetails": "string",
                "methodsDevelopment": True,
                "controlSet": True,
                "ancestry": True,
                "socialBehavioral": True,
                "populationHealth": True,
                "drugDevelopment": True,
                "commercialPurpose": True,
                "educational": True,
                "otherPurpose": True,
                "scientificApproaches": 'string',
                "intendToStudy": 'string',
                "findingsFromStudy": 'string',
                "focusOnUnderrepresentedPopulations": True,
                "workspaceDemographic": {
                    "raceEthnicity": ['AIAN', 'MENA'],
                    "age": ['AGE_0_11', 'AGE_65_74'],
                    "sexAtBirth": "INTERSEX",
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

        self.send_post('workbench/directory/workspaces', request_data=request_json)

        update_researchers_json = [
            {
                "userId": 0,
                "creationTime": "2019-11-26T21:21:13.056Z",
                "modifiedTime": "2019-11-26T21:21:13.056Z",
                "givenName": "string_modify2",
                "familyName": "string_modify2",
                "streetAddress1": "string2",
                "streetAddress2": "string2",
                "city": "string2",
                "state": "string2",
                "zipCode": "string2",
                "country": "string2",
                "ethnicity": "HISPANIC",
                "gender": ["WOMAN"],
                "race": ["ASIAN"],
                "affiliations": [
                    {
                        "institution": "institution_string_modify",
                        "role": "string",
                        "nonAcademicAffiliation": True
                    }
                ]
            }
        ]

        self.send_post('workbench/directory/researchers', request_data=update_researchers_json)

        researcher_dao = WorkbenchResearcherDao()
        self.assertEqual(researcher_dao.count(), 1)
        results = researcher_dao.get_all_with_children()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].givenName, 'string_modify2')
        self.assertEqual(results[0].gender, [2])
        self.assertEqual(len(results[0].workbenchInstitutionalAffiliations), 1)
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].institution, 'institution_string_modify')


