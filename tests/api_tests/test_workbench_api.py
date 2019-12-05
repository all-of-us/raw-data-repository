from tests.helpers.unittest_base import BaseTestCase
from rdr_service.dao.workbench_dao import WorkbenchResearcherDao, WorkbenchResearcherHistoryDao, \
    WorkbenchWorkspaceDao, WorkbenchWorkspaceHistoryDao
from rdr_service.participant_enums import WorkbenchWorkspaceUserRole


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
                "ethnicity": "string",
                "gender": "string",
                "race": "string",
                "affiliations": [
                    {
                        "institution": "string",
                        "role": "string",
                        "nonAcademicAffiliation": True
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
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].institution, 'string')

        researcher_history_dao = WorkbenchResearcherHistoryDao()
        results = researcher_history_dao.get_all_with_children()
        self.assertEqual(researcher_history_dao.count(), 1)
        self.assertEqual(results[0].userSourceId, 0)
        self.assertEqual(results[0].givenName, 'string')
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].institution, 'string')

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
                "ethnicity": "string",
                "gender": "string",
                "race": "string",
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
                "ethnicity": "string2",
                "gender": "string2",
                "race": "string2",
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
        self.send_post('workbench/directory/researchers', request_data=update_json)

        researcher_dao = WorkbenchResearcherDao()
        self.assertEqual(researcher_dao.count(), 2)
        results = researcher_dao.get_all_with_children()
        self.assertEqual(results[0].userSourceId, 0)
        self.assertEqual(results[0].givenName, 'string_modify')
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].institution, 'string_modify')

        self.assertEqual(results[1].userSourceId, 1)
        self.assertEqual(results[1].givenName, 'string2')
        self.assertEqual(results[1].workbenchInstitutionalAffiliations[1].institution, 'string22')

        researcher_history_dao = WorkbenchResearcherHistoryDao()
        self.assertEqual(researcher_history_dao.count(), 3)
        results = researcher_history_dao.get_all_with_children()
        self.assertEqual(results[0].userSourceId, 0)
        self.assertEqual(results[0].givenName, 'string')
        self.assertEqual(results[0].workbenchInstitutionalAffiliations[0].institution, 'string')

        self.assertEqual(results[1].userSourceId, 0)
        self.assertEqual(results[1].givenName, 'string_modify')
        self.assertEqual(results[1].workbenchInstitutionalAffiliations[0].institution, 'string_modify')

        self.assertEqual(results[2].userSourceId, 1)
        self.assertEqual(results[2].givenName, 'string2')
        self.assertEqual(len(results[2].workbenchInstitutionalAffiliations), 2)

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
                "ethnicity": "string",
                "gender": "string",
                "race": "string",
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
                "ethnicity": "string2",
                "gender": "string2",
                "race": "string2",
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
                "otherPurpose": True
            }
        ]

        self.send_post('workbench/directory/workspaces', request_data=request_json)

        workspace_dao = WorkbenchWorkspaceDao()
        self.assertEqual(workspace_dao.count(), 1)
        results = workspace_dao.get_all_with_children()
        self.assertEqual(results[0].workspaceSourceId, 0)
        self.assertEqual(results[0].name, 'string')
        self.assertEqual(results[0].workbenchWorkspaceUser[0].userId, 0)

        workspace_history_dao = WorkbenchWorkspaceHistoryDao()
        results = workspace_history_dao.get_all_with_children()
        self.assertEqual(workspace_history_dao.count(), 1)
        self.assertEqual(results[0].workspaceSourceId, 0)
        self.assertEqual(results[0].name, 'string')
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
                "otherPurpose": True
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
                "otherPurpose": True
            }
        ]

        self.send_post('workbench/directory/workspaces', request_data=update_json)
        workspace_dao = WorkbenchWorkspaceDao()
        self.assertEqual(workspace_dao.count(), 2)
        results = workspace_dao.get_all_with_children()
        self.assertEqual(results[0].workspaceSourceId, 0)
        self.assertEqual(results[0].name, 'string_modify')
        self.assertEqual(results[0].workbenchWorkspaceUser[0].userId, 1)
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
        self.assertEqual(results[0].workbenchWorkspaceUser[0].userId, 0)
        self.assertEqual(results[1].workspaceSourceId, 0)
        self.assertEqual(results[1].name, 'string_modify')
        self.assertEqual(results[1].workbenchWorkspaceUser[0].userId, 1)
        self.assertEqual(results[2].workspaceSourceId, 1)
        self.assertEqual(results[2].name, 'string2')
        if results[2].workbenchWorkspaceUser[0].userId == 0:
            self.assertEqual(results[2].workbenchWorkspaceUser[0].role, WorkbenchWorkspaceUserRole.READER)
            self.assertEqual(results[2].workbenchWorkspaceUser[1].role, WorkbenchWorkspaceUserRole.WRITER)
        else:
            self.assertEqual(results[2].workbenchWorkspaceUser[0].role, WorkbenchWorkspaceUserRole.WRITER)
            self.assertEqual(results[2].workbenchWorkspaceUser[1].role, WorkbenchWorkspaceUserRole.READER)
