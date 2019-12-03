from tests.helpers.unittest_base import BaseTestCase


class WorkbenchApiTest(BaseTestCase):
    def setUp(self):
        super(WorkbenchApiTest, self).setUp(with_data=False)

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

        post_result = self.send_post('workbench/directory/researchers', request_data=request_json)
        get_result = self.send_get('workbench/directory/researchers')
        for item in get_result['entry']:
            self.assertIn(item['resource'], post_result)

        get_history_result = self.send_get('workbench/directory/researchers/history')
        self.assertEqual(len(get_history_result['entry']), 1)

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
        update_result = self.send_post('workbench/directory/researchers', request_data=update_json)
        get_result = self.send_get('workbench/directory/researchers')
        self.assertEqual(len(get_result['entry']), 2)
        for item in get_result['entry']:
            self.assertIn(item['resource'], update_result)
        get_history_result = self.send_get('workbench/directory/researchers/history')
        self.assertEqual(len(get_history_result['entry']), 3)

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

        post_result = self.send_post('workbench/directory/workspaces', request_data=request_json)
        print(str(post_result))
        get_result = self.send_get('workbench/directory/workspaces')
        print(str(get_result))
