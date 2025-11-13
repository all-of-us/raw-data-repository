import datetime
from unittest import mock

from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao
from rdr_service.model.workbench_workspace import WorkbenchWorkspaceSnapshot
from rdr_service.participant_enums import WorkbenchWorkspaceStatus, WorkbenchWorkspaceSexAtBirth, \
    WorkbenchWorkspaceGenderIdentity, WorkbenchWorkspaceGeography, WorkbenchWorkspaceSexualOrientation, \
    WorkbenchWorkspaceAccessToCare, WorkbenchWorkspaceDisabilityStatus, WorkbenchWorkspaceEducationLevel, \
    WorkbenchWorkspaceIncomeLevel, WorkbenchWorkspaceAianResearchType, WorkbenchWorkspaceAccessTier
from rdr_service.workflow_management.researchers_offline.workbench_data_transfer_input_feed import \
    WorkbenchWorkspacesFeed
from tests.helpers.unittest_base import BaseTestCase


class WorkbenchWorkspacesDataFeedTest(BaseTestCase):
    def setUp(self):
        super().setUp()

    @mock.patch("rdr_service.dao.workbench_dao.WorkbenchWorkspaceDao.bq_row_to_dict")
    @mock.patch("google.cloud.bigquery.Client")
    def test_run_datafeed(self, mock_bq_client, mock_row_to_dict):
        time_1 = datetime.datetime(2025, 11, 1, 20, 1, 1, 976505)
        time_2 = datetime.datetime(2025, 11, 5, 10, 10, 15, 900005)

        record = [
            {
                "created": time_1,
                "modified": time_1,
                "workspaceSourceId": 11111,
                "name": "Test Workspace Name",
                "creationTime": time_1,
                "modifiedTime": time_1,
                "status": WorkbenchWorkspaceStatus('ACTIVE'),
                "excludeFromPublicDirectory": False,
                "reviewRequested": False,
                "diseaseFocusedResearch": False,
                "diseaseFocusedResearchName": "",
                "otherPurposeDetails": "",
                "methodsDevelopment": False,
                "controlSet": False,
                "ancestry": False,
                "socialBehavioral": False,
                "populationHealth": False,
                "drugDevelopment": False,
                "commercialPurpose": False,
                "educational": False,
                "otherPurpose": False,
                "scientificApproaches": None,
                "intendToStudy": None,
                "findingsFromStudy": None,
                "ethicalLegalSocialImplications": False,
                "focusOnUnderrepresentedPopulations": False,
                "raceEthnicity": [],
                "age": [],
                "sexAtBirth": WorkbenchWorkspaceSexAtBirth('INTERSEX'),
                "genderIdentity": WorkbenchWorkspaceGenderIdentity('UNSET'),
                "sexualOrientation": WorkbenchWorkspaceSexualOrientation('OTHER_THAN_STRAIGHT'),
                "geography": WorkbenchWorkspaceGeography('RURAL'),
                "disabilityStatus": WorkbenchWorkspaceDisabilityStatus('DISABILITY'),
                "accessToCare": WorkbenchWorkspaceAccessToCare('UNSET'),
                "educationLevel": WorkbenchWorkspaceEducationLevel('LESS_THAN_HIGH_SCHOOL'),
                "incomeLevel": WorkbenchWorkspaceIncomeLevel('BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT'),
                "accessTier": WorkbenchWorkspaceAccessTier('REGISTERED'),
                "others": "",
                "isReviewed": False,
                "cdrVersion": "All of Us Registered Tier Dataset v8",
                "aianResearchType": WorkbenchWorkspaceAianResearchType('NO_AI_AN_ANALYSIS'),
                "aianResearchDetails": "Test research details",
                "resource": "No resource payload. Data from VWB 2.0"
            },
            {
                "created": time_2,
                "modified": time_2,
                "workspaceSourceId": 22222,
                "name": "Test Workspace Name 2",
                "creationTime": time_2,
                "modifiedTime": time_2,
                "status": WorkbenchWorkspaceStatus('INACTIVE'),
                "excludeFromPublicDirectory": False,
                "reviewRequested": True,
                "diseaseFocusedResearch": True,
                "diseaseFocusedResearchName": "",
                "otherPurposeDetails": "",
                "methodsDevelopment": True,
                "controlSet": True,
                "ancestry": True,
                "socialBehavioral": True,
                "populationHealth": True,
                "drugDevelopment": True,
                "commercialPurpose": True,
                "educational": True,
                "otherPurpose": True,
                "scientificApproaches": None,
                "intendToStudy": None,
                "findingsFromStudy": None,
                "ethicalLegalSocialImplications": True,
                "focusOnUnderrepresentedPopulations": True,
                "raceEthnicity": [],
                "age": [],
                "sexAtBirth": WorkbenchWorkspaceSexAtBirth('INTERSEX'),
                "genderIdentity": WorkbenchWorkspaceGenderIdentity('UNSET'),
                "sexualOrientation": WorkbenchWorkspaceSexualOrientation('OTHER_THAN_STRAIGHT'),
                "geography": WorkbenchWorkspaceGeography('RURAL'),
                "disabilityStatus": WorkbenchWorkspaceDisabilityStatus('DISABILITY'),
                "accessToCare": WorkbenchWorkspaceAccessToCare('UNSET'),
                "educationLevel": WorkbenchWorkspaceEducationLevel('LESS_THAN_HIGH_SCHOOL'),
                "incomeLevel": WorkbenchWorkspaceIncomeLevel('BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT'),
                "accessTier": WorkbenchWorkspaceAccessTier('REGISTERED'),
                "others": "",
                "isReviewed": False,
                "cdrVersion": "All of Us Registered Tier Dataset v8",
                "aianResearchType": WorkbenchWorkspaceAianResearchType('NO_AI_AN_ANALYSIS'),
                "aianResearchDetails": "Test research details 2",
                "resource": "No resource payload. Data from VWB 2.0"
            },
            {
                "created": time_2,
                "modified": time_2,
                "workspaceSourceId": 33333,
                "name": "Test Workspace Name 3",
                "creationTime": time_2,
                "modifiedTime": time_2,
                "status": WorkbenchWorkspaceStatus('ACTIVE'),
                "excludeFromPublicDirectory": True,
                "cdrVersion": "All of Us Registered Tier Dataset v8",
                "aianResearchType": WorkbenchWorkspaceAianResearchType('NO_AI_AN_ANALYSIS'),
                "aianResearchDetails": "Test research details 3",
                "resource": "No resource payload. Data from VWB 2.0"
            }
        ]

        mock_bq_instance = mock_bq_client.return_value
        mock_bq_instance.query.return_value.result.return_value = record
        mock_row_to_dict.side_effect = [record[0], record[1], record[2]]

        workbench_feed = WorkbenchWorkspacesFeed()
        workbench_feed.get_datafeed_definition = mock.Mock(return_value={
            "create_mapping_sql": "mapping_sql",
            "streaming_data_sql": "streaming_sql",
            "destination_model": WorkbenchWorkspaceSnapshot
        })

        workbench_feed.run_datafeed("WorkbenchWorkspacesFeed")
        workbench_workspace_dao = WorkbenchWorkspaceDao()
        actual_rows = workbench_workspace_dao.get_all()

        self.assertEqual(actual_rows[0].workspaceSourceId, 11111)
        self.assertEqual(actual_rows[0].name, "Test Workspace Name")
        self.assertEqual(actual_rows[0].creationTime, datetime.datetime(2025, 11, 1, 20, 1, 1, 976505))
        self.assertEqual(actual_rows[0].modifiedTime, datetime.datetime(2025, 11, 1, 20, 1, 1, 976505))
        self.assertEqual(actual_rows[0].status, WorkbenchWorkspaceStatus('ACTIVE'))
        self.assertEqual(actual_rows[0].excludeFromPublicDirectory, False)
        self.assertEqual(actual_rows[0].reviewRequested, False)
        self.assertEqual(actual_rows[0].otherPurposeDetails, "")
        self.assertEqual(actual_rows[0].commercialPurpose, False)
        self.assertEqual(actual_rows[0].educational, False)
        self.assertEqual(actual_rows[0].otherPurpose, False)
        self.assertEqual(actual_rows[0].scientificApproaches, None)
        self.assertEqual(actual_rows[0].intendToStudy, None)
        self.assertEqual(actual_rows[0].ethicalLegalSocialImplications, False)
        self.assertEqual(actual_rows[0].focusOnUnderrepresentedPopulations, False)
        self.assertEqual(actual_rows[0].raceEthnicity, [])
        self.assertEqual(actual_rows[0].age, [])
        self.assertEqual(actual_rows[0].sexAtBirth, WorkbenchWorkspaceSexAtBirth('INTERSEX'))
        self.assertEqual(actual_rows[0].genderIdentity, None)
        self.assertEqual(actual_rows[0].sexualOrientation, WorkbenchWorkspaceSexualOrientation('OTHER_THAN_STRAIGHT'))
        self.assertEqual(actual_rows[0].geography, WorkbenchWorkspaceGeography('RURAL'))
        self.assertEqual(actual_rows[0].disabilityStatus, WorkbenchWorkspaceDisabilityStatus('DISABILITY'))
        self.assertEqual(actual_rows[0].accessToCare, None)
        self.assertEqual(actual_rows[0].educationLevel, WorkbenchWorkspaceEducationLevel('LESS_THAN_HIGH_SCHOOL'))
        self.assertEqual(actual_rows[0].incomeLevel, WorkbenchWorkspaceIncomeLevel('BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT'))
        self.assertEqual(actual_rows[0].accessTier, WorkbenchWorkspaceAccessTier('REGISTERED'))
        self.assertEqual(actual_rows[0].others, "")
        self.assertEqual(actual_rows[0].isReviewed, False)
        self.assertEqual(actual_rows[0].cdrVersion, "All of Us Registered Tier Dataset v8")
        self.assertEqual(actual_rows[0].aianResearchType, WorkbenchWorkspaceAianResearchType('NO_AI_AN_ANALYSIS'))
        self.assertEqual(actual_rows[0].aianResearchDetails, "Test research details")
        self.assertEqual(actual_rows[0].resource, "No resource payload. Data from VWB 2.0")

        self.assertEqual(actual_rows[1].workspaceSourceId, 22222)
        self.assertEqual(actual_rows[1].name, "Test Workspace Name 2")
        self.assertEqual(actual_rows[1].creationTime, datetime.datetime(2025, 11, 5, 10, 10, 15, 900005))
        self.assertEqual(actual_rows[1].modifiedTime, datetime.datetime(2025, 11, 5, 10, 10, 15, 900005))
        self.assertEqual(actual_rows[1].status, WorkbenchWorkspaceStatus('INACTIVE'))
        self.assertEqual(actual_rows[1].excludeFromPublicDirectory, False)
        self.assertEqual(actual_rows[1].reviewRequested, True)
        self.assertEqual(actual_rows[1].otherPurposeDetails, "")
        self.assertEqual(actual_rows[1].commercialPurpose, True)
        self.assertEqual(actual_rows[1].educational, True)
        self.assertEqual(actual_rows[1].otherPurpose, True)
        self.assertEqual(actual_rows[1].scientificApproaches, None)
        self.assertEqual(actual_rows[1].intendToStudy, None)
        self.assertEqual(actual_rows[1].ethicalLegalSocialImplications, True)
        self.assertEqual(actual_rows[1].focusOnUnderrepresentedPopulations, True)
        self.assertEqual(actual_rows[1].raceEthnicity, [])
        self.assertEqual(actual_rows[1].age, [])
        self.assertEqual(actual_rows[1].sexAtBirth, WorkbenchWorkspaceSexAtBirth('INTERSEX'))
        self.assertEqual(actual_rows[1].genderIdentity, None)
        self.assertEqual(actual_rows[1].sexualOrientation, WorkbenchWorkspaceSexualOrientation('OTHER_THAN_STRAIGHT'))
        self.assertEqual(actual_rows[1].geography, WorkbenchWorkspaceGeography('RURAL'))
        self.assertEqual(actual_rows[1].disabilityStatus, WorkbenchWorkspaceDisabilityStatus('DISABILITY'))
        self.assertEqual(actual_rows[1].accessToCare, None)
        self.assertEqual(actual_rows[1].educationLevel, WorkbenchWorkspaceEducationLevel('LESS_THAN_HIGH_SCHOOL'))
        self.assertEqual(actual_rows[1].incomeLevel,
                         WorkbenchWorkspaceIncomeLevel('BELOW_FEDERAL_POVERTY_LEVEL_200_PERCENT'))
        self.assertEqual(actual_rows[1].accessTier, WorkbenchWorkspaceAccessTier('REGISTERED'))
        self.assertEqual(actual_rows[1].others, "")
        self.assertEqual(actual_rows[1].isReviewed, False)
        self.assertEqual(actual_rows[1].cdrVersion, "All of Us Registered Tier Dataset v8")
        self.assertEqual(actual_rows[1].aianResearchType, WorkbenchWorkspaceAianResearchType('NO_AI_AN_ANALYSIS'))
        self.assertEqual(actual_rows[1].aianResearchDetails, "Test research details 2")
        self.assertEqual(actual_rows[1].resource, "No resource payload. Data from VWB 2.0")
