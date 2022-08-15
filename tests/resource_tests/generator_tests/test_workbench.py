#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from dateutil.parser import parse as dt_parse

from tests.helpers.unittest_base import BaseTestCase

from rdr_service.model.workbench_workspace import WorkbenchWorkspaceSnapshot
from rdr_service.model.workbench_researcher import WorkbenchResearcherHistory
from rdr_service.participant_enums import WorkbenchResearcherDisability
from rdr_service.resource.generators import WBWorkspaceGenerator, WBResearcherGenerator


class WorkbenchGeneratorTest(BaseTestCase):

    def setUp(self, with_data=True, with_consent_codes=False) -> None:
        super().setUp(with_data, with_consent_codes)

        self.workspace = WorkbenchWorkspaceSnapshot(
            id=1,
            created=None,
            modified=None,
            workspaceSourceId=1,
            name='test',
            age=[1, 3],
            raceEthnicity=[1, 5],
            resource='test'
        )

        self.researcher = WorkbenchResearcherHistory(
            id=1,
            created=None,
            modified=None,
            resource='test',
            degree=[1, 8],
            gender=[1, 2],
            userSourceId=1,
            givenName='test'
        )

        self.researcher2 = WorkbenchResearcherHistory(
            # Pre-DSV2 survey researcher record with nulls.
            id=2,
            created=None,
            modified=None,
            resource='test',
            userSourceId=0,
            creationTime=dt_parse('2022-07-19 18:19:10.000000'),
            modifiedTime=dt_parse('2022-07-19 18:19:10.000000'),
            givenName="John",
            familyName="Doe",
            email="test.person@example.com",
            ethnicity=2,
            gender=[3],
            race=[2],
            sexAtBirth=[],
            education=3,
            degree=[],
            disability=WorkbenchResearcherDisability.UNSET,
            identifiesAsLgbtq=True,
            lgbtqIdentity='',
            accessTierShortNames=[1],
            dsv2CompletionTime=None,
            dsv2EthnicCategories=None,
            dsv2EthnicityAiAnOther=None,
            dsv2EthnicityAsianOther=None,
            dsv2EthnicityBlackOther=None,
            dsv2EthnicityHispanicOther=None,
            dsv2EthnicityMeNaOther=None,
            dsv2EthnicityNhPiOther=None,
            dsv2EthnicityWhiteOther=None,
            dsv2EthnicityOther=None,
            dsv2GenderIdentities=None,
            dsv2GenderOther=None,
            dsv2SexualOrientations=None,
            dsv2OrientationOther=None,
            dsv2SexAtBirth=None,
            dsv2SexAtBirthOther=None,
            dsv2YearOfBirth=None,
            dsv2YearOfBirthPreferNot=None,
            dsv2DisabilityHearing=None,
            dsv2DisabilitySeeing=None,
            dsv2DisabilityConcentrating=None,
            dsv2DisabilityWalking=None,
            dsv2DisabilityDressing=None,
            dsv2DisabilityErrands=None,
            dsv2DisabilityOther=None,
            dsv2Education=None,
            dsv2Disadvantaged=None,
            dsv2SurveyComments=None,
        )

        self.researcher3 = WorkbenchResearcherHistory(
            # Post-DSV2 survey researcher record with nulls.
            id=3,
            created=None,
            modified=None,
            resource='test',
            userSourceId=0,
            creationTime = dt_parse('2022-07-19 18:19:10.000000'),
            modifiedTime = dt_parse('2022-07-19 18:19:10.000000'),
            givenName = "Jane",
            familyName = "Doe",
            email = "test.person@example.com",
            ethnicity = 2,
            gender = [3],
            race = [2],
            sexAtBirth = [],
            education = 3,
            degree = [],
            disability = WorkbenchResearcherDisability.YES,
            identifiesAsLgbtq = True,
            lgbtqIdentity = '',
            accessTierShortNames = [1],
            dsv2CompletionTime = dt_parse('2022-07-19 18:19:10.000000'),
            dsv2EthnicCategories = [46, 45, 47, 18, 1, 79],
            dsv2EthnicityAiAnOther = 2,
            dsv2EthnicityAsianOther = 1,
            dsv2EthnicityBlackOther = 2,
            dsv2EthnicityHispanicOther = 2,
            dsv2EthnicityMeNaOther = 2,
            dsv2EthnicityNhPiOther = 2,
            dsv2EthnicityWhiteOther = 2,
            dsv2EthnicityOther = '',
            dsv2GenderIdentities = [3, 4, 7],
            dsv2GenderOther = '',
            dsv2SexualOrientations = [3, 7],
            dsv2OrientationOther = '',
            dsv2SexAtBirth = 8,
            dsv2SexAtBirthOther = '',
            dsv2YearOfBirth = 1999,
            dsv2YearOfBirthPreferNot = False,
            dsv2DisabilityHearing = 2,
            dsv2DisabilitySeeing = 2,
            dsv2DisabilityConcentrating = 2,
            dsv2DisabilityWalking = 1,
            dsv2DisabilityDressing = 0,
            dsv2DisabilityErrands = 3,
            dsv2DisabilityOther = '',
            dsv2Education = 6,
            dsv2Disadvantaged = 1,
            dsv2SurveyComments = ''
        )

    def test_workbench_workspace_generator(self):
        """ Test the workbench workspace generator """

        # Test that the number of fields in the DAO model has not changed.
        # This test is to make sure the resource model is updated when the SA model has been changed.
        column_count = len(WorkbenchWorkspaceSnapshot.__table__.columns)
        self.assertEqual(column_count, 42)

        self.session.add(self.workspace)
        self.session.commit()

        gen = WBWorkspaceGenerator()
        res = gen.make_resource(1)
        self.assertIsNotNone(res)

        data = res.get_resource()
        self.assertIsInstance(data, dict)
        self.assertEqual(len(data.keys()), 48)
        self.assertEqual(data['workspace_source_id'], 1)
        self.assertEqual(data['name'], 'test')
        self.assertEqual(data['race_ethnicities'], [{'race_ethnicity': 'AIAN', 'race_ethnicity_id': 1},
                                                    {'race_ethnicity': 'MENA', 'race_ethnicity_id': 5}])
        self.assertEqual(data['ages'], [{'age': 'AGE_0_11', 'age_id': 1}, {'age': 'AGE_65_74', 'age_id': 3}])

    def test_workbench_researcher_generator(self):
        """ Test the workbench researcher generator """

        # Test that the number of fields in the DAO model has not changed.
        # This test is to make sure the resource model is updated when the SA model has been changed.
        column_count = len(WorkbenchResearcherHistory.__table__.columns)
        self.assertEqual(column_count, 54)

        self.session.add(self.researcher)
        self.session.commit()

        gen = WBResearcherGenerator()
        res = gen.make_resource(1)
        self.assertIsNotNone(res)

        data = res.get_resource()
        self.assertIsInstance(data, dict)
        self.assertEqual(len(data.keys()), 59)
        self.assertEqual(data['user_source_id'], 1)
        self.assertEqual(data['given_name'], 'test')
        self.assertEqual(data['genders'], [{'gender': 'MAN', 'gender_id': 1}, {'gender': 'WOMAN', 'gender_id': 2}])
        self.assertEqual(data['degrees'], [{'degree': 'PHD', 'degree_id': 1}, {'degree': 'MBA', 'degree_id': 8}])

    def test_workbench_researcher_dsv2_generator(self):
        self.session.add(self.researcher2)
        self.session.add(self.researcher3)
        self.session.commit()

        gen = WBResearcherGenerator()
        res = gen.make_resource(2)
        self.assertIsNotNone(res)

        res = gen.make_resource(2)
        self.assertIsNotNone(res)