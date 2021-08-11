#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from tests.helpers.unittest_base import BaseTestCase

from rdr_service.model.workbench_workspace import WorkbenchWorkspaceSnapshot
from rdr_service.model.workbench_researcher import WorkbenchResearcher
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

        self.researcher = WorkbenchResearcher(
            id=1,
            created=None,
            modified=None,
            resource='test',
            degree=[1, 8],
            gender=[1, 2],
            userSourceId=1,
            givenName='test',
            city='city'
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
        column_count = len(WorkbenchResearcher.__table__.columns)
        self.assertEqual(column_count, 25)

        self.session.add(self.researcher)
        self.session.commit()

        gen = WBResearcherGenerator()
        res = gen.make_resource(1)
        self.assertIsNotNone(res)

        data = res.get_resource()
        self.assertIsInstance(data, dict)
        self.assertEqual(len(data.keys()), 22)
        self.assertEqual(data['user_source_id'], 1)
        self.assertEqual(data['given_name'], 'test')
        self.assertEqual(data['city'], 'city')
        self.assertEqual(data['genders'], [{'gender': 'MAN', 'gender_id': 1}, {'gender': 'WOMAN', 'gender_id': 2}])
        self.assertEqual(data['degrees'], [{'degree': 'PHD', 'degree_id': 1}, {'degree': 'MBA', 'degree_id': 8}])
