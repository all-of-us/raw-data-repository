from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.metrics_ehr_service import MetricsEhrService
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.test.unit_test.unit_test_util import SqlTestBase


class MetricsEhrServiceTest(SqlTestBase):
    def setUp(self, with_data=True, use_mysql=True):
        super(MetricsEhrServiceTest, self).setUp(with_data=with_data, use_mysql=use_mysql)
        self.service = MetricsEhrService()
        self.hpo_dao = HPODao()
        self.org_dao = OrganizationDao()

        self.hpo_foo = self._make_hpo(hpoId=10, name="FOO", displayName="Foo")
        self.hpo_bar = self._make_hpo(hpoId=11, name="BAR", displayName="Bar")

        self.org_foo_a = self._make_org(
            organizationId=10, externalId="FOO_A", displayName="Foo A", hpoId=self.hpo_foo.hpoId
        )
        self.org_bar_a = self._make_org(
            organizationId=11, externalId="BAR_A", displayName="Bar A", hpoId=self.hpo_bar.hpoId
        )
        self.org_bar_b = self._make_org(
            organizationId=12, externalId="BAR_B", displayName="Bar B", hpoId=self.hpo_bar.hpoId
        )

    def _make_hpo(self, **kwargs):
        hpo = HPO(**kwargs)
        self.hpo_dao.insert(hpo)
        return hpo

    def _make_org(self, **kwargs):
        org = Organization(**kwargs)
        self.org_dao.insert(org)
        return org

    def test_get_organization_ids_from_hpo_ids(self):
        self.assertEqual(
            self.service._get_organization_ids_from_hpo_ids([self.hpo_foo.hpoId]), [self.org_foo_a.organizationId]
        )
        self.assertEqual(
            self.service._get_organization_ids_from_hpo_ids([self.hpo_bar.hpoId]),
            [self.org_bar_a.organizationId, self.org_bar_b.organizationId],
        )
        self.assertEqual(
            self.service._get_organization_ids_from_hpo_ids([self.hpo_foo.hpoId, self.hpo_bar.hpoId]),
            [self.org_foo_a.organizationId, self.org_bar_a.organizationId, self.org_bar_b.organizationId],
        )
