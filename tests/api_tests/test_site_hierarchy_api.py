from tests.helpers.unittest_base import BaseTestCase
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.site import Site
from rdr_service.model.site_enums import ObsoleteStatus
from rdr_service.participant_enums import OrganizationType


def _make_expected_response(**kwargs):
    full_site_hierarchy = {
        "data": [
            {
                "awardee_id": "PITT",
                "organization_id": "PITT_BANNER_HEALTH",
                "site_name": "Monroeville Urgent Care Center",
                "google_group": "hpo-site-monroeville",
            },
            {
                "awardee_id": "PITT",
                "organization_id": "PITT_BANNER_HEALTH",
                "site_name": "Phoenix Urgent Care Center",
                "google_group": "hpo-site-bannerphoenix",
            },
            {
                "awardee_id": "AZ_TUCSON",
                "organization_id": "AZ_TUCSON_BANNER_HEALTH",
                "site_name": "Phoenix clinic",
                "google_group": "hpo-site-clinic-phoenix",
            },
            {
                "awardee_id": "ILLINOIS",
                "organization_id": "ILLINOIS_1",
                "site_name": "Illinois Site 1",
                "google_group": "illinois-site-1",
            },
        ]
    }

    filtered_data = [
        item
        for item in full_site_hierarchy["data"]
        if all(item[key] == value for key, value in kwargs.items())
    ]

    return {"data": filtered_data} if kwargs else full_site_hierarchy


class SiteHierarchyApiTest(BaseTestCase):
    def setUp(self):
        super(SiteHierarchyApiTest, self).setUp()
        self.hpo_dao = HPODao()
        self.org_dao = OrganizationDao()
        self.site_dao = SiteDao()
        self.hpo_dao.insert(
            HPO(
                hpoId=60,
                name="ILLINOIS",
                displayName="illinois",
                organizationType=OrganizationType.HPO,
            )
        )
        self.org_dao.insert(
            Organization(
                organizationId=5,
                externalId="ILLINOIS_1",
                displayName="Illinois 1",
                hpoId=60,
            )
        )
        self.site_dao.insert(
            Site(
                siteId=4,
                siteName="Illinois Site 1",
                googleGroup="illinois-site-1",
                organizationId=5,
                hpoId=60,
                isObsolete=ObsoleteStatus.ACTIVE,
            )
        )
        self.site_dao.insert(
            Site(
                siteId=5,
                siteName="Illinois Site 2",
                googleGroup="illinois-site-2",
                organizationId=5,
                hpoId=60,
                isObsolete=ObsoleteStatus.OBSOLETE,
            )
        )

    def test_get_full_hierarchy(self):
        expected_response = _make_expected_response()
        expected_response_keys = expected_response["data"][-1].keys()

        response = self.send_get("SiteHierarchy")
        response_data_keys = response["data"][-1].keys()

        self.assertEqual(
            expected_response,
            response,
            msg="Response structure does not match the expected structure",
        )
        self.assertEqual(
            expected_response_keys, response_data_keys, msg="Items do not match"
        )

    def test_get_hierarchy_with_awardee_param(self):
        awardee_id = "PITT"
        expected_response = _make_expected_response(awardee_id=awardee_id)
        response = self.send_get(f"SiteHierarchy?awardee_id={awardee_id}")
        self.assertEqual(
            expected_response,
            response,
            msg="Response structure does not match expected structure",
        )

    def test_get_hierarchy_with_google_group_param(self):
        google_group = "hpo-site-clinic-phoenix"
        expected_response = _make_expected_response(google_group=google_group)
        response = self.send_get(f"SiteHierarchy?google_group={google_group}")
        self.assertEqual(
            expected_response,
            response,
            msg="Response structure does not match expected structure",
        )

    def test_get_hierarchy_with_organization_param(self):
        organization_id = "PITT_BANNER_HEALTH"
        expected_response = _make_expected_response(organization_id=organization_id)
        response = self.send_get(f"SiteHierarchy?organization_id={organization_id}")
        self.assertEqual(
            expected_response,
            response,
            msg="Response structure does not match expected structure",
        )

    def test_get_hierarchy_with_multiple_params(self):
        organization_id, google_group = "PITT_BANNER_HEALTH", "hpo-site-monroeville"
        expected_response = _make_expected_response(
            organization_id=organization_id, google_group=google_group
        )
        response = self.send_get(
            f"SiteHierarchy?organization_id={organization_id}&google_group={google_group}"
        )
        self.assertEqual(
            expected_response,
            response,
            msg="Response structure does not match expected structure",
        )

    def test_get_empty_response_with_obsolete_site(self):
        google_group = "illinois-site-2"
        expected_response = _make_expected_response(google_group=google_group)
        response = self.send_get(f"SiteHierarchy?google_group={google_group}")
        self.assertEqual(
            expected_response,
            response,
            msg="Response structure does not match expected structure. Response should be empty.",
        )

    def test_empty_response_no_records(self):
        awardee_id = "no_awardee"
        expected_response = _make_expected_response(awardee_id=awardee_id)
        response = self.send_get(f"SiteHierarchy?awardee_id={awardee_id}")
        self.assertEqual(
            expected_response,
            response,
            msg="Response structure does not match expected structure. Response should be empty.",
        )
