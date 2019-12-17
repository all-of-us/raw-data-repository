import datetime
import mock

from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.site import Site
from rdr_service.model.site_enums import EnrollingStatus, SiteStatus, ObsoleteStatus
from rdr_service.participant_enums import OrganizationType, UNSET_HPO_ID
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.mysql_helper_data import AZ_HPO_ID, PITT_HPO_ID, OBSOLETE_ID


def _make_awardee_resource(awardee_id, display_name, org_type, organizations=None):
    resource = {"displayName": display_name, "id": awardee_id, "type": org_type}
    if organizations:
        resource["organizations"] = organizations
    return resource


def _make_awardee_with_resource(resource, awardee_id):
    return {"fullUrl": "http://localhost/rdr/v1/Awardee/%s" % awardee_id, "resource": resource}


def _make_awardee(awardee_id, display_name, org_type, organizations=None):
    return _make_awardee_with_resource(
        _make_awardee_resource(awardee_id, display_name, org_type, organizations), awardee_id
    )


def _make_organization_dict(organization_id, display_name, sites=None):
    resource = {"displayName": display_name, "id": organization_id}
    if sites:
        resource["sites"] = sites
    return resource


class AwardeeApiTest(BaseTestCase):
    def setUp(self):
        super(AwardeeApiTest, self).setUp(with_data=False)
        self.org_dao = OrganizationDao()
        self.hpo_dao = HPODao()
        self.hpo_dao.insert(
            HPO(hpoId=UNSET_HPO_ID, name="UNSET", displayName="Unset", organizationType=OrganizationType.UNSET)
        )
        self.hpo_dao.insert(
            HPO(hpoId=PITT_HPO_ID, name="PITT", displayName="Pittsburgh",
                organizationType=OrganizationType.HPO, resourceId='abcdefg-123')
        )
        self.hpo_dao.insert(
            HPO(hpoId=AZ_HPO_ID, name="AZ_TUCSON", displayName="Arizona", organizationType=OrganizationType.HPO)
        )

    def test_active_and_inactive_site(self):
        self._setup_data()
        result = self.send_get("Awardee")
        active_only = result["entry"][1]["resource"]["organizations"]
        site = active_only[1]["sites"][0]["siteStatus"]
        self.assertEqual(site, "ACTIVE")
        self.assertEqual(result["entry"][0], _make_awardee("AZ_TUCSON", "Arizona", "HPO"))
        self.assertEqual(
            result["entry"][1], _make_awardee_with_resource(self._make_expected_pitt_awardee_resource(), "PITT")
        )

        result2 = self.send_get("Awardee?_inactive=true")
        not_active = result2["entry"][1]["resource"]["organizations"]
        site = not_active[1]["sites"][0]["siteStatus"]
        self.assertEqual(site, "INACTIVE")

    def test_get_awardees_no_organizations(self):
        result = self.send_get("Awardee")
        self.assertEqual(3, len(result["entry"]))
        self.assertEqual(_make_awardee("PITT", "Pittsburgh", "HPO"), result["entry"][1])
        self.assertEqual(_make_awardee("UNSET", "Unset", "UNSET"), result["entry"][2])

    def test_get_awardees_with_organizations(self):
        self._setup_data()
        result = self.send_get("Awardee?_inactive=true")
        self.assertEqual(3, len(result["entry"]))
        self.assertEqual(
            _make_awardee_with_resource(self._make_expected_pitt_awardee_resource(inactive=True), "PITT"),
            result["entry"][1],
        )
        self.assertEqual(_make_awardee("UNSET", "Unset", "UNSET"), result["entry"][2])

    def test_get_awardee_no_organizations(self):
        result = self.send_get("Awardee/PITT")
        self.assertEqual(_make_awardee_resource("PITT", "Pittsburgh", "HPO"), result)

    def test_get_awardee_with_organizations(self):
        self._setup_data()
        result = self.send_get("Awardee/PITT?_inactive=true")
        # we are now filtering out 'INACTIVE' sites by default.
        self.assertEqual(self._make_expected_pitt_awardee_resource(inactive=True), result)

    def test_get_awardee_with_organizations_active_only(self):
        self._setup_data()
        result = self.send_get("Awardee/PITT")
        # we are now filtering out 'INACTIVE' sites by default.
        self.assertEqual(self._make_expected_pitt_awardee_resource(), result)

    def test_get_not_enrolling_awardees_with_organizations(self):
        self._setup_unset_enrollment_site()
        result = self.send_get("Awardee?_inactive=true")
        self.assertEqual(self._make_expected_unset_enrollment_data(), result)

    def test_get_awardee_no_obsolete(self):
        self._setup_data()
        self._update_heirarchy_item_obsolete('org')
        self._setup_active_sitefor_obsolete_test()
        self._update_heirarchy_item_obsolete('site')
        result_pitt = self.send_get("Awardee/PITT?_obsolete=false")

        self.assertEqual(1, len(result_pitt['organizations']))
        self.assertEqual(1, len(result_pitt['organizations'][0]['sites']))

        self.hpo_dao.insert(
            HPO(hpoId=OBSOLETE_ID,
                name="OBSOLETE_HPO",
                displayName="Obso Leet",
                organizationType=OrganizationType.HPO,
                isObsolete=ObsoleteStatus.OBSOLETE)
        )
        result_all = self.send_get("Awardee?_obsolete=false")
        self.assertEqual(3, len(result_all['entry']))

        result_1_obsolete = self.send_get("Awardee/OBSOLETE_HPO?_obsolete=false")
        self.assertEqual('OBSOLETE_HPO', result_1_obsolete['id'])

    def _make_expected_pitt_awardee_resource(self, inactive=False):
        sites = [
            {
                "id": "aaaaaaa",
                "displayName": "Zebras Rock",
                "enrollingStatus": "INACTIVE",
                "siteStatus": "INACTIVE",
                "digitalSchedulingStatus": "None",
                "address": {},
            },
            {
                "id": "hpo-site-1",
                "displayName": "Site 1",
                "mayolinkClientNumber": 123456,
                "siteStatus": "ACTIVE",
                "digitalSchedulingStatus": "None",
                "enrollingStatus": "ACTIVE",
                "launchDate": "2016-01-01",
                "notes": "notes",
                "latitude": 12.1,
                "longitude": 13.1,
                "directions": "directions",
                "physicalLocationName": "locationName",
                "address": {"line": ["address1", "address2"], "city": "Austin", "state": "TX", "postalCode": "78751"},
                "phoneNumber": "555-555-5555",
                "adminEmails": ["alice@example.com", "bob@example.com"],
                "link": "http://www.example.com",
            },
        ]
        site = []
        if inactive:
            site = sites
        else:
            site.extend([i for i in sites if i["siteStatus"] == "ACTIVE"])
        org_2_dict = _make_organization_dict("AARDVARK_ORG", "Aardvarks Rock")
        org_1_dict = _make_organization_dict("ORG_1", "Organization 1", site)
        return _make_awardee_resource("PITT", "Pittsburgh", "HPO", [org_2_dict, org_1_dict])

    def _setup_data(self):
        organization_dao = OrganizationDao()
        site_dao = SiteDao()
        org_1 = organization_dao.insert(
            Organization(externalId="ORG_1", displayName="Organization 1", hpoId=PITT_HPO_ID)
        )
        organization_dao.insert(
            Organization(externalId="AARDVARK_ORG", displayName="Aardvarks Rock", hpoId=PITT_HPO_ID)
        )

        site_dao.insert(
            Site(
                siteName="Site 1",
                googleGroup="hpo-site-1",
                mayolinkClientNumber=123456,
                organizationId=org_1.organizationId,
                siteStatus=SiteStatus.ACTIVE,
                enrollingStatus=EnrollingStatus.ACTIVE,
                launchDate=datetime.datetime(2016, 1, 1),
                notes="notes",
                latitude=12.1,
                longitude=13.1,
                directions="directions",
                physicalLocationName="locationName",
                address1="address1",
                address2="address2",
                city="Austin",
                state="TX",
                zipCode="78751",
                phoneNumber="555-555-5555",
                adminEmails="alice@example.com, bob@example.com",
                link="http://www.example.com",
            )
        )
        site_dao.insert(
            Site(
                siteName="Zebras Rock",
                googleGroup="aaaaaaa",
                organizationId=org_1.organizationId,
                enrollingStatus=EnrollingStatus.INACTIVE,
                siteStatus=SiteStatus.INACTIVE,
            )
        )

    def _setup_unset_enrollment_site(self):
        site_dao = SiteDao()
        organization_dao = OrganizationDao()
        org_2 = organization_dao.insert(
            Organization(externalId="ORG_2", displayName="Organization 2", hpoId=PITT_HPO_ID)
        )
        site_dao.insert(
            Site(
                siteName="not enrolling site",
                googleGroup="not_enrolling_dot_com",
                organizationId=org_2.organizationId,
                enrollingStatus=EnrollingStatus.UNSET,
                siteStatus=SiteStatus.INACTIVE,
            )
        )

    def _make_expected_unset_enrollment_data(self):
        return {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {"displayName": "Arizona", "type": "HPO", "id": "AZ_TUCSON"},
                    "fullUrl": "http://localhost/rdr/v1/Awardee/AZ_TUCSON",
                },
                {
                    "resource": {
                        "displayName": "Pittsburgh",
                        "type": "HPO",
                        "id": "PITT",
                        "organizations": [
                            {
                                "displayName": "Organization 2",
                                "id": "ORG_2",
                                "sites": [
                                    {
                                        "siteStatus": "INACTIVE",
                                        "displayName": "not enrolling site",
                                        "id": "not_enrolling_dot_com",
                                        "digitalSchedulingStatus": "None",
                                        "address": {},
                                    }
                                ],
                            }
                        ],
                    },
                    "fullUrl": "http://localhost/rdr/v1/Awardee/PITT",
                },
                {
                    "resource": {"displayName": "Unset", "type": "UNSET", "id": "UNSET"},
                    "fullUrl": "http://localhost/rdr/v1/Awardee/UNSET",
                },
            ],
            "type": "searchset",
        }

    def _setup_active_sitefor_obsolete_test(self):
        site_dao = SiteDao()
        site_dao.insert(
            Site(
                siteName="Site 3",
                googleGroup="org-1-site-3",
                organizationId=1,
                enrollingStatus=EnrollingStatus.ACTIVE,
                siteStatus=SiteStatus.ACTIVE,
                latitude=100.0,
                longitude=110.0,
            )
        )

        site_dao.insert(
            Site(
                siteName="AA Site 1",
                googleGroup="aardvark-site-1",
                organizationId=2,
                siteStatus=SiteStatus.ACTIVE,
                enrollingStatus=EnrollingStatus.ACTIVE,
                latitude=24.1,
                longitude=24.1,
            )
        )

    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_lat_long_for_site')
    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_time_zone')
    def _update_heirarchy_item_obsolete(self, item, time_zone, lat_long):
        if item == 'org':
            request_json = {
                    "resourceType": "Organization",
                    "id": "a893282c-2717-4a20-b276-d5c9c2c0e51f",
                    'meta': {
                        'versionId': '2'
                    },
                    "extension": [],
                    "identifier": [
                        {
                            "system": "http://all-of-us.org/fhir/sites/organization-id",
                            "value": "AARDVARK_ORG"
                        }
                    ],
                    "active": False,
                    "type": [
                        {
                            "coding": [
                                {
                                    "code": "ORGANIZATION",
                                    "system": "http://all-of-us.org/fhir/sites/type"
                                }
                            ]
                        }
                    ],
                    "name": "Test update organization obsolete",
                    "partOf": {
                        "reference": "Organization/abcdefg-123"
                    }
                }
        else:
            lat_long.return_value = 100, 110
            time_zone.return_value = 'America/Los_Angeles'
            request_json = {
                "resourceType": "Organization",
                "id": "7d011d52-5de1-43e6-afa8-0943b15dc639",
                "meta": {
                    "versionId": "27"
                },
                "extension": [
                    {
                        "url": "http://all-of-us.org/fhir/sites/enrolling-status",
                        "valueString": "true"
                    },
                    {
                        "url": "http://all-of-us.org/fhir/sites/digital-scheduling-status",
                        "valueString": "true"
                    },
                    {
                        "url": "http://all-of-us.org/fhir/sites/ptsc-scheduling-status",
                        "valueString": "true"
                    }
                ],
                "identifier": [
                    {
                        "system": "http://all-of-us.org/fhir/sites/site-id",
                        "value": "org-1-site-3"
                    },
                    {
                        "system": "http://all-of-us.org/fhir/sites/google-group-identifier",
                        "value": "Good Site 3"
                    }
                ],
                "active": False,
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://all-of-us.org/fhir/sites/type",
                                "code": "SITE"
                            }
                        ]
                    }
                ],
                "name": "Site 3 Medical Center",
                "address": [
                    {
                        "line": [
                            "6644 E. Baywood Ave."
                        ],
                        "city": "Mesa",
                        "state": "AZ",
                        "postalCode": "85206"
                    }
                ],
                "partOf": {
                    "reference": "ORG_1"
                }
            }

        self.send_put('organization/hierarchy', request_data=request_json)
