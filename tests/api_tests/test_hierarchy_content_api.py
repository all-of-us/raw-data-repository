import datetime
import mock
from unittest import skip

from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.site import Site
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus
from rdr_service.participant_enums import UNSET_HPO_ID, OrganizationType

from tests.helpers.mysql_helper_data import PITT_HPO_ID, AZ_HPO_ID
from tests.helpers.unittest_base import BaseTestCase


def _make_awardee_resource(awardee_id, display_name, org_type, organizations=None):
    resource = {'displayName': display_name,
                'id': awardee_id,
                'type': org_type}
    if organizations:
        resource['organizations'] = organizations
    return resource


def _make_awardee_with_resource(resource, awardee_id):
    return {'fullUrl': 'http://localhost/rdr/v1/Awardee/%s' % awardee_id,
            'resource': resource}


def _make_awardee(awardee_id, display_name, org_type, organizations=None):
    return _make_awardee_with_resource(_make_awardee_resource(awardee_id, display_name,
                                                              org_type, organizations),
                                       awardee_id)


def _make_organization_dict(organization_id, display_name, sites=None):
    resource = {'displayName': display_name,
                'id': organization_id}
    if sites:
        resource['sites'] = sites
    return resource


class HierarchyContentApiTest(BaseTestCase):
    def setUp(self):
        super(HierarchyContentApiTest, self).setUp(with_data=False)

        hpo_dao = HPODao()
        hpo_dao.insert(HPO(hpoId=UNSET_HPO_ID, name='UNSET', displayName='Unset',
                           organizationType=OrganizationType.UNSET, resourceId='h123456'))
        hpo_dao.insert(HPO(hpoId=PITT_HPO_ID, name='PITT', displayName='Pittsburgh',
                           organizationType=OrganizationType.HPO, resourceId='h123457'))
        hpo_dao.insert(HPO(hpoId=AZ_HPO_ID, name='AZ_TUCSON', displayName='Arizona',
                           organizationType=OrganizationType.HPO, resourceId='h123458'))
        self.site_dao = SiteDao()
        self.org_dao = OrganizationDao()

    def test_create_new_hpo(self):
        self._setup_data()
        request_json = {
            "resourceType": "Organization",
            "id": "a893282c-2717-4a20-b276-d5c9c2c0e51f",
            'meta': {
                'versionId': '1'
            },
            "extension": [
                {
                    "url": "http://all-of-us.org/fhir/sites/awardee-type",
                    "valueString": "DV"
                }
            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/awardee-id",
                    "value": "TEST_HPO_NAME"
                }
            ],
            "active": True,
            "type": [
                {
                    "coding": [
                        {
                            "code": "AWARDEE",
                            "system": "http://all-of-us.org/fhir/sites/type"
                        }
                    ]
                }
            ],
            "name": "Test new HPO display name"
        }
        self.send_put('organization/hierarchy', request_data=request_json)
        result = self.send_get('Awardee/TEST_HPO_NAME')
        self.assertEquals(_make_awardee_resource('TEST_HPO_NAME', 'Test new HPO display name', 'DV'),
                          result)

    def test_update_existing_hpo(self):
        self._setup_data()
        request_json = {
            "resourceType": "Organization",
            "id": "a893282c-2717-4a20-b276-d5c9c2c0e51f",
            'meta': {
                'versionId': '2'
            },
            "extension": [
                {
                    "url": "http://all-of-us.org/fhir/sites/awardee-type",
                    "valueString": "DV"
                }
            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/awardee-id",
                    "value": "PITT"
                }
            ],
            "active": True,
            "type": [
                {
                    "coding": [
                        {
                            "code": "AWARDEE",
                            "system": "http://all-of-us.org/fhir/sites/type"
                        }
                    ]
                }
            ],
            "name": "Test update HPO display name"
        }
        self.send_put('organization/hierarchy', request_data=request_json)
        result = self.send_get('Awardee/PITT')
        self.assertEqual(result['displayName'], 'Test update HPO display name')
        self.assertEqual(result['type'], 'DV')

    def test_create_new_organization(self):
        self._setup_data()
        request_json = {
            "resourceType": "Organization",
            "id": "a893282c-2717-4a20-b276-d5c9c2c0e51f",
            'meta': {
                'versionId': '1'
            },
            "extension": [

            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/organization-id",
                    "value": "TEST_NEW_ORG"
                }
            ],
            "active": True,
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
            "name": "Test create organization display name",
            "partOf": {
                "reference": "Organization/h123457"
            }
        }

        result_before = self.send_get('Awardee/PITT')
        self.assertEqual(2, len(result_before['organizations']))

        self.send_put('organization/hierarchy', request_data=request_json)

        result_after = self.send_get('Awardee/PITT')
        self.assertEqual(3, len(result_after['organizations']))
        self.assertIn({'displayName': 'Test create organization display name', 'id': 'TEST_NEW_ORG'},
                      result_after['organizations'])

    def test_update_existing_organization(self):
        self._setup_data()
        request_json = {
            "resourceType": "Organization",
            "id": "a893282c-2717-4a20-b276-d5c9c2c0e51f",
            'meta': {
                'versionId': '2'
            },
            "extension": [

            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/organization-id",
                    "value": "AARDVARK_ORG"
                }
            ],
            "active": True,
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
            "name": "Test update organization display name",
            "partOf": {
                "reference": "Organization/h123457"
            }
        }

        result_before = self.send_get('Awardee/PITT')
        self.assertEqual(2, len(result_before['organizations']))

        self.send_put('organization/hierarchy', request_data=request_json)

        result_after = self.send_get('Awardee/PITT')
        self.assertEqual(2, len(result_after['organizations']))
        self.assertIn({'displayName': 'Test update organization display name', 'id': 'AARDVARK_ORG'},
                      result_after['organizations'])

    @skip('skipping due to new payload type')
    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_lat_long_for_site')
    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_time_zone')
    def test_create_hpo_org_site(self, time_zone, lat_long):
        hpo_json = {
            "resourceType": "Organization",
            "id": "a893282c-2717-4a20-b276-d5c9c2c0e51f",
            'meta': {
                'versionId': '1'
            },
            "extension": [
                {
                    "url": "http://all-of-us.org/fhir/sites/awardee-type",
                    "valueString": "DV"
                }
            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/awardee-id",
                    "value": "TEST_HPO_NAME"
                }
            ],
            "active": True,
            "type": [
                {
                    "coding": [
                        {
                            "code": "AWARDEE",
                            "system": "http://all-of-us.org/fhir/sites/type"
                        }
                    ]
                }
            ],
            "name": "Test new HPO display name"
        }
        self.send_put('organization/hierarchy', request_data=hpo_json)

        org_json = {
            "resourceType": "Organization",
            "id": "a893282c-2717-4a20-b276-d5c9c2c0e123",
            'meta': {
                'versionId': '1'
            },
            "extension": [

            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/organization-id",
                    "value": "TEST_NEW_ORG"
                }
            ],
            "active": True,
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
            "name": "Test create organization display name",
            "partOf": {
                "reference": "Organization/a893282c-2717-4a20-b276-d5c9c2c0e51f"
            }
        }

        self.send_put('organization/hierarchy', request_data=org_json)

        lat_long.return_value = 100, 110
        time_zone.return_value = 'America/Los_Angeles'
        site_json = {
            "resourceType": "Organization",
            "id": "a893282c-2717-4a20-b276-d5c9c2c0e234",
            'meta': {
                'versionId': '1'
            },
            "extension": [
                {
                    "url": "http://all-of-us.org/fhir/sites/enrollmentStatusActive",
                    "valueBoolean": True
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/digitalSchedulingStatusActive",
                    "valueBoolean": True
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/schedulingStatusActive",
                    "valueBoolean": True
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/notes",
                    "valueString": "This is a note about an organization"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/schedulingInstructions",
                    "valueString": "Please schedule appointments up to a week before intended date."
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/anticipatedLaunchDate",
                    "valueDate": "07-02-2010"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/locationName",
                    "valueString": "Thompson Building"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/directions",
                    "valueString": "Exit 95 N and make a left onto Fake Street"
                }
            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/site-id",
                    "value": "hpo-site-awesome-testing"
                },
                {
                    "system": "http://all-of-us.org/fhir/sites/mayo-link-identifier",
                    "value": "123456"
                },
                {
                    "system": "http://all-of-us.org/fhir/sites/google-group-identifier",
                    "value": "Awesome Genomics Testing"
                }
            ],
            "active": True,
            "type": [
                {
                    "coding": [
                        {
                            "code": "SITE",
                            "system": "http://all-of-us.org/fhir/sites/type"
                        }
                    ]
                }
            ],
            "name": "Awesome Genomics Testing",
            "partOf": {
                "reference": "Organization/a893282c-2717-4a20-b276-d5c9c2c0e123"
            },
            "address": [{
                "line": [
                    "1855 4th Street",
                    "AAC5/6"
                ],
                "city": "San Francisco",
                "state": "CA",
                "postalCode": "94158"
            }],
            "contact": [
                {
                    "telecom": [{
                        "system": "phone",
                        "value": "7031234567"
                    }]
                },
                {
                    "telecom": [{
                        "system": "email",
                        "value": "support@awesome-testing.com"
                    }]
                },
                {
                    "telecom": [{
                        "system": "url",
                        "value": "http://awesome-genomic-testing.com"
                    }]
                }
            ]
        }

        self.send_put('organization/hierarchy', request_data=site_json)

        result = self.send_get('Awardee/TEST_HPO_NAME')
        self.assertEqual({
            u'displayName': u'Test new HPO display name',
            u'type': u'DV',
            u'id': u'TEST_HPO_NAME',
            u'organizations': [{u'displayName': u'Test create organization display name',
                                u'id': u'TEST_NEW_ORG',
                                u'sites': [{u'mayolinkClientNumber': 123456,
                                            u'timeZoneId': u'America/Los_Angeles',
                                            u'displayName': u'Awesome Genomics Testing',
                                            u'notes': u'This is a note about an organization',
                                            u'launchDate': u'2010-07-02',
                                            u'notesEs': u'',
                                            u'enrollingStatus': u'ACTIVE',
                                            u'longitude': 110.0,
                                            u'schedulingInstructions': u'Please schedule appointments up '
                                                                       u'to a week before intended date.',
                                            u'latitude': 100.0,
                                            u'physicalLocationName': u'Thompson Building',
                                            u'phoneNumber': u'7031234567',
                                            u'siteStatus': u'ACTIVE',
                                            u'address': {
                                                u'postalCode': u'94158', u'city': u'San Francisco',
                                                u'line': [u'1855 4th Street', u'AAC5/6'], u'state': u'CA'
                                            },
                                            u'directions': u'Exit 95 N and make a left onto Fake Street',
                                            u'link': u'http://awesome-genomic-testing.com',
                                            u'id': u'hpo-site-awesome-testing',
                                            u'adminEmails': [u'support@awesome-testing.com'],
                                            u'digitalSchedulingStatus': u'ACTIVE'
                                            }]
                                }]
        }, result)

    @skip('skipping due to new payload type')
    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_lat_long_for_site')
    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_time_zone')
    def test_create_new_site(self, time_zone, lat_long):
        self._setup_data()
        lat_long.return_value = 100, 110
        time_zone.return_value = 'America/Los_Angeles'
        request_json = {
            "resourceType": "Organization",
            "id": "a893282c-2717-4a20-b276-d5c9c2c0e51f",
            'meta': {
                'versionId': '1'
            },
            "extension": [
                {
                    "url": "http://all-of-us.org/fhir/sites/enrolling-status",
                    "valueString": "true"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/digitalSchedulingStatusActive",
                    "valueBoolean": True
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/ptsc-scheduling-status",
                    "valueString": "true"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/notes",
                    "valueString": "This is a note about an organization"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/scheduling-instructions",
                    "valueString": "Please schedule appointments up to a week before intended date."
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/anticipated-launch-date",
                    "valueString": "0702201022"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/locationName",
                    "valueString": "Thompson Building"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/directions",
                    "valueString": "Exit 95 N and make a left onto Fake Street"
                }
            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/site-id",
                    "value": "hpo-site-awesome-testing"
                },
                {
                    "system": "http://all-of-us.org/fhir/sites/mayo-link-identifier",
                    "value": "123456"
                },
                {
                    "system": "http://all-of-us.org/fhir/sites/google-group-identifier",
                    "value": "Awesome Genomics Testing"
                }
            ],
            "active": True,
            "type": [
                {
                    "coding": [
                        {
                            "code": "SITE",
                            "system": "http://all-of-us.org/fhir/sites/type"
                        }
                    ]
                }
            ],
            "name": "Awesome Genomics Testing",
            "partOf": {
                "reference": "Organization/o123457"
            },
            "address": [{
                "line": [
                    "1855 4th Street",
                    "AAC5/6"
                ],
                "city": "San Francisco",
                "state": "CA",
                "postalCode": "94158"
            }],
            "contact": [
                {
                    "telecom": [{
                        "system": "phone",
                        "value": "7031234567"
                    }]
                },
                {
                    "telecom": [{
                        "system": "email",
                        "value": "support@awesome-testing.com"
                    }]
                },
                {
                    "telecom": [{
                        "system": "url",
                        "value": "http://awesome-genomic-testing.com"
                    }]
                }
            ]
        }

        self.send_put('organization/hierarchy', request_data=request_json)

        self.send_get('Awardee/PITT')

        existing_map = {entity.googleGroup: entity for entity in self.site_dao.get_all()}
        existing_entity = existing_map.get('hpo-site-awesome-testing')

        self.assertEqual(existing_entity.adminEmails, 'support@awesome-testing.com')
        self.assertEqual(existing_entity.siteStatus, SiteStatus('ACTIVE'))
        self.assertEqual(existing_entity.isObsolete, None)
        self.assertEqual(existing_entity.city, 'San Francisco')
        self.assertEqual(existing_entity.googleGroup, 'hpo-site-awesome-testing')
        self.assertEqual(existing_entity.state, 'CA')
        self.assertEqual(existing_entity.digitalSchedulingStatus, DigitalSchedulingStatus('ACTIVE'))
        self.assertEqual(existing_entity.mayolinkClientNumber, 123456)
        self.assertEqual(existing_entity.address1, '1855 4th Street')
        self.assertEqual(existing_entity.address2, 'AAC5/6')
        self.assertEqual(existing_entity.zipCode, '94158')
        self.assertEqual(existing_entity.directions, 'Exit 95 N and make a left onto Fake Street')
        self.assertEqual(existing_entity.notes, 'This is a note about an organization')
        self.assertEqual(existing_entity.enrollingStatus, EnrollingStatus('ACTIVE'))
        self.assertEqual(existing_entity.scheduleInstructions,
                         'Please schedule appointments up to a week before intended date.')
        self.assertEqual(existing_entity.physicalLocationName, 'Thompson Building')
        self.assertEqual(existing_entity.link, 'http://awesome-genomic-testing.com')
        self.assertEqual(existing_entity.launchDate, datetime.date(2010, 7, 2))
        self.assertEqual(existing_entity.phoneNumber, '7031234567')

    @skip('skipping due to new payload type')
    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_lat_long_for_site')
    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_time_zone')
    def test_update_existing_site(self, time_zone, lat_long):
        self._setup_data()
        lat_long.return_value = 100, 110
        time_zone.return_value = 'America/Los_Angeles'
        request_json = {
            "resourceType": "Organization",
            "id": "a893282c-2717-4a20-b276-d5c9c2c0e51f",
            'meta': {
                'versionId': '2'
            },
            "extension": [
                {
                    "url": "http://all-of-us.org/fhir/sites/enrollmentStatusActive",
                    "valueBoolean": True
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/digitalSchedulingStatusActive",
                    "valueBoolean": False
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/schedulingStatusActive",
                    "valueBoolean": True
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/notes",
                    "valueString": "This is a note about an organization"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/schedulingInstructions",
                    "valueString": "Please schedule appointments up to a week before intended date."
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/anticipatedLaunchDate",
                    "valueDate": "07-02-2010"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/locationName",
                    "valueString": "Thompson Building"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/directions",
                    "valueString": "Exit 95 N and make a left onto Fake Street"
                }
            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/site-id",
                    "value": "hpo-site-1"
                },
                {
                    "system": "http://all-of-us.org/fhir/sites/mayo-link-identifier",
                    "value": "123456"
                },
                {
                    "system": "http://all-of-us.org/fhir/sites/google-group-identifier",
                    "value": "Awesome Genomics Testing"
                }
            ],
            "active": True,
            "type": [
                {
                    "coding": [
                        {
                            "code": "SITE",
                            "system": "http://all-of-us.org/fhir/sites/type"
                        }
                    ]
                }
            ],
            "name": "Awesome Genomics Testing",
            "partOf": {
                "reference": "Organization/o123456"
            },
            "address": [{
                "line": [
                    "1855 4th Street",
                    "AAC5/6"
                ],
                "city": "San Francisco",
                "state": "CA",
                "postalCode": "94158"
            }],
            "contact": [
                {
                    "telecom": [{
                        "system": "phone",
                        "value": "7031234567"
                    }]
                },
                {
                    "telecom": [{
                        "system": "email",
                        "value": "support@awesome-testing.com"
                    }]
                },
                {
                    "telecom": [{
                        "system": "url",
                        "value": "http://awesome-genomic-testing.com"
                    }]
                }
            ]
        }

        self.send_put('organization/hierarchy', request_data=request_json)

        existing_map = {entity.googleGroup: entity for entity in self.site_dao.get_all()}
        existing_entity = existing_map.get('hpo-site-1')

        self.assertEqual(existing_entity.adminEmails, 'support@awesome-testing.com')
        self.assertEqual(existing_entity.siteStatus, SiteStatus('ACTIVE'))
        self.assertEqual(existing_entity.isObsolete, None)
        self.assertEqual(existing_entity.city, 'San Francisco')
        self.assertEqual(existing_entity.googleGroup, 'hpo-site-1')
        self.assertEqual(existing_entity.state, 'CA')
        self.assertEqual(existing_entity.digitalSchedulingStatus, DigitalSchedulingStatus('INACTIVE'))
        self.assertEqual(existing_entity.mayolinkClientNumber, 123456)
        self.assertEqual(existing_entity.address1, '1855 4th Street')
        self.assertEqual(existing_entity.address2, 'AAC5/6')
        self.assertEqual(existing_entity.zipCode, '94158')
        self.assertEqual(existing_entity.directions, 'Exit 95 N and make a left onto Fake Street')
        self.assertEqual(existing_entity.notes, 'This is a note about an organization')
        self.assertEqual(existing_entity.enrollingStatus, EnrollingStatus('ACTIVE'))
        self.assertEqual(existing_entity.scheduleInstructions,
                         'Please schedule appointments up to a week before intended date.')
        self.assertEqual(existing_entity.physicalLocationName, 'Thompson Building')
        self.assertEqual(existing_entity.link, 'http://awesome-genomic-testing.com')
        self.assertEqual(existing_entity.launchDate, datetime.date(2010, 7, 2))
        self.assertEqual(existing_entity.phoneNumber, '7031234567')

    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_lat_long_for_site')
    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_time_zone')
    def test_update_existing_site_new_payload(self, time_zone, lat_long):
        self._setup_data()
        lat_long.return_value = 100, 110
        time_zone.return_value = 'America/Los_Angeles'
        request_json = {
            "resourceType": "Organization",
            "id": "o123456",

            "meta": {
                "versionId": "2"
            },
            "extension": [
                {
                    "url": "http://all-of-us.org/fhir/sites/mayolink-client-#",
                    "valueString": "7035772"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/scheduling-instructions",
                    "valueString": "<p>Good job! Now that you've finished \"The Basics\" "
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/enrolling-status",
                    "valueString": "true"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/anticipated-launch-date",
                    "valueString": "1533081600"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/ptsc-scheduling-status",
                    "valueString": "true"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/notes-spanish",
                    "valueString": "<p>Addisu Testing update&nbsp;</p>\n"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/google-group-name",
                    "valueString": "HPO UPMC Dermatology Clinic"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/digital-scheduling-status",
                    "valueString": "false"
                }
            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/site-id",
                    "value": "hpo-site-upmcdermatologyclinic"
                }
            ],
            "active": True,
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
            "name": "UPMC Dermatology Clinic",
            "address": [
                {
                    "line": [
                        "3601 Fifth Avenue"
                    ],
                    "city": "Pittsburgh",
                    "state": "PA",
                    "postalCode": "15213"
                }
            ],
            "partOf": {
                "reference": "Organization/o123456"
            },
            "contact": [
                {
                    "telecom": [
                        {
                            "system": "email",
                            "value": "b@b.com"
                        }
                    ]
                }
            ]
        }

        self.send_put('organization/hierarchy', request_data=request_json)

        existing_map = {entity.googleGroup: entity for entity in self.site_dao.get_all()}
        existing_entity = existing_map.get('hpo-site-1')

        self.assertEqual(existing_entity.adminEmails, 'alice@example.com, bob@example.com')
        self.assertEqual(existing_entity.siteStatus, SiteStatus('ACTIVE'))
        self.assertEqual(existing_entity.isObsolete, None)
        self.assertEqual(existing_entity.enrollingStatus, EnrollingStatus(ACTIVE))
        self.assertEqual(existing_entity.schedulingInstructions, "<p>Good job! Now that you've finished \"The Basics\" ")
        self.assertEqual(existing_entity.mayolinkClientNumber, "7035772")
        self.assertEqual(existing_entity.notes_ES, "<p>Addisu Testing update&nbsp;</p>\n")

    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_lat_long_for_site')
    @mock.patch('rdr_service.dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
                '_get_time_zone')
    def test_insert_new_site_new_payload(self, time_zone, lat_long):
        self._setup_data()
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
                    "url": "http://all-of-us.org/fhir/sites/mayolink-client-#",
                    "valueString": "7036694"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/scheduling-instructions",
                    "valueString": "Someone from the All of Us Research Program may contact you by phone and/or email to schedule an appointment and to share more information about the Program. You may already have an appointment scheduled. To contact us directly, please call 877-268-2684 or email AllofUsAZ@email.arizona.edu. For a list of our locations, visit AllofUsAZ.org.<br><br>El personal del Programa Científico All of Us se comunicará con usted por teléfono y/o correo electrónico para hacer una cita y compartir más información acerca del programa. Quizás, usted ya tiene una cita. Para comunicarse directamente con nosotros, por favor llame al 877-268-2684 o envíe un correo electrónico a AllofUsAZ@email.arizona.edu. Para ver la lista de nuestras clínicas, visite AllofUsAZ.org."
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/enrolling-status",
                    "valueString": "true"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/anticipated-launch-date",
                    "valueString": "1527552000"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/ptsc-scheduling-status",
                    "valueString": "true"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/google-group-name",
                    "valueString": "HPO Banner Baywood"
                },
                {
                    "url": "http://all-of-us.org/fhir/sites/digital-scheduling-status",
                    "valueString": "false"
                }
            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/site-id",
                    "value": "hpo-site-bannerbaywood"
                }
            ],
            "active": True,
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
            "name": "Banner Baywood Medical Center",
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
                "reference": "Organization/o123457"
            },
            "contact": [
                {
                    "telecom": [
                        {
                            "system": "phone",
                            "value": "8772682684"
                        }
                    ]
                },
                {
                    "telecom": [
                        {
                            "system": "email",
                            "value": "jennifer.craig-muller@bannerhealth.com,mcoury@email.arizona.edu"
                        }
                    ]
                }
            ]
        }

        self.send_put('organization/hierarchy', request_data=request_json)

        existing_map = {entity.googleGroup: entity for entity in self.site_dao.get_all()}
        existing_entity = existing_map.get('hpo-site-bannerbaywood')

        self.assertEqual(existing_entity.adminEmails, 'jennifer.craig-muller@bannerhealth.com,mcoury@email.arizona.edu')
        self.assertEqual(existing_entity.siteStatus, SiteStatus('ACTIVE'))
        self.assertEqual(existing_entity.isObsolete, None)
        self.assertEqual(existing_entity.hpoId, 2)
        self.assertEqual(existing_entity.resourceId, '7d011d52-5de1-43e6-afa8-0943b15dc639')
        self.assertEqual(existing_entity.siteName, 'Banner Baywood Medical Center')

    def test_create_hpo_new_payload(self):
        self._setup_data()
        request_json = {
            "resourceType": "Organization",
            "id": "65b32423-f3c3-4f91-a0fb-db1d513c7e72",
            "meta": {
                "versionId": "25"
            },
            "extension": [
                {
                    "url": "http://all-of-us.org/fhir/sites/awardee-type",
                    "valueString": "HPO"
                }
            ],
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/awardee-id",
                    "value": "AZ_TUCSON"
                }
            ],
            "active": True,
            "type": [
                {
                    "coding": [
                        {
                            "system": "http://all-of-us.org/fhir/sites/type",
                            "code": "AWARDEE"
                        }
                    ]
                }
            ],
            "name": "Arizona"
        }
        self.send_put('organization/hierarchy', request_data=request_json)
        result = self.send_get('Awardee/AZ_TUCSON')
        self.assertEquals(_make_awardee_resource('AZ_TUCSON', 'Arizona', 'HPO'),
                          result)
        truthiness = self.send_get('Awardee/AZ_TUCSON')
        self.assertEqual(truthiness['type'], 'HPO')
        self.assertEqual(truthiness['id'], 'AZ_TUCSON')
        self.assertEqual(truthiness['displayName'], 'Arizona')

    def test_create_new_organization_new_payload(self):
        self._setup_data()
        request_json = {
            "resourceType": "Organization",
            "id": "35e40061-e2c4-4bc6-9441-1a8fee5f9dce",
            "meta": {
                "versionId": "25"
            },
            "identifier": [
                {
                    "system": "http://all-of-us.org/fhir/sites/organization-id",
                    "value": "AZ_TUCSON_BANNER_HEALTH"
                }
            ],
            "active": True,
            "type": [
                {
                    "coding": [
                        {
                            "system": "http://all-of-us.org/fhir/sites/type",
                            "code": "ORGANIZATION"
                        }
                    ]
                }
            ],
            "name": "Banner Health",
            "partOf": {
                "reference": "Organization/h123458"
            }
        }

        result_before = self.send_get('Awardee/PITT')
        self.assertEqual(2, len(result_before['organizations']))

        self.send_put('organization/hierarchy', request_data=request_json)

        result_after = self.send_get('Awardee/AZ_TUCSON')
        self.assertEqual(1, len(result_after['organizations']))
        self.assertEqual({'displayName':'Banner Health', 'id':'AZ_TUCSON_BANNER_HEALTH'}, result_after['organizations'][0])
        print(result_after)

    def _setup_data(self):
        organization_dao = OrganizationDao()
        site_dao = SiteDao()
        org_1 = organization_dao.insert(Organization(externalId='ORG_1', displayName='Organization 1',
                                                     hpoId=PITT_HPO_ID, resourceId='o123456'))
        organization_dao.insert(Organization(externalId='AARDVARK_ORG', displayName='Aardvarks Rock',
                                             hpoId=PITT_HPO_ID, resourceId='o123457'))

        site_dao.insert(Site(siteName='Site 1',
                             googleGroup='hpo-site-1',
                             mayolinkClientNumber=123456,
                             organizationId=org_1.organizationId,
                             siteStatus=SiteStatus.ACTIVE,
                             enrollingStatus=EnrollingStatus.ACTIVE,
                             launchDate=datetime.datetime(2016, 1, 1),
                             notes='notes',
                             latitude=12.1,
                             longitude=13.1,
                             directions='directions',
                             physicalLocationName='locationName',
                             address1='address1',
                             address2='address2',
                             city='Austin',
                             state='TX',
                             zipCode='78751',
                             phoneNumber='555-555-5555',
                             adminEmails='alice@example.com, bob@example.com',
                             link='http://www.example.com'))
        site_dao.insert(Site(siteName='Zebras Rock',
                             googleGroup='aaaaaaa',
                             organizationId=org_1.organizationId,
                             enrollingStatus=EnrollingStatus.INACTIVE,
                             siteStatus=SiteStatus.INACTIVE))
