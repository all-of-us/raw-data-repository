import datetime
import mock

from dao.hpo_dao import HPODao
from dao.organization_dao import OrganizationDao
from dao.site_dao import SiteDao
from model.hpo import HPO
from model.organization import Organization
from model.site import Site
from model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus
from participant_enums import UNSET_HPO_ID, OrganizationType

from test.unit_test.unit_test_util import FlaskTestBase, PITT_HPO_ID, AZ_HPO_ID


def _make_awardee_resource(awardee_id, display_name, org_type, organizations=None):
  resource = {'displayName': display_name,
              'id': awardee_id,
              'type': org_type }
  if organizations:
    resource['organizations'] = organizations
  return resource


def _make_awardee_with_resource(resource, awardee_id):
  return {'fullUrl': 'http://localhost/rdr/v1/Awardee/%s' % awardee_id,
          'resource': resource }


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


class HierarchyContentApiTest(FlaskTestBase):
  def setUp(self):
    super(HierarchyContentApiTest, self).setUp(with_data=False)

    hpo_dao = HPODao()
    hpo_dao.insert(HPO(hpoId=UNSET_HPO_ID, name='UNSET', displayName='Unset',
                       organizationType=OrganizationType.UNSET))
    hpo_dao.insert(HPO(hpoId=PITT_HPO_ID, name='PITT', displayName='Pittsburgh',
                       organizationType=OrganizationType.HPO))
    hpo_dao.insert(HPO(hpoId=AZ_HPO_ID, name='AZ_TUCSON', displayName='Arizona',
                       organizationType=OrganizationType.HPO))
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
        "identifier": {
          "value": "PITT"
        },
        "display": "awardee"
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
        "identifier": {
          "value": "PITT"
        },
        "display": "awardee"
      }
    }

    result_before = self.send_get('Awardee/PITT')
    self.assertEqual(2, len(result_before['organizations']))

    self.send_put('organization/hierarchy', request_data=request_json)

    result_after = self.send_get('Awardee/PITT')
    self.assertEqual(2, len(result_after['organizations']))
    self.assertIn({'displayName': 'Test update organization display name', 'id': 'AARDVARK_ORG'},
                  result_after['organizations'])

  @mock.patch('dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
              '_get_lat_long_for_site')
  @mock.patch('dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
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
        "identifier": {
          "value": "AARDVARK_ORG"
        },
        "display": "Organization"
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

  @mock.patch('dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
              '_get_lat_long_for_site')
  @mock.patch('dao.organization_hierarchy_sync_dao.OrganizationHierarchySyncDao.'
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
        "identifier": {
          "value": "ORG_1"
        },
        "display": "Organization"
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

  def _setup_data(self):
    organization_dao = OrganizationDao()
    site_dao = SiteDao()
    org_1 = organization_dao.insert(Organization(externalId='ORG_1',
                                                 displayName='Organization 1', hpoId=PITT_HPO_ID))
    organization_dao.insert(Organization(externalId='AARDVARK_ORG',
                                         displayName='Aardvarks Rock', hpoId=PITT_HPO_ID))

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
