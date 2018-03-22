import datetime

from dao.hpo_dao import HPODao
from dao.organization_dao import OrganizationDao
from dao.site_dao import SiteDao
from model.hpo import HPO
from model.organization import Organization
from model.site import Site
from model.site_enums import SiteStatus, EnrollingStatus
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

class AwardeeApiTest(FlaskTestBase):
  def setUp(self):
    super(AwardeeApiTest, self).setUp(with_data=False)

    hpo_dao = HPODao()
    hpo_dao.insert(HPO(hpoId=UNSET_HPO_ID, name='UNSET', displayName='Unset',
                       organizationType=OrganizationType.UNSET))
    hpo_dao.insert(HPO(hpoId=PITT_HPO_ID, name='PITT', displayName='Pittsburgh',
                       organizationType=OrganizationType.HPO))
    hpo_dao.insert(HPO(hpoId=AZ_HPO_ID, name='AZ_TUCSON', displayName='Arizona',
                       organizationType=OrganizationType.HPO))

  def test_active_and_inactive_site(self):
    self._setup_data()
    result = self.send_get('Awardee')
    active_only = result['entry'][1]['resource']['organizations']
    site = active_only[1]['sites'][0]['siteStatus']
    self.assertEqual(site, 'ACTIVE')
    self.assertEqual(result['entry'][0], _make_awardee('AZ_TUCSON', 'Arizona', 'HPO'))
    self.assertEqual(result['entry'][1], _make_awardee_with_resource(
                      self._make_expected_pitt_awardee_resource(), 'PITT'))

    result2 = self.send_get('Awardee?_inactive=true')
    not_active = result2['entry'][1]['resource']['organizations']
    site = not_active[1]['sites'][0]['siteStatus']
    self.assertEqual(site, 'INACTIVE')

  def test_get_awardees_no_organizations(self):
    result = self.send_get('Awardee')
    self.assertEquals(3, len(result['entry']))
    self.assertEquals(_make_awardee('PITT', 'Pittsburgh', 'HPO'), result['entry'][1])
    self.assertEquals(_make_awardee('UNSET', 'Unset', 'UNSET'), result['entry'][2])

  def test_get_awardees_with_organizations(self):
    self._setup_data()
    result = self.send_get('Awardee?_inactive=true')
    self.assertEquals(3, len(result['entry']))
    self.assertEquals(_make_awardee_with_resource(self._make_expected_pitt_awardee_resource(
                                                  inactive=True), 'PITT'),
                                                  result['entry'][1])
    self.assertEquals(_make_awardee('UNSET', 'Unset', 'UNSET'), result['entry'][2])

  def test_get_awardee_no_organizations(self):
    result = self.send_get('Awardee/PITT')
    self.assertEquals(_make_awardee_resource('PITT', 'Pittsburgh', 'HPO'), result)


  def test_get_awardee_with_organizations(self):
    self._setup_data()
    result = self.send_get('Awardee/PITT?_inactive=true')
    # we are now filtering out 'INACTIVE' sites by default.
    self.assertEqual(self._make_expected_pitt_awardee_resource(inactive=True), result)

  def test_get_awardee_with_organizations_active_only(self):
    self._setup_data()
    result = self.send_get('Awardee/PITT')
    # we are now filtering out 'INACTIVE' sites by default.
    self.assertEqual(self._make_expected_pitt_awardee_resource(), result)

  def test_get_not_enrolling_awardees_with_organizations(self):
    self._setup_unset_enrollment_site()
    result = self.send_get('Awardee?_inactive=true')
    self.assertEqual(self._make_expected_unset_enrollment_data(), result)

  def _make_expected_pitt_awardee_resource(self, inactive=False):
    sites = [{'id': 'aaaaaaa',
             'displayName': 'Zebras Rock',
             'enrollingStatus': 'INACTIVE',
             'siteStatus': 'INACTIVE',
             'address': {}
            },
             {'id': 'hpo-site-1',
              'displayName': 'Site 1',
              'mayolinkClientNumber': 123456,
              'siteStatus': 'ACTIVE',
              'enrollingStatus': 'ACTIVE',
              'launchDate': '2016-01-01',
              'notes': 'notes',
              'latitude': 12.1,
              'longitude': 13.1,
              'directions': 'directions',
              'physicalLocationName': 'locationName',
              'address': {
                'line': [ 'address1', 'address2'],
                'city': 'Austin',
                'state': 'TX',
                'postalCode': '78751'
              },
              'phoneNumber': '555-555-5555',
              'adminEmails': ['alice@example.com', 'bob@example.com'],
              'link': 'http://www.example.com' }]
    site = []
    if inactive:
      site = sites
    else:
      site.extend([i for i in sites if i['siteStatus'] == 'ACTIVE'])
    org_2_dict = _make_organization_dict('AARDVARK_ORG', 'Aardvarks Rock')
    org_1_dict = _make_organization_dict('ORG_1', 'Organization 1', site)
    return _make_awardee_resource('PITT', 'Pittsburgh', 'HPO', [org_2_dict, org_1_dict])

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


  def _setup_unset_enrollment_site(self):
    site_dao = SiteDao()
    organization_dao = OrganizationDao()
    org_2 = organization_dao.insert(Organization(externalId='ORG_2',
                                                 displayName='Organization 2', hpoId=PITT_HPO_ID))
    site_dao.insert(Site(siteName='not enrolling site',
                         googleGroup='not_enrolling_dot_com',
                         organizationId=org_2.organizationId,
                         enrollingStatus=EnrollingStatus.UNSET,
                         siteStatus=SiteStatus.INACTIVE))

  def _make_expected_unset_enrollment_data(self):
    return {'resourceType': 'Bundle', 'entry':
           [{'resource': {'displayName': 'Arizona', 'type': 'HPO', 'id': 'AZ_TUCSON'},
           'fullUrl': 'http://localhost/rdr/v1/Awardee/AZ_TUCSON'},
           {'resource': {'displayName': 'Pittsburgh', 'type': 'HPO', 'id': 'PITT', 'organizations':
           [{'displayName': 'Organization 2', 'id': 'ORG_2',
           'sites':
           [{'siteStatus': 'INACTIVE', 'displayName': 'not enrolling site', 'id': 'not_enrolling_dot_com',
           'address': {}}]}]}, 'fullUrl': 'http://localhost/rdr/v1/Awardee/PITT'},
           {'resource': {'displayName': 'Unset', 'type': 'UNSET', 'id': 'UNSET'},
           'fullUrl': 'http://localhost/rdr/v1/Awardee/UNSET'}], 'type': 'searchset'}

