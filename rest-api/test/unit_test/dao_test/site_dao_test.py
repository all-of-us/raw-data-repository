from unit_test_util import SqlTestBase, PITT_HPO_ID, UNSET_HPO_ID
from dao.site_dao import SiteDao
from model.site import Site

class SiteDaoTest(SqlTestBase):

  def setUp(self):
    super(SiteDaoTest, self).setUp()
    self.site_dao = SiteDao()

  def test_get_no_sites(self):
    self.assertIsNone(self.site_dao.get(9999))
    self.assertIsNone(self.site_dao.get_by_google_group('site@googlegroups.com'))

  def test_insert(self):
    site = Site(siteName='site', googleGroup='site@googlegroups.com',
                consortiumName='consortium', mayolinkClientNumber=12345, hpoId=PITT_HPO_ID)
    created_site = self.site_dao.insert(site)
    new_site = self.site_dao.get(created_site.siteId)
    site.siteId = created_site.siteId
    self.assertEquals(site.asdict(), new_site.asdict())
    self.assertEquals(site.asdict(),
                      self.site_dao.get_by_google_group('site@googlegroups.com').asdict())

  def test_update(self):
    site = Site(siteName='site', googleGroup='site@googlegroups.com',
                consortiumName='consortium', mayolinkClientNumber=12345, hpoId=PITT_HPO_ID)
    created_site = self.site_dao.insert(site)
    new_site = Site(siteId=created_site.siteId, siteName='site2',
                    googleGroup='site2@googlegroups.com',
                    consortiumName='consortium2', mayolinkClientNumber=123456, hpoId=UNSET_HPO_ID)
    self.site_dao.update(new_site)
    fetched_site = self.site_dao.get(created_site.siteId)
    self.assertEquals(new_site.asdict(), fetched_site.asdict())
    self.assertEquals(new_site.asdict(),
                      self.site_dao.get_by_google_group('site2@googlegroups.com').asdict())
    self.assertIsNone(self.site_dao.get_by_google_group('site@googlegroups.com'))

