from unit_test.unit_test_util import PITT_ORG_ID, AZ_ORG_ID
from unit_test_util import SqlTestBase, PITT_HPO_ID, UNSET_HPO_ID, AZ_HPO_ID
from dao.site_dao import SiteDao
from model.site import Site
from model.participant import Participant
from dao.participant_dao import ParticipantDao, ParticipantHistoryDao
from dao.participant_summary_dao import ParticipantSummaryDao

class SiteDaoTest(SqlTestBase):

  def setUp(self):
    super(SiteDaoTest, self).setUp()
    self.site_dao = SiteDao()
    self.participant_dao = ParticipantDao()
    self.ps_dao = ParticipantSummaryDao()
    self.ps_history = ParticipantHistoryDao()

  def test_get_no_sites(self):
    self.assertIsNone(self.site_dao.get(9999))
    self.assertIsNone(self.site_dao.get_by_google_group('site@googlegroups.com'))

  def test_insert(self):
    site = Site(siteName='site', googleGroup='site@googlegroups.com',
                mayolinkClientNumber=12345, hpoId=PITT_HPO_ID)
    created_site = self.site_dao.insert(site)
    new_site = self.site_dao.get(created_site.siteId)
    site.siteId = created_site.siteId
    self.assertEquals(site.asdict(), new_site.asdict())
    self.assertEquals(site.asdict(),
                      self.site_dao.get_by_google_group('site@googlegroups.com').asdict())

  def test_update(self):
    site = Site(siteName='site', googleGroup='site@googlegroups.com',
                mayolinkClientNumber=12345, hpoId=PITT_HPO_ID)
    created_site = self.site_dao.insert(site)
    new_site = Site(siteId=created_site.siteId, siteName='site2',
                    googleGroup='site2@googlegroups.com',
                    mayolinkClientNumber=123456, hpoId=UNSET_HPO_ID)
    self.site_dao.update(new_site)
    fetched_site = self.site_dao.get(created_site.siteId)
    self.assertEquals(new_site.asdict(), fetched_site.asdict())
    self.assertEquals(new_site.asdict(),
                      self.site_dao.get_by_google_group('site2@googlegroups.com').asdict())
    self.assertIsNone(self.site_dao.get_by_google_group('site@googlegroups.com'))

  def test_participant_pairing_updates_on_change(self):
    site = Site(siteName='site', googleGroup='site@googlegroups.com',
                mayolinkClientNumber=12345, hpoId=PITT_HPO_ID, organizationId=PITT_ORG_ID)
    created_site = self.site_dao.insert(site)
    p = Participant(participantId=1, biobankId=2, siteId=created_site.siteId)
    self.participant_dao.insert(p)
    fetch_p = self.participant_dao.get(p.participantId)
    update_site_parent = Site(siteId=created_site.siteId, siteName='site2',
                    googleGroup='site2@googlegroups.com',
                    mayolinkClientNumber=123456, hpoId=AZ_HPO_ID, organizationId=AZ_ORG_ID)
    self.site_dao.update(update_site_parent)
    updated_p = self.participant_dao.get(fetch_p.participantId)
    p_summary = self.ps_dao.insert(self.participant_summary(updated_p))
    ps = self.ps_dao.get(p_summary.participantId)
    ph = self.ps_history.get([updated_p.participantId, 1])

    self.assertEquals(update_site_parent.hpoId, updated_p.hpoId)
    self.assertEquals(update_site_parent.organizationId, updated_p.organizationId)
    self.assertEquals(p_summary.organizationId, update_site_parent.organizationId)
    self.assertEquals(p_summary.hpoId, update_site_parent.hpoId)
    self.assertEquals(ps.organizationId, update_site_parent.organizationId)
    self.assertEquals(ph.organizationId, update_site_parent.organizationId)


