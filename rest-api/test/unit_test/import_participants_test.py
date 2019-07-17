import csv
import os
import unittest
from app_util import ObjectView
from client import HttpException, Client
from dao.site_dao import SiteDao
from dao.participant_dao import ParticipantDao
from tools.import_participants import import_participant, setup_participants
from unit_test_util import FlaskTestBase, CloudStorageSqlTestBase
from tools.import_organizations import SiteImporter
from main_util import get_parser
from unit_test_util import generic_test_object, run_deferred_tasks

#_DEFAULT_INSTANCE = 'http://localhost:8080'
PARTICIPANT_FILE = 'test/test-data/healthpro_stable_participants.csv'
SITE_IMPORT_FILE = 'data/sites.csv'


class ParticipantImportTest(FlaskTestBase):
  def setUp(self):
    super(ParticipantImportTest, self).setUp()
#    self.instance = 'localhost'
#    self.creds_file = None
#    self.client = Client(parse_cli=False, default_instance=self.instance, creds_file=self.creds_file)
    self.site_dao = SiteDao()
    self.participant_dao = ParticipantDao()

    self.row = {'first_name': 'Participant 5', 'last_name': 'BannerTucson', 'gender_identity': 'GenderIdentity_Woman',
                'hpo_siteid': 'AZ_TUCSON', 'date_of_birth': '1980-11-01', 'email': '', 'zip_code': '20001',
                'withdrawalStatus': 'NOT_WITHDRAWN', 'suspensionStatus': 'NOT_SUSPENDED'}
    self.cqiv = (u'1', u'1')
    self.qtq = {(u'2', u'1'): {u'Gender_GenderIdentity': u'5708'}, (u'3', u'1'): {},
                (u'1', u'1'): {u'PIIName_First': u'9459', u'PIIName_Last': u'9461',
                               u'ConsentPII_EmailAddress': u'10783', u'PIIBirthInformation_BirthDate': u'9469',
                               u'StreetAddress_PIIZIP': u'9837'}, (u'4', u'1'): {}, (u'5', u'1'): {}}
    self.consent = {u'PIIName_First': u'9459', u'PIIName_Last': u'9461', u'ConsentPII_EmailAddress': u'10783',
                    u'PIIBirthInformation_BirthDate': u'9469', u'StreetAddress_PIIZIP': u'9837'}

  def test_import_from_file(self):
    args = generic_test_object(stub_geocoding=False, 
        instance=None,
        creds_file=None,
        project=self._app)

    with open(SITE_IMPORT_FILE, 'r') as csvfile:
      reader = csv.DictReader(csvfile)
      reader.next()
      row = reader.next()
      obj_row = ObjectView(row)
      obj_row.googleGroup = None
      setattr(obj_row, 'googleGroup', "".join(row['Site ID / Google Group'].split('-')))

      site_importer = SiteImporter(args)
      site_importer._insert_new_participants([obj_row], self._app)
    
    participants = self.participant_dao.get_all()
    print participants

