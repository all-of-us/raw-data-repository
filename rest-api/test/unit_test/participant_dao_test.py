import datetime

from dao.participant_dao import ParticipantDao
from model.participant import Participant
from unit_test_util import SqlTestBase
from clock import FakeClock

class ParticipantDaoTest(SqlTestBase):
  def setUp(self):
    super(ParticipantDaoTest, self).setUp()
    self.setup_data()
    self.dao = ParticipantDao()
  
  def test_insert(self):        
    p = Participant(participantId=1, version=1, biobankId=2)    
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(time):
      self.dao.insert(p)      
    self.assertEquals(1, p.participantId)
    self.assertEquals(time, p.lastModified)    
    self.assertEquals(time, p.signUpTime)
    self.assertEquals(2, p.biobankId)
    self.assertEquals(0, p.hpoId)
    
    p2 = self.dao.get(1)              
    self.assertObjEquals(p2, p)
    
    p2.providerLink = test_data.primary_provider_link('PITT') 
    time2 = datetime.datetime(2016, 1, 2)
    with FakeClock(time2):
      self.dao.update(p2)

    self.assertEquals(time, p2.lastModified)        
    
    # lastModified is updated on the updated object
    p3 = self.dao.get(1);
    self.assertEquals(time2, p3.lastModified)
    self.assertEquals(PITT_HPO_ID, p3.hpoId)
    self.assertEquals(time, p3.signUpTime)
    self.assertObjEqualsExceptLastModified(p2, p3)
        
    
    
    
    