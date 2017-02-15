import datetime
import test_data

from dao.participant_dao import ParticipantDao, ParticipantHistoryDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from participant_enums import UNSET_HPO_ID, UNMAPPED_HPO_ID
from unit_test_util import SqlTestBase, PITT_HPO_ID
from clock import FakeClock

class ParticipantDaoTest(SqlTestBase):
  def setUp(self):
    super(ParticipantDaoTest, self).setUp()
    self.setup_data()    
  
  def test_insert_and_update(self):            
    dao = ParticipantDao()
    participant_summary_dao = ParticipantSummaryDao()
    participant_history_dao = ParticipantHistoryDao()
    
    self.assertFalse(dao.get(1))
    self.assertFalse(participant_summary_dao.get(1))
    self.assertFalse(participant_history_dao.get([1, 1]))
    
    p = Participant(participantId=1, version=1, biobankId=2)    
    time = datetime.datetime(2016, 1, 1)
    with FakeClock(time):
      dao.insert(p)
    expected_participant = Participant(participantId=1, version=1, biobankId=2, lastModified=time, 
                                       signUpTime=time, hpoId=UNSET_HPO_ID)      
    self.assertEquals(expected_participant.asdict(), p.asdict())
    
    p2 = dao.get(1)              
    self.assertEquals(p.asdict(), p2.asdict())
    
    # Creating a participant also creates a ParticipantSummary and a ParticipantHistory row
    ps = participant_summary_dao.get(1)
    expected_ps = ParticipantSummary(participantId=1, biobankId=2,  
                                     signUpTime=time, hpoId=UNSET_HPO_ID,
                                     numBaselineSamplesArrived=0, numCompletedBaselinePPIModules=0)
    self.assertEquals(expected_ps.asdict(), ps.asdict())                                   
    ph = participant_history_dao.get([1, 1])
    expected_ph = ParticipantHistory(participantId=1, version=1, biobankId=2, lastModified=time, 
                                     signUpTime=time, hpoId=UNSET_HPO_ID)
    self.assertEquals(expected_ph.asdict(), ph.asdict())
    
    p2.providerLink = test_data.primary_provider_link('PITT') 
    time2 = datetime.datetime(2016, 1, 2)
    with FakeClock(time2):
      dao.update(p2)        
    
    # lastModified, hpoId, version is updated on p2 after being passed in
    p3 = dao.get(1);
    expected_participant = Participant(participantId=1, version=2, biobankId=2, lastModified=time2, 
                                       signUpTime=time, hpoId=PITT_HPO_ID,
                                       providerLink=p2.providerLink)
    self.assertEquals(expected_participant.asdict(), p3.asdict())
    self.assertEquals(p2.asdict(), p3.asdict())
    
    # Updating the participant provider link also updates the HPO ID on the participant summary.
    ps2 = participant_summary_dao.get(1)
    expected_ps = ParticipantSummary(participantId=1, biobankId=2,  
                                     signUpTime=time, hpoId=PITT_HPO_ID,
                                     numBaselineSamplesArrived=0, numCompletedBaselinePPIModules=0)
    self.assertEquals(expected_ps.asdict(), ps2.asdict())
    # And updating the participant adds a new ParticipantHistory row.
    ph2 = participant_history_dao.get([1, 1])
    self.assertEquals(expected_ph.asdict(), ph2.asdict())
    ph3 = participant_history_dao.get([1, 2])
    expected_ph3 = ParticipantHistory(participantId=1, version=2, biobankId=2, lastModified=time2, 
                                      signUpTime=time, hpoId=PITT_HPO_ID,
                                      providerLink=p2.providerLink)
    self.assertEquals(expected_ph3.asdict(), ph3.asdict())
    
    p2.providerLink = test_data.primary_provider_link('FOO')
    dao.update(p2)
    self.assertEquals(UNMAPPED_HPO_ID, p2.hpoId)
    ps3 = participant_summary_dao.get(1)
    self.assertEquals(UNMAPPED_HPO_ID, ps3.hpoId)
    
    
    
        
    
    
    
    