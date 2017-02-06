import datetime

from participant_enums import GenderIdentity, QuestionnaireStatus

from model.base import Base
from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from model.biobank_sample import BiobankSample
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderSample
from model.hpo import HPO
from model.measurements import PhysicalMeasurements
from model.questionnaire import Questionnaire, QuestionnaireHistory

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///:memory:', echo=True)
bar = Participant
Base.metadata.create_all(engine)

engine.execute('PRAGMA foreign_keys = ON;')
Session = sessionmaker(bind=engine)
session = Session()

hpo = HPO(id=1, name='UNSET')
session.add(hpo)
session.commit()

p = Participant(id=1, version=1, biobankId=2, hpoId=1)
session.add(p)
ph = ParticipantHistory(id=1, version=1, biobankId=2, hpoId=1)
session.add(ph)
session.commit()

ps = ParticipantSummary(id=1, biobankId=2, firstName='Bob', middleName='Q', lastName='Jones',
                        zipCode='78751', dateOfBirth=datetime.date.today(), 
                        genderIdentity=GenderIdentity.MALE, hpoId=1,
                        consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED, 
                        consentForStudyEnrollmentTime=datetime.datetime.now(),
                        numCompletedBaselinePPIModules=1,
                        numBaselineSamplesArrived=2)                      
session.add(ps)

sample1 = BiobankSample(id=1, participantId=1, familyId='a', sampleId='b', storageStatus='c',
                        type='d', testCode='e', treatments='f', expectedVolume='g',
                        quantity='h', containerType='i', collectionDate=datetime.datetime.now(),
                        disposalStatus='j', disposedDate=datetime.datetime.now(),
                        confirmedDate=datetime.datetime.now())
sample2 = BiobankSample(id=2, participantId=1, familyId='a', sampleId='b', storageStatus='c',
                        type='d', testCode='e', treatments='f', expectedVolume='g',
                        quantity='h', containerType='i', collectionDate=datetime.datetime.now(),
                        disposalStatus='j', disposedDate=datetime.datetime.now(),
                        parentSampleId=1, confirmedDate=datetime.datetime.now())
session.add(sample1)
session.add(sample2)

bo = BiobankOrder(id=1, participantId=1, created=datetime.datetime.now(), sourceSiteSystem='a',
                  sourceSiteValue='b', collected='c', processed='d', finalized='e')                  
bo.identifiers.append(BiobankOrderIdentifier(system='a', value='b'))
bo.samples.append(BiobankOrderSample(test='a', description='b', processingRequired=True,
                                     collected=datetime.datetime.now(), processed=datetime.datetime.now(),
                                     finalized=datetime.datetime.now()))

session.add(bo)

pm = PhysicalMeasurements(id=1, participantId=1, created=datetime.datetime.now(), resource='blah')
pm2 = PhysicalMeasurements(id=2, participantId=1, created=datetime.datetime.now(), resource='blah',
                           amendedMeasurementsId=1)
session.add(pm)

q = Questionnaire(id=1, version=1, created=datetime.datetime.now(), 
                  lastModified=datetime.datetime.now(), resource='what?')
qh = QuestionnaireHistory(id=1, version=1, created=datetime.datetime.now(), 
                          lastModified=datetime.datetime.now(), resource='what?')
session.add(q)
session.add(qh)

session.commit()
