import datetime

from participant_enums import GenderIdentity, QuestionnaireStatus

from model.base import Base
from model.config import Config
from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from model.biobank_sample import BiobankSample
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderSample
from model.hpo import HPO
from model.log_position import LogPosition
from model.measurements import PhysicalMeasurements
from model.metrics import MetricsVersion, MetricsBucket
from model.questionnaire import Questionnaire, QuestionnaireHistory, QuestionnaireQuestion
from model.questionnaire import QuestionnaireConcept
from model.questionnaire_response import QuestionnaireResponse, QuestionnaireAnswer

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
                        confirmedDate=datetime.datetime.now(), logPosition=LogPosition())
sample2 = BiobankSample(id=2, participantId=1, familyId='a', sampleId='b', storageStatus='c',
                        type='d', testCode='e', treatments='f', expectedVolume='g',
                        quantity='h', containerType='i', collectionDate=datetime.datetime.now(),
                        disposalStatus='j', disposedDate=datetime.datetime.now(),
                        parentSampleId=1, confirmedDate=datetime.datetime.now(), 
                        logPosition=LogPosition())
session.add(sample1)
session.add(sample2)

bo = BiobankOrder(id=1, participantId=1, created=datetime.datetime.now(), sourceSiteSystem='a',
                  sourceSiteValue='b', collected='c', processed='d', finalized='e', 
                  logPosition=LogPosition())                  
bo.identifiers.append(BiobankOrderIdentifier(system='a', value='b'))
bo.samples.append(BiobankOrderSample(test='a', description='b', processingRequired=True,
                                     collected=datetime.datetime.now(), 
                                     processed=datetime.datetime.now(),
                                     finalized=datetime.datetime.now()))

session.add(bo)

pm = PhysicalMeasurements(id=1, participantId=1, created=datetime.datetime.now(), resource='blah',
                          logPosition=LogPosition())
pm2 = PhysicalMeasurements(id=2, participantId=1, created=datetime.datetime.now(), resource='blah',
                           amendedMeasurementsId=1, logPosition=LogPosition())
session.add(pm)

q = Questionnaire(id=1, version=1, created=datetime.datetime.now(), 
                  lastModified=datetime.datetime.now(), resource='what?')
qh = QuestionnaireHistory(id=1, version=1, created=datetime.datetime.now(), 
                          lastModified=datetime.datetime.now(), resource='what?')
qh.questions.append(QuestionnaireQuestion(id=1, questionnaireId=1, questionnaireVersion=1, 
                                          linkId="1.2.3", text='What is your favorite color?', 
                                          concept_system='a', concept_code='b', 
                                          concept_display='c'))
qh.concepts.append(QuestionnaireConcept(id=1, questionnaireId=1, questionnaireVersion=1,
                                        conceptSystem='a', conceptCode='b'))                
session.add(q)
session.add(qh)
session.commit()

qr = QuestionnaireResponse(id=1, questionnaireId=1, questionnaireVersion=1, participantId=1,
                           created=datetime.datetime.now(), resource='blah')
qr.answers.append(QuestionnaireAnswer(id=1, questionnaireResponseId=1, questionId=1, 
                                      endTime=datetime.datetime.now(), valueSystem='a', 
                                      valueCode='b', valueDecimal=123, valueString='blah',
                                      valueDate=datetime.date.today()))

session.add(qr)
session.commit()

c = Config(configuration='blah')
mv = MetricsVersion(id=1, inProgress=False, complete=True, date=datetime.date.today(),
                    dataVersion=1)
session.add(mv)
session.add(c)
session.commit()

mb = MetricsBucket(metricsVersionId=1, date=datetime.date.today(),
                   hpoId='PITT', metrics='blah')
session.add(mb)
session.commit()
