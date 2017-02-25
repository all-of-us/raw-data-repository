import datetime

from participant_enums import GenderIdentity, QuestionnaireStatus

from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from model.biobank_stored_sample import BiobankStoredSample
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.code import Code, CodeType, CodeBook, CodeVersion
from model.hpo import HPO
from model.log_position import LogPosition
from model.measurements import PhysicalMeasurements
from model.metrics import MetricsVersion, MetricsBucket
from model.questionnaire import Questionnaire, QuestionnaireHistory, QuestionnaireQuestion
from model.questionnaire import QuestionnaireConcept
from model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from unit_test_util import SqlTestBase

class DatabaseTest(SqlTestBase):
  def setUp(self):
    super(DatabaseTest, self).setUp(with_data=False)

  def test_schema(self):
    session = self.get_database().make_session()

    hpo = HPO(hpoId=1, name='UNSET')
    code_book = CodeBook(codeBookId=1, created=datetime.datetime.now())
    session.add(hpo)
    session.add(code_book)
    session.commit()

    code1 = Code(codeId=1, codeBookId=1, system="a", value="b", display="c", topic="d",
                 type=CodeType.MODULE)
    code2 = Code(codeId=2, codeBookId=1, parentId=1, system="a", value="c", display="X", topic="d",
                 type=CodeType.QUESTION)
    code3 = Code(codeId=3, codeBookId=1, parentId=2, system="a", value="d", display="Y", topic="d",
                 type=CodeType.ANSWER)
    codeVersion1 = CodeVersion(codeId=1, codeBookId=1, system="a", value="b", display="c",
                               topic="d", type=CodeType.MODULE)
    codeVersion2 = CodeVersion(codeId=2, codeBookId=1, parentId=1, system="a", value="c", display="X", topic="d",
                               type=CodeType.QUESTION)
    codeVersion3 = CodeVersion(codeId=3, codeBookId=1, parentId=2, system="a", value="d", display="Y", topic="d",
                               type=CodeType.ANSWER)
    session.add(code1)
    session.add(code2)
    session.add(code3)
    session.add(codeVersion1)
    session.add(codeVersion2)
    session.add(codeVersion3)
    session.commit()

    session.commit()

    p = Participant(participantId=1, version=1, biobankId=2, hpoId=1, 
                    signUpTime=datetime.datetime.now(), lastModified=datetime.datetime.now())
    ps = ParticipantSummary(participantId=1, biobankId=2, firstName='Bob', middleName='Q', 
                            lastName='Jones', zipCode='78751', dateOfBirth=datetime.date.today(), 
                            genderIdentityId=1, hpoId=1,
                            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED, 
                            consentForStudyEnrollmentTime=datetime.datetime.now(),
                            numCompletedBaselinePPIModules=1,
                            numBaselineSamplesArrived=2)     
    p.participantSummary = ps
    session.add(p)
    ph = ParticipantHistory(participantId=1, version=1, biobankId=2, hpoId=1, 
                            signUpTime=datetime.datetime.now(), 
                            lastModified=datetime.datetime.now())
    session.add(ph)
    session.commit()

    sample1 = BiobankStoredSample(biobankStoredSampleId=1, participantId=1, familyId='a', 
                                  sampleId='b', storageStatus='c', type='d', testCode='e', 
                                  treatments='f', expectedVolume='g', quantity='h', 
                                  containerType='i', collectionDate=datetime.datetime.now(),
                                  disposalStatus='j', disposedDate=datetime.datetime.now(),
                                  confirmedDate=datetime.datetime.now(), logPosition=LogPosition())
    sample2 = BiobankStoredSample(biobankStoredSampleId=2, participantId=1, familyId='a', 
                                  sampleId='b', storageStatus='c', type='d', testCode='e', 
                                  treatments='f', expectedVolume='g', quantity='h', 
                                  containerType='i', collectionDate=datetime.datetime.now(),
                                  disposalStatus='j', disposedDate=datetime.datetime.now(),
                                  parentSampleId=1, confirmedDate=datetime.datetime.now(), 
                                  logPosition=LogPosition())
    session.add(sample1)
    session.add(sample2)

    bo = BiobankOrder(biobankOrderId=1, participantId=1, created=datetime.datetime.now(), 
                      sourceSiteSystem='a', sourceSiteValue='b', collected='c', processed='d', 
                      finalized='e', logPosition=LogPosition())                  
    bo.identifiers.append(BiobankOrderIdentifier(system='a', value='b'))
    bo.samples.append(BiobankOrderedSample(test='a', description='b', processingRequired=True,
                                           collected=datetime.datetime.now(), 
                                           processed=datetime.datetime.now(),
                                           finalized=datetime.datetime.now()))
    session.add(bo)

    pm = PhysicalMeasurements(physicalMeasurementsId=1, participantId=1, 
                              created=datetime.datetime.now(), resource='blah',
                              final=False, logPosition=LogPosition())
    pm2 = PhysicalMeasurements(physicalMeasurementsId=2, participantId=1, 
                               created=datetime.datetime.now(), resource='blah',
                               final=True, amendedMeasurementsId=1, logPosition=LogPosition())
    session.add(pm)
    session.add(pm2)

    q = Questionnaire(questionnaireId=1, version=1, created=datetime.datetime.now(), 
                      lastModified=datetime.datetime.now(), resource='what?')
    qh = QuestionnaireHistory(questionnaireId=1, version=1, created=datetime.datetime.now(), 
                              lastModified=datetime.datetime.now(), resource='what?')
    qh.questions.append(QuestionnaireQuestion(questionnaireQuestionId=1, questionnaireId=1, 
                                              questionnaireVersion=1, 
                                              linkId="1.2.3", codeId=2))
    qh.concepts.append(QuestionnaireConcept(questionnaireConceptId=1, questionnaireId=1, 
                                            questionnaireVersion=1,
                                            codeId=1))
    session.add(q)
    session.add(qh)
    session.commit()

    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1, 
                               participantId=1, created=datetime.datetime.now(), resource='blah')
    qr.answers.append(QuestionnaireResponseAnswer(questionnaireResponseAnswerId=1, 
                                                  questionnaireResponseId=1, questionId=1, 
                                                  endTime=datetime.datetime.now(), valueSystem='a', 
                                                  valueCodeId=3, valueDecimal=123, valueString='blah',
                                                  valueDate=datetime.date.today()))

    session.add(qr)
    session.commit()

    mv = MetricsVersion(metricsVersionId=1, inProgress=False, complete=True, 
                        date=datetime.date.today(), dataVersion=1)
    session.add(mv)
    session.commit()

    mb = MetricsBucket(metricsVersionId=1, date=datetime.date.today(),
                       hpoId='PITT', metrics='blah')
    session.add(mb)
    session.commit()
