import datetime
import isodate

from participant_enums import QuestionnaireStatus

from dateutil.tz import tzutc
from model.biobank_stored_sample import BiobankStoredSample
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.code import Code, CodeType, CodeBook, CodeHistory
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
    session = self.database.make_session()

    hpo = HPO(hpoId=1, name='UNSET')
    code_book = CodeBook(codeBookId=1, created=datetime.datetime.now(), latest=True, name="pmi",
                         system="http://foo/bar", version="v0.1.1")
    session.add(hpo)
    session.add(code_book)
    session.commit()

    code1 = Code(codeId=1, codeBookId=1, system="a", value="b", display=u"c", topic=u"d",
                 codeType=CodeType.MODULE, mapped=True, created=datetime.datetime.now())
    codeHistory1 = CodeHistory(codeId=1, codeBookId=1, system="a", value="b", display=u"c",
                               topic=u"d", codeType=CodeType.MODULE, mapped=True,
                               created=datetime.datetime.now())
    session.add(code1)
    session.add(codeHistory1)
    session.commit()

    code2 = Code(codeId=2, codeBookId=1, parentId=1, system="a", value="c", display=u"X", topic=u"d",
                 codeType=CodeType.QUESTION, mapped=True, created=datetime.datetime.now())
    codeHistory2 = CodeHistory(codeId=2, codeBookId=1, parentId=1, system="a", value="c",
                               display=u"X", topic=u"d",
                               codeType=CodeType.QUESTION, mapped=True,
                               created=datetime.datetime.now())
    session.add(code2)
    session.add(codeHistory2)
    session.commit()

    code3 = Code(codeId=3, codeBookId=1, parentId=2, system="a", value="d", display=u"Y",
                 topic=u"d", codeType=CodeType.ANSWER, mapped=False,
                 created=datetime.datetime.now())
    codeHistory3 = CodeHistory(codeId=3, codeBookId=1, parentId=2, system="a", value="d",
                               display=u"Y", topic=u"d",
                               codeType=CodeType.ANSWER, mapped=False,
                               created=datetime.datetime.now())
    session.add(code3)
    session.add(codeHistory3)
    session.commit()

    session.commit()

    p = self._participant_with_defaults(
        participantId=1, version=1, biobankId=2, clientId='fake@client.id', hpoId=hpo.hpoId,
        signUpTime=datetime.datetime.now(), lastModified=datetime.datetime.now())
    ps = self._participant_summary_with_defaults(
        participantId=1,
        biobankId=2,
        hpoId=hpo.hpoId,
        firstName=self.fake.first_name(),
        middleName=self.fake.first_name(),
        lastName=self.fake.last_name(),
        email=self.fake.email(),
        zipCode='78751',
        dateOfBirth=datetime.date.today(),
        genderIdentityId=1,
        consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
        consentForStudyEnrollmentTime=datetime.datetime.now(),
        numBaselineSamplesArrived=2)
    p.participantSummary = ps
    session.add(p)
    ph = self._participant_history_with_defaults(
        participantId=1, biobankId=2, clientId='fake@client.id', hpoId=hpo.hpoId,
        signUpTime=datetime.datetime.now(), lastModified=datetime.datetime.now())
    session.add(ph)
    session.commit()

    session.add(BiobankStoredSample(
        biobankStoredSampleId='WEB1234542',
        biobankId=p.biobankId,
        test='1UR10',
        confirmed=datetime.datetime.utcnow()))
    session.add(BiobankStoredSample(
        biobankStoredSampleId='WEB99999',  # Sample ID must be unique.
        biobankId=p.biobankId,  # Participant ID and test may be duplicated.
        test='1UR10',
        confirmed=datetime.datetime.utcnow()))

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
                                              linkId="1.2.3", codeId=2, repeats=True))
    qh.concepts.append(QuestionnaireConcept(questionnaireConceptId=1, questionnaireId=1,
                                            questionnaireVersion=1,
                                            codeId=1))
    session.add(q)
    session.add(qh)
    session.commit()

    qr = QuestionnaireResponse(questionnaireResponseId=1, questionnaireId=1, questionnaireVersion=1,
                               participantId=1, created=datetime.datetime.now(), resource='blah')
    qr.answers.append(QuestionnaireResponseAnswer(
        questionnaireResponseAnswerId=1,
        questionnaireResponseId=1,
        questionId=1,
        endTime=datetime.datetime.now(),
        valueSystem='a',
        valueCodeId=3,
        valueDecimal=123,
        valueString=self.fake.first_name(),
        valueDate=datetime.date.today()))

    session.add(qr)
    session.commit()

    mv = MetricsVersion(metricsVersionId=1, inProgress=False, complete=True,
                        date=datetime.datetime.utcnow(), dataVersion=1)
    session.add(mv)
    session.commit()

    mb = MetricsBucket(metricsVersionId=1, date=datetime.date.today(),
                       hpoId='PITT', metrics='blah')
    session.add(mb)
    session.commit()

  def _create_participant(self, session):
    hpo = HPO(hpoId=1, name='UNSET')
    session.add(hpo)
    session.commit()
    p = self._participant_with_defaults(
        participantId=1, version=1, biobankId=2, hpoId=hpo.hpoId,
        signUpTime=datetime.datetime.utcnow(), lastModified=datetime.datetime.utcnow(),
        clientId='c')
    session.add(p)
    session.commit()
    return p

  def test_schema_biobank_order_and_datetime_roundtrip(self):
    bo_id = 1
    now = isodate.parse_datetime('2016-01-04T10:28:50-04:00')

    write_session = self.database.make_session()
    p = self._create_participant(write_session)

    bo = BiobankOrder(
        biobankOrderId=bo_id,
        participantId=p.participantId,
        created=now,
        sourceSiteSystem='a',
        sourceSiteValue='b',
        collectedNote=r'written by ' + self.fake.last_name(),
        processedNote=u'd',
        finalizedNote=u'e',
        logPosition=LogPosition())
    bo.identifiers.append(BiobankOrderIdentifier(system='a', value='b'))
    bo.samples.append(BiobankOrderedSample(
        test='a',
        description=u'a test invented by ' + self.fake.first_name(),
        processingRequired=True,
        collected=now,
        processed=now,
        finalized=now))
    write_session.add(bo)
    write_session.commit()

    read_session = self.database.make_session()
    bo = read_session.query(BiobankOrder).get(bo_id)
    self.assertEquals(bo.created.isoformat(),
                      now.astimezone(tzutc()).replace(tzinfo=None).isoformat())
