import datetime
import json

from clock import FakeClock
from model.participant import Participant
from model.measurements import PhysicalMeasurements
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.physical_measurements_dao import PhysicalMeasurementsDao
from participant_enums import PhysicalMeasurementsStatus
from test_data import load_measurement_json, load_measurement_json_amendment
from unit_test_util import SqlTestBase
from werkzeug.exceptions import BadRequest

TIME_1 = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)

class PhysicalMeasurementsDaoTest(SqlTestBase):
  def setUp(self):
    super(PhysicalMeasurementsDaoTest, self).setUp()
    self.participant = Participant(participantId=1, biobankId=2)
    ParticipantDao().insert(self.participant)
    self.dao = PhysicalMeasurementsDao()
    self.measurement_json = json.dumps(load_measurement_json(self.participant.participantId,
                                                             TIME_1))

  def testInsert_noParticipantId(self):
    with self.assertRaises(BadRequest):
      self.dao.insert(PhysicalMeasurements(resource=self.measurement_json))

  def testInsert_wrongParticipantId(self):
    with self.assertRaises(BadRequest):
      self.dao.insert(PhysicalMeasurements(participantId=2, resource=self.measurement_json))

  def _with_id(self, resource, id_):
    measurements_json = json.loads(resource)
    measurements_json['id'] = id_
    return json.dumps(measurements_json)

  def testInsert_rightParticipantId(self):
    measurements_to_insert = PhysicalMeasurements(physicalMeasurementsId=1,
                                                  participantId=self.participant.participantId,
                                                  resource=self.measurement_json)
    summary = ParticipantSummaryDao().get(self.participant.participantId)
    self.assertIsNone(summary.physicalMeasurementsStatus)
    with FakeClock(TIME_2):
      measurements = self.dao.insert(measurements_to_insert)

    expected_measurements = PhysicalMeasurements(physicalMeasurementsId=1,
                                                 participantId=self.participant.participantId,
                                                 resource=self._with_id(self.measurement_json, '1'),
                                                 created=TIME_2,
                                                 final=True,
                                                 logPositionId=1)
    self.assertEquals(expected_measurements.asdict(), measurements.asdict())
    measurements = self.dao.get(measurements.physicalMeasurementsId)
    self.assertEquals(expected_measurements.asdict(), measurements.asdict())
    # Completing physical measurements changes the participant summary status
    summary = ParticipantSummaryDao().get(self.participant.participantId)
    self.assertEquals(PhysicalMeasurementsStatus.COMPLETED, summary.physicalMeasurementsStatus)

  def testInsert_amend(self):
    measurements_to_insert = PhysicalMeasurements(physicalMeasurementsId=1,
                                                  participantId=self.participant.participantId,
                                                  resource=self.measurement_json)
    with FakeClock(TIME_2):
      measurements = self.dao.insert(measurements_to_insert)

    amendment_json = load_measurement_json_amendment(self.participant.participantId,
                                                     measurements.physicalMeasurementsId,
                                                     TIME_2)
    measurements_2 = PhysicalMeasurements(physicalMeasurementsId=2,
                                          participantId=self.participant.participantId,
                                          resource=json.dumps(amendment_json))
    with FakeClock(TIME_3):
      new_measurements = self.dao.insert(measurements_2)

    measurements = self.dao.get(measurements.physicalMeasurementsId)
    amended_json = json.loads(measurements.resource)
    self.assertEquals('amended', amended_json['entry'][0]['resource']['status'])
    self.assertEquals('1', amended_json['id'])

    amendment_json = json.loads(new_measurements.resource)
    self.assertEquals('2', amendment_json['id'])
    self.assertTrue(new_measurements.final)
    self.assertEquals(TIME_3, new_measurements.created)

