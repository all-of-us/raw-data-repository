import httplib
import datetime
import main
import json

from dao.physical_measurements_dao import PhysicalMeasurementsDao
from model.measurements import Measurement
from test.unit_test.unit_test_util import FlaskTestBase
from test_data import load_measurement_json, load_measurement_json_amendment, data_path

class PhysicalMeasurementsApiTest(FlaskTestBase):

  def setUp(self):
    super(PhysicalMeasurementsApiTest, self).setUp()
    self.participant_id = self.create_participant()
    self.participant_id_2 = self.create_participant()

  def insert_measurements(self, now=None):
    measurements_1 = load_measurement_json(self.participant_id, now)
    measurements_2 = load_measurement_json(self.participant_id_2, now)
    path_1 = 'Participant/%s/PhysicalMeasurements' % self.participant_id
    path_2 = 'Participant/%s/PhysicalMeasurements' % self.participant_id_2
    self.send_post(path_1, measurements_1)
    self.send_post(path_2, measurements_2)

  def test_insert_before_consent_fails(self):
    measurements_1 = load_measurement_json(self.participant_id)
    path_1 = 'Participant/%s/PhysicalMeasurements' % self.participant_id
    self.send_post(path_1, measurements_1, expected_status=httplib.BAD_REQUEST)

  def test_insert(self):
    self.send_consent(self.participant_id)
    self.send_consent(self.participant_id_2)
    now = datetime.datetime.now()
    self.insert_measurements(now.isoformat())

    response = self.send_get('Participant/%s/PhysicalMeasurements' % self.participant_id)
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))

    physical_measurements_id = response['entry'][0]['resource']['id']
    pm_id = int(physical_measurements_id)
    physical_measurements = PhysicalMeasurementsDao().get_with_children(physical_measurements_id)
    em1 = Measurement(measurementId=pm_id * 1000,
                      physicalMeasurementsId=pm_id,
                      codeSystem="http://loinc.org",
                      codeValue="55284-4",
                      measurementTime=now,
                      bodySiteCodeSystem="http://snomed.info/sct",
                      bodySiteCodeValue="368209003")
    bp1 = Measurement(measurementId=pm_id * 1000 + 1,
                      physicalMeasurementsId=pm_id,
                      codeSystem="http://loinc.org",
                      codeValue="8480-6",
                      measurementTime=now,
                      valueDecimal=109.0,
                      valueUnit="mm[Hg]",
                      parentId=em1.measurementId)
    bp2 = Measurement(measurementId=pm_id * 1000 + 2,
                      physicalMeasurementsId=pm_id,
                      codeSystem="http://loinc.org",
                      codeValue="8462-4",
                      measurementTime=now,
                      valueDecimal=44.0,
                      valueUnit="mm[Hg]",
                      parentId=em1.measurementId)
    m1 = physical_measurements.measurements[0]
    self.assertEquals(em1.asdict(), m1.asdict())
    self.assertEquals(2, len(m1.measurements))
    self.assertEquals(bp1.asdict(), m1.measurements[0].asdict())
    self.assertEquals(bp2.asdict(), m1.measurements[1].asdict())

    response = self.send_get('Participant/%s/PhysicalMeasurements' % self.participant_id_2)
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))

  def test_insert_and_amend(self):
    self.send_consent(self.participant_id)
    measurements_1 = load_measurement_json(self.participant_id)
    path_1 = 'Participant/%s/PhysicalMeasurements' % self.participant_id
    response = self.send_post(path_1, measurements_1)
    measurements_2 = load_measurement_json_amendment(self.participant_id, response['id'])
    self.send_post(path_1, measurements_2)

    response = self.send_get('Participant/%s/PhysicalMeasurements' % self.participant_id)
    self.assertEquals(2, len(response['entry']))
    self.assertEquals("amended", response['entry'][0]['resource']['entry'][0]['resource']['status'])

  def test_insert_with_qualifiers(self):
    self.send_consent(self.participant_id)
    now = datetime.datetime.now()
    with open(data_path('physical_measurements_2.json')) as measurements_file:
      json_text = measurements_file.read() % {
        'participant_id': self.participant_id,
        'authored_time': now.isoformat()
      }
    measurements_json = json.loads(json_text)
    path_1 = 'Participant/%s/PhysicalMeasurements' % self.participant_id
    self.send_post(path_1, measurements_json)

    response = self.send_get('Participant/%s/PhysicalMeasurements' % self.participant_id)
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))

    physical_measurements_id = response['entry'][0]['resource']['id']
    pm_id = int(physical_measurements_id)
    physical_measurements = PhysicalMeasurementsDao().get_with_children(physical_measurements_id)

    em1_id = pm_id * 1000
    bp1 = Measurement(measurementId=pm_id * 1000 + 1,
                      physicalMeasurementsId=pm_id,
                      codeSystem="http://loinc.org",
                      codeValue="8480-6",
                      measurementTime=now,
                      valueDecimal=109.0,
                      valueUnit="mm[Hg]",
                      parentId=em1_id)
    bp2 = Measurement(measurementId=pm_id * 1000 + 2,
                      physicalMeasurementsId=pm_id,
                      codeSystem="http://loinc.org",
                      codeValue="8462-4",
                      measurementTime=now,
                      valueDecimal=44.0,
                      valueUnit="mm[Hg]",
                      parentId=em1_id)
    bp3 = Measurement(measurementId=pm_id * 1000 + 3,
                      physicalMeasurementsId=pm_id,
                      codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-evaluation",
                      codeValue="arm-circumference",
                      measurementTime=now,
                      valueDecimal=32.0,
                      valueUnit="cm",
                      parentId=em1_id)
    em1 = Measurement(measurementId=pm_id * 1000,
                      physicalMeasurementsId=pm_id,
                      codeSystem="http://loinc.org",
                      codeValue="55284-4",
                      measurementTime=now,
                      bodySiteCodeSystem="http://snomed.info/sct",
                      bodySiteCodeValue="368209003",
                      measurements = [bp1, bp2, bp3])

    pm_height_system = "http://terminology.pmi-ops.org/CodeSystem/protocol-modifications-height"
    q1 = Measurement(measurementId=pm_id * 1000 + 4,
                     physicalMeasurementsId=pm_id,
                     codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-evaluation",
                     codeValue="protocol-modifications-height",
                     measurementTime=now,
                     valueCodeSystem=pm_height_system,
                     valueCodeValue="hair-style")

    em2 = Measurement(measurementId=pm_id * 1000 + 5,
                      physicalMeasurementsId=pm_id,
                      codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-evaluation",
                      codeValue="pre-pregnancy-weight",
                      measurementTime=now,
                      valueDecimal=28.0,
                      valueUnit="kg")
    pm_weight_system = "http://terminology.pmi-ops.org/CodeSystem/protocol-modifications-weight"
    q2 = Measurement(measurementId=pm_id * 1000 + 6,
                     physicalMeasurementsId=pm_id,
                     codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-evaluation",
                     codeValue="protocol-modifications-weight",
                     measurementTime=now,
                     valueCodeSystem=pm_weight_system,
                     valueCodeValue="other")
    em3 = Measurement(measurementId=pm_id * 1000 + 7,
                      physicalMeasurementsId=pm_id,
                      codeSystem="http://loinc.org",
                      codeValue="39156-5",
                      measurementTime=now,
                      valueDecimal=24.2,
                      valueUnit="kg/m2")
    # Skip a bunch -- could add these later
    em4 = Measurement(measurementId=pm_id * 1000 + 14,
                      physicalMeasurementsId=pm_id,
                      codeSystem="http://loinc.org",
                      codeValue="8302-2",
                      measurementTime=now,
                      valueDecimal=111.5,
                      valueUnit="cm",
                      qualifiers=[q1])
    em5 = Measurement(measurementId=pm_id * 1000 + 15,
                      physicalMeasurementsId=pm_id,
                      codeSystem="http://loinc.org",
                      codeValue="29463-7",
                      measurementTime=now,
                      valueDecimal=30.1,
                      valueUnit="kg",
                      qualifiers=[q2])
    m = {measurement.measurementId:measurement.asdict(follow = {'measurements': {},
                                                                'qualifiers': {}}) for measurement
         in physical_measurements.measurements}

    for em in [em1, bp1, bp2, bp3, q1, em2, q2, em3, em4, em5]:
      self.assertEquals(em.asdict(follow = {'measurements': {},
                                            'qualifiers': {}}), m.get(em.measurementId))


  def test_physical_measurements_sync(self):
    self.send_consent(self.participant_id)
    self.send_consent(self.participant_id_2)
    sync_response = self.send_get('PhysicalMeasurements/_history')
    self.assertEquals('Bundle', sync_response['resourceType'])
    self.assertEquals('history', sync_response['type'])
    link = sync_response.get('link')
    self.assertIsNone(link)
    self.assertTrue(len(sync_response['entry']) == 0)

    self.insert_measurements()

    sync_response = self.send_get('PhysicalMeasurements/_history?_count=1')
    self.assertTrue(sync_response.get('entry'))
    link = sync_response.get('link')
    self.assertTrue(link)
    self.assertEquals("next", link[0]['relation'])
    self.assertEquals(1, len(sync_response['entry']))
    prefix_index = link[0]['url'].index(main.PREFIX)
    relative_url = link[0]['url'][prefix_index + len(main.PREFIX):]

    sync_response_2 = self.send_get(relative_url)
    self.assertEquals(1, len(sync_response_2['entry']))
    self.assertNotEquals(sync_response['entry'][0], sync_response_2['entry'][0])
