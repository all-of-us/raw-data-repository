import clock
import json
import logging

from concepts import Concept
import fhirclient.models.observation
from fhirclient.models.fhirabstractbase import FHIRValidationError
from sqlalchemy.orm import subqueryload
from dao.base_dao import BaseDao
from dao.participant_dao import ParticipantDao, raise_if_withdrawn
from dao.participant_summary_dao import ParticipantSummaryDao
from model.log_position import LogPosition
from model.measurements import PhysicalMeasurements, Measurement
from participant_enums import PhysicalMeasurementsStatus
from werkzeug.exceptions import BadRequest

_AMENDMENT_URL = 'http://terminology.pmi-ops.org/StructureDefinition/amends'
_OBSERVATION_RESOURCE_TYPE = 'Observation'
_QUALIFIED_BY_RELATED_TYPE = 'qualified-by'

class PhysicalMeasurementsDao(BaseDao):

  def __init__(self):
    super(PhysicalMeasurementsDao, self).__init__(PhysicalMeasurements,
                                                  order_by_ending=['logPositionId'])

  def get_id(self, obj):
    return obj.physicalMeasurementsId

  def get_with_session(self, session, obj_id, **kwargs):
    result = super(PhysicalMeasurementsDao, self).get_with_session(session, obj_id, **kwargs)
    if result:
      ParticipantDao().validate_participant_reference(session, result)
    return result

  def get_with_children(self, physical_measurements_id):
    with self.session() as session:
      query = session.query(PhysicalMeasurements) \
          .options(subqueryload(PhysicalMeasurements.measurements).subqueryload(
              Measurement.measurements)) \
          .options(subqueryload(PhysicalMeasurements.measurements).subqueryload(
              Measurement.qualifiers))
      return query.get(physical_measurements_id)

  @staticmethod
  def handle_measurement(measurement_map, measurement):
    """Populating measurement_map with information extracted from measurement and its 
    descendants."""
    code_concept = Concept(measurement.codeSystem, measurement.codeValue)
    measurement_data = measurement_map.get(code_concept)
    if not measurement_data:
      measurement_data = {'bodySites': set(), 'types': set(), 'units': set(),
                          'codes': set(), 'submeasurements': set()}
      measurement_map[code_concept] = measurement_data
    if measurement.bodySiteCodeSystem:
      measurement_data['bodySites'].add(Concept(measurement.bodySiteCodeSystem,
                                         measurement.bodySiteCodeValue))
    if measurement.valueString:
      measurement_data['types'].add('string')
    if measurement.valueDecimal:
      measurement_data['types'].add('decimal')
      min_decimal = measurement_data.get('min')
      max_decimal = measurement_data.get('max')
      if min_decimal is None or min_decimal > measurement.valueDecimal:
        measurement_data['min'] = measurement.valueDecimal
      if max_decimal is None or max_decimal < measurement.valueDecimal:
        measurement_data['max'] = measurement.valueDecimal
    if measurement.valueUnit:
      measurement_data['units'].add(measurement.valueUnit)
    if measurement.valueCodeSystem:
      measurement_data['codes'].add(Concept(measurement.valueCodeSystem,
                                     measurement.valueCodeValue))
    if measurement.valueDateTime:
      measurement_data['types'].add('date')
    for sub_measurement in measurement.measurements:
      measurement_data['submeasurements'].add(Concept(sub_measurement.codeSystem,
                                               sub_measurement.codeValue))
      PhysicalMeasurementsDao.handle_measurement(measurement_map, sub_measurement)

  def get_distinct_measurements(self):
    with self.session() as session:
      measurement_map = {}
      for pms in session.query(PhysicalMeasurements).yield_per(100):
        try:
          parsed_pms = PhysicalMeasurementsDao.from_client_json(json.loads(pms.resource),
                                                                pms.participantId)
          for measurement in parsed_pms.measurements:
            PhysicalMeasurementsDao.handle_measurement(measurement_map, measurement)
        except FHIRValidationError as e:
          logging.error("Could not parse measurements as FHIR: %s; exception = %s" % (pms.resource,
                                                                                      e))
      return measurement_map

  def _initialize_query(self, session, query_def):
    participant_id = None
    for field_filter in query_def.field_filters:
      if field_filter.field_name == 'participantId':
        participant_id = field_filter.value
        break
    # Sync queries don't specify a participant ID, and can return measurements for participants
    # who have subsequently withdrawn; for all requests that do specify a participant ID,
    # make sure the participant exists and is not withdrawn.
    if participant_id:
      ParticipantDao().validate_participant_id(session, participant_id)
    return super(PhysicalMeasurementsDao, self)._initialize_query(session, query_def)

  def _measurements_as_dict(self, measurements):
    result = measurements.asdict()
    del result['physicalMeasurementsId']
    del result['created']
    del result['logPositionId']
    result['resource'] = json.loads(result['resource'])
    if result['resource'].get('id'):
      del result['resource']['id']
    return result

  def insert_with_session(self, session, obj):
    if obj.logPosition is not None:
      raise BadRequest('%s.logPosition must be auto-generated.' % self.model_type.__name__)
    obj.logPosition = LogPosition()
    obj.final = True
    obj.created = clock.CLOCK.now()
    resource_json = json.loads(obj.resource)
    for extension in resource_json['entry'][0]['resource'].get('extension', []):
      url = extension.get('url', '')
      if url != _AMENDMENT_URL:
        logging.info('Ignoring unsupported extension for PhysicalMeasurements: %r.' % url)
        continue
      self._update_amended(obj, extension, url, session)
      break
    self._update_participant_summary(session, obj.created, obj.participantId)
    existing_measurements = (session.query(PhysicalMeasurements)
                             .filter(PhysicalMeasurements.participantId == obj.participantId)
                             .all())
    if existing_measurements:
      new_dict = self._measurements_as_dict(obj)
      for measurements in existing_measurements:
        if self._measurements_as_dict(measurements) == new_dict:
          # If there are already measurements that look exactly like this, return them
          # without inserting new measurements.
          return measurements

    measurement_count = 0
    for measurement in obj.measurements:
      measurement.physicalMeasurementsId = obj.physicalMeasurementsId
      measurement.measurementId = PhysicalMeasurementsDao.make_measurement_id(
          obj.physicalMeasurementsId, measurement_count)
      measurement_count += 1
      for sub_measurement in measurement.measurements:
        sub_measurement.physicalMeasurementsId = obj.physicalMeasurementsId
        sub_measurement.measurementId = PhysicalMeasurementsDao.make_measurement_id(
          obj.physicalMeasurementsId, measurement_count)
        measurement_count += 1

    super(PhysicalMeasurementsDao, self).insert_with_session(session, obj)
    # Flush to assign an ID to the measurements, as the client doesn't provide one.
    session.flush()
    # Update the resource to contain the ID.
    resource_json['id'] = str(obj.physicalMeasurementsId)
    obj.resource = json.dumps(resource_json)
    return obj

  def _update_participant_summary(self, session, created, participant_id):
    if participant_id is None:
      raise BadRequest('participantId is required')
    participant_summary_dao = ParticipantSummaryDao()
    participant = ParticipantDao().get_for_update(session, participant_id)
    if not participant:
      raise BadRequest("Can't submit physical measurements for unknown participant %s"
                       % participant_id)
    participant_summary = participant.participantSummary
    if not participant_summary:
      raise BadRequest("Can't submit physical measurements for participant %s without consent" %
                       participant_id)
    raise_if_withdrawn(participant_summary)
    if (not participant_summary.physicalMeasurementsStatus or
        participant_summary.physicalMeasurementsStatus == PhysicalMeasurementsStatus.UNSET):
      participant_summary.physicalMeasurementsStatus = PhysicalMeasurementsStatus.COMPLETED
      if not participant_summary.physicalMeasurementsTime:
        participant_summary.physicalMeasurementsTime = created
      participant_summary_dao.update_enrollment_status(participant_summary)
      session.merge(participant_summary)

  def insert(self, obj):
    if obj.physicalMeasurementsId:
      return super(PhysicalMeasurementsDao, self).insert(obj)
    return self._insert_with_random_id(obj, ['physicalMeasurementsId'])

  def _update_amended(self, obj, extension, url, session):
    """Finds the measurements that are being amended; sets the resource status to 'amended',
    the 'final' flag to False, and sets the new measurements' amendedMeasurementsId field to
    its ID."""
    value_ref = extension.get('valueReference')
    if value_ref is None:
      raise BadRequest('No valueReference in extension %r.' % url)
    ref = value_ref.get('reference')
    if ref is None:
      raise BadRequest('No reference in extension %r.' % url)
    type_name, ref_id = ref.split('/')
    if type_name != 'PhysicalMeasurements':
      raise BadRequest('Bad reference type in extension %r: %r.' % (url, ref))

    try:
      amended_measurement_id = int(ref_id)
    except ValueError:
      raise BadRequest('Invalid ref id: %r' % ref_id)

    amended_measurement = self.get_with_session(session, amended_measurement_id)
    if amended_measurement is None:
      raise BadRequest('Amendment references unknown PhysicalMeasurement %r.' % ref_id)
    amended_resource_json = json.loads(amended_measurement.resource)
    amended_resource = amended_resource_json['entry'][0]['resource']
    amended_resource['status'] = 'amended'
    amended_measurement.final = False
    amended_measurement.resource = json.dumps(amended_resource_json)
    session.merge(amended_measurement)
    obj.amendedMeasurementsId = amended_measurement_id

  @staticmethod
  def make_measurement_id(physical_measurements_id, measurement_count):
    # To generate unique IDs for measurements that are randomly distributed for different
    # participants (without having to randomly insert and check for the existence of IDs for each
    # measurement row), we multiply the parent physical measurements ID (nine digits) by 1000 and
    # add the measurement count within physical_measurements. This must not reach 1000 to avoid
    # collisions; log an error if we start getting anywhere close. (We don't expect to.)
    assert measurement_count < 1000
    if measurement_count == 900:
      logging.error("measurement_count > 900; nearing limit of 1000.")
    return (physical_measurements_id * 1000) + measurement_count

  @staticmethod
  def from_component(observation, component):
    if not component.code or not component.code.coding:
      logging.warning('Skipping component without coding: %s' % component.as_json())
      return None
    value_string = None
    value_decimal = None
    value_unit = None
    value_code_system = None
    value_code_value = None
    value_date_time = None
    if component.valueQuantity:
      value_decimal = component.valueQuantity.value
      value_unit = component.valueQuantity.code
    if component.valueDateTime:
      value_date_time = component.valueDateTime.date
    if component.valueString:
      value_string = component.valueString
    if component.valueCodeableConcept and component.valueCodeableConcept.coding:
      # TODO: use codebook codes for PMI codes?
      value_code_system = component.valueCodeableConcept.coding[0].system
      value_code_value = component.valueCodeableConcept.coding[0].code
    return Measurement(codeSystem=component.code.coding[0].system,
                       codeValue=component.code.coding[0].code,
                       measurementTime=observation.effectiveDateTime.date,
                       valueString=value_string,
                       valueDecimal=value_decimal,
                       valueUnit=value_unit,
                       valueCodeSystem=value_code_system,
                       valueCodeValue=value_code_value,
                       valueDateTime=value_date_time)
  @staticmethod
  def from_observation(observation, full_url, qualifier_map, first_pass):
    if first_pass:
      if observation.related:
        # Skip anything with a related observation on the first pass.
        return None
    else:
      if not observation.related:
        # Skip anything *without* a related observation on the second pass.
        return None
    if not observation.effectiveDateTime:
      logging.warning('Skipping observation without effectiveDateTime: %s'
                      % observation.as_json())
      return None
    if not observation.code or not observation.code.coding:
      logging.warning('Skipping observation without coding: %s' % observation.as_json())
      return None
    body_site_code_system = None
    body_site_code_value = None
    value_string = None
    value_decimal = None
    value_unit = None
    value_code_system = None
    value_code_value = None
    value_date_time = None
    if observation.bodySite and observation.bodySite.coding:
      body_site_code_system = observation.bodySite.coding[0].system
      body_site_code_value = observation.bodySite.coding[0].code
    if observation.valueQuantity:
      value_decimal = observation.valueQuantity.value
      value_unit = observation.valueQuantity.code
    if observation.valueDateTime:
      value_date_time = observation.valueDateTime.date
    if observation.valueString:
      value_string = observation.valueString
    if observation.valueCodeableConcept and observation.valueCodeableConcept.coding:
      # TODO: use codebook codes for PMI codes?
      value_code_system = observation.valueCodeableConcept.coding[0].system
      value_code_value = observation.valueCodeableConcept.coding[0].code
    measurements = []
    if observation.component:
      for component in observation.component:
        child = PhysicalMeasurementsDao.from_component(observation, component)
        if child:
          measurements.append(child)
    qualifiers = []
    if observation.related:
      for related in observation.related:
        if (related.type == _QUALIFIED_BY_RELATED_TYPE and related.target
            and related.target.reference):
          qualifier = qualifier_map.get(related.target.reference)
          if qualifier:
            qualifiers.append(qualifier)
          else:
            logging.warning('Could not find qualifier %s' % related.target.reference)
    result = Measurement(codeSystem=observation.code.coding[0].system,
                         codeValue=observation.code.coding[0].code,
                         measurementTime=observation.effectiveDateTime.date,
                         bodySiteCodeSystem=body_site_code_system,
                         bodySiteCodeValue=body_site_code_value,
                         valueString=value_string,
                         valueDecimal=value_decimal,
                         valueUnit=value_unit,
                         valueCodeSystem=value_code_system,
                         valueCodeValue=value_code_value,
                         valueDateTime=value_date_time,
                         measurements=measurements,
                         qualifiers=qualifiers)
    if first_pass:
      qualifier_map[full_url] = result
    return result

  @staticmethod
  def from_client_json(resource_json, participant_id=None, **unused_kwargs):
    #pylint: disable=unused-argument
    measurements = []
    observations = []
    qualifier_map = {}
    for entry in resource_json['entry']:
      resource = entry.get('resource')
      if resource and resource.get('resourceType') == _OBSERVATION_RESOURCE_TYPE:
        observations.append((entry['fullUrl'], fhirclient.models.observation.Observation(resource)))
    # Take two passes over the observations; once to find all the qualifiers and observations
    # without related qualifiers, and a second time to find all observations with related
    # qualifiers.
    for first_pass in [True, False]:
      for fullUrl, observation in observations:
        measurement = PhysicalMeasurementsDao.from_observation(observation,
                                                               fullUrl,
                                                               qualifier_map, first_pass)
        if measurement:
          measurements.append(measurement)
    return PhysicalMeasurements(participantId=participant_id, resource=json.dumps(resource_json),
                                measurements=measurements)

