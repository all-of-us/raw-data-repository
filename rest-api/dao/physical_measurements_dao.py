import json
import logging

from dao.base_dao import BaseDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.log_position import LogPosition
from model.measurements import PhysicalMeasurements
from participant_enums import PhysicalMeasurementsStatus
from werkzeug.exceptions import BadRequest

_AMENDMENT_URL = 'http://terminology.pmi-ops.org/StructureDefinition/amends'

class PhysicalMeasurementsDao(BaseDao):

  def __init__(self):
    super(PhysicalMeasurementsDao, self).__init__(PhysicalMeasurements,
                                                  order_by_ending=['logPositionId'])

  def get_id(self, obj):
    return obj.physicalMeasurementsId

  def insert_with_session(self, session, obj):
    if obj.logPosition is not None:
      raise BadRequest('%s.logPosition must be auto-generated.' % self.model_type.__name__)
    obj.logPosition = LogPosition()
    obj.final = True
    resource_json = json.loads(obj.resource)
    for extension in resource_json['entry'][0]['resource'].get('extension', []):
      url = extension.get('url', '')
      if url != _AMENDMENT_URL:
        logging.info('Ignoring unsupported extension for PhysicalMeasurements: %r.' % url)
        continue
      self._update_amended(obj, extension, url, session)
      break
    self._update_participant_summary(session, obj.participantId)
    super(PhysicalMeasurementsDao, self).insert_with_session(session, obj)   
    # Flush to assign an ID to the measurements, as the client doesn't provide one.
    session.flush()
    # Update the resource to contain the ID.
    resource_json['id'] = str(obj.physicalMeasurementsId)
    obj.resource = json.dumps(resource_json)
    return obj

  def _update_participant_summary(self, session, participant_id):
    if participant_id is None:
      raise BadRequest('participantId is required')    
    participant_summary_dao = ParticipantSummaryDao()
    participant_summary = participant_summary_dao.get_with_session(session, participant_id)
    if not participant_summary:
      raise BadRequest('Can''t submit physical measurements for participant %s without consent' %
                       participant_id)
    if (not participant_summary.physicalMeasurementsStatus or
        participant_summary.physicalMeasurementsStatus == PhysicalMeasurementsStatus.UNSET):
      participant_summary.physicalMeasurementsStatus = PhysicalMeasurementsStatus.COMPLETED
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