import datetime

from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from rdr_service.api_util import HEALTHPRO, get_awardee_id_from_name
from rdr_service.app_util import auth_required
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_counts_over_time_service import ParticipantCountsOverTimeService
from rdr_service.participant_enums import EnrollmentStatus, EnrollmentStatusV2, MetricsAPIVersion, \
  Stratifications

DATE_FORMAT = '%Y-%m-%d'
DAYS_LIMIT_FOR_REALTIME_DATA = 100  # provisional, per design doc
DAYS_LIMIT_FOR_HISTORY_DATA = 600


class ParticipantCountsOverTimeApi(Resource):

  @auth_required(HEALTHPRO)
  def get(self):
    self.service = ParticipantCountsOverTimeService()
    self.hpo_dao = HPODao()

    params = {
      'stratification': request.args.get('stratification'),
      'start_date': request.args.get('startDate'),
      'end_date': request.args.get('endDate'),
      'history': request.args.get('history'),
      'enrollment_statuses': request.args.get('enrollmentStatus'),
      'sample_time_def': request.args.get('filterBy'),
      'awardees': request.args.get('awardee'),
      'version': request.args.get('version')
    }

    filters = self.validate_params(params)
    results = self.service.get_filtered_results(**filters)

    return results

  def validate_params(self, params):

    filters = {}

    # Validate stratifications
    try:
      filters['stratification'] = Stratifications(params['stratification'])
    except TypeError:
      raise BadRequest('Invalid stratification: %s' % params['stratification'])

    if filters['stratification'] in [Stratifications.FULL_STATE, Stratifications.FULL_CENSUS,
                                     Stratifications.FULL_AWARDEE, Stratifications.GEO_STATE,
                                     Stratifications.GEO_CENSUS, Stratifications.GEO_AWARDEE,
                                     Stratifications.LIFECYCLE]:
      # Validate dates
      if not params['end_date']:
        raise BadRequest('end date should not be empty')
      try:
        end_date = datetime.datetime.strptime(params['end_date'], DATE_FORMAT).date()
        start_date = end_date
      except ValueError:
        raise BadRequest('Invalid end date: %s' % params['end_date'])

      filters['start_date'] = start_date
      filters['end_date'] = end_date
    else:
      # Validate dates
      if not params['start_date'] or not params['end_date']:
        raise BadRequest('Start date and end date should not be empty')
      try:
        start_date = datetime.datetime.strptime(params['start_date'], DATE_FORMAT).date()
      except ValueError:
        raise BadRequest('Invalid start date: %s' % params['start_date'])
      try:
        end_date = datetime.datetime.strptime(params['end_date'], DATE_FORMAT).date()
      except ValueError:
        raise BadRequest('Invalid end date: %s' % params['end_date'])
      date_diff = abs((end_date - start_date).days)
      if params['history'] != 'TRUE' and date_diff > DAYS_LIMIT_FOR_REALTIME_DATA:
        raise BadRequest('Difference between start date and end date '
                         'should not be greater than %s days' % DAYS_LIMIT_FOR_REALTIME_DATA)
      if params['history'] == 'TRUE' and date_diff > DAYS_LIMIT_FOR_HISTORY_DATA:
        raise BadRequest('Difference between start date and end date '
                         'should not be greater than %s days' % DAYS_LIMIT_FOR_HISTORY_DATA)

      filters['start_date'] = start_date
      filters['end_date'] = end_date

    # Validate awardees, get ID list
    awardee_ids = []
    if params['awardees'] is not None:
      awardees = params['awardees'].split(',')
      for awardee in awardees:
        if awardee != '':
          awardee_id = get_awardee_id_from_name({'awardee': awardee}, self.hpo_dao)
          if awardee_id is None:
            raise BadRequest('Invalid awardee name: %s' % awardee)
          awardee_ids.append(awardee_id)
    filters['awardee_ids'] = awardee_ids

    try:
      filters['version'] = MetricsAPIVersion(int(params['version'])) if params['version'] else None
    except ValueError:
      filters['version'] = None

    # Validate enrollment statuses
    enrollment_status_strs = []
    if params['enrollment_statuses'] is not None:
      enrollment_statuses = params['enrollment_statuses'].split(',')
      try:
        enrollment_status_strs = [str(EnrollmentStatusV2(val))
                                  if filters['version'] == MetricsAPIVersion.V2
                                  else str(EnrollmentStatus(val)) for val in enrollment_statuses]
      except TypeError:
        valid_enrollment_statuses = EnrollmentStatusV2.to_dict() \
          if filters['version'] == MetricsAPIVersion.V2 else EnrollmentStatus.to_dict()
        for enrollment_status in enrollment_statuses:
          if enrollment_status != '':
            if enrollment_status not in valid_enrollment_statuses:
              raise BadRequest('Invalid enrollment status: %s' % enrollment_status)
    filters['enrollment_statuses'] = enrollment_status_strs

    if params['sample_time_def'] and params['sample_time_def'] not in ['STORED', 'ORDERED']:
      raise BadRequest('Invalid value for parameter filterBy: %s' % params['sample_time_def'])
    else:
      filters['sample_time_def'] = params['sample_time_def']

    if params['history'] and params['history'] not in ['TRUE', 'FALSE']:
      raise BadRequest('Invalid value for parameter history: %s' % params['history'])
    else:
      filters['history'] = params['history']

    return filters

