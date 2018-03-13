import datetime
# import json
import logging

from flask.ext.restful import Resource
from flask import request
from werkzeug.exceptions import BadRequest

from api_util import HEALTHPRO
from api_util import get_awardee_id_from_name
from app_util import auth_required
from dao.hpo_dao import HPODao
from dao.participant_counts_over_time_service import ParticipantCountsOverTimeService
from participant_enums import EnrollmentStatus
from participant_enums import Stratifications

DATE_FORMAT = '%Y-%m-%d'
DAYS_LIMIT = 100  # provisional, per design doc


class ParticipantCountsOverTimeApi(Resource):

  @auth_required(HEALTHPRO)
  def get(self):
    self.hpo_dao = HPODao()

    # TODO: After enrollment status is filterable,
    # wire in 'organization', 'site', 'withdrawalStatus', and 'bucketSize'.
    enrollment_status = request.args.get('enrollmentStatus')
    awardee = request.args.get('awardee')
    stratification = request.args.get('stratification')
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')

    params = {
      'enrollment_statuses': enrollment_status,
      'awardees': awardee,
      'stratification': stratification,
      'start_date': start_date,
      'end_date': end_date
    }

    # Most parameters accepted by this API can have multiple, comma-delimited
    # values.  Arrange them into lists.
    for param in params:
      value = params[param]
      if param in ['start_date', 'end_date', 'stratification']:
        params[param] = value
        continue
      if value is None:
        params[param] = []
      else:
        params[param] = value.split(',')

    params = self.validate_params(params)

    start_date = params['start_date']
    end_date = params['end_date']
    filters = "awardee=" + params['awardees'][0]

    logging.info('params')
    logging.info(params)

    ParticipantCountsOverTimeService().get_strata_by_filter(start_date, end_date, filters)


  def validate_params(self, params):

    start_date_str = params['start_date']
    end_date_str = params['end_date']
    enrollment_statuses = params['enrollment_statuses']
    awardees = params['awardees']
    stratification = params['stratification']

    # Validate dates
    if not start_date_str or not end_date_str:
      raise BadRequest('Start date and end date should not be empty')
    try:
      start_date = datetime.datetime.strptime(start_date_str, DATE_FORMAT).date()
    except ValueError:
      raise BadRequest('Invalid start date: %s' % start_date_str)
    try:
      end_date = datetime.datetime.strptime(end_date_str, DATE_FORMAT).date()
    except ValueError:
      raise BadRequest('Invalid end date: %s' % end_date_str)
    date_diff = abs((end_date - start_date).days)
    if date_diff > DAYS_LIMIT:
      raise BadRequest('Difference between start date and end date ' \
                       'should not be greater than %s days' % DAYS_LIMIT)
    params['start_date'] = start_date_str
    params['end_date'] = end_date_str

    # Validate awardees, get ID list
    awardee_ids = []
    for awardee in awardees:
      if awardee != '':
        awardee_id = get_awardee_id_from_name({'awardee': awardee}, self.hpo_dao)
        if awardee_id == None:
          raise BadRequest('Invalid awardee name: %s' % awardee)
        awardee_ids.append(awardee_ids)
    params['awardee_ids'] = awardee_ids

    # Validate enrollment statuses
    try:
      params["enrollment_statuses"] = [EnrollmentStatus(val) for val in enrollment_statuses]
    except TypeError:
      valid_enrollment_statuses = EnrollmentStatus.to_dict()
      for enrollment_status in enrollment_statuses:
        if enrollment_status not in valid_enrollment_statuses:
          raise BadRequest('Invalid enrollment status: %s' % enrollment_status)

    # Validate stratifications
    try:
      params['stratification'] = Stratifications(params['stratification'])
    except TypeError:
      raise BadRequest('Invalid stratification: %s' % stratification)

    return params

