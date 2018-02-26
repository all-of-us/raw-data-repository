import datetime
import json
import logging

from flask.ext.restful import Resource
from flask import request
from werkzeug.exceptions import BadRequest

from api_util import HEALTHPRO
from api_util import get_awardee_id_from_name
from app_util import auth_required
from dao.hpo_dao import HPODao
from participant_enums import EnrollmentStatus

DATE_FORMAT = '%Y-%m-%d'
DAYS_LIMIT = 100  # provisional, per design doc


class ParticipantCountsOverTimeApi(Resource):

  @auth_required(HEALTHPRO)
  def get(self):
    self.hpo_dao = HPODao()

    enrollment_status = request.args.get('enrollmentStatus')
    awardee = request.args.get('awardee')
    organization = request.args.get('organization')
    site = request.args.get('site')
    withdrawal_status = request.args.get('withdrawalStatus')
    stratification = request.args.get('stratification')
    start_date_str = request.args.get('startDate')
    end_date_str = request.args.get('endDate')

    params = {
      'enrollment_statuses': enrollment_status,
      'awardees': awardee,
      'organizations': organization,
      'sites': site,
      'withdrawal_statuses': withdrawal_status,
      'stratifications': stratification,
      'start_date': start_date_str,
      'end_date': end_date_str
    }

    # Most parameters accepted by this API can have multiple, comma-delimited
    # values.  Arrange them into lists.
    for param in params:
      if param not in ['start_date', 'end_date']:
        continue
      params[param] = param.split(',')

    params = self.validate_params(params)


  def validate_params(self, params):

    enrollment_statuses = params['enrollment_statuses']
    awardees = params['awardees']
    organizations = params['organizations']
    sites = params['sites']
    withdrawal_statuses = params['withdrawal_statuses']
    stratifications = params['stratifications']
    start_date_str = params['start_dates']
    end_date_str = params['end_dates']

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

    awardee_ids = []
    for awardee in awardees:
      awardee_id = get_awardee_id_from_name(params, self.hpo_dao)
      if awardee_id == None:
        raise BadRequest('Invalid awardee name: %s' % awardee)
      awardee_ids.append(awardee_ids)

    params['awardee_ids'] = awardee_ids

    return params

