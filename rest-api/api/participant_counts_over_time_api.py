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
    hpo_dao = HPODao()

    enrollment_status = request.args.get('enrollmentStatus')
    awardee = request.args.get('awardee')
    organization = request.args.get('organization')
    site = request.args.get('site')
    withdrawal_status = request.args.get('withdrawalStatus')
    stratification = request.args.get('stratification')
    start_date_str = request.args.get('startDate')
    end_date_str = request.args.get('endDate')

    resource_json = {
      'enrollment_status': enrollment_status,
      'awardee': awardee,
      'organization': organization,
      'site': site,
      'withdrawal_status': withdrawal_status,
      'stratification': stratification,
      'start_date': start_date_str,
      'end_date': end_date_str
    }

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

    awardee_id = get_awardee_id_from_name(resource_json, hpo_dao)
    if awardee_id == None:
      raise BadRequest('Invalid awardee name: %s' % awardee)
