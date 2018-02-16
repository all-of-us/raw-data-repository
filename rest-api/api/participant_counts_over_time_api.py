import datetime

from api_util import HEALTHPRO
from app_util import auth_required
from flask.ext.restful import Resource
from flask import request
from werkzeug.exceptions import BadRequest

DATE_FORMAT = '%Y-%m-%d'
DAYS_LIMIT = 7


class ParticipantCountsOverTimeApi(Resource):

  @auth_required(HEALTHPRO)
  def get(self):
    enrollment_status = request.args.get('enrollment_status')
    awardee = request.args.get('awardee')
    organization = request.args.get('organization')
    site = request.args.get('site')
    withdrawal_status = request.args.get('withdrawal_status')
    stratification = request.args.get('stratification')
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')

    if not start_date_str or not end_date_str:
      raise BadRequest("Start date and end date should not be empty")
    try:
      start_date = datetime.datetime.strptime(start_date_str, DATE_FORMAT).date()
    except ValueError:
      raise BadRequest("Invalid start date: %s" % start_date_str)
    try:
      end_date = datetime.datetime.strptime(end_date_str, DATE_FORMAT).date()
    except ValueError:
      raise BadRequest("Invalid end date: %s" % end_date_str)
    date_diff = abs((end_date - start_date).days)
    if date_diff > DAYS_LIMIT:
      raise BadRequest("Difference between start date and end date " \
                       "should not be greater than %s days" % DAYS_LIMIT)