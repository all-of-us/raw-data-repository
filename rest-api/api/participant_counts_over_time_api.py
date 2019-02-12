import datetime

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
DAYS_LIMIT_FOR_REALTIME_DATA = 100  # provisional, per design doc
DAYS_LIMIT_FOR_HISTORY_DATA = 600


class ParticipantCountsOverTimeApi(Resource):

  @auth_required(HEALTHPRO)
  def get(self):

    self.service = ParticipantCountsOverTimeService()
    self.hpo_dao = HPODao()

    # TODO: After enrollment status is filterable,
    # wire in 'organization', 'site', 'withdrawalStatus', and 'bucketSize'.
    # Withdrawn participants are currently always excluded per SQL in
    # ParticipantCountsOverTimeService; eventually want to make that filterable.
    enrollment_statuses = request.args.get('enrollmentStatus')
    filter_by = request.args.get('filterBy')
    history = request.args.get('history')
    awardees = request.args.get('awardee')
    stratification_str = request.args.get('stratification')
    start_date_str = request.args.get('startDate')
    end_date_str = request.args.get('endDate')

    params = {
      'enrollment_statuses': enrollment_statuses,
      'awardees': awardees,
      'stratification': stratification_str,
      'start_date': start_date_str,
      'end_date': end_date_str,
    }

    params = self.validate_params(start_date_str, end_date_str, stratification_str,
                                  enrollment_statuses, awardees, history)

    start_date = params['start_date']
    end_date = params['end_date']
    stratification = params['stratification']

    filters = params

    del filters['start_date']
    del filters['end_date']
    del filters['stratification']

    results = self.service.get_filtered_results(start_date, end_date,
                                                filters, filter_by, history,
                                                stratification=stratification)

    return results

  def validate_params(self, start_date_str, end_date_str, stratification_str,
                      enrollment_statuses, awardees, history):
    """Validates URL parameters, and converts human-friendly values to canonical form

    :param start_date_str: Start date string, e.g. '2018-01-01'
    :param end_date_str: End date string, e.g. '2018-01-31'
    :param stratification_str: How to stratify (layer) results, as in a stacked bar chart
    :param enrollment_statuses: enrollment level filters
    :param awardees: awardee name filters
    :param history: indicate if it's for historical data
    :return: Validated parameters in canonical form
    """

    params = {}

    # Validate stratifications
    try:
      params['stratification'] = Stratifications(stratification_str)
    except TypeError:
      raise BadRequest('Invalid stratification: %s' % stratification_str)

    if params['stratification'] in [Stratifications.FULL_STATE, Stratifications.FULL_CENSUS,
                                    Stratifications.FULL_AWARDEE, Stratifications.LIFECYCLE]:
      # Validate dates
      if not end_date_str:
        raise BadRequest('end date should not be empty')
      start_date = datetime.datetime.strptime('2017-01-01', DATE_FORMAT).date()
      try:
        end_date = datetime.datetime.strptime(end_date_str, DATE_FORMAT).date()
      except ValueError:
        raise BadRequest('Invalid end date: %s' % end_date_str)

      params['start_date'] = start_date
      params['end_date'] = end_date
    else:
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
      if history != 'TRUE' and date_diff > DAYS_LIMIT_FOR_REALTIME_DATA:
        raise BadRequest('Difference between start date and end date '
                         'should not be greater than %s days' % DAYS_LIMIT_FOR_REALTIME_DATA)
      if history == 'TRUE' and date_diff > DAYS_LIMIT_FOR_HISTORY_DATA:
        raise BadRequest('Difference between start date and end date '
                         'should not be greater than %s days' % DAYS_LIMIT_FOR_HISTORY_DATA)

      params['start_date'] = start_date
      params['end_date'] = end_date

    # Validate awardees, get ID list
    awardee_ids = []
    if awardees is not None:
      awardees = awardees.split(',')
      for awardee in awardees:
        if awardee != '':
          awardee_id = get_awardee_id_from_name({'awardee': awardee}, self.hpo_dao)
          if awardee_id == None:
            raise BadRequest('Invalid awardee name: %s' % awardee)
          awardee_ids.append(awardee_id)
    params['awardee_ids'] = awardee_ids

    # Validate enrollment statuses
    if enrollment_statuses is not None:
      enrollment_statuses = enrollment_statuses.split(',')
      try:
        params['enrollment_statuses'] = [EnrollmentStatus(val) for val in enrollment_statuses]
      except TypeError:
        valid_enrollment_statuses = EnrollmentStatus.to_dict()
        for enrollment_status in enrollment_statuses:
          if enrollment_status != '':
            if enrollment_status not in valid_enrollment_statuses:
              raise BadRequest('Invalid enrollment status: %s' % enrollment_status)

    return params

