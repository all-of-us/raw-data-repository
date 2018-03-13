import json

import datetime
from api.base_api import BaseApi, make_sync_results_for_request
from api_util import PTC_HEALTHPRO_AWARDEE, AWARDEE,  DEV_MAIL
from app_util import auth_required, get_validated_user_info
from dao.base_dao import json_serial
from dao.participant_summary_dao import ParticipantSummaryDao
from flask import request
from werkzeug.exceptions import Forbidden, BadRequest
from base64 import urlsafe_b64decode, urlsafe_b64encode
from protorpc import messages

class ParticipantSummaryApi(BaseApi):
  def __init__(self):
    super(ParticipantSummaryApi, self).__init__(ParticipantSummaryDao())

  @auth_required(PTC_HEALTHPRO_AWARDEE)
  def get(self, p_id=None):
    user_awardee = None
    user_email, user_info = get_validated_user_info()
    if AWARDEE in user_info['roles']:
      if user_email == DEV_MAIL:
        user_awardee = request.args.get('awardee')
      else:
        user_awardee = user_info['awardee']

    # data only for user_awardee, assert that query has same awardee
    if p_id:
      if user_awardee:
        if user_email != DEV_MAIL:
          raise Forbidden
      return super(ParticipantSummaryApi, self).get(p_id)
    else:
      if user_awardee:
        # make sure request has awardee
        requested_awardee = request.args.get('awardee')
        if requested_awardee != user_awardee:
          raise Forbidden
      return super(ParticipantSummaryApi, self)._query('participantId')

  def _make_query(self):
    query = super(ParticipantSummaryApi, self)._make_query()
    if self._is_last_modified_sync():
      # roll back last modified time
      for filters in query.field_filters:
        if filters.field_name == 'lastModified':
          # set time delta subtract
          time_delta = filters.value - datetime.timedelta(0, 300)
          filters.value = time_delta

      query.always_return_token = True
    return query

  def _make_bundle(self, results, id_field, participant_id):
    if self._is_last_modified_sync():
      return make_sync_results_for_request(self.dao, results)

    return super(ParticipantSummaryApi, self)._make_bundle(results, id_field,
                                                                   participant_id)

  def _is_last_modified_sync(self):
    return request.args.get('_sync') == 'true'


  def _make_pagination_token(self, item_dict, field_names):
    vals = [item_dict.get(field_name) for field_name in field_names]
    vals_json = json.dumps(vals, default=json_serial)
    return urlsafe_b64encode(vals_json)
