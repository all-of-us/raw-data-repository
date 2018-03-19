from api.base_api import BaseApi, make_sync_results_for_request
from api_util import PTC_HEALTHPRO_AWARDEE, AWARDEE, DEV_MAIL
from app_util import auth_required, get_validated_user_info
from dao.participant_summary_dao import ParticipantSummaryDao
from flask import request
from werkzeug.exceptions import Forbidden, InternalServerError


class ParticipantSummaryApi(BaseApi):
  def __init__(self):
    super(ParticipantSummaryApi, self).__init__(ParticipantSummaryDao())

  @auth_required(PTC_HEALTHPRO_AWARDEE)
  def get(self, p_id=None):
    auth_awardee = None
    user_email, user_info = get_validated_user_info()
    if AWARDEE in user_info['roles']:
      if user_email == DEV_MAIL:
        auth_awardee = request.args.get('awardee')
      else:
        try:
          if user_info['awardee']:
            auth_awardee = user_info['awardee']

        except KeyError:
          raise InternalServerError("Config error for awardee")

    # data only for user_awardee, assert that query has same awardee
    if p_id:
      if auth_awardee and user_email != DEV_MAIL:
        raise Forbidden
      return super(ParticipantSummaryApi, self).get(p_id)
    else:
      if auth_awardee:
        # make sure request has awardee
        requested_awardee = request.args.get('awardee')
        if requested_awardee != auth_awardee:
          raise Forbidden
      return super(ParticipantSummaryApi, self)._query('participantId')



  def _make_query(self):
    query = super(ParticipantSummaryApi, self)._make_query()
    if self._is_last_modified_sync():
      query.always_return_token = True

    return query

  def _make_bundle(self, results, id_field, participant_id):
    if self._is_last_modified_sync():
      return make_sync_results_for_request(self.dao, results)
    return super(ParticipantSummaryApi, self)._make_bundle(results, id_field, participant_id)

  def _is_last_modified_sync(self):
    return request.args.get('_sync') == 'true'
