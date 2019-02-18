from api.base_api import BaseApi, make_sync_results_for_request
from api_util import PTC_HEALTHPRO_AWARDEE, AWARDEE, DEV_MAIL
from app_util import auth_required, get_validated_user_info
from dao.participant_summary_dao import ParticipantSummaryDao
from flask import request
from model.participant_summary import ParticipantSummary
from model.hpo import HPO
from werkzeug.exceptions import Forbidden, InternalServerError, BadRequest


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
    query.always_return_token = self._get_request_arg_bool('_sync')
    query.backfill_sync = self._get_request_arg_bool('_backfill', True)
    return query

  def _make_bundle(self, results, id_field, participant_id):
    if self._get_request_arg_bool('_sync'):
      return make_sync_results_for_request(self.dao, results)
    return super(ParticipantSummaryApi, self)._make_bundle(results, id_field, participant_id)


class ParticipantSummaryModifiedApi(BaseApi):
  """
  API to return participant_id and last_modified fields
  """
  def __init__(self):
    super(ParticipantSummaryModifiedApi, self).__init__(ParticipantSummaryDao())

  def get(self):
    """
    Return participant_id and last_modified for all records or a subset based
    on the awardee parameter.
    """
    response = list()
    user_email, user_info = get_validated_user_info()
    request_awardee = None

    with self.dao.session() as session:

      # validate parameter when passed an awardee.
      if 'awardee' in request.args:
        request_awardee = request.args.get('awardee')
        hpo = session.query(HPO.hpoId).filter(HPO.name == request_awardee).first()
        if not hpo:
          raise BadRequest('invalid awardee')

      # verify user has access to the requested awardee.
      if AWARDEE in user_info['roles'] and user_email != DEV_MAIL:
        try:
          if not request_awardee or user_info['awardee'] != request_awardee:
            raise Forbidden
        except KeyError:
          raise InternalServerError("config error for awardee")

      query = session.query(ParticipantSummary.participantId, ParticipantSummary.lastModified)
      query = query.order_by(ParticipantSummary.participantId)
      if request_awardee:
        query = query.filter(ParticipantSummary.hpoId == hpo.hpoId)

      items = query.all()
      for item in items:
        response.append({'participantId': 'P{0}'.format(item.participantId),
                         'lastModified': item.lastModified.isoformat()})

    return response
