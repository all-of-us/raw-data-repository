import logging

from api.base_api import BaseApi
from api_util import auth_required, PTC_AND_HEALTHPRO
from dao.participant_summary_dao import ParticipantSummaryDao


class ParticipantSummaryApi(BaseApi):
  def __init__(self):
    super(ParticipantSummaryApi, self).__init__(ParticipantSummaryDao())

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id=None):
    if p_id:
      return super(ParticipantSummaryApi, self).get(p_id)
    else:
      return super(ParticipantSummaryApi, self)._query('participantId')
