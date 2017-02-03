"""Participant and ParticipantSummary APIs"""

import api_util
import base_api
import datetime
import logging
import offline.age_range_pipeline
import offline.participant_summary_pipeline
import participant_dao
import participant_summary

from api_util import PTC, PTC_AND_HEALTHPRO
from query import OrderBy


_PARTICIPANT_SUMMARY_ORDER = OrderBy("sortKey", True)


class ParticipantAPI(base_api.BaseApi):
  def __init__(self):
    super(ParticipantAPI, self).__init__(participant_dao.DAO())

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, id_=None, a_id=None):
    return super(ParticipantAPI, self).get(id_, a_id)

  @api_util.auth_required(PTC)
  def post(self, a_id=None):
    return super(ParticipantAPI, self).post(a_id)

  @api_util.auth_required(PTC)
  def put(self, id_, a_id=None):
    return super(ParticipantAPI, self).put(id_, a_id)

  @api_util.auth_required(PTC)
  def patch(self, id_, a_id=None):
    return super(ParticipantAPI, self).patch(id_, a_id)


class ParticipantSummaryAPI(base_api.BaseApi):
  def __init__(self):
    super(ParticipantSummaryAPI, self).__init__(participant_summary.DAO())

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, id_=None):
    if id_:
      return super(ParticipantSummaryAPI, self).get(participant_summary.SINGLETON_SUMMARY_ID, id_)
    else:
      return super(ParticipantSummaryAPI, self).query("participantId", _PARTICIPANT_SUMMARY_ORDER)


@api_util.auth_required_cron
def regenerate_participant_summaries():
  # TODO(danrodney): check to see if it's already running?
  logging.info("=========== Starting participant summary regeneration pipeline ============")
  offline.participant_summary_pipeline.ParticipantSummaryPipeline().start()
  return '{"metrics-pipeline-status": "started"}'


@api_util.auth_required_cron
def update_participant_summary_age_ranges():
  # TODO(danrodney): check to see if it's already running?
  logging.info("=========== Starting age range update pipeline ============")
  offline.age_range_pipeline.AgeRangePipeline(datetime.datetime.utcnow()).start()
  return '{"metrics-pipeline-status": "started"}'
