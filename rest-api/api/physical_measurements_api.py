import app_util
import config

from api.base_api import BaseApi, DEFAULT_MAX_RESULTS, get_sync_results_for_request
from api_util import HEALTHPRO, PTC_AND_HEALTHPRO, PTC
from flask import request
from dao.physical_measurements_dao import PhysicalMeasurementsDao
from query import Query, Operator, FieldFilter


class PhysicalMeasurementsApi(BaseApi):
  def __init__(self):
    super(PhysicalMeasurementsApi, self).__init__(PhysicalMeasurementsDao())

  @app_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, id_=None, p_id=None):
    return super(PhysicalMeasurementsApi, self).get(id_, participant_id=p_id)

  @app_util.auth_required(HEALTHPRO)
  def post(self, p_id):
    return super(PhysicalMeasurementsApi, self).post(p_id)

  @app_util.auth_required(HEALTHPRO)
  def patch(self, id_, p_id):
    resource = request.get_json(force=True)
    return self.dao.patch(id_, resource, p_id)

  def list(self, participant_id=None):
    query = Query([FieldFilter('participantId', Operator.EQUALS, participant_id)],
                  None, DEFAULT_MAX_RESULTS, request.args.get('_token'))
    results = self.dao.query(query)
    return self._make_bundle(results, 'id', participant_id)


@app_util.auth_required(PTC)
def sync_physical_measurements():
  max_results = config.getSetting(config.MEASUREMENTS_ENTITIES_PER_SYNC, 100)
  return get_sync_results_for_request(PhysicalMeasurementsDao(), max_results)
