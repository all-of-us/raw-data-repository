from flask import request

from rdr_service import app_util, config
from rdr_service.api.base_api import BaseApi, DEFAULT_MAX_RESULTS, get_sync_results_for_request, log_api_request
from rdr_service.api_util import HEALTHPRO, PTC, PTC_AND_HEALTHPRO
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.query import FieldFilter, Operator, Query


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
        resource = self.get_request_json()
        obj = self.dao.patch(id_, resource, p_id)
        log_api_request(log=request.log_record, model_obj=obj)
        return obj

    def list(self, participant_id=None):
        query = Query(
            [FieldFilter("participantId", Operator.EQUALS, participant_id)],
            None,
            DEFAULT_MAX_RESULTS,
            request.args.get("_token"),
        )
        results = self.dao.query(query)
        return self._make_bundle(results, "id", participant_id)


@app_util.auth_required(PTC)
def sync_physical_measurements():
    max_results = config.getSetting(config.MEASUREMENTS_ENTITIES_PER_SYNC, 100)
    return get_sync_results_for_request(PhysicalMeasurementsDao(), max_results)
