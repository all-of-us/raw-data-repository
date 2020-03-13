from flask import request

from rdr_service.api.base_api import UpdatableApi
from rdr_service.api_util import HEALTHPRO, PTC_AND_HEALTHPRO
from rdr_service.app_util import auth_required
from rdr_service.dao.biobank_order_dao import BiobankOrderDao


class BiobankOrderApi(UpdatableApi):
    def __init__(self):
        super(BiobankOrderApi, self).__init__(BiobankOrderDao(), get_returns_children=True)

    @auth_required(HEALTHPRO)
    def post(self, p_id):
        return super(BiobankOrderApi, self).post(participant_id=p_id)

    @auth_required(PTC_AND_HEALTHPRO)
    def get(self, p_id=None, bo_id=None):  # pylint: disable=unused-argument
        return super(BiobankOrderApi, self).get(id_=bo_id, participant_id=p_id)

    @auth_required(HEALTHPRO)
    def put(self, p_id, bo_id):  # pylint: disable=unused-argument
        return super(BiobankOrderApi, self).put(bo_id, participant_id=p_id)

    @auth_required(HEALTHPRO)
    def patch(self, p_id, bo_id):  # pylint: disable=unused-argument
        return super(BiobankOrderApi, self).patch(bo_id)

    def list(self, participant_id):
        kwargs = {
            'participant_id': participant_id,
            'kit_id': request.args.get('kitId'),
            'start_date': request.args.get('startDate'),
            'end_date': request.args.get('endDate'),
            'origin': request.args.get('origin'),
            'page': request.args.get('page'),
            'page_size': request.args.get('pageSize')
        }
        return BiobankOrderDao().handle_list_queries(**kwargs)




