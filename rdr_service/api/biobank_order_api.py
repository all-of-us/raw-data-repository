from flask import request

from werkzeug.exceptions import BadRequest
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
        kit_id = request.args.get('kit-id')
        dao = BiobankOrderDao()
        if participant_id is not None:
            # return all biobank order by participant id
            items = dao.get_biobank_orders_with_children_for_participant(participant_id)
            result = {'data': [], 'total': len(items)}
            for item in items:
                response_json = self.dao.to_client_json(item)
                result['data'].append(response_json)
            return result
        elif kit_id is not None:
            # return all biobank order by kit id
            items = dao.get_biobank_order_by_kit_id(kit_id)
            result = {'data': [], 'total': len(items)}
            for item in items:
                response_json = self.dao.to_client_json(item)
                result['data'].append(response_json)
            return result
        else:
            raise BadRequest("invalid parameters")


