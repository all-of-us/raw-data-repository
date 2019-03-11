from api.base_api import UpdatableApi
from api_util import PTC, PTC_AND_HEALTHPRO, VIBRENT_BARCODE_URL
from app_util import auth_required
from dao.dv_order_dao import DvOrderDao
from flask import request


class DvOrderApi(UpdatableApi):

  def __init__(self):
    super(DvOrderApi, self).__init__(DvOrderDao())

  @auth_required(PTC)
  def post(self, p_id):
    return super(DvOrderApi, self).post(participant_id=p_id)

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id, order_id=None):  # pylint: disable=unused-argument
    return super(DvOrderApi, self).get(order_id)

  @auth_required(PTC)
  def put(self, p_id, bo_id):  # pylint: disable=unused-argument
    resource = request.get_json(force=True)
    # update table with resource
    # @TODO: update model in dao (should probably compare resource w/ existing model)
    if resource['extension'][0]['url'] == VIBRENT_BARCODE_URL:
      # send to mayolink
      self.dao.send_order(resource)
