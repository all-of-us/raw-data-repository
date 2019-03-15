from api.base_api import UpdatableApi
from api_util import PTC, PTC_AND_HEALTHPRO, VIBRENT_BARCODE_URL
from app_util import auth_required
from dao.dv_order_dao import DvOrderDao
from flask import request
from werkzeug.exceptions import BadRequest


class DvOrderApi(UpdatableApi):

  def __init__(self):
    super(DvOrderApi, self).__init__(DvOrderDao())

  @auth_required(PTC)
  def post(self):
    resource = request.get_json(force=True)
    p_id = resource['contained'][2]['identifier'][0]['value']
    if not p_id:
      raise BadRequest('Request must include participant id and must be of type integer')
    return super(DvOrderApi, self).post(participant_id=p_id)

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id, order_id=None):  # pylint: disable=unused-argument
    return super(DvOrderApi, self).get(order_id)

  @auth_required(PTC)
  def put(self, bo_id):  # pylint: disable=unused-argument
    resource = request.get_json(force=True)
    barcode_url = resource.get('extension')[0].get('url', "No barcode url")
    p_id = resource['contained'][2]['identifier'][0]['value']
    if not p_id:
      raise BadRequest('Request must include participant id and must be of type integer')
    response = super(DvOrderApi, self).put(bo_id, participant_id=p_id, skip_etag=True)
    if barcode_url == VIBRENT_BARCODE_URL:
      # send order to mayolink api
      # If these fail we don't need to return that in response, RDR will capture and try to resend.
      self.dao.send_order(resource)
      self.dao.insert_biobank_order(p_id, resource)

    return response


# Response (POST)
# shall
# return Location
# header
# with the literal reference ID and version ID, in the following format:
#   Location: [base] / [type] / [id] / _history / [vid]
