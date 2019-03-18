from api.base_api import UpdatableApi
from api_util import PTC, PTC_AND_HEALTHPRO, VIBRENT_BARCODE_URL
from app_util import auth_required, ObjDict
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
    response = super(DvOrderApi, self).post(participant_id=p_id)
    order_id = resource['identifier'][0]['code']
    response[2]['Location'] = '/rdr/v1/SupplyRequest/{}'.format(order_id)
    return response

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id, order_id):  # pylint: disable=unused-argument
    if order_id:
      pk = {'participant_id': p_id, 'order_id': order_id}
      obj = ObjDict(pk)
      id_ = self.dao.get_id(obj)[0]
    else:
      raise BadRequest('Must include order ID to retrieve DV orders.')
    return super(DvOrderApi, self).get(id_=id_, participant_id=p_id)

  @auth_required(PTC)
  def put(self, bo_id):  # pylint: disable=unused-argument
    resource = request.get_json(force=True)
    barcode_url = resource.get('extension')[0].get('url', "No barcode url")
    p_id = resource['contained'][2]['identifier'][0]['value']
    merged_resource = None
    if not p_id:
      raise BadRequest('Request must include participant id and must be of type int')
    if barcode_url == VIBRENT_BARCODE_URL:
      # send order to mayolink api
      # If these fail we don't need to return that in response, RDR will capture and try to resend.
      _id = self.dao.get_id(ObjDict({'participant_id': p_id, 'order_id': bo_id}))
      ex_obj = self.dao.get(_id)
      if not ex_obj.barcode:
        # Send to mayolink and create internal biobank order
        response = self.dao.send_order(resource)
        merged_resource = merge_dicts(response, resource)
        self.dao.insert_biobank_order(p_id, merged_resource)

    if merged_resource:
      response = super(DvOrderApi, self).put(bo_id, participant_id=p_id, skip_etag=True,
                                               resource=merged_resource)
    else:
      response = super(DvOrderApi, self).put(bo_id, participant_id=p_id, skip_etag=True)

    return response


def merge_dicts(dict_a, dict_b):
  """Recursively merge dictionary b into dictionary a.
  """
  def _merge_dicts_(a, b):
    for key in set(a.keys()).union(b.keys()):
      if key in a and key in b:
        if isinstance(a[key], dict) and isinstance(b[key], dict):
          yield (key, dict(_merge_dicts_(a[key], b[key])))
        elif b[key] is not None:
          yield (key, b[key])
        else:
          yield (key, a[key])
      elif key in a:
        yield (key, a[key])
      elif b[key] is not None:
        yield (key, b[key])

  return dict(_merge_dicts_(dict_a, dict_b))
