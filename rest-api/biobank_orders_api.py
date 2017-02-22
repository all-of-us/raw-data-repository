from api_util import auth_required, format_json_date, parse_date, HEALTHPRO, PTC_AND_HEALTHPRO
from base_api import BaseApi
from dao.biobank_order_dao import BiobankOrderDao
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier

from flask import request

from werkzeug.exceptions import BadRequest, NotFound


class BiobankOrderAPI(BaseApi):
  def __init__(self):
    super(BiobankOrderAPI, self).__init__(BiobankOrderDao())

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, participant_id, biobank_order_id):
    model = self.dao.get(biobank_order_id)
    if model is None:
      raise NotFound('No Biobankorder %d.' % biobank_order_id)
    if model.participantId != get_numeric_id(participant_id):
      raise NotFound(
          'BiobankOrder %d is not associated with %r.' % (biobank_order_id, participant_id))
    return self.make_response_for_resource(model.asdict())

  @auth_required(HEALTHPRO)
  def post(self, participant_id, biobank_order_id):
    model = BiobankOrder(
        participantId=get_numeric_id(participant_id),
        biobankOrderId=int(biobank_order_id))
    req_json = request.get_json(force=True)
    model.fromdict(req_json)
    model.created = parse_date(model.created)
    for identifier_json in req_json['identifier']:
      identifier = BiobankOrderIdentifier()
      identifier.fromdict(identifier_json, allow_pk=True)
      model.identifiers.append(identifier)
    self.dao.insert(model)
    resp_json = model.asdict()
    format_json_date(resp_json, 'created')
    return self.make_response_for_resource(resp_json)

  # TODO(mwf) List, if used.


def get_numeric_id(participant_id):
  if participant_id[0] != 'P':
    raise BadRequest('Bad participant ID format: %r.' % participant_id)
  return int(participant_id[1:])
