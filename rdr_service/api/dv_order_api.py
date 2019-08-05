import logging
import dateutil
from api.base_api import UpdatableApi
from rdr_service.api_util import PTC, PTC_AND_HEALTHPRO, VIBRENT_FHIR_URL
from app_util import auth_required, ObjDict
from rdr_service.dao.dv_order_dao import DvOrderDao
from fhir_utils import SimpleFhirR4Reader
from flask import request
from rdr_service.model.utils import from_client_participant_id
from rdr_service.participant_enums import OrderShipmentTrackingStatus
from werkzeug.exceptions import BadRequest, MethodNotAllowed, Conflict


class DvOrderApi(UpdatableApi):

  def __init__(self):
    super(DvOrderApi, self).__init__(DvOrderDao())

  @staticmethod
  def _lookup_resource_type_method(resource_type_method_map, raw_resource):
    if not isinstance(raw_resource, dict):
      raise BadRequest('invalid FHIR resource')
    try:
      resource_type = raw_resource['resourceType']
    except KeyError:
      raise BadRequest("payload is missing resourceType")
    try:
      return resource_type_method_map[resource_type]
    except KeyError:
      raise MethodNotAllowed("Method not allowed for resource type {}".format(resource_type))

  @auth_required(PTC)
  def post(self):
    try:
      resource = request.get_json(force=True)
    except BadRequest:
      raise BadRequest('missing FHIR resource')

    method = self._lookup_resource_type_method(
      {
       'SupplyRequest': self._post_supply_request,
       'SupplyDelivery': self._post_supply_delivery
      },
      resource
    )
    return method(resource)

  def _post_supply_delivery(self, resource):
    fhir_resource = SimpleFhirR4Reader(resource)
    patient = fhir_resource.patient
    pid = patient.identifier
    p_id = from_client_participant_id(pid.value)
    bo_id = fhir_resource.basedOn[0].identifier.value
    pk = {'participantId': p_id, 'order_id': bo_id}
    obj = ObjDict(pk)
    if not self.dao.get_id(obj):
      raise Conflict('Existing SupplyRequest for order required for SupplyDelivery')

    response = super(DvOrderApi, self).put(bo_id, participant_id=p_id, skip_etag=True)
    response[2]['Location'] = '/rdr/v1/SupplyDelivery/{}'.format(bo_id)
    if response[1] == 200:
      created_response = list(response)
      created_response[1] = 201
      return tuple(created_response)
    return response

  def _post_supply_request(self, resource):
    fhir_resource = SimpleFhirR4Reader(resource)
    patient = fhir_resource.contained.get(resourceType='Patient')
    pid = patient.identifier.get(
      system=VIBRENT_FHIR_URL + 'participantId').value
    p_id = from_client_participant_id(pid)
    response = super(DvOrderApi, self).post(participant_id=p_id)
    order_id = fhir_resource.identifier.get(system=VIBRENT_FHIR_URL + 'orderId').value
    response[2]['Location'] = '/rdr/v1/SupplyRequest/{}'.format(order_id)
    if response[1] == 200:
      created_response = list(response)
      created_response[1] = 201
      return tuple(created_response)
    return response

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id=None, order_id=None):  # pylint: disable=unused-argument

    if not p_id:
      raise BadRequest('invalid participant id')
    if not order_id:
      raise BadRequest('must include order ID to retrieve DV orders.')

    pk = {'participant_id': p_id, 'order_id': order_id}
    obj = ObjDict(pk)
    id_ = self.dao.get_id(obj)[0]

    return super(DvOrderApi, self).get(id_=id_, participant_id=p_id)

  @auth_required(PTC)
  def put(self, bo_id=None):  # pylint: disable=unused-argument

    if bo_id is None:
      raise BadRequest('invalid order id')
    try:
      resource = request.get_json(force=True)
    except BadRequest:
      raise BadRequest('missing FHIR order document')

    method = self._lookup_resource_type_method(
      {
        'SupplyRequest': self._put_supply_request,
        'SupplyDelivery': self._put_supply_delivery
      },
      resource
    )
    return method(resource, bo_id)

  def _put_supply_request(self, resource, bo_id):

    # handle invalid FHIR documents
    try:
      fhir_resource = SimpleFhirR4Reader(resource)
      pid = fhir_resource.contained.get(
           resourceType='Patient').identifier.get(system=VIBRENT_FHIR_URL + 'participantId')
      p_id = from_client_participant_id(pid.value)
    except AttributeError as e:
      raise BadRequest(e.message)
    except Exception as e:
      raise BadRequest(e.message)

    if not p_id:
      raise BadRequest('Request must include participant id')
    response = super(DvOrderApi, self).put(bo_id, participant_id=p_id, skip_etag=True)

    return response

  def _put_supply_delivery(self, resource, bo_id):

    merged_resource = None
    # handle invalid FHIR documents
    try:
      fhir = SimpleFhirR4Reader(resource)
      participant_id = fhir.patient.identifier.value
      p_id = from_client_participant_id(participant_id)
      update_time = dateutil.parser.parse(fhir.occurrenceDateTime)
      carrier_name = fhir.extension.get(url=VIBRENT_FHIR_URL + 'carrier').valueString

      eta = None
      if hasattr(fhir['extension'], VIBRENT_FHIR_URL + 'expected-delivery-date'):
        eta = dateutil.parser.parse(fhir.extension.get(url=VIBRENT_FHIR_URL + "expected-delivery-date").valueDateTime)

      tracking_status = fhir.extension.get(
        url=VIBRENT_FHIR_URL + 'tracking-status').valueString
      tracking_id = fhir.identifier.get(
        system=VIBRENT_FHIR_URL + 'trackingId').value
    except AttributeError as e:
      raise BadRequest(e.message)
    except Exception as e:
      raise BadRequest(e.message)

    tracking_status_enum = getattr(
      OrderShipmentTrackingStatus,
      tracking_status.upper(),
      OrderShipmentTrackingStatus.UNSET
    )

    biobank_dv_order_id = self.dao.get_id(ObjDict({
      'participantId': p_id,
      'order_id': int(bo_id)
    }))
    order = self.dao.get(biobank_dv_order_id)
    order.shipmentLastUpdate = update_time.date()
    order.shipmentCarrier = carrier_name
    if eta:
      order.shipmentEstArrival = eta.date()
    order.shipmentStatus = tracking_status_enum
    if not p_id:
      raise BadRequest('Request must include participant id')

    _id = self.dao.get_id(ObjDict({'participantId': p_id, 'order_id': int(bo_id)}))
    ex_obj = self.dao.get(_id)
    if (tracking_status == 'enroute' and ex_obj.trackingId != tracking_id) or \
        (tracking_status == 'delivered' and ex_obj.shipmentStatus != 'enroute' and ex_obj.tracking_id != tracking_id):
      # Send to mayolink and create internal biobank order
      response = self.dao.send_order(resource, p_id)
      merged_resource = merge_dicts(response, resource)
      merged_resource['id'] = _id
      logging.info('Sending salivary order to biobank for participant: %s', p_id)
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


def _make_response(self, obj):
  result = super(DvOrderApi, self)._make_response(obj)
  etag = super(DvOrderApi, self)._make_etag(obj.version)
  return result, 201, {'ETag': etag}

