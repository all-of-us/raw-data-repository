import datetime
import logging

import clock
from api.mayolink_api import MayoLinkApi
from api_util import format_json_code, get_code_id, format_json_enum, parse_date, \
  VIBRENT_BARCODE_URL, VIBRENT_FHIR_URL, VIBRENT_ORDER_URL
from app_util import ObjectView
from dao.base_dao import UpdatableDao
from dao.biobank_order_dao import BiobankOrderDao
from dao.code_dao import CodeDao
from dao.participant_summary_dao import ParticipantSummaryDao
from fhir_utils import SimpleFhirR4Reader
from model.biobank_dv_order import BiobankDVOrder
from model.biobank_order import BiobankOrderedSample, BiobankOrderIdentifier, BiobankOrder
from model.utils import to_client_participant_id
from participant_enums import BiobankOrderStatus, OrderShipmentTrackingStatus
from sqlalchemy.orm import load_only
from werkzeug.exceptions import BadRequest


class DvOrderDao(UpdatableDao):

  def __init__(self):
    self.code_dao = CodeDao()
    super(DvOrderDao, self).__init__(BiobankDVOrder)
    self.biobank_address = {'city': "Rochester", 'state': "MN",
                'postalCode': "55901", 'line': ["3050 Superior Drive NW"]}


  def send_order(self, resource, pid):
    m = MayoLinkApi()
    order = self._filter_order_fields(resource, pid)
    response = m.post(order)
    return self.to_client_json(response, for_update=True)

  def _filter_order_fields(self, resource, pid):
    fhir_resource = SimpleFhirR4Reader(resource)
    summary = ParticipantSummaryDao().get(pid)
    if not summary:
      raise BadRequest('No summary for particpant id: {}'.format(pid))
    code_dict = summary.asdict()
    format_json_code(code_dict, self.code_dao, 'genderIdentityId')
    format_json_code(code_dict, self.code_dao, 'stateId')
    # MayoLink api has strong opinions on what should be sent and the order of elements. Dont touch.
    order = {
      'order': {
        'collected': fhir_resource.authoredOn,
        'account': '',
        'number': fhir_resource.extension.get(url=VIBRENT_BARCODE_URL).valueString,
        'patient': {'medical_record_number': str(summary.biobankId),
                    'first_name': '*',
                    'last_name': str(summary.biobankId),
                    'middle_name': '',
                    'birth_date': '3/3/1933',
                    'gender': code_dict['genderIdentity'],
                    'address1': summary.streetAddress,
                    'address2': summary.streetAddress2,
                    'city': summary.city,
                    'state': code_dict['state'],
                    'postal_code': str(summary.zipCode),
                    'phone': str(summary.phoneNumber),
                    'account_number': None,
                    'race': summary.race,
                    'ethnic_group': None
                    },
        'physician': {'name': None,
                      'phone': None,
                      'npi': None
                      },
        'report_notes': fhir_resource.extension.get(
          url=VIBRENT_ORDER_URL).valueString,
        'tests': {'test': {'code': '1SAL2',
                           'name': 'PMI Saliva, FDA Kit',
                           'comments': None
                           }
                  },
        'comments': 'Salivary Kit Order, direct from participant'
       }}
    return order

  def to_client_json(self, model, for_update=False):
    if for_update:
      result = dict()
      reduced_model = model['orders']['order']
      result['status'] = reduced_model['status']
      result['barcode'] = reduced_model['reference_number']
      result['received'] = reduced_model['received']
      result['biobankOrderId'] = reduced_model['number']
      result['biobankId'] = reduced_model['patient']['medical_record_number'] # biobank order id
    else:
      result = model.asdict()
      result['orderStatus'] = format_json_enum(result, 'orderStatus')
      result['shipmentStatus'] = format_json_enum(result, 'shipmentStatus')
      format_json_code(result, self.code_dao, 'stateId')
      result['state'] = result['state'][-2:]  # Get the abbreviation
      del result['id']  # PK for model

    result = {k: v for k, v in result.items() if v is not None}
    if 'participantId' in result:
      result['participantId'] = to_client_participant_id(result['participantId'])
    return result

  def from_client_json(self, resource_json, id_=None, expected_version=None,
                       participant_id=None, client_id=None): #pylint: disable=unused-argument
    """Initial loading of the DV order table does not include all attributes."""
    fhir_resource = SimpleFhirR4Reader(resource_json)
    order = BiobankDVOrder(participantId=participant_id)
    order.participantId = participant_id
    order.modified = datetime.datetime.now()
    if resource_json['resourceType'] == 'SupplyDelivery':
      order.order_id = int(fhir_resource.basedOn[0].identifier.value)
      existing_obj = self.get(self.get_id(order))
      existing_obj.shipmentStatus = fhir_resource.extension.get(
        url=VIBRENT_FHIR_URL + 'tracking-status').valueString
      existing_obj.shipmentCarrier = fhir_resource.extension.get(
        url=VIBRENT_FHIR_URL + 'carrier').valueString
      existing_obj.shipmentEstArrival = parse_date(fhir_resource.extension.get(
        url=VIBRENT_FHIR_URL + 'expected-delivery-date').valueDateTime)
      existing_obj.trackingId = fhir_resource.identifier.get(
        system=VIBRENT_FHIR_URL + 'trackingId').value
      # USPS status
      existing_obj.shipmentStatus = self._enumerate_order_shipment_tracking_status(
                                    fhir_resource.status)
      # USPS status time
      existing_obj.shipmentLastUpdate = parse_date(fhir_resource.occurrenceDateTime)
      order_address = fhir_resource.contained.get(resourceType='Location').get('address')
      order_address.stateId = get_code_id(order_address, self.code_dao, 'state', 'State_')
      existing_obj.address = {'city': existing_obj.city,
                              'state': existing_obj.stateId, 'postalCode': existing_obj.zipCode,
                              'line': [existing_obj.streetAddress1]}

      if existing_obj.streetAddress2 is not None and existing_obj.streetAddress2 != '':
        existing_obj.address['line'].append(existing_obj.streetAddress2)

      address = {'city': order_address.city, 'state': order_address.stateId,
                 'postalCode': order_address.postalCode, 'line': order_address.line}

      if existing_obj.address != address and address != self.biobank_address:
        logging.warn('Address change detected: Using new address ({}) for participant ({})'.format(
                      order_address, order.participantId))

        existing_obj.city = address['city']
        existing_obj.stateId = address['state']
        existing_obj.streetAddress1 = address['line'][0]
        existing_obj.zipCode = address['postalCode']
        try:
          existing_obj.streetAddress2 = address['line'][1]
        except IndexError:
          pass
      elif address == self.biobank_address:
        existing_obj.biobankCity = self.biobank_address['city']
        existing_obj.biobankStateId = get_code_id(self.biobank_address, self.code_dao,
                                                  'state', 'State_')
        existing_obj.biobankStreetAddress1 = self.biobank_address['line'][0]
        existing_obj.biobankZipCode = self.biobank_address['postalCode']

      return existing_obj

    if resource_json['resourceType'] == 'SupplyRequest':
      order.order_id = fhir_resource.identifier.get(
                       system=VIBRENT_FHIR_URL + 'orderId').value
      if hasattr(fhir_resource, 'authoredOn'):
        order.order_date = parse_date(fhir_resource.authoredOn)

      order.supplier = fhir_resource.contained.get(resourceType='Organization').id
      order.supplierStatus = fhir_resource.status

      fhir_device = fhir_resource.contained.get(resourceType='Device')
      order.itemName = fhir_device.deviceName.get(type='manufacturer-name').name
      order.itemSKUCode = fhir_device.identifier.get(
                          system=VIBRENT_FHIR_URL + 'SKU').value
      order.itemQuantity = fhir_resource.quantity.value

      fhir_patient = fhir_resource.contained.get(resourceType='Patient')
      fhir_address = fhir_patient.address[0]
      order.streetAddress1 = fhir_address.line[0]
      order.streetAddress2 = '\n'.join(fhir_address.line[1:])
      order.city = fhir_address.city
      order.stateId = get_code_id(fhir_address, self.code_dao, 'state', 'State_')
      order.zipCode = fhir_address.postalCode

      order.orderType = fhir_resource.extension.get(
        url=VIBRENT_ORDER_URL).valueString
      if id_ is None:
        order.version = 1
        order.created = datetime.datetime.now()
      else:
        # A put request may add new attributes
        order.id = self.get_id(order)[0]
        order.created = self._get_created_date(participant_id, id_)
        order.version = expected_version
        order.biobankStatus = fhir_resource.status
        if hasattr(fhir_resource, 'barcode'):
          order.barcode = fhir_resource.barcode
          order.biobankTrackingId = fhir_resource.biobankId
          order.biobankOrderId = fhir_resource.biobankOrderId
          order.biobankReceived = parse_date(fhir_resource.received)

    return order

  def insert_biobank_order(self, pid, resource):
    obj = BiobankOrder()
    obj.participantId = long(pid)
    obj.created = clock.CLOCK.now()
    obj.created = datetime.datetime.now()
    obj.orderStatus = BiobankOrderStatus.UNSET
    obj.biobankOrderId = resource['biobankOrderId']
    test = self.get(resource['id'])
    obj.dvOrders = [test]

    bod = BiobankOrderDao()
    obj.samples = [BiobankOrderedSample(
      test='1SAL2', processingRequired=False, description=u'salivary pilot kit')]
    self._add_identifiers_and_main_id(obj, ObjectView(resource))
    bod.insert(obj)

  def _add_identifiers_and_main_id(self, order, resource):
    order.identifiers = []
    for i in resource.identifier:
      try:
        if i['system'] == 'orderId':
          order.identifiers.append(BiobankOrderIdentifier(system=BiobankDVOrder._VIBRENT_ID_SYSTEM,
                                                          value=i['value']))
        if i['system'] == 'fulfillmentId':
          order.identifiers.append(BiobankOrderIdentifier(system=BiobankDVOrder._VIBRENT_ID_SYSTEM
                                                          + '/fulfillmentId', value=i['value']))
      except AttributeError:
        raise BadRequest(
          'No identifier for system %r, required for primary key.' %
            BiobankDVOrder._VIBRENT_ID_SYSTEM)

  def _get_created_date(self, pid, id_):
    with self.session() as session:
      query = session.query(BiobankDVOrder.created).filter_by(
        participantId=pid).filter_by(
        order_id=id_)
      return query.first()[0]

  def get_etag(self, id_, pid):
    with self.session() as session:
      query = session.query(BiobankDVOrder.version).filter_by(
        participantId=pid).filter_by(
        order_id=id_)
      return query.first()[0]

  def _do_update(self, session, obj, existing_obj): #pylint: disable=unused-argument
    obj.version += 1
    session.merge(obj)

  def get_id(self, obj):
    with self.session() as session:
      query = session.query(BiobankDVOrder.id).filter_by(
        participantId=obj.participantId).filter_by(
        order_id=obj.order_id)
      return query.first()

  def get_biobank_info(self, order):
    with self.session() as session:
      query = session.query(BiobankDVOrder).options(
        load_only("barcode", "biobankOrderId", "biobankStatus", "biobankReceived"))\
                                       .filter_by(participantId=order.participantId)\
                                                 .filter_by(order_id=order.order_id)
      return query.first()

  def _enumerate_order_shipment_tracking_status(self, status):
    # @TODO: Get all possible status from PTSC
    if status.lower() == 'in progress':
      return OrderShipmentTrackingStatus.ENROUTE

