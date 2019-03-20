import copy
import datetime

from api.mayolink_api import MayoLinkApi
from api_util import format_json_code, get_code_id, format_json_enum, parse_date
from app_util import ObjectView
from dao.base_dao import UpdatableDao
from dao.biobank_order_dao import BiobankOrderDao
from dao.code_dao import CodeDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_dv_order import BiobankDVOrder
from model.biobank_order import BiobankOrderedSample, BiobankOrderIdentifier
from participant_enums import BiobankOrderStatus
from werkzeug.exceptions import BadRequest


class DvOrderDao(UpdatableDao):

  def __init__(self):
    self.code_dao = CodeDao()
    super(DvOrderDao, self).__init__(BiobankDVOrder)

  def send_order(self, resource):
    m = MayoLinkApi()
    order = self._filter_order_fields(resource)
    response = m.post(order)
    return self.to_client_json(response, for_update=True)

  def _filter_order_fields(self, resource):
    # @TODO: confirm that a summary is actually required for this pilot
    summary = None
    if resource['contained'][2]['resourceType'] == 'Patient':
      summary = ParticipantSummaryDao().get(resource['contained'][2]['identifier'][0]['value'])
    if not summary:
      raise BadRequest('No summary for particpant id: {}'.format(summary.participantId))
    code_dict = summary.asdict()
    format_json_code(code_dict, self.code_dao, 'genderIdentityId')
    format_json_code(code_dict, self.code_dao, 'stateId')
    # MayoLink api has strong opinions on what should be sent and the order of elements. Dont touch.
    order = {
      'order': {
        'collected': resource['authoredOn'],
        'account': '',
        'number': resource['extension'][0]['valueString'],
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
        'report_notes': resource['extension'][1]['valueString'],
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
    return result

  def from_client_json(self, resource_json, id_=None, expected_version=None,
                       participant_id=None, client_id=None): #pylint: disable=unused-argument
    """Initial loading of the DV order table does not include all attributes."""
    if resource_json['resourceType'] == 'SupplyRequest':
      resource = resource_json
      order = BiobankDVOrder(participantId=participant_id)

      order.participantId = participant_id
      order.modified = datetime.datetime.now()
      order.orderId = resource['identifier'][0]['code']
      order.orderDate = resource['authoredOn']
      order.supplier = resource['contained'][0]['id']
      order.supplierStatus = resource['status']  # @TODO: confirm right status
      order.itemName = resource['contained'][1]['deviceName'][0]['name']
      order.itemSKUCode = resource['contained'][1]['identifier'][0]['code']
      order.itemSNOMEDCode = resource['contained'][1]['identifier'][1]['code']
      order.itemQuantity = resource['quantity']['value']
      order.streetAddress1 = resource['contained'][2]['address'][0]['line'][0]
      if len(resource['contained'][2]['address'][0]['line']) > 1:
        order.streetAddress2 = resource['contained'][2]['address'][0]['line'][1]
      order.city = resource['contained'][2]['address'][0]['city']
      order.stateId = get_code_id(resource['contained'][2]['address'][0], self.code_dao, 'state',
                                  'State_')
      order.zipCode = resource['contained'][2]['address'][0]['postalCode']
      order.orderType = resource['extension'][0]['valueString']
      if id_ is None:
        order.version = 1
        order.created = datetime.datetime.now()
      else:
        # A put request may add new attributes
        order.id = self.get_id(order)[0]
        order.created = self._get_created_date(participant_id, id_)
        order.version = expected_version
        order.barcode = resource['barcode']
        # @TODO: foreign key to biobank order.biobank order id. implement in DA-953
        order.biobankOrderId = resource['biobankOrderId']
        order.biobankStatus = resource['status']
        order.biobankReceived = parse_date(resource['received'])
    return order

  def insert_biobank_order(self, pid, resource):
    obj = ObjectView(resource)
    obj.logPosition = None
    obj.participantId = pid
    obj.created = obj.received
    obj.orderStatus = BiobankOrderStatus.UNSET
    obj.lastModified = datetime.datetime.now()
    obj.created = datetime.datetime.now()  # @todo: confirm not ptsc created time

    bod = BiobankOrderDao()
    obj_copy = copy.deepcopy(obj)
    obj_copy.samples = [BiobankOrderedSample(
      test='1SAL2', processingRequired=False, description=u'salivary pilot kit')]
    bod._add_samples_to_resource(obj, obj_copy)
    self._add_identifiers_and_main_id(obj, obj_copy)
    bod.insert(obj)

  def _add_identifiers_and_main_id(self, order, resource):
    order.identifiers = []
    for i in resource.identifier:
      try:
        if i['system'] == 'orderId':
          order.identifiers.append(BiobankOrderIdentifier(system=BiobankDVOrder._VIBRENT_ID_SYSTEM,
                                                          value=i['code']))
        if i['system'] == 'fulfillmentId':
          order.identifiers.append(BiobankOrderIdentifier(system=BiobankDVOrder._VIBRENT_ID_SYSTEM
                                                          + '/fulfillmentId', value=i['code']))
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
        participantId=obj.participant_id).filter_by(
        order_id=obj.order_id)
      return query.first()

