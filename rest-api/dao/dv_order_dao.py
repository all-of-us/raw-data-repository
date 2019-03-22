import datetime

from api.mayolink_api import MayoLinkApi
from api_util import format_json_code, get_code_id, format_json_enum, parse_date
from dao.base_dao import UpdatableDao
from dao.code_dao import CodeDao
from dao.participant_summary_dao import ParticipantSummaryDao
from fhir_utils import SimpleFhirR4Reader
from model.biobank_dv_order import BiobankDVOrder
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
      result['biobank_order_id'] = reduced_model['number']
      result['biobank_id'] = reduced_model['patient']['medical_record_number'] # biobank order id
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
      fhir_resource = SimpleFhirR4Reader(resource_json)
      order = BiobankDVOrder(participantId=participant_id)

      order.participant_id = participant_id
      order.modified = datetime.datetime.now()
      order.order_id = fhir_resource.identifier.get(system='orderId').code
      order.order_date = fhir_resource.authoredOn
      order.supplier = fhir_resource.contained.get(resourceType='Organization').id
      order.supplierStatus = fhir_resource.status  # @TODO: confirm right status

      fhir_device = fhir_resource.contained.get(resourceType='Device')
      order.itemName = fhir_device.deviceName.get(type='manufacturer-name').name
      order.itemSKUCode = fhir_device.identifier.get(system='SKU').code
      order.itemSNOMEDCode = fhir_device.identifier.get(system='SNOMED').code
      order.itemQuantity = fhir_resource.quantity.value

      fhir_patient = fhir_resource.contained.get(resourceType='Patient')
      fhir_address = fhir_patient.address[0]
      order.streetAddress1 = fhir_address.line[0]
      order.streetAddress2 = '\n'.join(fhir_address.line[1:])
      order.city = fhir_address.city
      order.stateId = get_code_id(fhir_address, self.code_dao, 'state', 'State_')
      order.zipCode = fhir_address.postalCode

      order.orderType = fhir_resource.extension.get(
        url="http://vibrenthealth.com/fhir/order-type"
      ).valueString
      if id_ is None:
        order.version = 1
        order.created = datetime.datetime.now()
      else:
        # A put request may add new attributes
        order.id = self.get_id(order)[0]
        order.created = self._get_created_date(participant_id, id_)
        order.version = expected_version
        order.barcode = fhir_resource.barcode  # NOTE: not in the FHIR spec
        # @TODO: foreign key to biobank order.biobank order id. implement in DA-953
        # order.biobankOrderId = resource['biobank_order_id']
        order.biobankStatus = fhir_resource.status
        order.biobankReceived = parse_date(fhir_resource.received)  # NOTE: not in the FHIR spec
    return order

  def insert_biobank_order(self, pid, resource):
    # @TODO: implement in DA-953
    print resource, pid

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

