import datetime

from api.mayolink_api import MayoLinkApi
from api_util import format_json_code, get_code_id, format_json_enum
from dao.base_dao import UpdatableDao
from dao.code_dao import CodeDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_dv_order import BiobankDVOrder
from werkzeug.exceptions import BadRequest


class DvOrderDao(UpdatableDao):

  def __init__(self):
    self.code_dao = CodeDao()
    super(DvOrderDao, self).__init__(BiobankDVOrder)

  def send_order(self, resource):
    # barcode = resource['extension'][0]['valueString']
    # @TODO: Don't resend if you've sent it once !!!!!
    m = MayoLinkApi()
    order = self._filter_order_fields(resource)
    m.post(order)
    self.to_client_json(BiobankDVOrder)

  def _filter_order_fields(self, resource):
    # @TODO: add check for pid in case it's not in 2nd index
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

  def to_client_json(self, model):
    result = model.asdict()
    result['orderStatus'] = format_json_enum(result, 'orderStatus')
    result['shipmentStatus'] = format_json_enum(result, 'shipmentStatus')
    format_json_code(result, self.code_dao, 'stateId')
    result['state'] = result['state'][-2:] # Get the abbreviation
    result = {k: v for k, v in result.iteritems() if v is not None}
    del result['id'] #PK for model
    return result


  def from_client_json(self, resource_json, id_=None, expected_version=None,
                       participant_id=None, client_id=None):
    """Initial loading of the DV order table does not include all attributes."""
    # resource = _FhirDVOrder(resource_json)
    resource = resource_json
    try:
      order = BiobankDVOrder(participantId=participant_id)
      order.version = 1
      order.created = datetime.datetime.now()
      order.modified = datetime.datetime.now()
      order.order_id = resource['identifier'][0]['code']  # @TODO: confirm with PTSC
      order.order_date = resource['authoredOn']
      order.supplier = resource['contained'][0]['id']
      order.supplierStatus = resource['status']  # @TODO: confirm right status
      order.itemName = resource['contained'][1]['deviceName'][0]['name']
      order.itemSKUCode = resource['contained'][1]['identifier'][0]['code']
      order.itemSNOMEDCode = resource['contained'][1]['identifier'][1]['code']
      order.itemQuantity = resource['quantity']['value']
      order.streetAddress1 = resource['contained'][2]['address'][0]['line'][0]
      # This is an assumption...
      if len(resource['contained'][2]['address'][0]['line']) > 1:
        order.streetAddress2 = resource['contained'][2]['address'][0]['line'][1]
      order.city = resource['contained'][2]['address'][0]['city']
      order.stateId = get_code_id(resource['contained'][2]['address'][0], self.code_dao, 'state',
                                  'State_')
      order.zipCode = resource['contained'][2]['address'][0]['postalCode']
      order.orderType = resource['extension'][0]['valueString']

      return order

    except AttributeError as e:
      print e, '<<<<<<<<<< ERROR'



  def insert_biobank_order(self, pid, resource):
    print resource, pid
