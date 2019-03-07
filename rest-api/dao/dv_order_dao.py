from api_util import format_json_code
from dao.base_dao import FhirMixin, FhirProperty, UpdatableDao
from dao.code_dao import CodeDao
from dao.participant_summary_dao import ParticipantSummaryDao
from fhirclient.models import fhirdate
from fhirclient.models.backboneelement import BackboneElement
from fhirclient.models.domainresource import DomainResource
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.identifier import Identifier
from model.biobank_order import BiobankOrder
from werkzeug.exceptions import BadRequest


def _ToFhirDate(dt):
  if not dt:
    return None
  return FHIRDate.with_json(dt.isoformat())


class _FhirBiobankOrderNotes(FhirMixin, BackboneElement):
  """Notes sub-element."""
  resource_name = "BiobankOrderNotes"
  _PROPERTIES = [
    FhirProperty('collected', str),
    FhirProperty('processed', str),
    FhirProperty('finalized', str),
    ]


class _FhirBiobankOrderedSample(FhirMixin, BackboneElement):
  """Sample sub-element."""
  resource_name = "BiobankOrderedSample"
  _PROPERTIES = [
    FhirProperty('test', str, required=True),
    FhirProperty('description', str, required=True),
    FhirProperty('processing_required', bool, required=True),
    FhirProperty('collected', fhirdate.FHIRDate),
    FhirProperty('processed', fhirdate.FHIRDate),
    FhirProperty('finalized', fhirdate.FHIRDate),
    ]


class _FhirBiobankOrderHandlingInfo(FhirMixin, BackboneElement):
  """Information about what user and site handled an order."""
  resource_name = "BiobankOrderHandlingInfo"
  _PROPERTIES = [
    FhirProperty('author', Identifier),
    FhirProperty('site', Identifier),
    ]


class _FhirBiobankOrder(FhirMixin, DomainResource):
  """FHIR client definition of the expected JSON structure for a BiobankOrder resource."""
  resource_name = 'BiobankOrder'
  _PROPERTIES = [
    FhirProperty('subject', str, required=True),
    FhirProperty('identifier', Identifier, is_list=True, required=True),
    FhirProperty('created', fhirdate.FHIRDate, required=True),
    FhirProperty('samples', _FhirBiobankOrderedSample, is_list=True, required=True),
    FhirProperty('notes', _FhirBiobankOrderNotes),

    FhirProperty('created_info', _FhirBiobankOrderHandlingInfo),
    FhirProperty('collected_info', _FhirBiobankOrderHandlingInfo),
    FhirProperty('processed_info', _FhirBiobankOrderHandlingInfo),
    FhirProperty('finalized_info', _FhirBiobankOrderHandlingInfo),
    FhirProperty('cancelledInfo', _FhirBiobankOrderHandlingInfo),
    FhirProperty('restoredInfo', _FhirBiobankOrderHandlingInfo),
    FhirProperty('restoredSiteId', int, required=False),
    FhirProperty('restoredUsername', str, required=False),
    FhirProperty('amendedInfo', _FhirBiobankOrderHandlingInfo),
    FhirProperty('version', int, required=False),
    FhirProperty('status', str, required=False),
    FhirProperty('amendedReason', str, required=False)
    ]




class DvOrderDao(UpdatableDao):
  def __init__(self):
    super(DvOrderDao, self).__init__(BiobankOrder) # @TODO: change to new model in init

  def _send_order(self, resource):
    barcode = resource['extension'][0]['valueString']
    order = self._filter_order_fields(resource)
    print resource

  def _filter_order_fields(self, resource):
    # @TODO: WHERE TO PUT BARCODE ?
    # @TODO: add loop to check for pid incase it's not in element 2
    summary = None
    if resource['contained'][2]['resourceType'] == 'Patient':
      summary = ParticipantSummaryDao().get(resource['contained'][2]['identifier'][0]['value'])
    if not summary:
      raise BadRequest('No summary for particpant id: {}'.format(summary.participantId))
    code_dao = CodeDao()
    code_dict = summary.asdict()
    format_json_code(code_dict, code_dao, 'genderIdentityId')
    format_json_code(code_dict, code_dao, 'stateId')

    order = {'collected': resource['authoredOn'],
             'account': '',  # @TODO: add rdr account in configs
             'number': '',  # @TODO: add order id (biobank_order_identifier.value)
             'report_notes': resource['extension'][1]['valueString'],
             'comments': 'Salivary Kit Order, direct from participant',
             'patient': {'medical_record_number': summary.biobankId,
                         'first_name': summary.firstName,
                         'last_name': summary.lastName,
                         'middle_name': summary.middleName,
                         'birth_date': summary.dateOfBirth,
                         'gender': code_dict['genderIdentity'],
                         'address1': summary.streetAddress,
                         'address2': summary.streetAddress2,
                         'city': summary.city,
                         'state': code_dict['state'],
                         'postal_code': summary.zipCode,
                         'phone': summary.phoneNumber,
                         'account_number': None,
                         'race': summary.race,
                         'ethnic_group': None
                         },
             'physician': {'name': None,
                           'phone': None,
                           'npi': None
                           },
             'tests': {'test': {'code': '1SAL2',
                                'name': 'PMI Saliva, FDA Kit',
                                'comments': None
                                }
                       }
             }
    return {'orders': order}
