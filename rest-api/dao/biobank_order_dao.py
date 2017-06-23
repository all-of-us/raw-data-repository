import logging

from code_constants import BIOBANK_TESTS_SET, SITE_ID_SYSTEM, HEALTHPRO_USERNAME_SYSTEM
from dao.base_dao import BaseDao, FhirMixin, FhirProperty
from dao.participant_dao import ParticipantDao, raise_if_withdrawn
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.site_dao import SiteDao
from model.biobank_order import BiobankOrder, BiobankOrderedSample, BiobankOrderIdentifier
from model.log_position import LogPosition
from model.participant import Participant
from model.utils import to_client_participant_id

from fhirclient.models.backboneelement import BackboneElement
from fhirclient.models.domainresource import DomainResource
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.identifier import Identifier
from fhirclient.models import fhirdate
from sqlalchemy.orm import subqueryload
from werkzeug.exceptions import BadRequest, Conflict

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
    # TODO: get rid of this once HealthPro switches over (DA-280)
    FhirProperty('author', Identifier),
    FhirProperty('identifier', Identifier, is_list=True, required=True),
    FhirProperty('created', fhirdate.FHIRDate, required=True),
    FhirProperty('samples', _FhirBiobankOrderedSample, is_list=True, required=True),
    FhirProperty('notes', _FhirBiobankOrderNotes),
    # TODO: get rid of this once HealthPro switches over (DA-280)
    FhirProperty('source_site', Identifier),
    # TODO: get rid of this once HealthPro switches over (DA-280)
    FhirProperty('finalized_site', Identifier),

    FhirProperty('created_info', _FhirBiobankOrderHandlingInfo),
    FhirProperty('collected_info', _FhirBiobankOrderHandlingInfo),
    FhirProperty('processed_info', _FhirBiobankOrderHandlingInfo),
    FhirProperty('finalized_info', _FhirBiobankOrderHandlingInfo)
  ]

class BiobankOrderDao(BaseDao):
  def __init__(self):
    super(BiobankOrderDao, self).__init__(BiobankOrder)

  def get_id(self, obj):
    return obj.biobankOrderId

  def _order_as_dict(self, order):
    result = order.asdict(follow={'identifiers': {}, 'samples': {}})
    del result['created']
    del result['logPositionId']
    for identifier in result.get('identifiers', []):
      del identifier['biobankOrderId']
    samples = result.get('samples')
    if samples:
      for sample in samples:
        del sample['biobankOrderId']
    return result

  def insert_with_session(self, session, obj):
    if obj.logPosition is not None:
      raise BadRequest('%s.logPosition must be auto-generated.' % self.model_type.__name__)
    obj.logPosition = LogPosition()
    if obj.biobankOrderId is None:
      raise BadRequest('Client must supply biobankOrderId.')
    existing_order = self.get_with_children_in_session(session, obj.biobankOrderId)
    if existing_order:
      existing_order_dict = self._order_as_dict(existing_order)
      new_dict = self._order_as_dict(obj)
      if existing_order_dict == new_dict:
        # If an existing matching order exists, just return it without trying to create it again.
        return existing_order
      else:
        raise Conflict('Order with ID %s already exists' % obj.biobankOrderId)
    return super(BiobankOrderDao, self).insert_with_session(session, obj)

  def _validate_model(self, session, obj):
    if obj.participantId is None:
      raise BadRequest('participantId is required')
    participant_summary = ParticipantSummaryDao().get_with_session(session, obj.participantId)
    if not participant_summary:
      raise BadRequest("Can't submit order for participant %s without consent" % obj.participantId)
    raise_if_withdrawn(participant_summary)
    for sample in obj.samples:
      self._validate_order_sample(sample)
    # TODO(mwf) FHIR validation for identifiers?
    # Verify that no identifier is in use by another order.
    for identifier in obj.identifiers:
      for existing in (session.query(BiobankOrderIdentifier)
          .filter_by(system=identifier.system)
          .filter_by(value=identifier.value)
          .filter(BiobankOrderIdentifier.biobankOrderId != obj.biobankOrderId)):
        raise BadRequest(
            'Identifier %s is already in use by order %s' % (identifier, existing.biobankOrderId))

  def _validate_order_sample(self, sample):
    # TODO(mwf) Make use of FHIR validation?
    if sample.test not in BIOBANK_TESTS_SET:
      raise BadRequest('Invalid test value %r not in %s.' % (sample.test, BIOBANK_TESTS_SET))

  def get_with_session(self, session, obj_id, **kwargs):
    result = super(BiobankOrderDao, self).get_with_session(session, obj_id, **kwargs)
    if result:
      ParticipantDao().validate_participant_reference(session, result)
    return result

  def get_with_children_in_session(self, session, obj_id):
    return (session.query(BiobankOrder)
        .options(subqueryload(BiobankOrder.identifiers), subqueryload(BiobankOrder.samples))
        .get(obj_id))

  def get_with_children(self, obj_id):
    with self.session() as session:
      return self.get_with_children_in_session(session, obj_id)

  def get_ordered_samples_for_participant(self, participant_id):
    """Retrieves all ordered samples for a participant."""
    with self.session() as session:
      return (session.query(BiobankOrderedSample)
                  .join(BiobankOrder)
                  .filter(BiobankOrder.participantId == participant_id)
                  .all())

  def get_ordered_samples_sample(self, session, percentage, batch_size):
    """Retrieves the biobank ID, collected time, and test for a percentage of ordered samples.
    Used in fake data generation."""
    return (session.query(Participant.biobankId, BiobankOrderedSample.collected,
                          BiobankOrderedSample.test)
                .join(BiobankOrder)
                .join(BiobankOrderedSample)
                .filter(Participant.biobankId % 100 < percentage * 100)
                .yield_per(batch_size))

  def _parse_handling_info(self, handling_info):
    site_id = None
    username = None
    if handling_info.site:
      if handling_info.site.system != SITE_ID_SYSTEM:
        raise BadRequest('Invalid site system: %s' % handling_info.site.system)
      site = SiteDao().get_by_google_group(handling_info.site.value)
      if not site:
        raise BadRequest('Unrecognized site: %s' % handling_info.site.value)
      site_id = site.siteId
    if handling_info.author:
      if handling_info.author.system != HEALTHPRO_USERNAME_SYSTEM:
        raise BadRequest('Invalid author system: %s' % handling_info.author.system)
      username = handling_info.author.value
    return username, site_id

  def _to_handling_info(self, username, site_id):
    if not username and not site_id:
      return None
    info = _FhirBiobankOrderHandlingInfo()
    if site_id:
      site = SiteDao().get(site_id)
      info.site = Identifier()
      info.site.system = SITE_ID_SYSTEM
      info.site.value = site.googleGroup
    if username:
      info.author = Identifier()
      info.author.system = HEALTHPRO_USERNAME_SYSTEM
      info.author.value = username
    return info

  # pylint: disable=unused-argument
  def from_client_json(self, resource_json, participant_id=None, client_id=None):
    resource = _FhirBiobankOrder(resource_json)
    if not resource.created.date:  # FHIR warns but does not error on bad date values.
      raise BadRequest('Invalid created date %r.' % resource.created.origval)

    order = BiobankOrder(
        participantId=participant_id,
        created=resource.created.date)

    site_dao = SiteDao()

    # TODO: require this once HealthPro switches over (DA-280)
    if resource.created_info:
      order.sourceUsername, order.sourceSiteId = self._parse_handling_info(resource.created_info)

    if resource.collected_info:
      order.collectedUsername, order.collectedSiteId = \
        self._parse_handling_info(resource.collected_info)
    if resource.processed_info:
      order.processedUsername, order.processedSiteId = \
        self._parse_handling_info(resource.processed_info)
    if resource.finalized_info:
      order.finalizedUsername, order.finalizedSiteId = \
        self._parse_handling_info(resource.finalized_info)

    # TODO: get rid of this once HealthPro switches over (DA-280)
    if resource.source_site:
      if resource.source_site.system == SITE_ID_SYSTEM:
        site = site_dao.get_by_google_group(resource.source_site.value)
        if not site:
          logging.warning('Unrecognized source site: %s', resource.source_site.value)
        else:
          order.sourceSiteId = site.siteId
      else:
        logging.warning('Unrecognized site system: %s', resource.source_site.system)
    
    if not order.sourceSiteId:
      raise BadRequest('Either createdInfo or sourceSite must be provided.')

    # TODO: get rid of this once HealthPro switches over (DA-280)
    if resource.finalized_site:
      if resource.finalized_site.system != SITE_ID_SYSTEM:
        raise BadRequest('Invalid site system: %s' % resource.finalized_site.system)
      site = site_dao.get_by_google_group(resource.finalized_site.value)
      if not site:
        raise BadRequest('Unrecognized finalized site: %s' % resource.finalized_site.value)
      order.finalizedSiteId = site.siteId

    # TODO: get rid of this once HealthPro switches over (DA-280)
    if resource.author:
      if resource.author.system == HEALTHPRO_USERNAME_SYSTEM:
        order.finalizedUsername = resource.author.value
      else:
        raise BadRequest('Unrecognized author system: %s' % resource.author.system)

    if resource.notes:
      order.collectedNote = resource.notes.collected
      order.processedNote = resource.notes.processed
      order.finalizedNote = resource.notes.finalized
    if resource.subject != self._participant_id_to_subject(participant_id):
      raise BadRequest(
          'Participant ID %d from path and %r in request do not match, should be %r.'
          % (participant_id, resource.subject, self._participant_id_to_subject(participant_id)))
    self._add_identifiers_and_main_id(order, resource)
    self._add_samples(order, resource)
    return order

  @classmethod
  def _add_identifiers_and_main_id(cls, order, resource):
    found_main_id = False
    for i in resource.identifier:
      order.identifiers.append(BiobankOrderIdentifier(system=i.system, value=i.value))
      if i.system == BiobankOrder._MAIN_ID_SYSTEM:
        order.biobankOrderId = i.value
        found_main_id = True
    if not found_main_id:
      raise BadRequest(
          'No identifier for system %r, required for primary key.' % BiobankOrder._MAIN_ID_SYSTEM)

  @classmethod
  def _add_samples(cls, order, resource):
    all_tests = sorted([s.test for s in resource.samples])
    if len(set(all_tests)) != len(all_tests):
      raise BadRequest('Duplicate test in sample list for order: %s.' % (all_tests,))
    for s in resource.samples:
      order.samples.append(BiobankOrderedSample(
          biobankOrderId=order.biobankOrderId,
          test=s.test,
          description=s.description,
          processingRequired=s.processing_required,
          collected=s.collected and s.collected.date,
          processed=s.processed and s.processed.date,
          finalized=s.finalized and s.finalized.date))

  @classmethod
  def _participant_id_to_subject(cls, participant_id):
    return 'Patient/%s' % to_client_participant_id(participant_id)

  @classmethod
  def _add_samples_to_resource(cls, resource, model):
    resource.samples = []
    for sample in model.samples:
      client_sample = _FhirBiobankOrderedSample()
      client_sample.test = sample.test
      client_sample.description = sample.description
      client_sample.processing_required = sample.processingRequired
      client_sample.collected = _ToFhirDate(sample.collected)
      client_sample.processed = _ToFhirDate(sample.processed)
      client_sample.finalized = _ToFhirDate(sample.finalized)
      resource.samples.append(client_sample)

  @classmethod
  def _add_identifiers_to_resource(cls, resource, model):
    resource.identifier = []
    for identifier in model.identifiers:
      fhir_id = Identifier()
      fhir_id.system = identifier.system
      fhir_id.value = identifier.value
      resource.identifier.append(fhir_id)

  def to_client_json(self, model):
    resource = _FhirBiobankOrder()
    resource.subject = self._participant_id_to_subject(model.participantId)
    resource.created = _ToFhirDate(model.created)
    resource.notes = _FhirBiobankOrderNotes()
    resource.notes.collected = model.collectedNote
    resource.notes.processed = model.processedNote
    resource.notes.finalized = model.finalizedNote
    resource.source_site = Identifier()
    resource.created_info = self._to_handling_info(model.sourceUsername, model.sourceSiteId)
    resource.collected_info = self._to_handling_info(model.collectedUsername, model.collectedSiteId)
    resource.processed_info = self._to_handling_info(model.processedUsername, model.processedSiteId)
    resource.finalized_info = self._to_handling_info(model.finalizedUsername, model.finalizedSiteId)

    # TODO: remove this once HealthPro switches over (DA-280)
    if resource.created_info and resource.created_info.site:
      resource.source_site = Identifier()
      resource.source_site.system = resource.created_info.site.system
      resource.source_site.value = resource.created_info.site.value

    # TODO: remove this once HealthPro switches over (DA-280)
    if resource.finalized_info:
      if resource.finalized_info.site:
        resource.finalized_site = Identifier()
        resource.finalized_site.system = resource.finalized_info.site.system
        resource.finalized_site.value = resource.finalized_info.site.value
      if resource.finalized_info.author:
        resource.author = Identifier()
        resource.author.system = resource.finalized_info.author.system
        resource.author.value = resource.finalized_info.author.value

    self._add_identifiers_to_resource(resource, model)
    self._add_samples_to_resource(resource, model)
    client_json = resource.as_json()  # also validates required fields
    client_json['id'] = model.biobankOrderId
    del client_json['resourceType']
    return client_json
