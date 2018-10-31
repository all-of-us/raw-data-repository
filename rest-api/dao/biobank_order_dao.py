import clock
from api_util import get_site_id_by_site_value as get_site
from code_constants import BIOBANK_TESTS_SET, SITE_ID_SYSTEM, HEALTHPRO_USERNAME_SYSTEM
from dao.base_dao import UpdatableDao, FhirMixin, FhirProperty
from dao.participant_dao import ParticipantDao, raise_if_withdrawn
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.site_dao import SiteDao
from model.biobank_order import BiobankOrder, BiobankOrderedSample, BiobankOrderIdentifier,\
  BiobankOrderIdentifierHistory, BiobankOrderedSampleHistory, BiobankOrderHistory
from model.log_position import LogPosition
from model.participant import Participant
from model.utils import to_client_participant_id
from participant_enums import OrderStatus, BiobankOrderStatus

from fhirclient.models.backboneelement import BackboneElement
from fhirclient.models.domainresource import DomainResource
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.identifier import Identifier
from fhirclient.models import fhirdate
from sqlalchemy import or_
from sqlalchemy.orm import subqueryload
from werkzeug.exceptions import BadRequest, Conflict, PreconditionFailed


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


class BiobankOrderDao(UpdatableDao):
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
    obj.version = 1
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
    self._update_participant_summary(session, obj)
    inserted_obj = super(BiobankOrderDao, self).insert_with_session(session, obj)
    ParticipantDao().add_missing_hpo_from_site(
        session, inserted_obj.participantId, inserted_obj.collectedSiteId)
    return inserted_obj

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

  def get_with_children_in_session(self, session, obj_id, for_update=False):
    query = session.query(BiobankOrder).options(subqueryload(BiobankOrder.identifiers),
                                                 subqueryload(BiobankOrder.samples))

    if for_update:
      query = query.with_for_update()

    existing_obj = query.get(obj_id)
    return existing_obj

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

  def _get_order_status_and_time(self, sample, order):
    if sample.finalized:
      return (OrderStatus.FINALIZED, sample.finalized)
    if sample.processed:
      return (OrderStatus.PROCESSED, sample.processed)
    if sample.collected:
      return (OrderStatus.COLLECTED, sample.collected)
    return (OrderStatus.CREATED, order.created)

  def _update_participant_summary(self, session, obj):
    participant_summary_dao = ParticipantSummaryDao()
    participant_summary = participant_summary_dao.get_for_update(session, obj.participantId)
    if not participant_summary:
      raise BadRequest("Can't submit biospecimens for participant %s without consent" %
                       obj.participantId)
    raise_if_withdrawn(participant_summary)
    self._set_participant_summary_fields(obj, participant_summary)
    participant_summary_dao.update_enrollment_status(participant_summary)

  def _set_participant_summary_fields(self, obj, participant_summary):
    participant_summary.biospecimenStatus = OrderStatus.FINALIZED
    participant_summary.biospecimenOrderTime = obj.created
    participant_summary.biospecimenSourceSiteId = obj.sourceSiteId
    participant_summary.biospecimenCollectedSiteId = obj.collectedSiteId
    participant_summary.biospecimenProcessedSiteId = obj.processedSiteId
    participant_summary.biospecimenFinalizedSiteId = obj.finalizedSiteId
    participant_summary.lastModified = clock.CLOCK.now()

    for sample in obj.samples:
      status_field = 'sampleOrderStatus' + sample.test
      status, time = self._get_order_status_and_time(sample, obj)
      setattr(participant_summary, status_field, status)
      setattr(participant_summary, status_field + 'Time', time)

  def _get_non_cancelled_biobank_orders(self, session, participantId):
    # look up latest order without cancelled status
    return session.query(BiobankOrder).filter(BiobankOrder.participantId ==
                                              participantId).filter(or_(BiobankOrder.orderStatus !=
                                                                    BiobankOrderStatus.CANCELLED,
                                                                    BiobankOrder.orderStatus == None
                                                                    )).order_by(
      BiobankOrder.created).all()

  def _refresh_participant_summary(self, session, obj):
    # called when cancelled or amendments (maybe restore)
    participant_summary_dao = ParticipantSummaryDao()
    participant_summary = participant_summary_dao.get_for_update(session, obj.participantId)
    non_cancelled_orders = self._get_non_cancelled_biobank_orders(session, obj.participantId)

    participant_summary.biospecimenStatus = OrderStatus.UNSET
    participant_summary.biospecimenOrderTime = None
    participant_summary.biospecimenSourceSiteId = None
    participant_summary.biospecimenCollectedSiteId = None
    participant_summary.biospecimenProcessedSiteId = None
    participant_summary.biospecimenFinalizedSiteId = None
    participant_summary.lastModified = clock.CLOCK.now()
    for sample in obj.samples:
      status_field = 'sampleOrderStatus' + sample.test
      setattr(participant_summary, status_field, OrderStatus.UNSET)
      setattr(participant_summary, status_field + 'Time', None)

    if len(non_cancelled_orders) > 0:
      for order in non_cancelled_orders:
        self._set_participant_summary_fields(order, participant_summary)
    participant_summary_dao.update_enrollment_status(participant_summary)

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
  def from_client_json(self, resource_json, id_=None, expected_version=None,
    participant_id=None, client_id=None):
    resource = _FhirBiobankOrder(resource_json)
    if not resource.created.date:  # FHIR warns but does not error on bad date values.
      raise BadRequest('Invalid created date %r.' % resource.created.origval)

    order = BiobankOrder(
        participantId=participant_id,
        created=resource.created.date.replace(tzinfo=None))

    if not resource.created_info:
      raise BadRequest('Created Info is required, but was missing in request.')
    order.sourceUsername, order.sourceSiteId = self._parse_handling_info(
        resource.created_info)
    order.collectedUsername, order.collectedSiteId = self._parse_handling_info(
        resource.collected_info)
    if order.collectedSiteId is None:
      raise BadRequest('Collected site is required in request.')
    order.processedUsername, order.processedSiteId = self._parse_handling_info(
        resource.processed_info)
    order.finalizedUsername, order.finalizedSiteId = self._parse_handling_info(
        resource.finalized_info)

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
    if resource.amendedReason:
      order.amendedReason = resource.amendedReason
    if resource.amendedInfo:
      order.amendedUsername, order.amendedSiteId = self._parse_handling_info(resource.amendedInfo)
    order.version = expected_version
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
          collected=s.collected and s.collected.date.replace(tzinfo=None),
          processed=s.processed and s.processed.date.replace(tzinfo=None),
          finalized=s.finalized and s.finalized.date.replace(tzinfo=None)))

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
    resource.amendedReason = model.amendedReason

    restored = getattr(model, 'restoredSiteId')
    if model.orderStatus == BiobankOrderStatus.CANCELLED:
      resource.status = str(BiobankOrderStatus.CANCELLED)
      resource.cancelledInfo = self._to_handling_info(model.cancelledUsername,
                                                       model.cancelledSiteId)

    elif restored:
      resource.status = str(BiobankOrderStatus.UNSET)
      resource.restoredInfo = self._to_handling_info(model.restoredUsername,
                                                       model.restoredSiteId)

    elif model.orderStatus == BiobankOrderStatus.AMENDED:
      resource.status = str(BiobankOrderStatus.AMENDED)
      resource.amendedInfo = self._to_handling_info(model.amendedUsername,
                                                      model.amendedSiteId)

    self._add_identifiers_to_resource(resource, model)
    self._add_samples_to_resource(resource, model)
    client_json = resource.as_json()  # also validates required fields
    client_json['id'] = model.biobankOrderId
    del client_json['resourceType']
    return client_json

  def _do_update(self, session, order, existing_obj):
    order.lastModified = clock.CLOCK.now()
    order.biobankOrderId = existing_obj.biobankOrderId
    order.orderStatus = BiobankOrderStatus.AMENDED
    order.amendedTime = clock.CLOCK.now()
    order.logPosition = LogPosition()
    order.version += 1
    # Ensure that if an order was previously cancelled/restored those columns are removed.
    self._clear_cancelled_and_restored_fields(order)

    super(BiobankOrderDao, self)._do_update(session, order, existing_obj)
    session.add(order.logPosition)

    self._refresh_participant_summary(session, order)
    self._update_history(session, order)
    self._update_identifier_history(session, order)
    self._update_sample_history(session, order)

  def update_with_patch(self, id_, resource, expected_version):
    """creates an atomic patch request on an object. It will fail if the object
    doesn't exist already, or if obj.version does not match the version of the existing object.
    May modify the passed in object."""
    with self.session() as session:
      obj = self.get_with_children_in_session(session, id_, for_update=True)
      return self._do_update_with_patch(session, obj, resource, expected_version)

  def _do_update_with_patch(self, session, order, resource, expected_version):
    self._validate_patch_update(order, resource, expected_version)
    order.lastModified = clock.CLOCK.now()
    order.logPosition = LogPosition()
    order.version += 1
    if resource['status'].lower() == 'cancelled':
      order.amendedReason = resource['amendedReason']
      order.cancelledUsername = resource['cancelledInfo']['author']['value']
      order.cancelledSiteId = get_site(resource['cancelledInfo'])
      order.cancelledTime = clock.CLOCK.now()
      order.orderStatus = BiobankOrderStatus.CANCELLED
    elif resource['status'].lower() == 'restored':
      order.amendedReason = resource['amendedReason']
      order.restoredUsername = resource['restoredInfo']['author']['value']
      order.restoredSiteId = get_site(resource['restoredInfo'])
      order.restoredTime = clock.CLOCK.now()
      order.orderStatus = BiobankOrderStatus.UNSET
    else:
      raise BadRequest('status must be restored or cancelled for patch request.')

    super(BiobankOrderDao, self)._do_update(session, order, resource)
    self._update_history(session, order)
    self._update_identifier_history(session, order)
    self._update_sample_history(session, order)
    self._refresh_participant_summary(session, order)
    return order

  def _validate_patch_update(self, model, resource, expected_version):
    if expected_version != model.version:
      raise PreconditionFailed('Expected version was %s; stored version was %s' % \
                               (expected_version, model.version))
    required_cancelled_fields = ['amendedReason', 'cancelledInfo', 'status']
    required_restored_fields = ['amendedReason', 'restoredInfo', 'status']
    if 'status' not in resource:
      raise BadRequest('status of cancelled/restored is required')

    if resource['status'] == 'cancelled':
      if model.orderStatus == BiobankOrderStatus.CANCELLED:
        raise BadRequest('Can not cancel an order that is already cancelled.')
      for field in required_cancelled_fields:
        if field not in resource:
          raise BadRequest('%s is required for a cancelled biobank order' % field)
      if 'site' not in resource['cancelledInfo'] or 'author' not in resource['cancelledInfo']:
        raise BadRequest('author and site are required for cancelledInfo')

    elif resource['status'] == 'restored':
      if model.orderStatus != BiobankOrderStatus.CANCELLED:
        raise BadRequest('Can not restore an order that is not cancelled.')
      for field in required_restored_fields:
        if field not in resource:
          raise BadRequest('%s is required for a restored biobank order' % field)
      if 'site' not in resource['restoredInfo'] or 'author' not in resource['restoredInfo']:
        raise BadRequest('author and site are required for restoredInfo')


  @staticmethod
  def _update_history(session, order):
    # Increment the version and add a new history entry.
    session.flush()
    history = BiobankOrderHistory()
    history.fromdict(order.asdict(follow=['logPosition']), allow_pk=True)
    history.logPositionId = order.logPosition.logPositionId
    session.add(history)

  @staticmethod
  def _update_identifier_history(session, order):
    session.flush()
    for identifier in order.identifiers:
      history = BiobankOrderIdentifierHistory()
      history.fromdict(identifier.asdict(), allow_pk=True)
      history.version = order.version
      history.biobankOrderId = order.biobankOrderId
      session.add(history)

  @staticmethod
  def _update_sample_history(session, order):
    session.flush()
    for sample in order.samples:
      history = BiobankOrderedSampleHistory()
      history.fromdict(sample.asdict(), allow_pk=True)
      history.version = order.version
      history.biobankOrderId = order.biobankOrderId
      session.add(history)

  @staticmethod
  def _clear_cancelled_and_restored_fields(order):
    #pylint: disable=unused-argument
    """ Just in case these fields have values, we don't want them in the most recent record for an
    amendment, they will exist in history tables."""
    order.restoredUsername = None
    order.restoredTime = None
    order.cancelledUsername = None
    order.cancelledTime = None
    order.restoredSiteId = None
    order.cancelledSiteId = None
    order.status = BiobankOrderStatus.UNSET
