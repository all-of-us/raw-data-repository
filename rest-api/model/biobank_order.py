from fhirclient.models import fhirdate
from fhirclient.models.backboneelement import BackboneElement
from fhirclient.models.domainresource import DomainResource
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.identifier import Identifier
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, UnicodeText
from werkzeug.exceptions import BadRequest

from model.base import Base, FhirMixin, FhirProperty
from model.utils import to_client_participant_id


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
    FhirProperty('processing_required', bool),
    FhirProperty('collected', fhirdate.FHIRDate),
    FhirProperty('processed', fhirdate.FHIRDate),
    FhirProperty('finalized', fhirdate.FHIRDate),
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
    FhirProperty('source_site', Identifier, required=True),
  ]


class BiobankOrder(Base):
  """An order requesting samples.

  The order contains a list of samples stored in BiobankOrderedSample; the actual delivered and
  stored samples are tracked in BiobankStoredSample. Our reconciliation report compares the two.
  """
  __tablename__ = 'biobank_order'
  _MAIN_ID_SYSTEM = 'https://orders.mayomedicallaboratories.com'

  # A GUID for the order, provided by Biobank. This is the ID assigned in HealthPro, which is sent
  # to us as an identifier with the mayomedicallaboritories.com "system".
  # We omit autoincrement=False to avoid warnings & instead validate clients provide an ID upstream.
  biobankOrderId = Column('biobank_order_id', String(80), primary_key=True)

  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'),
                         nullable=False)

  # For syncing new orders.
  logPositionId = Column('log_position_id', Integer, ForeignKey('log_position.log_position_id'),
                         nullable=False)
  logPosition = relationship('LogPosition')

  # The lab (client site) sending the order.
  sourceSiteSystem = Column('source_site_system', String(80))
  sourceSiteValue = Column('source_site_value', Integer)

  # Additional fields stored for future use.
  created = Column('created', DateTime, nullable=False)
  collectedNote = Column('collected_note', UnicodeText)
  processedNote = Column('processed_note', UnicodeText)
  finalizedNote = Column('finalized_note', UnicodeText)
  identifiers = relationship('BiobankOrderIdentifier', cascade='all, delete-orphan')
  samples = relationship('BiobankOrderedSample', cascade='all, delete-orphan')

  @staticmethod
  # pylint: disable=unused-argument
  def from_client_json(resource_json, participant_id=None, client_id=None):
    resource = _FhirBiobankOrder(resource_json)
    order = BiobankOrder(
        participantId=participant_id,
        sourceSiteSystem=resource.source_site.system,
        sourceSiteValue=int(resource.source_site.value),
        created=resource.created.date,
        collectedNote=resource.notes.collected,
        processedNote=resource.notes.processed,
        finalizedNote=resource.notes.finalized)
    if resource.subject != order._participant_id_to_subject():
      raise BadRequest(
          'Participant ID %d from path and %r in request do not match.'
          % (participant_id, resource.subject))
    BiobankOrder._add_identifiers_and_main_id(order, resource)
    BiobankOrder._add_samples(order, resource)
    return order

  @staticmethod
  def _add_identifiers_and_main_id(order, resource):
    found_main_id = False
    for i in resource.identifier:
      order.identifiers.append(BiobankOrderIdentifier(system=i.system, value=i.value))
      if i.system == BiobankOrder._MAIN_ID_SYSTEM:
        order.biobankOrderId = i.value
        found_main_id = True
    if not found_main_id:
      raise BadRequest(
          'No identifier for system %r, required for primary key.' % BiobankOrder._MAIN_ID_SYSTEM)

  @staticmethod
  def _add_samples(order, resource):
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

  def _participant_id_to_subject(self):
    return 'Patient/%s' % to_client_participant_id(self.participantId)

  def _add_samples_to_resource(self, resource):
    resource.samples = []
    for sample in self.samples:
      client_sample = _FhirBiobankOrderedSample()
      client_sample.test = sample.test
      client_sample.description = sample.description
      client_sample.processing_required = sample.processingRequired
      client_sample.collected = _ToFhirDate(sample.collected)
      client_sample.processed = _ToFhirDate(sample.processed)
      client_sample.finalized = _ToFhirDate(sample.finalized)
      resource.samples.append(client_sample)

  def _add_identifiers_to_resource(self, resource):
    resource.identifier = []
    for identifier in self.identifiers:
      fhir_id = Identifier()
      fhir_id.system = identifier.system
      fhir_id.value = identifier.value
      resource.identifier.append(fhir_id)

  def to_client_json(self):
    resource = _FhirBiobankOrder()
    resource.subject = self._participant_id_to_subject()
    resource.created = _ToFhirDate(self.created)
    resource.notes = _FhirBiobankOrderNotes()
    resource.notes.collected = self.collectedNote
    resource.notes.processed = self.processedNote
    resource.notes.finalized = self.finalizedNote
    resource.source_site = Identifier()
    resource.source_site.system = self.sourceSiteSystem
    resource.source_site.value = str(self.sourceSiteValue)
    self._add_identifiers_to_resource(resource)
    self._add_samples_to_resource(resource)
    client_json = resource.as_json()  # also validates required fields
    client_json['id'] = self.biobankOrderId
    del client_json['resourceType']
    return client_json


class BiobankOrderIdentifier(Base):
  """Arbitrary IDs for a BiobankOrder in other systems.

  Other clients may create these, but they must be unique within each system.
  """
  __tablename__ = 'biobank_order_identifier'
  system = Column('system', String(80), primary_key=True)
  value = Column('value', String(80), primary_key=True)
  biobankOrderId = Column(
      'biobank_order_id', Integer, ForeignKey('biobank_order.biobank_order_id'), nullable=False)


class BiobankOrderedSample(Base):
  """Samples listed by a Biobank order.

  These are distinct from BiobankStoredSamples, which tracks received samples. The two should
  eventually match up, but we see BiobankOrderedSamples first and track them separately.
  """
  __tablename__ = 'biobank_ordered_sample'
  biobankOrderId = Column(
      'order_id', String(80), ForeignKey('biobank_order.biobank_order_id'), primary_key=True)

  # Unique within an order, though the same test may be redone in another order for the participant.
  test = Column('test', String(80), primary_key=True)

  # Free text description of the sample.
  description = Column('description', UnicodeText, nullable=False)

  processingRequired = Column('processing_required', Boolean, nullable=False)
  collected = Column('collected', DateTime)
  processed = Column('processed', DateTime)
  finalized = Column('finalized', DateTime)
