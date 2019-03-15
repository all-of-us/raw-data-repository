from model.base import Base
from model.utils import UTCDateTime6, Enum
from participant_enums import OrderShipmentStatus, OrderShipmentTrackingStatus
from sqlalchemy import Column, DateTime, Integer, String, ForeignKey, Date, UniqueConstraint, Text


class BiobankDVOrder(Base):
  """
  Direct Volunteer kit order shipment record
  """
  __tablename__ = 'biobank_dv_order'

  # Primary Key
  id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)
  # have mysql set the creation data for each new order
  created = Column('created', DateTime, nullable=True)
  # have mysql always update the modified data when the record is changed
  modified = Column('modified', DateTime, nullable=True)

  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'),
                         nullable=False)

  # identifier/code (system=OrderId)
  order_id = Column('order_id', Integer)
  # authored: date supplier was requested to send item.
  order_date = Column('order_date', Date)

  #
  # Supplier info
  #
  # supplier
  supplier = Column('supplier', String(80), nullable=True)
  # status
  supplierStatus = Column('supplier_status', String(30), nullable=True)

  #
  # Ordered item information
  #
  # itemReference/Device/DeviceName/Name
  itemName = Column('item_name', String(80), nullable=True)
  # itemReference/Device/Identifier[0] (system=SKU)
  itemSKUCode = Column('item_sku_code', String(80), nullable=True)
  # itemReference/Device/Identifier[1] (system=SNOMED)
  itemSNOMEDCode = Column('item_snomed_code', String(80), nullable=True)
  itemQuantity = Column('item_quantity', Integer, default=1)

  #
  # participant ship-to address
  #
  streetAddress1 = Column('street_address_1', String(255))
  streetAddress2 = Column('street_address_2', String(255))
  city = Column('city', String(255))
  stateId = Column('state_id', Integer, ForeignKey('code.code_id'))
  zipCode = Column('zip_code', String(10))

  #
  # biobank ship-to address
  #
  bioBankStreetAddress1 = Column('biobank_street_address_1', String(255))
  bioBankStreetAddress2 = Column('biobank_street_address_2', String(255))
  bioBankCity = Column('biobank_city', String(255))
  bioBankStateId = Column('biobank_state_id', Integer, ForeignKey('code.code_id'))
  bioBankZipCode = Column('biobank_zip_code', String(10))

  # occurenceDateTime
  shipmentLastUpdate = Column('shipment_last_update', Date, nullable=True)

  # To participant tracking id. identifier/code (system=trackingId).
  trackingId = Column('tracking_id', String(80), nullable=True)
  # To biobank tracking id. partOf/identifier/code (system=trackingId).
  bioBankTrackingId = Column('biobank_tracking_id', String(80), nullable=True)

  #
  # PTSC extensions
  #
  # order-type
  orderType = Column('order_type', String(80), nullable=True)
  # fullfillment-status
  orderStatus = Column('order_status', Enum(OrderShipmentStatus),
                       default=OrderShipmentStatus.UNSET)
  # carrier
  shipmentCarrier = Column('shipment_carrier', String(80), nullable=True)
  # expected-delivery-date
  shipmentEstArrival = Column('shipment_est_arrival', Date, nullable=True)
  # tracking-status
  shipmentStatus = Column('shipment_status', Enum(OrderShipmentTrackingStatus),
                          default=OrderShipmentTrackingStatus.UNSET)

  # barcode
  barcode = Column('barcode', String(80), nullable=True)

  #
  # Mayolink API response
  #
  biobankOrderId = Column('biobank_order_id', String(80),
                          ForeignKey('biobank_order.biobank_order_id'), nullable=True)

  biobankReference = Column('biobank_reference', String(80), nullable=True)
  biobankStatus = Column('biobank_status', String(30), nullable=True)
  biobankReceived = Column('biobank_received', UTCDateTime6, nullable=True)
  biobankRequisition = Column('biobank_requisition', Text, nullable=True)

  __table_args__ = (
    UniqueConstraint('participant_id', 'order_id', name='uidx_partic_id_order_id'),
  )
