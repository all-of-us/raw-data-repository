from sqlalchemy import (
    BigInteger, Column, DateTime,
    ForeignKey, Integer, String,
    Text, UniqueConstraint, event, Boolean
)
from sqlalchemy.sql import expression

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.hpo import HPO
from rdr_service.model.utils import Enum, UTCDateTime6
from rdr_service.participant_enums import OrderShipmentStatus, OrderShipmentTrackingStatus


class BiobankMailKitOrder(Base):
    # WARNING: any time this table is modified, check to see if the history table should be modified as well
    # (especially when adding or removing columns)

    # mapping a user_info.clientID (from config) to a system identifier
    ID_SYSTEM = {
        'vibrent': "http://vibrenthealth.com",
        'careevolution': "http://carevolution.be",
        'example': "system-test"
    }

    __tablename__ = "biobank_mail_kit_order"

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    """The unique, internal id assigned to a biobank mailkit order"""

    created = Column("created", DateTime, nullable=True)
    """Datetime when the order was first created"""
    modified = Column("modified", DateTime, nullable=True)
    """the time at which the biobank order was most recently modified"""

    version = Column("version", Integer, nullable=False)
    """
    Incrementing version, starts at 1 and is incremented on each update. The history table will have multiple versions
    ranging from 1 to the number of times the record has been updated. Each of these different versions will show
    the values that have changed.
    """

    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"), nullable=False)
    """
    PMI-specific ID generated by the RDR and used for tracking/linking participant data.
    Human-readable 10-character string beginning with P.
    """
    # identifier/code (system=OrderId)
    order_id = Column("order_id", BigInteger)
    """
    The globally unique ID created by HealthPro when a biobank order is created. This order ID is pushed to MayoLINK
    when the biobank order is created in their system. As requested/suggested by Mayo, it should be 12 alphanumeric
    characters long
    """

    order_date = Column("order_date", DateTime)
    """the date the supplier was requested to send the item; equivalent to _authored fields"""

    #
    # Supplier info
    #
    supplier = Column("supplier", String(80), nullable=True)
    """Who is supplying the kit to the participant (usually Genotech)"""
    supplierStatus = Column("supplier_status", String(30), nullable=True)
    """Genotech's status for the shipment of the kit to the participant"""

    #
    # Ordered item information
    #
    # itemReference/Device/DeviceName/Name
    itemName = Column("item_name", String(80), nullable=True)
    """PTSC payload; device name"""
    # itemReference/Device/Identifier[0] (system=SKU)
    itemSKUCode = Column("item_sku_code", String(80), nullable=True)
    """
    Sku of the order
    @rdr_dictionary_show_unique_values
    """
    # itemReference/Device/Identifier[1] (system=SNOMED)
    itemSNOMEDCode = Column("item_snomed_code", String(80), nullable=True)
    """snomed code"""
    itemQuantity = Column("item_quantity", Integer, default=1)
    """How many samples are in that order"""

    streetAddress1 = Column("street_address_1", String(255))
    """The street address of the participant ship to address"""
    streetAddress2 = Column("street_address_2", String(255))
    """The street address (line 2) of the participant ship to address"""
    city = Column("city", String(255))
    """City of participant's ship-to address"""
    stateId = Column("state_id", Integer, ForeignKey("code.code_id"))
    """The state of the participant's ship-to address; references code id for state id"""
    zipCode = Column("zip_code", String(10))
    """Zip code for the participant's ship-to address"""

    #
    # biobank ship-to address
    #
    biobankStreetAddress1 = Column("biobank_street_address_1", String(255))
    """Street address for biobank"""
    biobankStreetAddress2 = Column("biobank_street_address_2", String(255))
    """Street address (line 2) for biobank"""
    biobankCity = Column("biobank_city", String(255))
    """City the biobank is located"""
    biobankStateId = Column("biobank_state_id", Integer, ForeignKey("code.code_id"))
    """State the biobank is located; references code id for state id"""
    biobankZipCode = Column("biobank_zip_code", String(10))
    """Zip code for biobank"""

    shipmentLastUpdate = Column("shipment_last_update", DateTime, nullable=True)
    """The datetime of last update to shipment based on USPS order_status"""
    # (Genotek->Participant, then updated to Participat->Biobank)
    # identifier/code (system=trackingId).
    trackingId = Column("tracking_id", String(80), nullable=True)
    """Tracking id number from Genotech for participant then participant to biobank"""
    # Represents biobank_id. partOf/identifier/code (system=trackingId).
    biobankTrackingId = Column("biobank_tracking_id", String(80), nullable=True)
    """PTSC payload - tracking id for biobank order"""

    # PTSC extensions
    orderType = Column("order_type", String(80), nullable=True)
    """
    Whether it was part of a salivary order or the salivary pilot; easy way to filter DVs out of biobank_orders
    (if it contains value here, it should be a DV order)
    @rdr_dictionary_show_unique_values
    """

    orderStatus = Column("order_status", Enum(OrderShipmentStatus), default=OrderShipmentStatus.UNSET)
    """The fulfillment status of the order"""

    shipmentCarrier = Column("shipment_carrier", String(80), nullable=True)
    """
    The carrier for the biobank order shipment; which carrier to query for order_status
    @rdr_dictionary_show_unique_values
    """

    shipmentEstArrival = Column("shipment_est_arrival", DateTime, nullable=True)
    """The estimated arrival datetime for the shipment"""

    shipmentStatus = Column(
        "shipment_status", Enum(OrderShipmentTrackingStatus), default=OrderShipmentTrackingStatus.UNSET
    )
    """The shipment status of the biobank order"""

    barcode = Column("barcode", String(80), nullable=True, index=True)
    """Barcode from Genotech for tracking purposes."""

    #
    # Mayolink API response
    #
    biobankOrderId = Column(
        "biobank_order_id", String(80), ForeignKey("biobank_order.biobank_order_id"), unique=True, nullable=True
    )
    """
    The globally unique ID created by HealthPro when a biobank order is created. This order ID is pushed to
    MayoLINK when the biobank order is created in their system. As requested/suggested by Mayo, it should be
    12 alphanumeric characters long.
    """

    biobankStatus = Column("biobank_status", String(30), nullable=True)
    """
    Response from the Mayo Clinic API about the RDR creating the biobank order
    @rdr_dictionary_show_unique_values
    """

    biobankReceived = Column("biobank_received", UTCDateTime6, nullable=True)
    """The datetime when the biobank order was received"""

    biobankRequisition = Column("biobank_requisition", Text, nullable=True)
    """PTSC payload field - unclear"""

    isTestSample = Column("is_test_sample", Boolean, default=False)

    pilotSource = Column("pilot_source", String(256), nullable=True)

    associatedHpoId = Column("associated_hpo_id", Integer, ForeignKey(HPO.hpoId))
    """
    Participant's paired HPO at the time of receiving the mail-kit order.
    None for DV participants
    """

    is_exam_one_order = Column(Boolean, default=False, server_default=expression.false())

    __table_args__ = (UniqueConstraint("participant_id", "order_id", name="uidx_partic_id_order_id"),)


event.listen(BiobankMailKitOrder, "before_insert", model_insert_listener)
event.listen(BiobankMailKitOrder, "before_update", model_update_listener)
