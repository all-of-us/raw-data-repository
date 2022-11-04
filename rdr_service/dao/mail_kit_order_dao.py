from datetime import datetime
import json
import logging
import re
from typing import List

import pytz
from sqlalchemy.orm import load_only, Session
from werkzeug.exceptions import BadRequest, Conflict, NotFound

from rdr_service.code_constants import UNMAPPED, UNSET
from rdr_service import clock
from rdr_service.services.mayolink_client import MayoLinkClient, MayoLinkOrder, MayoLinkTest,\
    MayolinkTestPassthroughFields
from rdr_service.api_util import (
    DV_BARCODE_URL,
    DV_FHIR_URL,
    DV_FULFILLMENT_URL,
    DV_ORDER_URL,
    format_json_code,
    format_json_enum,
    get_code_id,
    parse_date,
)
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.fhir_utils import SimpleFhirR4Reader
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample, \
    MayolinkCreateOrderHistory
from rdr_service.model.participant import Participant
from rdr_service.model.utils import to_client_participant_id
from rdr_service.offline.biobank_samples_pipeline import _PMI_OPS_SYSTEM
from rdr_service.participant_enums import BiobankOrderStatus, OrderShipmentStatus, OrderShipmentTrackingStatus,\
    UNSET_HPO_ID


# Timezones for MayoLINK
_UTC = pytz.utc
_US_CENTRAL = pytz.timezone("US/Central")


class MailKitOrderDao(UpdatableDao):
    def __init__(self):
        self.code_dao = CodeDao()
        super(MailKitOrderDao, self).__init__(BiobankMailKitOrder)
        # used for testing
        self.biobank_address = {
            "city": "Rochester",
            "state": "MN",
            "postalCode": "55901",
            "line": ["3050 Superior Drive NW"],
            "type": "postal",
            "use": "work",
        }

    def send_order(self, portal_order_id: int, participant_id: int, collected_time_utc: datetime, report_notes: str):
        order, is_version_two = self._create_mayolink_order(
            participant_id=participant_id,
            portal_order_id=portal_order_id,
            collected_time_utc=collected_time_utc,
            report_notes=report_notes
        )
        mayo = MayoLinkClient(credentials_key='version_two' if is_version_two else 'default')
        response = mayo.post(order)
        return self.to_client_json(response, for_update=True)

    def _create_mayolink_order(self, participant_id: int, portal_order_id: int, collected_time_utc, report_notes: str):
        summary = ParticipantSummaryDao().get(participant_id)
        if not summary:
            raise BadRequest("No summary for participant id: {}".format(participant_id))
        code_dict = summary.asdict()
        format_json_code(code_dict, self.code_dao, "genderIdentityId")
        format_json_code(code_dict, self.code_dao, "stateId")
        if "genderIdentity" in code_dict and code_dict["genderIdentity"]:
            if code_dict["genderIdentity"] == "GenderIdentity_Woman":
                gender_val = "F"
            elif code_dict["genderIdentity"] == "GenderIdentity_Man":
                gender_val = "M"
            else:
                gender_val = "U"
        else:
            gender_val = "U"

        with self.session() as session:
            barcode = session.query(BiobankMailKitOrder.barcode).filter(
                BiobankMailKitOrder.order_id == portal_order_id
            ).scalar()

        # We should have the barcode at this point.
        # Put an error message in the logs if not so this order can be investigated.
        if not barcode:
            logging.error(f'Barcode missing for order {portal_order_id}')

        order_test = MayoLinkTest(
            code='1SAL2',
            name='PMI Saliva, FDA Kit'
        )
        order = MayoLinkOrder(
            collected_datetime_utc=collected_time_utc,
            number=barcode,
            biobank_id=summary.biobankId,
            sex=gender_val,
            address1=summary.streetAddress,
            address2=summary.streetAddress2,
            city=summary.city,
            state=code_dict["state"][-2:] if code_dict["state"] not in (UNMAPPED, UNSET) else '',
            postal_code=str(summary.zipCode),
            phone=str(summary.phoneNumber),
            race=str(summary.race),
            report_notes=report_notes,
            tests=[order_test],
            comments='Salivary Kit Order, direct from participant'
        )

        is_version_two = barcode and len(barcode) > 14
        if is_version_two:
            order_test.passthrough_fields = MayolinkTestPassthroughFields(field1=barcode)

            # The system that the biobank uses to process orders can't take race strings greater than 20 characters
            # and the race data isn't needed for V2 orders
            order.race = None

        return order, is_version_two

    def to_client_json(self, model, for_update=False):
        if for_update:
            result = dict()
            reduced_model = model["orders"]["order"]
            result["biobankStatus"] = reduced_model["status"]
            result["barcode"] = reduced_model["reference_number"]
            result["received"] = reduced_model["received"]
            result["biobankOrderId"] = reduced_model["number"]
            result["biobankTrackingId"] = reduced_model["patient"]["medical_record_number"]
        else:
            result = model.asdict()
            result["orderStatus"] = format_json_enum(result, "orderStatus")
            result["shipmentStatus"] = format_json_enum(result, "shipmentStatus")
            format_json_code(result, self.code_dao, "stateId")
            result["state"] = result["state"][-2:]  # Get the abbreviation
            del result["id"]  # PK for model

        result = {k: v for k, v in list(result.items()) if v is not None}
        if "participantId" in result:
            result["participantId"] = to_client_participant_id(result["participantId"])
        return result

    def from_client_json(self, resource_json, id_=None, expected_version=None, participant_id=None, client_id=None
    ):  # pylint: disable=unused-argument
        """Initial loading of the DV order table does not include all attributes."""
        fhir_resource = SimpleFhirR4Reader(resource_json)
        order = BiobankMailKitOrder(participantId=participant_id)
        order.participantId = participant_id

        if resource_json["resourceType"].lower() == "supplydelivery":
            order.order_id = int(fhir_resource.basedOn[0].identifier.value)
            existing_obj = self.get(self.get_id(order))
            if not existing_obj:
                raise NotFound("existing order record not found")

            # handling of biobankStatus from Mayolink API
            try:
                existing_obj.biobankStatus = resource_json['biobankStatus']
            except KeyError:
                # resource will only have biobankStatus on a PUT
                pass

            existing_obj.shipmentStatus = self._enumerate_order_tracking_status(
                fhir_resource.extension.get(url=DV_FHIR_URL + "tracking-status").valueString
            )
            existing_obj.shipmentCarrier = fhir_resource.extension.get(url=DV_FHIR_URL + "carrier").valueString

            # shipmentEstArrival
            # The fhir_resource.get() method
            # will raise an exception on "expected-delivery-date"
            # if the resource doesn't have that path
            delivery_date_url = [extension.url for extension in fhir_resource["extension"]
                                 if extension.url == DV_FHIR_URL + "expected-delivery-date"]
            if delivery_date_url:
                existing_obj.shipmentEstArrival = parse_date(
                    fhir_resource.extension.get(url=DV_FHIR_URL + "expected-delivery-date").valueDateTime)

            existing_obj.trackingId = fhir_resource.identifier.get(system=DV_FHIR_URL + "trackingId").value
            # USPS status
            existing_obj.orderStatus = self._enumerate_order_shipping_status(fhir_resource.status)
            # USPS status time
            existing_obj.shipmentLastUpdate = parse_date(fhir_resource.occurrenceDateTime)
            order_address = fhir_resource.contained.get(resourceType="Location").get("address")
            address_use = fhir_resource.contained.get(resourceType="Location").get("address").get("use")
            order_address.stateId = get_code_id(order_address, self.code_dao, "state", "State_")
            existing_obj.address = {
                "city": existing_obj.city,
                "state": existing_obj.stateId,
                "postalCode": existing_obj.zipCode,
                "line": [existing_obj.streetAddress1],
            }

            if existing_obj.streetAddress2 is not None and existing_obj.streetAddress2 != "":
                existing_obj.address["line"].append(existing_obj.streetAddress2)

            if address_use.lower() == "home":
                existing_obj.city = order_address.city
                existing_obj.stateId = order_address.stateId
                existing_obj.streetAddress1 = order_address.line[0]
                existing_obj.zipCode = order_address.postalCode

                if len(order_address._obj["line"][0]) > 1:
                    try:
                        existing_obj.streetAddress2 = order_address._obj["line"][1]
                    except IndexError:
                        pass

            elif address_use.lower() == "work":
                existing_obj.biobankCity = order_address.city
                existing_obj.biobankStateId = order_address.stateId
                existing_obj.biobankStreetAddress1 = order_address.line[0]
                existing_obj.biobankZipCode = order_address.postalCode

            if hasattr(fhir_resource, "biobankTrackingId"):
                existing_obj.biobankTrackingId = fhir_resource.biobankTrackingId
                existing_obj.biobankReceived = parse_date(fhir_resource.received)

            return existing_obj

        if resource_json["resourceType"].lower() == "supplyrequest":
            order.order_id = int(fhir_resource.identifier.get(system=DV_FHIR_URL + "orderId").value)
            if id_ and int(id_) != order.order_id:
                raise Conflict("url order id param does not match document order id")

            if hasattr(fhir_resource, "authoredOn"):
                order.order_date = parse_date(fhir_resource.authoredOn)

            order.supplier = fhir_resource.contained.get(resourceType="Organization").id
            order.created = clock.CLOCK.now()
            order.supplierStatus = fhir_resource.extension.get(url=DV_FULFILLMENT_URL).valueString

            fhir_device = fhir_resource.contained.get(resourceType="Device")
            order.itemName = fhir_device.deviceName.get(type="manufacturer-name").name
            order.itemSKUCode = fhir_device.identifier.get(system=DV_FHIR_URL + "SKU").value
            order.itemQuantity = fhir_resource.quantity.value

            fhir_patient = fhir_resource.contained.get(resourceType="Patient")
            fhir_address = fhir_patient.address[0]
            order.streetAddress1 = fhir_address.line[0]
            order.streetAddress2 = "\n".join(fhir_address.line[1:])
            order.city = fhir_address.city
            order.stateId = get_code_id(fhir_address, self.code_dao, "state", "State_")
            order.zipCode = fhir_address.postalCode

            order.orderType = fhir_resource.extension.get(url=DV_ORDER_URL).valueString
            order.is_exam_one_order = order.orderType == 'Exam One Order'

            if id_ is None:
                order.version = 1
            else:
                # A put request may add new attributes
                existing_obj = self.get(self.get_id(order))
                if not existing_obj:
                    raise NotFound("existing order record not found")

                order.id = existing_obj.id
                order.version = expected_version
                if order.supplierStatus.lower() == "shipped":
                    barcode = fhir_resource.extension.get(url=DV_BARCODE_URL).valueString

                    # Put a warning in the logs for later investigation if the barcode looks suspicious
                    barcode_character_count = len(barcode)
                    if barcode_character_count not in (14, 16):
                        logging.warning(
                            f'Potentially invalid barcode provided for order {order.order_id} (barcode: {barcode})'
                        )

                    # remove non-alpha num chars from barcode
                    if barcode and not barcode.isalnum():
                        barcode = re.sub(r'\W+', '', barcode)

                    if len(barcode) > 20:
                        # MayoLINK system expects barcodes to be 20 characters or less
                        raise BadRequest('Given barcode exceeds maximum character length')

                    order.barcode = barcode

            with self.session() as session:
                participant = session.query(Participant).filter(
                    Participant.participantId == participant_id
                ).one_or_none()
                if participant is None:
                    raise NotFound(f'Unable to find participant {to_client_participant_id(participant_id)}')
                elif participant.hpoId != UNSET_HPO_ID:
                    order.associatedHpoId = participant.hpoId

        return order

    def insert_biobank_order(self, participant_id: int, mail_kit_order: BiobankMailKitOrder, session,
                             order_origin_client_id: str, biobank_order_id: str, mayo_reference_number: str,
                             additional_identifiers=None):
        obj = BiobankOrder()
        obj.participantId = participant_id
        obj.created = clock.CLOCK.now()
        obj.finalizedTime = obj.created
        obj.orderStatus = BiobankOrderStatus.UNSET
        obj.biobankOrderId = biobank_order_id
        obj.orderOrigin = order_origin_client_id
        obj.mailKitOrders = [mail_kit_order]
        obj.samples = [BiobankOrderedSample(test="1SAL2", processingRequired=False, description="salivary pilot kit")]
        self._add_identifiers_and_main_id(
            order=obj,
            mayo_reference_number=mayo_reference_number,
            portal_order_id=mail_kit_order.order_id,
            order_origin_client_id=order_origin_client_id,
            additional_identifiers=additional_identifiers
        )
        biobank_order_dao = BiobankOrderDao()
        biobank_order_dao.insert_with_session(session, obj)

    def insert_mayolink_create_order_history(self, participant_id: int, biobank_order_id: str,
                                             mayolink_order_status: str, response_payload, request_payload=None):
        mayolink_create_order_history = MayolinkCreateOrderHistory()
        mayolink_create_order_history.requestParticipantId = participant_id
        mayolink_create_order_history.requestTestCode = '1SAL2'
        mayolink_create_order_history.requestOrderId = biobank_order_id
        mayolink_create_order_history.requestOrderStatus = mayolink_order_status
        try:
            mayolink_create_order_history.requestPayload = json.dumps(request_payload) if request_payload else None
            mayolink_create_order_history.responsePayload = json.dumps(response_payload)
        except TypeError:
            logging.info(f"TypeError when create mayolink_create_order_history")
        biobank_order_dao = BiobankOrderDao()
        biobank_order_dao.insert_mayolink_create_order_history(mayolink_create_order_history)

    def _add_identifiers_and_main_id(self, order, mayo_reference_number: str, order_origin_client_id: str,
                                     portal_order_id: int, additional_identifiers=None):
        try:
            portal_id_system = BiobankMailKitOrder.ID_SYSTEM[order_origin_client_id]
        except KeyError:
            raise BadRequest(
                f"No identifier for clientID {order_origin_client_id}"
            )

        order.identifiers = [
            BiobankOrderIdentifier(system=_PMI_OPS_SYSTEM, value=mayo_reference_number),
            BiobankOrderIdentifier(system=portal_id_system, value=str(portal_order_id))
        ]

        for i in additional_identifiers or []:
            try:
                if i["system"].lower() == DV_FHIR_URL + "trackingid":
                    order.identifiers.append(
                        BiobankOrderIdentifier(system=f"{portal_id_system}/trackingId", value=i["value"])
                    )
            except AttributeError:
                raise BadRequest(f"No identifier for system {portal_id_system}, required for primary key.")

    def get_etag(self, id_, pid):
        with self.session() as session:
            query = session.query(BiobankMailKitOrder.version).filter_by(participantId=pid).filter_by(order_id=id_)
            result = query.first()
            if result:
                return result[0]

        return None

    def _do_update(self, session, obj, existing_obj):  # pylint: disable=unused-argument
        obj.version += 1
        session.merge(obj)

    def get_id(self, obj):
        with self.session() as session:
            query = (
                session.query(BiobankMailKitOrder.id)
                .filter_by(participantId=obj.participantId)
                .filter_by(order_id=obj.order_id)
            )
            return query.first()

    def get_biobank_info(self, order):
        with self.session() as session:
            query = (
                session.query(BiobankMailKitOrder)
                .options(load_only("barcode", "biobankOrderId", "biobankStatus", "biobankReceived"))
                .filter_by(participantId=order.participantId)
                .filter_by(order_id=order.order_id)
            )
            return query.first()

    def _enumerate_order_shipping_status(self, status):
        if status.lower() == "in-progress" or status.lower() == "active":
            return OrderShipmentStatus.SHIPPED
        elif status.lower() == "completed":
            return OrderShipmentStatus.FULFILLMENT
        else:
            return OrderShipmentStatus.UNSET

    def _enumerate_order_tracking_status(self, value):
        if value.lower() == "in_transit":
            return OrderShipmentTrackingStatus.IN_TRANSIT
        elif value.lower() == "delivered":
            return OrderShipmentTrackingStatus.DELIVERED
        else:
            return OrderShipmentTrackingStatus.UNSET

    @classmethod
    def get_with_barcode(cls, barcode, session: Session) -> List[BiobankMailKitOrder]:
        return session.query(BiobankMailKitOrder).filter(BiobankMailKitOrder.barcode == barcode).all()
