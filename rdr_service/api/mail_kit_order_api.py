from dateutil.parser import parse
from flask import request
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import BadRequest, Conflict, MethodNotAllowed

from rdr_service.api.base_api import UpdatableApi
from rdr_service.api_util import PTC, PTC_AND_HEALTHPRO, DV_FHIR_URL, DV_ORDER_URL
from rdr_service.app_util import ObjDict, auth_required, get_oauth_id, get_account_origin_id
from rdr_service.dao.mail_kit_order_dao import MailKitOrderDao
from rdr_service.fhir_utils import SimpleFhirR4Reader
from rdr_service.model.utils import from_client_participant_id
from rdr_service.participant_enums import OrderShipmentTrackingStatus
from rdr_service.services.biobank_order import BiobankOrderService


class MailKitOrderApi(UpdatableApi):
    def __init__(self):
        super(MailKitOrderApi, self).__init__(MailKitOrderDao())

    @staticmethod
    def _lookup_resource_type_method(resource_type_method_map, raw_resource):
        if not isinstance(raw_resource, dict):
            raise BadRequest("invalid FHIR resource")
        try:
            resource_type = raw_resource["resourceType"]
        except KeyError:
            raise BadRequest("payload is missing resourceType")
        try:
            return resource_type_method_map[resource_type]
        except KeyError:
            raise MethodNotAllowed("Method not allowed for resource type {}".format(resource_type))

    @auth_required(PTC)
    def post(self):
        try:
            resource = request.get_json(force=True)
            user_email = get_oauth_id()
            resource['auth_user'] = user_email
        except BadRequest:
            raise BadRequest("missing FHIR resource")

        method = self._lookup_resource_type_method(
            {
                "SupplyRequest": self._post_supply_request,
                "SupplyDelivery": self._post_supply_delivery
            }, resource
        )
        return method(resource)

    def _to_mayo(self, fhir):
        """
        Test to see if this Supply Delivery object is going to Mayo or not
        :param fhir: fhir supply delivery object
        :return: True if destination address is Mayo, otherwise False
        """
        if not fhir or fhir.resourceType != "SupplyDelivery":
            raise ValueError("Argument must be a Supply Delivery FHIR object")

        # check shipping address is Mayo's address
        # for item in fhir.contained:
        #   if item.resourceType == 'Location' and item.address.city == 'Rochester' \
        #             and item.address.state == 'MN' and \
        #             '55901' in item.address.postalCode and item.address.line[0] == '3050 Superior Drive NW':
        #     return True

        # check tracking numbers for mayo.
        tid_to_mayo = None
        if hasattr(fhir, "identifier"):
            for item in fhir.identifier:
                if "trackingId" in item.system:
                    tid_to_mayo = item.value

        if tid_to_mayo:
            return True
        else:
            return False

    def _post_supply_delivery(self, resource):
        try:
            fhir = SimpleFhirR4Reader(resource)
            patient = fhir.patient
            patient_id_obj = patient.identifier
            participant_id_int = from_client_participant_id(patient_id_obj.value)
            bo_id = fhir.basedOn[0].identifier.value
            portal_order_id = self.dao.get_id(
                ObjDict({"participantId": participant_id_int, "order_id": int(bo_id)})
            )
            tracking_status = fhir.extension.get(
                url=DV_FHIR_URL + "tracking-status"
            ).valueString.lower()
        except AttributeError as e:
            raise BadRequest(e)
        except Exception as e:
            raise BadRequest(e)

        if not portal_order_id:
            raise Conflict(
                "Existing SupplyRequest for order required for SupplyDelivery"
            )
        dvo = self.dao.get(portal_order_id)
        if not dvo:
            raise Conflict(
                "Existing SupplyRequest for order required for SupplyDelivery"
            )

        merged_resource = None
        # Note: POST tracking status should be either 'enroute/in_transit'. PUT should only be 'delivered'.
        if (
            tracking_status in ["in_transit", "enroute", "delivered"]
            and self._to_mayo(fhir)
            and not dvo.biobankOrderId
        ):
            BiobankOrderService.post_mailkit_order_delivery(
                mailkit_order=dvo,
                collected_time_utc=parse(fhir.occurrenceDateTime),
                order_origin_client_id=get_account_origin_id(),
                report_notes=fhir.extension.get(url=DV_ORDER_URL).valueString,
                request_json=resource
            )

        response = super(MailKitOrderApi, self)\
            .put(
                bo_id,
                participant_id=participant_id_int,
                skip_etag=True,
                resource=merged_resource
            )

        response[2]["Location"] = f"/rdr/v1/SupplyDelivery/{bo_id}"
        response[2]['auth_user'] = resource['auth_user']
        if response[1] == 200:
            created_response = list(response)
            created_response[1] = 201
            return tuple(created_response)
        return response

    def _post_supply_request(self, resource):
        """
        Return response when POSTed to /SupplyRequest, handling scenarios where partners POST again after receiving 502.

        From October 2023, partner reported intermittent 502 errors when POSTing to /SupplyRequest.
        Even though we saved their data in our DB on their receiving a 502, they weren't aware of this.
        Due to their uncertainty about the first POST's success, they'd POST again with an updated payload,
        to which we'd respond with a 500, since we expect them to send a PUT when updating. The try/except is
        workaround: if they POST again with the same order_id after they get 502, we treat the subsequent POST
        as a PUT, updating the existing resource with their new payload. For more details, refer to ROC-1740.
        """
        fhir_resource = SimpleFhirR4Reader(resource)
        patient = fhir_resource.contained.get(resourceType="Patient")
        pid = patient.identifier.get(system=DV_FHIR_URL + "participantId").value
        p_id = from_client_participant_id(pid)
        order_id = fhir_resource.identifier.get(system=DV_FHIR_URL + "orderId").value
        try:
            response = super(MailKitOrderApi, self).post(participant_id=p_id)
        except IntegrityError as e:
            constraint_name = "uidx_partic_id_order_id"
            if constraint_name in str(e):
                # Catch error due to repeated POSTs with the same order_id for a pid.
                # Treat as a PUT, since 'supplier_status' in the payload is updated, as seen in logs.
                response = self.put(bo_id=order_id)
            else:
                raise Conflict(e.orig)

        response[2]["Location"] = "/rdr/v1/SupplyRequest/{}".format(order_id)
        response[2]['auth_user'] = resource['auth_user']
        if response[1] == 200:
            created_response = list(response)
            created_response[1] = 201
            return tuple(created_response)
        return response

    @auth_required(PTC_AND_HEALTHPRO)
    def get(self, p_id=None, order_id=None):  # pylint: disable=unused-argument

        if not p_id:
            raise BadRequest("invalid participant id")
        if not order_id:
            raise BadRequest("must include order ID to retrieve DV orders.")

        pk = {"participant_id": p_id, "order_id": order_id}
        obj = ObjDict(pk)
        id_ = self.dao.get_id(obj)[0]

        return super(MailKitOrderApi, self).get(id_=id_, participant_id=p_id)

    @auth_required(PTC)
    def put(self, bo_id=None):  # pylint: disable=unused-argument

        if bo_id is None:
            raise BadRequest("invalid order id")
        try:
            resource = request.get_json(force=True)
        except BadRequest:
            raise BadRequest("missing FHIR order document")

        method = self._lookup_resource_type_method(
            {
                "SupplyRequest": self._put_supply_request,
                "SupplyDelivery": self._put_supply_delivery
            },
            resource
        )
        return method(resource, bo_id)

    def _put_supply_request(self, resource, bo_id):

        # handle invalid FHIR documents
        try:
            fhir_resource = SimpleFhirR4Reader(resource)
            pid = fhir_resource.contained.get(resourceType="Patient").identifier.get(
                system=DV_FHIR_URL + "participantId"
            )
            p_id = from_client_participant_id(pid.value)
        except AttributeError as e:
            raise BadRequest(str(e))
        except Exception as e:
            raise BadRequest(str(e))

        if not p_id:
            raise BadRequest("Request must include participant id")
        response = super(MailKitOrderApi, self).put(bo_id, participant_id=p_id, skip_etag=True)

        return response

    def _put_supply_delivery(self, resource, bo_id):
        # handle invalid FHIR documents
        try:
            fhir = SimpleFhirR4Reader(resource)
            participant_id = fhir.patient.identifier.value
            p_id = from_client_participant_id(participant_id)
            update_time = parse(fhir.occurrenceDateTime)
            carrier_name = fhir.extension.get(
                url=DV_FHIR_URL + "carrier"
            ).valueString

            eta = None
            if hasattr(fhir["extension"], DV_FHIR_URL + "expected-delivery-date"):
                eta = parse(
                    fhir.extension.get(
                        url=DV_FHIR_URL + "expected-delivery-date"
                    ).valueDateTime
                )

            tracking_status = fhir.extension.get(
                url=DV_FHIR_URL + "tracking-status"
            ).valueString
            if tracking_status:
                tracking_status = tracking_status.lower()
        except AttributeError as e:
            raise BadRequest(str(e))
        except Exception as e:
            raise BadRequest(str(e))

        _id = self.dao.get_id(ObjDict({"participantId": p_id, "order_id": int(bo_id)}))
        if not _id:
            raise Conflict(
                "Existing SupplyRequest for order required for SupplyDelivery"
            )
        dvo = self.dao.get(_id)
        if not dvo:
            raise Conflict(
                "Existing SupplyRequest for order required for SupplyDelivery"
            )

        tracking_status_enum = getattr(
            OrderShipmentTrackingStatus,
            tracking_status.upper(),
            OrderShipmentTrackingStatus.UNSET,
        )

        dvo.shipmentLastUpdate = update_time.date()
        dvo.shipmentCarrier = carrier_name
        if eta:
            dvo.shipmentEstArrival = eta.date()
        dvo.shipmentStatus = tracking_status_enum
        if not p_id:
            raise BadRequest("Request must include participant id")

        response = super(MailKitOrderApi, self).put(
            bo_id, participant_id=p_id, skip_etag=True, resource=resource
        )
        return response


def _make_response(self, obj):
    result = super(MailKitOrderApi, self)._make_response(obj)
    etag = super(MailKitOrderApi, self)._make_etag(obj.version)
    return result, 201, {"ETag": etag}
