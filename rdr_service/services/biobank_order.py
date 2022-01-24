from datetime import datetime
import logging

from dateutil.parser import parse

from rdr_service.dao.mail_kit_order_dao import MailKitOrderDao
from rdr_service.dao.participant_dao import raise_if_withdrawn
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.participant_summary import ParticipantSummary


class BiobankOrderService:
    @classmethod
    def post_mailkit_order_delivery(cls, mailkit_order: BiobankMailKitOrder, collected_time_utc: datetime,
                                    order_origin_client_id: str, report_notes: str, request_json: dict = None):
        summary_dao = ParticipantSummaryDao()
        mailkit_dao = MailKitOrderDao()
        with summary_dao.session() as session:
            summary: ParticipantSummary = summary_dao.get_for_update(
                session=session,
                obj_id=mailkit_order.participantId
            )
            if summary is not None:  # Later code handles the error if it's missing
                raise_if_withdrawn(summary)

            logging.info(f"Sending salivary order to biobank for participant: {summary.participantId}")
            mayolink_response = mailkit_dao.send_order(
                participant_id=summary.participantId,
                portal_order_id=mailkit_order.order_id,
                collected_time_utc=collected_time_utc,
                report_notes=report_notes
            )
            mayo_order_id = mayolink_response['biobankOrderId']

            mailkit_dao.insert_biobank_order(
                participant_id=summary.participantId,
                mail_kit_order=mailkit_order,
                session=session,
                order_origin_client_id=order_origin_client_id,
                biobank_order_id=mayo_order_id,
                mayo_reference_number=mayolink_response['barcode'],
                additional_identifiers=request_json.get('identifier') if request_json else None
            )
            mailkit_dao.insert_mayolink_create_order_history(
                participant_id=summary.participantId,
                biobank_order_id=mayo_order_id,
                mayolink_order_status=mayolink_response['biobankStatus'],
                request_payload=request_json,
                response_payload=mayolink_response
            )

            mailkit_order.biobankStatus = mayolink_response['biobankStatus']
            mailkit_order.biobankReceived = parse(mayolink_response['received'])
            mailkit_order.biobankOrderId = mayo_order_id
            mailkit_order.biobankTrackingId = mayolink_response['biobankTrackingId']
