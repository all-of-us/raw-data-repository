from datetime import datetime
from typing import Optional

import argparse
from sqlalchemy.orm import Session

from rdr_service.dao.mail_kit_order_dao import MailKitOrderDao
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.participant import Participant
from rdr_service.services.biobank_order import BiobankOrderService
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase, logger

tool_cmd = 'biobank_orders'
tool_desc = 'Utility script for managing biobank orders'


class BiobankOrdersTool(ToolBase):
    def run(self):
        super(BiobankOrdersTool, self).run()

        if self.args.command == 'send-to-mayolink':
            self.upload_order_to_mayolink(barcode=self.args.barcode)

    def _load_mail_kit(self, barcode: str, session: Session) -> Optional[BiobankMailKitOrder]:
        mail_kit_orders = MailKitOrderDao.get_with_barcode(barcode=barcode, session=session)
        if not mail_kit_orders:
            logger.error(f'Unable to find order with barcode "{barcode}"')
            return None
        elif len(mail_kit_orders) > 1:
            logger.error(f'Found too many orders with barcode "{barcode}"')
            return None
        else:
            return mail_kit_orders[0]

    def upload_order_to_mayolink(self, barcode):
        with self.get_session() as session:
            mail_kit_order = self._load_mail_kit(barcode=barcode, session=session)
            if mail_kit_order is None:
                logger.error('Unable to send order')
                return

            participant_origin = session.query(Participant.participantOrigin).filter(
                Participant.participantId == mail_kit_order.participantId
            ).scalar()

        logger.info(f'Posting order {mail_kit_order.order_id} to MayoLINK...')
        BiobankOrderService.post_mailkit_order_delivery(
            mailkit_order=mail_kit_order,
            collected_time_utc=datetime.utcnow(),
            order_origin_client_id=participant_origin,
            report_notes=mail_kit_order.orderType
        )
        logger.info(f'Order sent to MayoLINK.')


def add_additional_arguments(parser: argparse.ArgumentParser):
    subparsers = parser.add_subparsers(dest='command', required=True)

    mayolink_parser = subparsers.add_parser('send-to-mayolink')
    mayolink_parser.add_argument(
        '--barcode',
        help='The barcode for the order to push to MayoLINK'
    )


def run():
    return cli_run(tool_cmd, tool_desc, BiobankOrdersTool, add_additional_arguments)
