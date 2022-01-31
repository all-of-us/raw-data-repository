from datetime import datetime

import argparse

from rdr_service.dao.mail_kit_order_dao import MailKitOrderDao
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

    def upload_order_to_mayolink(self, barcode):
        dao = MailKitOrderDao()
        with self.get_session() as session:
            mail_kit_order = dao.get_with_barcode(barcode=barcode, session=session)
            if mail_kit_order is None:
                logger.error(f'Unable to find order with barcode "{barcode}"')

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
