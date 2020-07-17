#! /bin/env python
#
# Finalize un-finalized biobank orders with an input file that gives finalized times
#

import argparse
import csv
import logging
import sys

from rdr_service.dao import database_factory
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderHistory, BiobankOrderedSampleHistory
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.utils import from_client_participant_id
from rdr_service.participant_enums import OrderStatus
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "finalize-orders"
tool_desc = "Finalize un-finalized biobank orders with an input file that gives finalized times."


class FinalizeOrdersClass(object):
    def __init__(self, args, gcp_env):
        self.args = args
        self.gcp_env = gcp_env

    @staticmethod
    def load_biobank_order(session, participant_id, mayolink_id):
        has_error = False

        biobank_order = session.query(BiobankOrder).filter(
            BiobankOrder.biobankOrderId == mayolink_id
        ).one_or_none()
        if biobank_order is not None and biobank_order.participantId != participant_id:
            print('Validation Error: Participant id mis-matched on biobank order')
            has_error = True

        return biobank_order, has_error

    @staticmethod
    def finalize_biobank_order(session, biobank_order: BiobankOrder, finalized_datetime):
        biobank_order.finalizedTime = finalized_datetime

        biobank_order_history = session.query(BiobankOrderHistory).filter(
            BiobankOrderHistory.biobankOrderId == biobank_order.biobankOrderId,
            BiobankOrderHistory.version == biobank_order.version
        ).one()  # I'm expecting each order to be backed by a history object
        biobank_order_history.finalizedTime = finalized_datetime

    @staticmethod
    def finalize_biobank_ordered_samples(session, biobank_order: BiobankOrder,
                                         participant_summary: ParticipantSummary, finalized_datetime):
        for sample in biobank_order.samples:
            sample.finalized = finalized_datetime

            ordered_sample_history = session.query(BiobankOrderedSampleHistory).filter(
                BiobankOrderedSampleHistory.biobankOrderId == biobank_order.biobankOrderId,
                BiobankOrderedSampleHistory.test == sample.test
            ).order_by(BiobankOrderedSampleHistory.version.desc()).first()
            ordered_sample_history.finalized = finalized_datetime

            order_status_field = 'sampleOrderStatus' + sample.test
            if hasattr(participant_summary, order_status_field):
                setattr(participant_summary, order_status_field, OrderStatus.FINALIZED)
                setattr(participant_summary, order_status_field + 'Time', finalized_datetime)

    def run(self):
        proxy_pid = self.gcp_env.activate_sql_proxy()
        if not proxy_pid:
            _logger.error("activating google sql proxy failed.")
            return 1

        with open(self.args.input_file) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            with database_factory.make_server_cursor_database().session() as session:
                for csv_record in csv_reader:
                    participant_id = from_client_participant_id(csv_record['Participant ID'])
                    order_mayolink_id = csv_record['MayoLINK ID']

                    biobank_order, has_error = self.load_biobank_order(session, participant_id, order_mayolink_id)
                    if biobank_order is None:
                        print('Biobank order not found')
                        has_error = True

                    participant_summary = session.query(ParticipantSummary).filter(
                        ParticipantSummary.participantId == participant_id
                    ).one_or_none()
                    if participant_summary is None:
                        print('Participant summary not found')
                        has_error = True

                    if has_error:
                        print('record causing the above error:', csv_record)
                    else:
                        finalized_datetime = csv_record['Finalized Time (UTC)']
                        self.finalize_biobank_order(session, biobank_order, finalized_datetime)
                        self.finalize_biobank_ordered_samples(session, biobank_order, participant_summary,
                                                              finalized_datetime)

                    session.commit()  # Finish finalizing current order

        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument('--input-file', required=True, help='File providing Biobank order IDs and finalization times')

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = FinalizeOrdersClass(args, gcp_env)
        return process.run()


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
