from typing import List

from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.sample_order_status import SampleOrderStatus
from rdr_service.model.sample_receipt_status import SampleReceiptStatus
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'sample-status-backfill'
tool_desc = 'Backfill the sample status tables from data present in participant summary'


class SampleStatusBackfillTool(ToolBase):
    def run(self):
        super().run()

        with self.get_session() as session:
            last_participant_id = 0
            summary_list = self.get_next_summary_batch(session, last_participant_id)

            while summary_list:
                last_participant_id = summary_list[-1].participantId
                for summary in summary_list:
                    self.migrate_statuses(session, summary)

                session.commit()
                print(f'completed batch ending with P{last_participant_id}')

                summary_list = self.get_next_summary_batch(session, last_participant_id)

    @classmethod
    def get_next_summary_batch(cls, session, last_participant_id) -> List[ParticipantSummary]:
        return (
            session.query(ParticipantSummary)
            .filter(ParticipantSummary.participantId > last_participant_id)
            .order_by(ParticipantSummary.participantId)
            .limit(500).all()
        )

    def migrate_statuses(self, session, participant_summary: ParticipantSummary):
        for test_code in [
            '1SST8', '2SST8', '1SS08', '1PST8', '2PST8',
            '1PS08', '1PS4A', '1PS4B', '2PS4A', '2PS4B',
            '1HEP4', '1ED04', '1ED10', '2ED10', '1UR10',
            '1UR90', '1SAL', '1SAL2', '1ED02', '1CFD9',
            '1PXR2', 'DV1SAL2', '2ED02', '2ED04', '2SAL0'
        ]:
            receipt_status_value = getattr(participant_summary, f'sampleStatus{test_code}')
            order_status_value = getattr(participant_summary, f'sampleOrderStatus{test_code}')

            if (
                receipt_status_value
                and not self.db_has_receipt_status(session, participant_summary.participantId, test_code)
            ):
                receipt_status_time = getattr(participant_summary, f'sampleStatus{test_code}Time')
                self.insert_receipt_status(
                    session, participant_summary.participantId, test_code, receipt_status_value, receipt_status_time
                )

            if (
                order_status_value
                and not self.db_has_order_status(session, participant_summary.participantId, test_code)
            ):
                order_status_time = getattr(participant_summary, f'sampleOrderStatus{test_code}Time')
                self.insert_order_status(
                    session, participant_summary.participantId, test_code, order_status_value, order_status_time
                )

    def db_has_receipt_status(self, session, participant_id, code):
        status = session.query(SampleReceiptStatus).filter(
            SampleReceiptStatus.participant_id == participant_id,
            SampleReceiptStatus.test_code == code
        ).one_or_none()
        return status is not None

    def insert_receipt_status(self, session, participant_id, code, status, timestamp):
        session.add(
            SampleReceiptStatus(
                participant_id=participant_id,
                test_code=code,
                status=status,
                status_time=timestamp
            )
        )

    def db_has_order_status(self, session, participant_id, code):
        status = session.query(SampleOrderStatus).filter(
            SampleOrderStatus.participant_id == participant_id,
            SampleOrderStatus.test_code == code
        ).one_or_none()
        return status is not None

    def insert_order_status(self, session, participant_id, code, status, timestamp):
        session.add(
            SampleOrderStatus(
                participant_id=participant_id,
                test_code=code,
                status=status,
                status_time=timestamp
            )
        )


def run():
    return cli_run(tool_cmd, tool_desc, SampleStatusBackfillTool)
