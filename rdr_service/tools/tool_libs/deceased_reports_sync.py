from dateutil import parser

from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'deceased-sync'
tool_desc = 'Sync deceased reports from Redcap to an environment'

import csv
from datetime import date
from dataclasses import dataclass
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import RetentionStatus, RetentionType
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao


@dataclass
class RetentionData:
    is_eligible: bool
    eligible_date: date or None
    is_actively_retained: bool
    is_passively_retained: bool


class DeceasedSyncTool(ToolBase):
    @classmethod
    def _read_date(cls, date_str):
        if date_str == 'NULL':
            return None
        else:
            return parser.parse(date_str).date()

    @classmethod
    def _retention_type_matches(cls, our_type, scott_active, scott_passive):
        if our_type == RetentionType.UNSET:
            return scott_active == False and scott_passive == False
        elif our_type == RetentionType.ACTIVE:
            return scott_active == True and scott_passive == False
        elif our_type == RetentionType.PASSIVE:
            return scott_active == False and scott_passive == True
        elif our_type == RetentionType.ACTIVE_AND_PASSIVE:
            return scott_active == True and scott_passive == True

    def validate_data(self, data):
        with self.get_session() as session:
            for participant_id, data in data.items():
                summary = session.query(ParticipantSummary).filter(
                    ParticipantSummary.participantId == participant_id
                ).one_or_none()
                if summary:
                    is_eligible = summary.retentionEligibleStatus == RetentionStatus.ELIGIBLE
                    eligible_date = summary.retentionEligibleTime.date() if summary.retentionEligibleTime else None
                    retention_type = ParticipantSummaryDao.calculate_retention_type(summary)
                else:
                    is_eligible = False
                    eligible_date = None
                    retention_type = RetentionType.UNSET

                diff_message = ''
                if is_eligible != data.is_eligible:
                    diff_message += f'| status {summary.retentionEligibleStatus} != {data.is_eligible} |'
                if eligible_date != data.eligible_date:
                    diff_message += f'| date {eligible_date} != {data.eligible_date}'
                if not self._retention_type_matches(
                    retention_type, data.is_actively_retained, data.is_passively_retained
                ):
                    diff_message += f'| type {retention_type} != ' \
                                    f'{"active" if data.is_actively_retained else "not_active"} and ' \
                                    f'{"passive" if data.is_passively_retained else "not_passive"}'

                if diff_message != '':
                    print(f'error with P{participant_id}: ', diff_message)

    def run(self):
        super(DeceasedSyncTool, self).run()

        participant_data = {}

        batch_count = 0
        with open('/Users/skaggskd/Downloads/2021-06-08_retention_and_ubr_data.csv', 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                participant_id = int(row['participant_id'])
                participant_data[participant_id] = RetentionData(
                    is_eligible=row['retention_eligible'] == '1',
                    eligible_date=self._read_date(row['retention_eligible_date']),
                    is_actively_retained=row['actively_retained'] == '1',
                    is_passively_retained=row['passively_retained'] == '1'
                )

                batch_count += 1
                if batch_count > 1000:
                    self.validate_data(participant_data)

                    participant_data = {}
                    batch_count = 0


def run():
    return cli_run(tool_cmd, tool_desc, DeceasedSyncTool)
