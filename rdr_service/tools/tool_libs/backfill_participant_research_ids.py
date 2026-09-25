import argparse

from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from rdr_service.dao.participant_research_ids_dao import ParticipantResearchIdsDao

tool_cmd = 'backfill-research-ids'
tool_desc = 'Backfill the ParticipantResearchIds table with random research IDs. '


class ResearchIdBackfillTool(ToolBase):
    def run(self):
        super().run()
        id_map_dao = ParticipantResearchIdsDao()
        if self.args.backfill_missing_controlled_tier_plus_ids:
            missing_ids = id_map_dao.get_participants_missing_controlled_tier_plus_ids(self.args.participant_count)
            id_map_dao.insert_missing_controlled_tier_plus_ids(missing_ids)
        else:
            participants = id_map_dao.get_new_participants(self.args.participant_count)
            id_map_dao.insert_new_participants(participants)

def add_additional_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        '--participant-count',
        type=int,
        help='Number of participants to backfill',
        required=True
    )
    parser.add_argument(
        '--backfill-missing-controlled-tier-plus-ids',
        action='store_true',
        help='Generate controlled_tier_plus_id for participants that already have participant_research_ids rows'
    )


def run():
    return cli_run(tool_cmd, tool_desc, ResearchIdBackfillTool, add_additional_arguments)
