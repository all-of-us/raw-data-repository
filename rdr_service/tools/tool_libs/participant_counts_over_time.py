import datetime

from rdr_service import clock
from rdr_service.dao.participant_counts_over_time_service import ParticipantCountsOverTimeService
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from rdr_service.participant_enums import MetricsCronJobStage

tool_cmd = 'participant-counts-over-time'
tool_desc = 'Test updating Participant Counts Over Time Queries to use BQ'

class ParticipantCountsOverTime(ToolBase):

    def __init__(self, args, gcp_env=None, tool_name=None, replica=False):
        super().__init__(args, gcp_env, tool_name, replica)

    def run(self):
        super(ParticipantCountsOverTime, self).run()

        stage_one_start_date = clock.CLOCK.now().date() - datetime.timedelta(days=30)
        stage_one_end_date = clock.CLOCK.now().date() + datetime.timedelta(days=10)
        # the first participant is registered at 2017-05-31
        stage_two_start_date = datetime.datetime.strptime("2017-05-30", "%Y-%m-%d").date()
        stage_two_end_date = clock.CLOCK.now().date() - datetime.timedelta(days=31)

        data_sync = ParticipantCountsOverTimeService()

        # Delete the temp tables
        data_sync.clean_tmp_tables()

        # Create the temp tables
        data_sync.init_tmp_table()  # Should have ~ 1 million records

        # calculate data in last 30 days
        data_sync.refresh_metrics_cache_data(stage_one_start_date, stage_one_end_date, MetricsCronJobStage.STAGE_ONE)

        # calculate data earlier than 30 days
        data_sync.refresh_metrics_cache_data(stage_two_start_date, stage_two_end_date, MetricsCronJobStage.STAGE_TWO)

        # Delete the temp tables
        data_sync.clean_tmp_tables()


def run():
    cli_run(tool_cmd, tool_desc, ParticipantCountsOverTime)
