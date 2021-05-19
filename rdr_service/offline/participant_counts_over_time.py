import datetime
import logging

from rdr_service import clock
from rdr_service.dao.participant_counts_over_time_service import ParticipantCountsOverTimeService
from rdr_service.participant_enums import MetricsCronJobStage


def calculate_participant_metrics():
    stage_one_start_date = clock.CLOCK.now().date() - datetime.timedelta(days=30)
    stage_one_end_date = clock.CLOCK.now().date() + datetime.timedelta(days=10)
    # the first participant is registered at 2017-05-31
    stage_two_start_date = datetime.datetime.strptime("2017-05-30", "%Y-%m-%d").date()
    stage_two_end_date = clock.CLOCK.now().date() - datetime.timedelta(days=31)

    # call metrics functions
    service = ParticipantCountsOverTimeService()
    service.init_tmp_table()
    # calculate data in last 30 days
    service.refresh_metrics_cache_data(stage_one_start_date, stage_one_end_date, MetricsCronJobStage.STAGE_ONE)
    logging.info('calculate participant metrics stage one is done.')
    # calculate data earlier than 30 days
    service.refresh_metrics_cache_data(stage_two_start_date, stage_two_end_date, MetricsCronJobStage.STAGE_TWO)
    logging.info('calculate participant metrics stage two is done.')
    service.clean_tmp_tables()
