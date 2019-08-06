from rdr_service.dao.participant_counts_over_time_service import ParticipantCountsOverTimeService


def calculate_participant_metrics():
    # call metrics functions
    service = ParticipantCountsOverTimeService()
    service.refresh_metrics_cache_data()
