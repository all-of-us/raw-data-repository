
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.services.retention_calculation import RetentionEligibility
from rdr_service.participant_enums import RetentionType


class RetentionEligibleMetricsDao(UpdatableDao):

    def __init__(self):
        super(RetentionEligibleMetricsDao, self).__init__(RetentionEligibleMetrics, order_by_ending=["id"])

    @classmethod
    def find_metric(cls, metrics_obj: RetentionEligibleMetrics, retention_data_cache) -> (int, bool):
        """
        Used to check the db for existing metrics objects for a participant. If a metrics object exists, then its
        id is returned as the first value. The second parameter is used to indicate whether the database has the
        same values, returning True if an upsert is needed to bring the new data into the database.
        """

        db_result = retention_data_cache.get(metrics_obj.participantId)

        if not db_result:
            return None, True

        is_same_data = (
            db_result.retentionEligibleStatus == metrics_obj.retentionEligibleStatus
            and (
                db_result.retentionType == metrics_obj.retentionType
                or (
                    db_result.retentionType is None
                    and metrics_obj.retentionType == RetentionType.UNSET
                )
            ) and db_result.retentionEligibleTime == metrics_obj.retentionEligibleTime
            and db_result.lastActiveRetentionActivityTime == metrics_obj.lastActiveRetentionActivityTime
        )

        # DA-3966 RDR values may not have been calculated yet using latest business logic.  Recognize additional
        # situations where the record should be forced through an update/RDR value recalculation
        needs_update = not is_same_data or (db_result.retentionEligible and not db_result.rdr_retention_eligible)
        if not needs_update and db_result.lastActiveRetentionActivityTime:
            # Additional check: It's expected that RDR last activity date calculated with latest logic could be ahead
            # of PTSC's (e.g., only RDR calculations included EtM responses), but RDR values shouldn't be lagging
            if (not db_result.rdr_last_retention_activity_time or
                  db_result.lastActiveRetentionActivityTime.date() > db_result.rdr_last_retention_activity_time.date()):
                needs_update = True

        return db_result.id, needs_update  # returning the id and whether an update is needed

    @classmethod
    def get_existing_record(cls, participant_id, session) -> RetentionEligibleMetrics:
        return session.query(RetentionEligibleMetrics).filter(
            RetentionEligibleMetrics.participantId == participant_id
        ).first()

    @classmethod
    def upsert_retention_data(cls, participant_id, retention_data: RetentionEligibility, session):
        db_record = cls.get_existing_record(participant_id=participant_id, session=session)

        if not db_record:
            db_record = RetentionEligibleMetrics(participantId=participant_id)
            session.add(db_record)

        is_same_data = (
            db_record.rdr_retention_eligible == retention_data.is_eligible
            and db_record.rdr_retention_eligible_time == retention_data.retention_eligible_date
            and db_record.rdr_last_retention_activity_time == retention_data.last_active_retention_date
            and db_record.rdr_is_actively_retained == retention_data.is_actively_retained
            and db_record.rdr_is_passively_retained == retention_data.is_passively_retained
        )
        if not is_same_data:
            db_record.rdr_retention_eligible = retention_data.is_eligible
            db_record.rdr_retention_eligible_time = retention_data.retention_eligible_date
            db_record.rdr_last_retention_activity_time = retention_data.last_active_retention_date
            db_record.rdr_is_actively_retained = retention_data.is_actively_retained
            db_record.rdr_is_passively_retained = retention_data.is_passively_retained

            # update participant summary on the session
            ParticipantSummaryDao.update_with_retention_data(
                participant_id=participant_id,
                retention_data=retention_data,
                session=session
            )
            session.commit()

    def upsert_all_with_session(self, session, retention_eligible_metrics_records):
        update_queue = list()
        for record in retention_eligible_metrics_records:
            session.merge(record)
            # Batch up records for resource rebuilding.
            update_queue.append(int(record.participantId))

        session.commit()
        return len(update_queue)
