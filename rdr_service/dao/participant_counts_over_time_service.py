import datetime
import logging
import warnings

from werkzeug.exceptions import BadRequest

from rdr_service.dao.base_dao import BaseDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.metrics_cache_dao import (
    MetricsAgeCacheDao,
    MetricsCacheJobStatusDao,
    MetricsEnrollmentStatusCacheDao,
    MetricsGenderCacheDao,
    MetricsLanguageCacheDao,
    MetricsLifecycleCacheDao,
    MetricsRaceCacheDao,
    MetricsRegionCacheDao,
    MetricsParticipantOriginCacheDao
)
from rdr_service.model.metrics_cache import MetricsCacheJobStatus
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import (
    EnrollmentStatus,
    EnrollmentStatusV2,
    MetricsAPIVersion,
    MetricsCacheType,
    Stratifications,
    TEST_EMAIL_PATTERN,
    TEST_HPO_NAME,
    WithdrawalStatus,
    MetricsCronJobStage
)
from rdr_service.dao.metrics_cache_dao import TEMP_TABLE_PREFIX


class ParticipantCountsOverTimeService(BaseDao):
    def __init__(self):
        super(ParticipantCountsOverTimeService, self).__init__(ParticipantSummary, alembic=True)
        self.test_hpo_id = HPODao().get_by_name(TEST_HPO_NAME).hpoId
        self.test_email_pattern = TEST_EMAIL_PATTERN
        self.start_date = datetime.datetime.strptime("2017-05-30", "%Y-%m-%d").date()
        self.end_date = datetime.datetime.now().date() + datetime.timedelta(days=10)
        self.stage_number = MetricsCronJobStage.STAGE_ONE
        self.cronjob_time = datetime.datetime.now().replace(microsecond=0)

    def init_tmp_table(self):
        with self.session() as session:
            hpo_dao = HPODao()
            hpo_list = hpo_dao.get_all()
            for hpo in hpo_list:
                if hpo.hpoId == self.test_hpo_id:
                    continue
                temp_table_name = TEMP_TABLE_PREFIX + str(hpo.hpoId)
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    session.execute('DROP TABLE IF EXISTS {};'.format(temp_table_name))
                # generated columns can not be inserted any value, need to drop them
                exclude_columns = [
                    'health_data_stream_sharing_status_v_3_1',
                    'health_data_stream_sharing_status_v_3_1_time',
                    'retention_eligible_time',
                    'retention_eligible_status',
                    'was_ehr_data_available'
                ]
                session.execute('CREATE TABLE {} LIKE participant_summary'.format(temp_table_name))

                indexes_cursor = session.execute('SHOW INDEX FROM {}'.format(temp_table_name))
                for exclude_column_name in exclude_columns:
                    session.execute('ALTER TABLE {} DROP COLUMN  {}'.format(temp_table_name, exclude_column_name))

                index_name_list = []
                for index in indexes_cursor:
                    index_name_list.append(index[2])
                index_name_list = list(set(index_name_list))

                for index_name in index_name_list:
                    if index_name != 'PRIMARY':
                        session.execute('ALTER TABLE {} DROP INDEX  {}'.format(temp_table_name, index_name))

                # The ParticipantSummary table requires these, but there may not be a participant_summary for
                # all participants that we insert
                session.execute('ALTER TABLE {} MODIFY first_name VARCHAR(255)'.format(temp_table_name))
                session.execute('ALTER TABLE {} MODIFY last_name VARCHAR(255)'.format(temp_table_name))
                session.execute('ALTER TABLE {} MODIFY suspension_status SMALLINT'.format(temp_table_name))
                session.execute('ALTER TABLE {} MODIFY participant_origin VARCHAR(80)'.format(temp_table_name))
                session.execute('ALTER TABLE {} MODIFY deceased_status SMALLINT'.format(temp_table_name))
                session.execute('ALTER TABLE {} MODIFY is_ehr_data_available TINYINT(1)'.format(temp_table_name))
                session.execute('ALTER TABLE {} MODIFY was_participant_mediated_ehr_available TINYINT(1)'
                                 .format(temp_table_name))

                columns_cursor = session.execute('SELECT * FROM {} LIMIT 0'.format(temp_table_name))

                participant_fields = ['participant_id', 'biobank_id', 'sign_up_time', 'withdrawal_status',
                                      'hpo_id', 'organization_id', 'site_id', 'participant_origin']

                def get_field_name(name):
                    if name in participant_fields:
                        return 'p.' + name
                    else:
                        return 'ps.' + name

                columns = map(get_field_name, columns_cursor.keys())
                columns_str = ','.join(columns)

                participant_sql = """
                  INSERT INTO
                  """ + temp_table_name + """
                  SELECT
                  """ + columns_str + """
                  FROM participant p
                  left join participant_summary ps on p.participant_id = ps.participant_id
                  WHERE p.hpo_id <> :test_hpo_id
                  AND p.is_ghost_id IS NOT TRUE
                  AND p.is_test_participant IS NOT TRUE
                  AND (ps.email IS NULL OR NOT ps.email LIKE :test_email_pattern)
                  AND p.withdrawal_status = :not_withdraw
                  AND p.hpo_id = :hpo_id
                """
                params = {'test_hpo_id': self.test_hpo_id, 'test_email_pattern': self.test_email_pattern,
                          'not_withdraw': int(WithdrawalStatus.NOT_WITHDRAWN), 'hpo_id': hpo.hpoId}

                session.execute('CREATE INDEX idx_sign_up_time ON {} (sign_up_time)'.format(temp_table_name))
                session.execute('CREATE INDEX idx_date_of_birth ON {} (date_of_birth)'.format(temp_table_name))
                session.execute('CREATE INDEX idx_consent_time ON {} (consent_for_study_enrollment_time)'
                                .format(temp_table_name))
                session.execute('CREATE INDEX idx_member_time ON {} (enrollment_status_member_time)'
                                .format(temp_table_name))
                session.execute('CREATE INDEX idx_sample_time ON {} (enrollment_status_core_stored_sample_time)'
                                .format(temp_table_name))
                session.execute('CREATE INDEX idx_participant_origin ON {} (participant_origin)'
                                .format(temp_table_name))

                session.execute(participant_sql, params)
                logging.info('crete temp table for hpo_id: ' + str(hpo.hpoId))

            session.execute('DROP TABLE IF EXISTS metrics_tmp_participant_origin;')
            session.execute('CREATE TABLE metrics_tmp_participant_origin (participant_origin VARCHAR(50))')
            participant_origin_sql = """
                INSERT INTO metrics_tmp_participant_origin
                SELECT DISTINCT participant_origin FROM participant
            """
            session.execute(participant_origin_sql)

            logging.info('Init temp table for metrics cron job.')

    def clean_tmp_tables(self):
        with self.session() as session:
            hpo_dao = HPODao()
            hpo_list = hpo_dao.get_all()
            for hpo in hpo_list:
                if hpo.hpoId == self.test_hpo_id:
                    continue
                temp_table_name = TEMP_TABLE_PREFIX + str(hpo.hpoId)
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    session.execute('DROP TABLE IF EXISTS {};'.format(temp_table_name))

    def refresh_metrics_cache_data(self, start_date, end_date, stage_number):
        self.start_date = start_date
        self.end_date = end_date
        self.stage_number = stage_number

        # For public metrics job, calculate new result for stage one, and copy history result for stage two
        if stage_number == MetricsCronJobStage.STAGE_ONE:
            self.refresh_data_for_metrics_cache(MetricsLifecycleCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
            logging.info("Refresh MetricsLifecycleCache for Public Metrics API done.")
            self.refresh_data_for_metrics_cache(MetricsGenderCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
            logging.info("Refresh MetricsGenderCache for Public Metrics API done.")
            self.refresh_data_for_metrics_cache(MetricsAgeCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
            logging.info("Refresh MetricsAgeCache for Public Metrics API done.")
            self.refresh_data_for_metrics_cache(MetricsRaceCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
            logging.info("Refresh MetricsRaceCache for Public Metrics API done.")
        elif stage_number == MetricsCronJobStage.STAGE_TWO:
            self.refresh_data_for_public_metrics_cache_stage_two(
                MetricsLifecycleCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
            self.refresh_data_for_public_metrics_cache_stage_two(
                MetricsGenderCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
            self.refresh_data_for_public_metrics_cache_stage_two(
                MetricsAgeCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API))
            self.refresh_data_for_public_metrics_cache_stage_two(
                MetricsRaceCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API))

        self.refresh_data_for_metrics_cache(MetricsEnrollmentStatusCacheDao())
        logging.info("Refresh MetricsEnrollmentStatusCache done.")
        self.refresh_data_for_metrics_cache(MetricsRegionCacheDao())
        logging.info("Refresh MetricsRegionCache done.")
        self.refresh_data_for_metrics_cache(MetricsLanguageCacheDao())
        logging.info("Refresh MetricsLanguageCache done.")
        self.refresh_data_for_metrics_cache(MetricsGenderCacheDao(MetricsCacheType.METRICS_V2_API))
        logging.info("Refresh MetricsGenderCache for Metrics2API done.")
        self.refresh_data_for_metrics_cache(MetricsRaceCacheDao(MetricsCacheType.METRICS_V2_API))
        logging.info("Refresh MetricsRaceCache for Metrics2API done.")

    def refresh_data_for_metrics_cache(self, dao):
        status_dao = MetricsCacheJobStatusDao()
        if self.stage_number == MetricsCronJobStage.STAGE_ONE:
            kwargs = dict(
                cacheTableName=dao.table_name,
                type=str(dao.cache_type),
                inProgress=True,
                stage_one_complete=False,
                stage_two_complete=False,
                dateInserted=self.cronjob_time,
            )
            job_status_obj = MetricsCacheJobStatus(**kwargs)
            status_dao.insert(job_status_obj)

        hpo_dao = HPODao()
        hpo_list = hpo_dao.get_all()
        for hpo in hpo_list:
            if hpo.hpoId == self.test_hpo_id:
                continue
            self.insert_cache_by_hpo(dao, hpo.hpoId)

        status_dao.set_to_complete(dao.cache_type, dao.table_name, self.cronjob_time, self.stage_number)
        if self.stage_number == MetricsCronJobStage.STAGE_TWO:
            dao.delete_old_records()

    def refresh_data_for_public_metrics_cache_stage_two(self, dao):
        if self.stage_number != MetricsCronJobStage.STAGE_TWO:
            return
        status_dao = MetricsCacheJobStatusDao()
        last_success_stage_two = status_dao.get_last_complete_stage_two_data_inserted_time(dao.table_name,
                                                                                           dao.cache_type)
        if not last_success_stage_two:
            logging.info(f'No last success stage two found for {dao.table_name}, calculate new data for stage two')
            self.refresh_data_for_metrics_cache(dao)
        else:
            dao.update_historical_cache_data(self.cronjob_time, last_success_stage_two.dateInserted,
                                             self.start_date, self.end_date)
            status_dao.set_to_complete(dao.cache_type, dao.table_name, self.cronjob_time, self.stage_number)
            dao.delete_old_records(n_days_ago=30)

    def insert_cache_by_hpo(self, dao, hpo_id):
        sql_arr = dao.get_metrics_cache_sql(hpo_id)

        params = {'hpo_id': hpo_id, 'start_date': self.start_date, 'end_date': self.end_date,
                  'date_inserted': self.cronjob_time}
        with dao.session() as session:
            for sql in sql_arr:
                session.execute(sql, params)

    def get_filtered_results(
        self, stratification, start_date, end_date, history, awardee_ids, enrollment_statuses, sample_time_def,
        participant_origins, version
    ):
        """Queries DB, returns results in format consumed by front-end

    :param start_date: Start date object
    :param end_date: End date object
    :param awardee_ids: indicate awardee ids
    :param enrollment_statuses: indicate the enrollment status
    :param sample_time_def: indicate how to filter the core participant
    :param history: query for history data from metrics cache table
    :param stratification: How to stratify (layer) results, as in a stacked bar chart
    :param version: indicate the version of the result filter
    :param participant_origins: indicate the participant origins
    :return: Filtered, stratified results by date
    """

        # Filters for participant_summary (ps) and participant (p) table
        # filters_sql_ps is used in the general case when we're querying participant_summary
        # filters_sql_p is used when also LEFT OUTER JOINing p and ps
        facets = {
            "enrollment_statuses": [
                EnrollmentStatusV2(val) if version == MetricsAPIVersion.V2 else EnrollmentStatus(val)
                for val in enrollment_statuses
            ],
            "awardee_ids": awardee_ids,
        }
        filters_sql_ps = self.get_facets_sql(facets, stratification)
        filters_sql_p = self.get_facets_sql(facets, stratification, participant_origins, table_prefix="p")

        if str(history) == "TRUE" and stratification == Stratifications.TOTAL:
            dao = MetricsEnrollmentStatusCacheDao(version=version)
            return dao.get_total_interested_count(start_date, end_date, awardee_ids, enrollment_statuses,
                                                  participant_origins)
        elif str(history) == "TRUE" and stratification == Stratifications.ENROLLMENT_STATUS:
            dao = MetricsEnrollmentStatusCacheDao(version=version)
            return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids, enrollment_statuses,
                                                     participant_origins)
        elif str(history) == "TRUE" and stratification == Stratifications.GENDER_IDENTITY:
            dao = MetricsGenderCacheDao(version=version)
            return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids, enrollment_statuses,
                                                     participant_origins)
        elif str(history) == "TRUE" and stratification == Stratifications.AGE_RANGE:
            dao = MetricsAgeCacheDao()
            return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids, enrollment_statuses,
                                                     participant_origins)
        elif str(history) == "TRUE" and stratification == Stratifications.RACE:
            dao = MetricsRaceCacheDao(version=version)
            return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids, enrollment_statuses,
                                                     participant_origins)
        elif str(history) == "TRUE" and stratification in [
            Stratifications.FULL_STATE,
            Stratifications.FULL_CENSUS,
            Stratifications.FULL_AWARDEE,
            Stratifications.GEO_STATE,
            Stratifications.GEO_CENSUS,
            Stratifications.GEO_AWARDEE,
        ]:
            dao = MetricsRegionCacheDao(version=version)
            return dao.get_latest_version_from_cache(end_date, stratification, awardee_ids, enrollment_statuses,
                                                     participant_origins)
        elif str(history) == "TRUE" and stratification == Stratifications.LANGUAGE:
            dao = MetricsLanguageCacheDao()
            return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids, enrollment_statuses)
        elif str(history) == "TRUE" and stratification == Stratifications.LIFECYCLE:
            dao = MetricsLifecycleCacheDao(version=version)
            return dao.get_latest_version_from_cache(end_date, awardee_ids, enrollment_statuses, participant_origins)
        elif stratification == Stratifications.PARTICIPANT_ORIGIN:
            dao = MetricsParticipantOriginCacheDao()
            return dao.get_participant_origins()
        elif stratification == Stratifications.TOTAL:
            strata = ["TOTAL"]
            sql = self.get_total_sql(filters_sql_ps)
        elif version == MetricsAPIVersion.V2 and stratification == Stratifications.ENROLLMENT_STATUS:
            strata = [str(val) for val in EnrollmentStatusV2]
            sql = self.get_enrollment_status_sql(filters_sql_p, sample_time_def, version)
        elif stratification == Stratifications.ENROLLMENT_STATUS:
            strata = [str(val) for val in EnrollmentStatus]
            sql = self.get_enrollment_status_sql(filters_sql_p, sample_time_def)
        elif stratification == Stratifications.EHR_CONSENT:
            strata = ["EHR_CONSENT"]
            sql = self.get_total_sql(filters_sql_ps, ehr_count=True)
        elif stratification == Stratifications.EHR_RATIO:
            strata = ["EHR_RATIO"]
            sql = self.get_ratio_sql(filters_sql_ps)
        else:
            raise BadRequest("Invalid stratification: %s" % stratification)

        params = {"start_date": start_date, "end_date": end_date}

        results_by_date = []

        with self.session() as session:
            cursor = session.execute(sql, params)

        # Iterate through each result (by date), transforming tabular SQL results
        # into expected list-of-dictionaries response format
        try:
            results = cursor.fetchall()
            for result in results:
                date = result[-1]
                metrics = {}
                values = result[:-1]
                for i, value in enumerate(values):
                    key = strata[i]
                    if value is None or (
                        stratification == Stratifications.ENROLLMENT_STATUS
                        and enrollment_statuses
                        and key not in enrollment_statuses
                    ):
                        value = 0
                    metrics[key] = float(value) if stratification == Stratifications.EHR_RATIO else int(value)
                results_by_date.append({"date": str(date), "metrics": metrics})
        finally:
            cursor.close()

        return results_by_date

    def get_facets_sql(self, facets, stratification, participant_origins=None, table_prefix="ps"):
        """Helper function to transform facets/filters selection into SQL

    :param facets: Object representing facets and filters to apply to query results
    :param stratification: How to stratify (layer) results, as in a stacked bar chart
    :param participant_origins: indicate array of participant_origins
    :param table_prefix: Either 'ps' (for participant_summary) or 'p' (for participant)
    :return: SQL for 'WHERE' clause, reflecting filters specified in UI
    """

        facets_sql = "WHERE "
        facets_sql_list = []

        facet_map = {"awardee_ids": "hpo_id", "enrollment_statuses": "enrollment_status"}

        # the SQL for ENROLLMENT_STATUS stratify is using the enrollment status time
        # instead of enrollment status
        if "enrollment_statuses" in facets and stratification == Stratifications.ENROLLMENT_STATUS:
            del facets["enrollment_statuses"]
            del facet_map["enrollment_statuses"]

        for facet in facets:
            filter_prefix = table_prefix
            filters_sql = []
            db_field = facet_map[facet]
            filters = facets[facet]

            allow_null = False
            if db_field == "enrollment_status":
                filter_prefix = "ps"
                allow_null = True

            # TODO:
            # Consider using an IN clause with bound parameters, instead, which
            # would be simpler than this,
            #
            # TODO:
            # Consider using bound parameters here instead of inlining the values
            # in the SQL. We do that in other places using this function:
            #
            # dao/database_utils.py#L16
            #
            # This may help the SQL perform slightly better since the execution
            # plan for the query can be cached when the only thing changing are
            # the bound params.
            for q_filter in filters:
                if str(q_filter) != "":
                    filter_sql = filter_prefix + "." + db_field + " = " + str(int(q_filter))
                    if allow_null and str(int(q_filter)) == "1":
                        filters_sql.append("(" + filter_sql + " or " + filter_prefix + "." + db_field + " IS NULL)")
                    else:
                        filters_sql.append(filter_sql)
            if len(filters_sql) > 0:
                filters_sql = "(" + " OR ".join(filters_sql) + ")"
                facets_sql_list.append(filters_sql)

        if len(facets_sql_list) > 0:
            facets_sql += " AND ".join(facets_sql_list) + " AND"

        # TODO: use bound parameters
        # See https://github.com/all-of-us/raw-data-repository/pull/669/files/a08be0ffe445da60ebca13b41d694368e4d42617#diff-6c62346e0cbe4a7fd7a45af6d4559c3e  # pylint: disable=line-too-long
        facets_sql += " %(table_prefix)s.hpo_id != %(test_hpo_id)s " % {
            "table_prefix": table_prefix,
            "test_hpo_id": self.test_hpo_id,
        }
        facets_sql += ' AND (ps.email IS NULL OR NOT ps.email LIKE "%(test_email_pattern)s")' % {
            "test_email_pattern": self.test_email_pattern
        }
        facets_sql += " AND %(table_prefix)s.withdrawal_status = %(not_withdrawn)i" % {
            "table_prefix": table_prefix,
            "not_withdrawn": WithdrawalStatus.NOT_WITHDRAWN,
        }
        facets_sql += " AND p.is_ghost_id IS NOT TRUE AND p.is_test_participant IS NOT TRUE"

        if participant_origins:
            facets_sql += " AND p.participant_origin in ({}) "\
                .format(",".join(["'" + origin + "'" for origin in participant_origins]))

        return facets_sql

    @staticmethod
    def get_total_sql(filters_sql, ehr_count=False):
        if ehr_count:
            # date consented
            date_field = "ps.consent_for_electronic_health_records_time"
        else:
            # date joined
            date_field = "p.sign_up_time"

        return """
        SELECT
          SUM(ps_sum.cnt * (ps_sum.day <= calendar.day)) registered_count,
          calendar.day start_date
        FROM calendar,
        (
          SELECT
            COUNT(*) cnt,
            DATE(%(date_field)s) day
          FROM participant p
          LEFT OUTER JOIN participant_summary ps
            ON p.participant_id = ps.participant_id
          %(filters)s
          GROUP BY day
        ) ps_sum
        WHERE calendar.day >= :start_date
        AND calendar.day <= :end_date
        GROUP BY calendar.day
        ORDER BY calendar.day;
      """ % {
            "filters": filters_sql,
            "date_field": date_field,
        }

    @staticmethod
    def get_ratio_sql(filters_sql):
        return """
      select
        ifnull(
          (
            select count(*)
            from participant p
            LEFT OUTER JOIN participant_summary ps
              ON p.participant_id = ps.participant_id
            %(filters)s
              and ps.consent_for_electronic_health_records_time <= calendar.day
          ) / (
            select count(*)
            from participant p
            LEFT OUTER JOIN participant_summary ps
              ON p.participant_id = ps.participant_id
            %(filters)s
              and p.sign_up_time <= calendar.day
          ),
          0
        ) ratio,
        calendar.day start_date
      from calendar
      where calendar.day >= :start_date
        and calendar.day <= :end_date
      order by calendar.day;
    """ % {
            "filters": filters_sql
        }

    def get_enrollment_status_sql(self, filters_sql_p, filter_by="ORDERED", version=None):

        core_sample_time_field_name = "enrollment_status_core_ordered_sample_time"
        if filter_by == "STORED":
            core_sample_time_field_name = "enrollment_status_core_stored_sample_time"

        if version == MetricsAPIVersion.V2:
            sql = """
        SELECT
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(p.sign_up_time) AS sign_up_time,
                   DATE(ps.consent_for_study_enrollment_time) AS consent_for_study_enrollment_time,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(p.sign_up_time), DATE(ps.consent_for_study_enrollment_time)
          ) AS results
          WHERE c.day>=DATE(sign_up_time) AND consent_for_study_enrollment_time IS NULL
        ),0) AS registered,
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(ps.consent_for_study_enrollment_time) AS consent_for_study_enrollment_time,
                   DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(ps.consent_for_study_enrollment_time), DATE(ps.enrollment_status_member_time)
          ) AS results
          WHERE consent_for_study_enrollment_time IS NOT NULL AND c.day>=DATE(consent_for_study_enrollment_time) AND (enrollment_status_member_time IS NULL OR c.day < DATE(enrollment_status_member_time))
        ),0) AS participant,
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                   DATE(ps.%(core_sample_time_field_name)s) AS %(core_sample_time_field_name)s,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(ps.enrollment_status_member_time), DATE(ps.%(core_sample_time_field_name)s)
          ) AS results
          WHERE enrollment_status_member_time IS NOT NULL AND day>=DATE(enrollment_status_member_time) AND (%(core_sample_time_field_name)s IS NULL OR day < DATE(%(core_sample_time_field_name)s))
        ),0) AS fully_consented,
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(ps.%(core_sample_time_field_name)s) AS %(core_sample_time_field_name)s,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(ps.%(core_sample_time_field_name)s)
          ) AS results
          WHERE %(core_sample_time_field_name)s IS NOT NULL AND day>=DATE(%(core_sample_time_field_name)s)
        ),0) AS core_participant,
        day
        FROM calendar c
        WHERE c.day BETWEEN :start_date AND :end_date
        """ % {
                "filters_p": filters_sql_p,
                "core_sample_time_field_name": core_sample_time_field_name,
            }
        else:
            sql = """
        SELECT
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(p.sign_up_time) AS sign_up_time,
                   DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(p.sign_up_time), DATE(ps.enrollment_status_member_time)
          ) AS results
          WHERE c.day>=DATE(sign_up_time) AND (enrollment_status_member_time IS NULL OR c.day < DATE(enrollment_status_member_time))
        ),0) AS registered_participants,
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                   DATE(ps.%(core_sample_time_field_name)s) AS %(core_sample_time_field_name)s,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(ps.enrollment_status_member_time), DATE(ps.%(core_sample_time_field_name)s)
          ) AS results
          WHERE enrollment_status_member_time IS NOT NULL AND day>=DATE(enrollment_status_member_time) AND (%(core_sample_time_field_name)s IS NULL OR day < DATE(%(core_sample_time_field_name)s))
        ),0) AS member_participants,
        IFNULL((
          SELECT SUM(results.enrollment_count)
          FROM
          (
            SELECT DATE(ps.%(core_sample_time_field_name)s) AS %(core_sample_time_field_name)s,
                   count(*) enrollment_count
            FROM participant p
                   LEFT JOIN participant_summary ps ON p.participant_id = ps.participant_id
            %(filters_p)s
            GROUP BY DATE(ps.%(core_sample_time_field_name)s)
          ) AS results
          WHERE %(core_sample_time_field_name)s IS NOT NULL AND day>=DATE(%(core_sample_time_field_name)s)
        ),0) AS full_participants,
        day
        FROM calendar c
        WHERE c.day BETWEEN :start_date AND :end_date
        """ % {
                "filters_p": filters_sql_p,
                "core_sample_time_field_name": core_sample_time_field_name,
            }

        return sql
