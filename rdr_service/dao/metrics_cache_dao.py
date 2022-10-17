import datetime
import json

import sqlalchemy
from sqlalchemy import and_, desc, func, or_, distinct

from rdr_service.census_regions import census_regions
from rdr_service.code_constants import PPI_SYSTEM
from rdr_service.dao.base_dao import BaseDao, UpdatableDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.model.site import Site
from rdr_service.model.participant import Participant
from rdr_service.model.metrics_cache import (
    MetricsAgeCache,
    MetricsCacheJobStatus,
    MetricsEnrollmentStatusCache,
    MetricsGenderCache,
    MetricsLanguageCache,
    MetricsLifecycleCache,
    MetricsRaceCache,
    MetricsRegionCache,
)
from rdr_service.participant_enums import (
    AGE_BUCKETS_METRICS_V2_API,
    AGE_BUCKETS_PUBLIC_METRICS_EXPORT_API,
    EnrollmentStatus,
    EnrollmentStatusV2,
    GenderIdentity,
    MetricsAPIVersion,
    MetricsCacheType,
    Stratifications,
    MetricsCronJobStage
)

TEMP_TABLE_PREFIX = 'metrics_tmp_participant_'

class MetricsCacheJobStatusDao(UpdatableDao):
    def __init__(self):
        super(MetricsCacheJobStatusDao, self).__init__(MetricsCacheJobStatus)

    def set_to_complete(self, cache_type, table_name, cronjob_time, stage_number):
        with self.session() as session:
            if stage_number == MetricsCronJobStage.STAGE_ONE:
                update_value = {MetricsCacheJobStatus.stage_one_complete: True}
            else:
                update_value = {MetricsCacheJobStatus.stage_two_complete: True}
            query = (
                sqlalchemy
                    .update(MetricsCacheJobStatus)
                    .where(and_(MetricsCacheJobStatus.dateInserted >= cronjob_time.replace(microsecond=0),
                                MetricsCacheJobStatus.cacheTableName == table_name,
                                MetricsCacheJobStatus.type == cache_type,
                                MetricsCacheJobStatus.inProgress.is_(True)
                                ))
                    .values(update_value)
            )
            session.execute(query)

    def get_last_complete_data_inserted_time(self, table_name, cache_type=None):
        with self.session() as session:
            query = session.query(MetricsCacheJobStatus.dateInserted)
            query = query.filter(MetricsCacheJobStatus.cacheTableName == table_name,
                                 MetricsCacheJobStatus.stage_one_complete.is_(True))
            if cache_type:
                query = query.filter(MetricsCacheJobStatus.type == str(cache_type))
            query = query.order_by(desc(MetricsCacheJobStatus.id))
            record = query.first()
            return record

    def get_last_complete_stage_two_data_inserted_time(self, table_name, cache_type=None):
        with self.session() as session:
            query = session.query(MetricsCacheJobStatus.dateInserted)
            query = query.filter(MetricsCacheJobStatus.cacheTableName == table_name,
                                 MetricsCacheJobStatus.stage_one_complete.is_(True),
                                 MetricsCacheJobStatus.stage_two_complete.is_(True))
            if cache_type:
                query = query.filter(MetricsCacheJobStatus.type == str(cache_type))
            query = query.order_by(desc(MetricsCacheJobStatus.id))
            record = query.first()
            return record


class MetricsEnrollmentStatusCacheDao(BaseDao):
    def __init__(self, cache_type=MetricsCacheType.METRICS_V2_API, version=None):
        super(MetricsEnrollmentStatusCacheDao, self).__init__(MetricsEnrollmentStatusCache)
        self.version = version
        self.table_name = MetricsEnrollmentStatusCache.__tablename__
        try:
            self.cache_type = MetricsCacheType(str(cache_type))
        except TypeError:
            raise TypeError("Invalid metrics cache type")

    def get_serving_version_with_session(self, session):
        status_dao = MetricsCacheJobStatusDao()
        record = status_dao.get_last_complete_data_inserted_time(self.table_name)
        if record is not None:
            return record
        else:
            return (session.query(MetricsEnrollmentStatusCache)
                    .order_by(MetricsEnrollmentStatusCache.dateInserted.desc())
                    .first())

    def get_active_buckets(self, start_date=None, end_date=None, hpo_ids=None, participant_origins=None):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is None:
                return None
            last_inserted_date = last_inserted_record.dateInserted

            if self.cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API:
                query = session.query(MetricsEnrollmentStatusCache.date,
                                      func.sum(MetricsEnrollmentStatusCache.registeredCount)
                                      .label('registeredCount'),
                                      func.sum(MetricsEnrollmentStatusCache.participantCount)
                                      .label('participantCount'),
                                      func.sum(MetricsEnrollmentStatusCache.consentedCount)
                                      .label('consentedCount'),
                                      func.sum(MetricsEnrollmentStatusCache.coreCount)
                                      .label('coreCount'))

                query = query.filter(MetricsEnrollmentStatusCache.dateInserted == last_inserted_date)
                if start_date:
                    query = query.filter(MetricsEnrollmentStatusCache.date >= start_date)
                if end_date:
                    query = query.filter(MetricsEnrollmentStatusCache.date <= end_date)

                if hpo_ids:
                    query = query.filter(MetricsEnrollmentStatusCache.hpoId.in_(hpo_ids))

                return query.group_by(MetricsEnrollmentStatusCache.date).all()
            else:
                query = session.query(MetricsEnrollmentStatusCache.date,
                                      MetricsEnrollmentStatusCache.hpoName,
                                      func.sum(MetricsEnrollmentStatusCache.registeredCount)
                                      .label('registeredCount'),
                                      func.sum(MetricsEnrollmentStatusCache.participantCount)
                                      .label('participantCount'),
                                      func.sum(MetricsEnrollmentStatusCache.consentedCount)
                                      .label('consentedCount'),
                                      func.sum(MetricsEnrollmentStatusCache.coreCount)
                                      .label('coreCount')
                                      ).filter(MetricsEnrollmentStatusCache.dateInserted == last_inserted_date)
                if start_date:
                    query = query.filter(MetricsEnrollmentStatusCache.date >= start_date)
                if end_date:
                    query = query.filter(MetricsEnrollmentStatusCache.date <= end_date)

                if hpo_ids:
                    query = query.filter(MetricsEnrollmentStatusCache.hpoId.in_(hpo_ids))

                if participant_origins:
                    query = query.filter(MetricsEnrollmentStatusCache.participantOrigin.in_(participant_origins))

                return query.group_by(MetricsEnrollmentStatusCache.date, MetricsEnrollmentStatusCache.hpoName).all()

    def get_latest_version_from_cache(self, start_date, end_date, hpo_ids=None,
                                      enrollment_statuses=None, participant_origins=None):
        buckets = self.get_active_buckets(start_date, end_date, hpo_ids, participant_origins)
        if buckets is None:
            return []
        operation_funcs = {
            MetricsCacheType.PUBLIC_METRICS_EXPORT_API: self.to_public_metrics_client_json,
            MetricsCacheType.METRICS_V2_API: self.to_metrics_client_json
        }
        return operation_funcs[self.cache_type](buckets, enrollment_statuses)

    def delete_old_records(self, n_days_ago=7):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is not None:
                last_date_inserted = last_inserted_record.dateInserted
                seven_days_ago = last_date_inserted - datetime.timedelta(days=n_days_ago)
                delete_sql = """
                delete from metrics_enrollment_status_cache where date_inserted < :seven_days_ago
                """
                params = {'seven_days_ago': seven_days_ago}
                session.execute(delete_sql, params)

    def to_metrics_client_json(self, result_set, enrollment_statuses=None):
        client_json = []
        if self.version == MetricsAPIVersion.V2:
            for record in result_set:
                new_item = {
                    'date': record.date.isoformat(),
                    'hpo': record.hpoName,
                    'metrics': {
                        'registered': int(record.registeredCount) if not enrollment_statuses or str(
                          EnrollmentStatusV2.REGISTERED) in enrollment_statuses else 0,
                        'participant': int(record.participantCount) if not enrollment_statuses or str(
                          EnrollmentStatusV2.PARTICIPANT) in enrollment_statuses else 0,
                        'consented': int(record.consentedCount) if not enrollment_statuses or str(
                          EnrollmentStatusV2.FULLY_CONSENTED) in enrollment_statuses else 0,
                        'core': int(record.coreCount) if not enrollment_statuses or str(
                          EnrollmentStatusV2.CORE_PARTICIPANT) in enrollment_statuses else 0,
                    }
                }
                client_json.append(new_item)
        else:
            for record in result_set:
                new_item = {
                    'date': record.date.isoformat(),
                    'hpo': record.hpoName,
                    'metrics': {
                        'registered': int(record.registeredCount) + int(record.participantCount),
                        'consented': int(record.consentedCount),
                        'core': int(record.coreCount)
                    }
                }
                client_json.append(new_item)
        return client_json

    def to_public_metrics_client_json(self, result_set, enrollment_statuses=None):
        # pylint: disable=unused-argument
        client_json = []
        for record in result_set:
            new_item = {
                'date': record.date.isoformat(),
                'metrics': {
                    # research hub still use 3 tiers status
                    'registered': int(record.registeredCount) + int(record.participantCount),
                    'consented': int(record.consentedCount),
                    'core': int(record.coreCount)
                }
            }
            client_json.append(new_item)
        return client_json

    def get_total_interested_count(self, start_date, end_date, hpo_ids=None, enrollment_statuses=None,
                                   participant_origins=None):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is None:
                return []
            last_inserted_date = last_inserted_record.dateInserted

            if hpo_ids:
                filters_hpo = ' (' + ' OR '.join('hpo_id=' + str(x) for x in hpo_ids) + ') AND '
            else:
                filters_hpo = ''

            if participant_origins:
                filters_origin = ' (' + ' OR '.join('participant_origin=\'' + str(x) + '\''
                                                    for x in participant_origins) + ') AND '
            else:
                filters_origin = ''

            select_field_mapping = {
                str(EnrollmentStatusV2.REGISTERED): 'SUM(registered_count)',
                str(EnrollmentStatusV2.PARTICIPANT): 'SUM(participant_count)',
                str(EnrollmentStatusV2.FULLY_CONSENTED): 'SUM(consented_count)',
                str(EnrollmentStatusV2.CORE_PARTICIPANT): 'SUM(core_count)'
            }

            if enrollment_statuses and self.version == MetricsAPIVersion.V2:
                select_field_str = '+'.join(select_field_mapping[key] for key in enrollment_statuses)
            else:
                select_field_str = '+'.join(select_field_mapping[key] for key in select_field_mapping)

            sql = """
            SELECT (""" + select_field_str + """) AS registered_count,
            date AS start_date
            FROM metrics_enrollment_status_cache
            WHERE %(filters_hpo)s
            %(filters_origin)s
            date_inserted=:date_inserted
            AND date >= :start_date
            AND date <= :end_date
            GROUP BY date;
            """ % {'filters_hpo': filters_hpo, 'filters_origin': filters_origin}
            params = {'start_date': start_date, 'end_date': end_date, 'date_inserted': last_inserted_date}

            results_by_date = []

            cursor = session.execute(sql, params)
            try:
                results = cursor.fetchall()
                for result in results:
                    date = result[1]
                    metrics = {'TOTAL': int(result[0])}
                    results_by_date.append({
                        'date': str(date),
                        'metrics': metrics
                    })

            finally:
                cursor.close()

            return results_by_date

    def get_metrics_cache_sql(self, hpo_id):
        temp_table_name = TEMP_TABLE_PREFIX + str(hpo_id)
        sql = """
            insert into metrics_enrollment_status_cache
              SELECT
                :date_inserted AS date_inserted,
                :hpo_id AS hpo_id,
                (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS name,
                c.day AS date,
                IFNULL((
                  SELECT SUM(results.enrollment_count)
                  FROM
                  (
                    SELECT DATE(ps.sign_up_time) AS sign_up_time,
                           DATE(ps.consent_for_study_enrollment_time) AS consent_for_study_enrollment_time,
                           ps.participant_origin,
                           count(*) enrollment_count
                    FROM {temp_table_name} ps
                    GROUP BY DATE(ps.sign_up_time), DATE(ps.consent_for_study_enrollment_time), ps.participant_origin
                  ) AS results
                  WHERE c.day>=DATE(sign_up_time) AND (consent_for_study_enrollment_time IS NULL OR DATE(consent_for_study_enrollment_time)>c.day)
                  and results.participant_origin = d.participant_origin
                ),0) AS registered_count,
                IFNULL((
                  SELECT SUM(results.enrollment_count)
                  FROM
                  (
                    SELECT DATE(ps.consent_for_study_enrollment_time) AS consent_for_study_enrollment_time,
                           DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                           ps.participant_origin,
                           count(*) enrollment_count
                    FROM {temp_table_name} ps
                    GROUP BY DATE(ps.consent_for_study_enrollment_time), DATE(ps.enrollment_status_member_time), ps.participant_origin
                  ) AS results
                  WHERE consent_for_study_enrollment_time IS NOT NULL AND c.day>=DATE(consent_for_study_enrollment_time) AND (enrollment_status_member_time IS NULL OR c.day < DATE(enrollment_status_member_time))
                  and results.participant_origin = d.participant_origin
                ),0) AS participant_count,
                IFNULL((
                  SELECT SUM(results.enrollment_count)
                  FROM
                  (
                    SELECT DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                           DATE(ps.enrollment_status_core_stored_sample_time) AS enrollment_status_core_stored_sample_time,
                           ps.participant_origin,
                           count(*) enrollment_count
                    FROM {temp_table_name} ps
                    GROUP BY DATE(ps.enrollment_status_member_time), DATE(ps.enrollment_status_core_stored_sample_time), ps.participant_origin
                  ) AS results
                  WHERE enrollment_status_member_time IS NOT NULL AND day>=DATE(enrollment_status_member_time) AND (enrollment_status_core_stored_sample_time IS NULL OR day < DATE(enrollment_status_core_stored_sample_time))
                  and results.participant_origin = d.participant_origin
                ),0) AS consented_count,
                IFNULL((
                  SELECT SUM(results.enrollment_count)
                  FROM
                  (
                    SELECT DATE(ps.enrollment_status_core_stored_sample_time) AS enrollment_status_core_stored_sample_time,
                           ps.participant_origin, count(*) enrollment_count
                    FROM {temp_table_name} ps
                    GROUP BY DATE(ps.enrollment_status_core_stored_sample_time), ps.participant_origin
                  ) AS results
                  WHERE enrollment_status_core_stored_sample_time IS NOT NULL AND day>=DATE(enrollment_status_core_stored_sample_time)
                  and results.participant_origin = d.participant_origin
                ),0) AS core_count,
                d.participant_origin
              FROM calendar c, metrics_tmp_participant_origin d
              WHERE c.day BETWEEN :start_date AND :end_date
              ;
        """.format(temp_table_name=temp_table_name)

        return [sql]


class MetricsGenderCacheDao(BaseDao):
    def __init__(self, cache_type=MetricsCacheType.METRICS_V2_API, version=None):
        super(MetricsGenderCacheDao, self).__init__(MetricsGenderCache)
        try:
            self.cache_type = MetricsCacheType(str(cache_type))
            self.version = version
            self.table_name = MetricsGenderCache.__tablename__
            self.gender_names = ['UNSET', 'Woman', 'Man', 'Transgender', 'PMI_Skip', 'Non-Binary',
                                 'Other/Additional Options', 'Prefer not to say',
                                 'More than one gender identity']
        except TypeError:
            raise TypeError("Invalid metrics cache type")

    def get_serving_version_with_session(self, session):
        if self.version == MetricsAPIVersion.V2:
            status_dao = MetricsCacheJobStatusDao()
            record = status_dao.get_last_complete_data_inserted_time(self.table_name, self.cache_type)
            if record is not None:
                return record
            else:
                return (session
                        .query(MetricsGenderCache)
                        .filter(MetricsGenderCache.type == str(self.cache_type))
                        .order_by(MetricsGenderCache.dateInserted.desc())
                        .first())
        else:
            status_dao = MetricsCacheJobStatusDao()
            record = status_dao.get_last_complete_data_inserted_time(self.table_name,
                                                                     MetricsCacheType.METRICS_V2_API)
            if record is not None:
                return record
            else:
                return (session
                        .query(MetricsGenderCache)
                        .filter(MetricsGenderCache.type == MetricsCacheType.METRICS_V2_API)
                        .order_by(MetricsGenderCache.dateInserted.desc())
                        .first())

    def get_active_buckets(self, start_date=None, end_date=None, hpo_ids=None,
                           enrollment_statuses=None, participant_origins=None):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is None:
                return []
            last_inserted_date = last_inserted_record.dateInserted

            if hpo_ids:
                filters_hpo = ' (' + ' OR '.join('hpo_id=' + str(x) for x in hpo_ids) + ') AND '
            else:
                filters_hpo = ''

            if participant_origins:
                filters_hpo += ' (' + ' OR '.join('participant_origin=\'' + str(x) + '\''
                                                  for x in participant_origins) + ') AND '

            if self.cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API:
                if enrollment_statuses:
                    status_filter_list = []
                    for status in enrollment_statuses:
                        if status == str(EnrollmentStatus.INTERESTED):
                            status_filter_list.append('registered')
                        elif status == str(EnrollmentStatus.MEMBER):
                            status_filter_list.append('consented')
                        elif status == str(EnrollmentStatus.FULL_PARTICIPANT):
                            status_filter_list.append('core')
                    filters_hpo += ' (' + ' OR '.join('enrollment_status=\'' + str(x) + '\''
                                                      for x in status_filter_list) + ') AND '
                sql = """
                  SELECT date_inserted, date, CONCAT('{',group_concat(result),'}') AS json_result FROM
                  (
                    SELECT date_inserted, date, CONCAT('"',gender_name, '":', gender_count) AS result
                    FROM
                    (
                      SELECT date_inserted, date, gender_name, SUM(gender_count) AS gender_count
                      FROM metrics_gender_cache
                      WHERE %(filters_hpo)s
                      date_inserted=:date_inserted
                      and type = :cache_type
                      AND date BETWEEN :start_date AND :end_date
                      GROUP BY date_inserted, date, gender_name
                    ) x
                  ) a
                  GROUP BY date_inserted, date
                """ % {'filters_hpo': filters_hpo}
            else:
                if enrollment_statuses and self.version == MetricsAPIVersion.V2:
                    status_filter_list = []
                    for status in enrollment_statuses:
                        if status == str(EnrollmentStatusV2.REGISTERED):
                            status_filter_list.append('registered')
                        elif status == str(EnrollmentStatusV2.PARTICIPANT):
                            status_filter_list.append('participant')
                        elif status == str(EnrollmentStatusV2.FULLY_CONSENTED):
                            status_filter_list.append('consented')
                        elif status == str(EnrollmentStatusV2.CORE_PARTICIPANT):
                            status_filter_list.append('core')
                    filters_hpo += ' (' + ' OR '.join('enrollment_status=\'' + str(x) + '\''
                                                      for x in status_filter_list) + ') AND '
                sql = """
                  SELECT date_inserted, hpo_id, hpo_name, date, CONCAT('{',group_concat(result),'}') AS json_result FROM
                  (
                    SELECT date_inserted, hpo_id, hpo_name, date, CONCAT('"',gender_name, '":', gender_count) AS result FROM
                    (
                      SELECT date_inserted, hpo_id,hpo_name,date,gender_name, SUM(gender_count) AS gender_count
                      FROM metrics_gender_cache
                      WHERE %(filters_hpo)s
                      date_inserted=:date_inserted
                      and type = :cache_type
                      AND date BETWEEN :start_date AND :end_date
                      GROUP BY date_inserted, hpo_id, hpo_name, date, gender_name
                    ) x
                  ) a
                  GROUP BY date_inserted, hpo_id, hpo_name, date
                """ % {'filters_hpo': filters_hpo}

            if self.version == MetricsAPIVersion.V2:
                params = {'start_date': start_date, 'end_date': end_date, 'date_inserted': last_inserted_date,
                          'cache_type': self.cache_type}
            else:
                params = {'start_date': start_date, 'end_date': end_date, 'date_inserted': last_inserted_date,
                          'cache_type': MetricsCacheType.METRICS_V2_API}

            cursor = session.execute(sql, params)
            try:
                results = cursor.fetchall()
            finally:
                cursor.close()

            return results

    def get_latest_version_from_cache(self, start_date, end_date, hpo_ids=None,
                                      enrollment_statuses=None, participant_origins=None):
        buckets = self.get_active_buckets(start_date, end_date, hpo_ids, enrollment_statuses, participant_origins)
        if buckets is None:
            return []
        operation_funcs = {
            MetricsCacheType.PUBLIC_METRICS_EXPORT_API: self.to_public_metrics_client_json,
            MetricsCacheType.METRICS_V2_API: self.to_metrics_client_json
        }
        return operation_funcs[self.cache_type](buckets)

    def update_historical_cache_data(self, new_date_inserted_time, last_date_inserted_time, start_date, end_date):
        with self.session() as session:
            query = (
                sqlalchemy
                    .update(MetricsGenderCache)
                    .where(and_(MetricsGenderCache.dateInserted == last_date_inserted_time,
                           MetricsGenderCache.date >= start_date,
                           MetricsGenderCache.date <= end_date,
                           MetricsGenderCache.type == self.cache_type)
                           )
                    .values({MetricsGenderCache.dateInserted: new_date_inserted_time})
            )
            session.execute(query)

    def delete_old_records(self, n_days_ago=7):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is not None:
                last_date_inserted = last_inserted_record.dateInserted
                seven_days_ago = last_date_inserted - datetime.timedelta(days=n_days_ago)
                delete_sql = """
                  delete from metrics_gender_cache where date_inserted < :seven_days_ago and type = :type
                """
                params = {'seven_days_ago': seven_days_ago, 'type': self.cache_type}
                session.execute(delete_sql, params)

    def to_metrics_client_json(self, result_set):
        client_json = []
        for record in result_set:
            new_item = {
                'date': record.date.isoformat(),
                'hpo': record.hpo_name,
                'metrics': json.loads(record.json_result)
            }

            if 'UNMAPPED' not in new_item['metrics']:
                new_item['metrics']['UNMAPPED'] = 0
            for gender_name in self.gender_names:
                if gender_name not in new_item['metrics']:
                    new_item['metrics'][gender_name] = 0
            client_json.append(new_item)
        return client_json

    def to_public_metrics_client_json(self, result_set):
        client_json = []
        for record in result_set:
            new_item = {
                'date': record.date.isoformat(),
                'metrics': json.loads(record.json_result)
            }
            if 'UNMAPPED' not in new_item['metrics']:
                new_item['metrics']['UNMAPPED'] = 0
            for gender_name in self.gender_names:
                if gender_name not in new_item['metrics']:
                    new_item['metrics'][gender_name] = 0
            client_json.append(new_item)
        return client_json

    def get_metrics_cache_sql(self, hpo_id):
        temp_table_name = TEMP_TABLE_PREFIX + str(hpo_id)
        sql_arr = []

        if self.cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API:
            gender_code_dict = {
                'GenderIdentity_Woman': 354,
                'GenderIdentity_Transgender': 355,
                'GenderIdentity_Man': 356,
                'GenderIdentity_AdditionalOptions': 357,
                'GenderIdentity_NonBinary': 358,
                'PMI_PreferNotToAnswer': 924,
                'PMI_Skip': 930
            }

            for k in gender_code_dict:
                code = CodeDao().get_code(PPI_SYSTEM, k)
                if code is not None:
                    gender_code_dict[k] = code.codeId

            answers_table_sql = """
                (SELECT
                      participant_id,
                      MAX(GenderIdentity_Woman) AS GenderIdentity_Woman,
                      MAX(GenderIdentity_Transgender) AS GenderIdentity_Transgender,
                      MAX(GenderIdentity_Man) AS GenderIdentity_Man,
                      MAX(GenderIdentity_AdditionalOptions) AS GenderIdentity_AdditionalOptions,
                      MAX(GenderIdentity_NonBinary) AS GenderIdentity_NonBinary,
                      MAX(PMI_PreferNotToAnswer) AS PMI_PreferNotToAnswer,
                      MAX(PMI_Skip) AS PMI_Skip,
                      COUNT(*) as Number_of_Answer
                FROM (
                      SELECT participant_id,
                             CASE WHEN code_id = {GenderIdentity_Woman} THEN 1 ELSE 0 END   AS GenderIdentity_Woman,
                             CASE WHEN code_id = {GenderIdentity_Transgender} THEN 1 ELSE 0 END   AS GenderIdentity_Transgender,
                             CASE WHEN code_id = {GenderIdentity_Man} THEN 1 ELSE 0 END   AS GenderIdentity_Man,
                             CASE WHEN code_id = {GenderIdentity_AdditionalOptions} THEN 1 ELSE 0 END   AS GenderIdentity_AdditionalOptions,
                             CASE WHEN code_id = {GenderIdentity_NonBinary} THEN 1 ELSE 0 END   AS GenderIdentity_NonBinary,
                             CASE WHEN code_id = {PMI_PreferNotToAnswer} THEN 1 ELSE 0 END   AS PMI_PreferNotToAnswer,
                             CASE WHEN code_id = {PMI_Skip} THEN 1 ELSE 0 END   AS PMI_Skip
                      FROM participant_gender_answers
                      ) x
                GROUP BY participant_id)
            """.format(GenderIdentity_Woman=gender_code_dict['GenderIdentity_Woman'],
                     GenderIdentity_Transgender=gender_code_dict['GenderIdentity_Transgender'],
                     GenderIdentity_Man=gender_code_dict['GenderIdentity_Man'],
                     GenderIdentity_AdditionalOptions=
                                                     gender_code_dict['GenderIdentity_AdditionalOptions'],
                     GenderIdentity_NonBinary=gender_code_dict['GenderIdentity_NonBinary'],
                     PMI_PreferNotToAnswer=gender_code_dict['PMI_PreferNotToAnswer'],
                     PMI_Skip=gender_code_dict['PMI_Skip']
                     )

            gender_conditions = [
                ' pga.participant_id IS NULL ',
                ' pga.GenderIdentity_Woman=1 ',
                ' pga.GenderIdentity_Man=1 ',
                ' pga.GenderIdentity_Transgender=1 ',
                ' pga.PMI_Skip=1 ',
                ' pga.GenderIdentity_NonBinary=1 ',
                ' pga.GenderIdentity_AdditionalOptions=1 ',
                ' pga.PMI_PreferNotToAnswer=1 ',
                ' pga.Number_of_Answer>1 AND pga.PMI_Skip=0 AND pga.PMI_PreferNotToAnswer=0 ',
            ]
            sql = """insert into metrics_gender_cache """
            sub_queries = []
            sql_template = """
            SELECT
              :date_inserted AS date_inserted,
              '{cache_type}' as type,
              'core' as enrollment_status,
              :hpo_id AS hpo_id,
              (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
              c.day AS date,
              '{gender_name}' AS gender_name,
              IFNULL((
                SELECT SUM(results.gender_count)
                FROM
                (
                  SELECT DATE(ps.sign_up_time) as day,
                         DATE(ps.enrollment_status_core_stored_sample_time) as enrollment_status_core_stored_sample_time,
                         COUNT(*) gender_count
                  FROM {temp_table_name} ps
                  LEFT JOIN {answers_table_sql} pga ON ps.participant_id = pga.participant_id
                  WHERE {gender_condition}
                  GROUP BY DATE(ps.sign_up_time), DATE(ps.enrollment_status_core_stored_sample_time)
                ) AS results
                WHERE results.day <= c.day
                AND enrollment_status_core_stored_sample_time IS NOT NULL
                AND DATE(enrollment_status_core_stored_sample_time) <= c.day
              ),0) AS gender_count,
              '' as participant_origin
            FROM calendar c
            WHERE c.day BETWEEN :start_date AND :end_date
            UNION ALL
            SELECT
              :date_inserted AS date_inserted,
              '{cache_type}' as type,
              'registered' as enrollment_status,
              :hpo_id AS hpo_id,
              (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
              c.day AS date,
              '{gender_name}' AS gender_name,
              IFNULL((
                SELECT SUM(results.gender_count)
                FROM
                (
                  SELECT DATE(ps.sign_up_time) as day,
                         DATE(ps.enrollment_status_member_time) as enrollment_status_member_time,
                         COUNT(*) gender_count
                  FROM {temp_table_name} ps
                  LEFT JOIN {answers_table_sql} pga ON ps.participant_id = pga.participant_id
                  WHERE {gender_condition}
                  GROUP BY DATE(ps.sign_up_time), DATE(ps.enrollment_status_member_time)
                ) AS results
                WHERE results.day <= c.day
                AND (enrollment_status_member_time is null or DATE(enrollment_status_member_time)>c.day)
              ),0) AS gender_count,
              '' as participant_origin
            FROM calendar c
            WHERE c.day BETWEEN :start_date AND :end_date
            UNION ALL
            SELECT
              :date_inserted AS date_inserted,
              '{cache_type}' as type,
              'consented' as enrollment_status,
              :hpo_id AS hpo_id,
              (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
              c.day AS date,
              '{gender_name}' AS gender_name,
              IFNULL((
                SELECT SUM(results.gender_count)
                FROM
                (
                  SELECT DATE(ps.sign_up_time) as day,
                         DATE(ps.enrollment_status_member_time) as enrollment_status_member_time,
                         DATE(ps.enrollment_status_core_stored_sample_time) as enrollment_status_core_stored_sample_time,
                         COUNT(*) gender_count
                  FROM {temp_table_name} ps
                  LEFT JOIN {answers_table_sql} pga ON ps.participant_id = pga.participant_id
                  WHERE {gender_condition}
                  GROUP BY DATE(ps.sign_up_time), DATE(ps.enrollment_status_member_time), DATE(ps.enrollment_status_core_stored_sample_time)
                ) AS results
                WHERE results.day <= c.day
                AND enrollment_status_member_time IS NOT NULL
                AND DATE(enrollment_status_member_time) <= c.day
                AND (enrollment_status_core_stored_sample_time is null or DATE(enrollment_status_core_stored_sample_time)>c.day)
              ),0) AS gender_count,
              '' as participant_origin
            FROM calendar c
            WHERE c.day BETWEEN :start_date AND :end_date
            """
            for gender_name, gender_condition in zip(self.gender_names, gender_conditions):
                sub_query = sql_template.format(cache_type=self.cache_type,
                                                temp_table_name=temp_table_name,
                                                gender_name=gender_name,
                                                gender_condition=gender_condition,
                                                answers_table_sql=answers_table_sql)
                sub_queries.append(sub_query)

            sql += ' UNION ALL '.join(sub_queries)
            sql_arr.append(sql)
        else:
            enrollment_status_criteria_arr = [
                ('registered', 'c.day>=DATE(ps.sign_up_time) '
                               'AND (ps.consent_for_study_enrollment_time IS NULL OR '
                               'DATE(ps.consent_for_study_enrollment_time)>c.day)'),
                ('participant', 'ps.consent_for_study_enrollment_time IS NOT NULL '
                                'AND c.day>=DATE(ps.consent_for_study_enrollment_time) '
                                'AND (ps.enrollment_status_member_time IS NULL '
                                'OR c.day < DATE(ps.enrollment_status_member_time))'),
                ('consented', 'ps.enrollment_status_member_time IS NOT NULL '
                              'AND c.day>=DATE(ps.enrollment_status_member_time) '
                              'AND (ps.enrollment_status_core_stored_sample_time IS NULL '
                              'OR c.day < DATE(ps.enrollment_status_core_stored_sample_time))'),
                ('core', 'ps.enrollment_status_core_stored_sample_time IS NOT NULL '
                         'AND c.day>=DATE(ps.enrollment_status_core_stored_sample_time)')
            ]
            gender_conditions = [
                ' ps.gender_identity IS NULL ',
                ' ps.gender_identity=' + str(GenderIdentity.GenderIdentity_Woman.number) + ' ',
                ' ps.gender_identity=' + str(GenderIdentity.GenderIdentity_Man.number) + ' ',
                ' ps.gender_identity=' + str(GenderIdentity.GenderIdentity_Transgender.number) + ' ',
                ' ps.gender_identity=' + str(GenderIdentity.PMI_Skip.number) + ' ',
                ' ps.gender_identity=' + str(GenderIdentity.GenderIdentity_NonBinary.number) + ' ',
                ' ps.gender_identity=' + str(GenderIdentity.GenderIdentity_AdditionalOptions.number) + ' ',
                ' ps.gender_identity=' + str(GenderIdentity.PMI_PreferNotToAnswer.number) + ' ',
                ' ps.gender_identity=' + str(GenderIdentity.GenderIdentity_MoreThanOne.number) + ' ',
            ]

            for item in enrollment_status_criteria_arr:
                sql = """insert into metrics_gender_cache """
                sub_queries = []
                sql_template = """
                SELECT
                  :date_inserted AS date_inserted,
                  '{cache_type}' AS type,
                  '{enrollment_status}' AS enrollment_status,
                  :hpo_id AS hpo_id,
                  (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
                  c.day AS date,
                  '{gender_name}' AS gender_name,
                  COUNT(*) as gender_count,
                  participant_origin
                FROM {temp_table_name} ps, calendar c
                WHERE {enrollment_status_criteria}
                AND {gender_condition}
                AND c.day BETWEEN :start_date AND :end_date
                GROUP BY date, participant_origin
                """
                for gender_name, gender_condition in zip(self.gender_names, gender_conditions):
                    sub_query = sql_template.format(cache_type=self.cache_type,
                                                    temp_table_name=temp_table_name,
                                                    enrollment_status=item[0],
                                                    gender_name=gender_name,
                                                    enrollment_status_criteria=item[1],
                                                    gender_condition=gender_condition)
                    sub_queries.append(sub_query)

                sql += ' UNION ALL '.join(sub_queries)
                sql_arr.append(sql)
        return sql_arr


class MetricsAgeCacheDao(BaseDao):

    def __init__(self, cache_type=MetricsCacheType.METRICS_V2_API):
        super(MetricsAgeCacheDao, self).__init__(MetricsAgeCache)
        try:
            self.cache_type = MetricsCacheType(str(cache_type))
            self.table_name = MetricsAgeCache.__tablename__
        except TypeError:
            raise TypeError("Invalid metrics cache type")

        if cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API:
            self.age_ranges = AGE_BUCKETS_PUBLIC_METRICS_EXPORT_API
        else:
            self.age_ranges = AGE_BUCKETS_METRICS_V2_API

    def get_serving_version_with_session(self, session):
        status_dao = MetricsCacheJobStatusDao()
        record = status_dao.get_last_complete_data_inserted_time(self.table_name, self.cache_type)
        if record is not None:
            return record
        else:
            return (session.query(MetricsAgeCache)
                    .filter(MetricsAgeCache.type == str(self.cache_type))
                    .order_by(MetricsAgeCache.dateInserted.desc())
                    .first())

    def get_active_buckets(self, start_date=None, end_date=None, hpo_ids=None,
                           enrollment_statuses=None, participant_origins=None):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is None:
                return []
            last_inserted_date = last_inserted_record.dateInserted

            if hpo_ids:
                filters_hpo = ' (' + ' OR '.join('hpo_id=' + str(x) for x in hpo_ids) + ') AND '
            else:
                filters_hpo = ''

            if participant_origins:
                filters_hpo += ' (' + ' OR '.join('participant_origin=\'' + str(x) + '\''
                                                  for x in participant_origins) + ') AND '

            if self.cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API:
                if enrollment_statuses:
                    status_filter_list = []
                    for status in enrollment_statuses:
                        if status == str(EnrollmentStatus.INTERESTED):
                            status_filter_list.append('registered')
                            status_filter_list.append('participant')
                        elif status == str(EnrollmentStatus.MEMBER):
                            status_filter_list.append('consented')
                        elif status == str(EnrollmentStatus.FULL_PARTICIPANT):
                            status_filter_list.append('core')
                    filters_hpo += ' (' + ' OR '.join('enrollment_status=\'' + str(x) + '\''
                                                      for x in status_filter_list) + ') AND '
                sql = """
                  SELECT date_inserted, date, CONCAT('{',group_concat(result),'}') AS json_result FROM
                  (
                    SELECT date_inserted, date, CONCAT('"',age_range, '":', age_count) AS result
                    FROM
                    (
                      select date_inserted, date, age_range, SUM(age_count) AS age_count
                      FROM metrics_age_cache
                      WHERE %(filters_hpo)s
                      date_inserted=:date_inserted
                      AND date BETWEEN :start_date AND :end_date
                      AND type=:cache_type
                      GROUP BY date_inserted, date, age_range
                    ) x
                  ) a
                  GROUP BY date_inserted, date
                """ % {'filters_hpo': filters_hpo}
            else:
                if enrollment_statuses:
                    status_filter_list = []
                    for status in enrollment_statuses:
                        if status == str(EnrollmentStatusV2.REGISTERED):
                            status_filter_list.append('registered')
                        elif status == str(EnrollmentStatusV2.PARTICIPANT):
                            status_filter_list.append('participant')
                        elif status == str(EnrollmentStatusV2.FULLY_CONSENTED):
                            status_filter_list.append('consented')
                        elif status == str(EnrollmentStatusV2.CORE_PARTICIPANT):
                            status_filter_list.append('core')
                    filters_hpo += ' (' + ' OR '.join('enrollment_status=\'' + str(x) + '\''
                                                      for x in status_filter_list) + ') AND '
                sql = """
                  SELECT date_inserted, hpo_id, hpo_name, date, CONCAT('{',group_concat(result),'}') AS json_result FROM
                  (
                    SELECT date_inserted, hpo_id, hpo_name, date, CONCAT('"',age_range, '":', age_count) AS result FROM
                    (
                      SELECT date_inserted, hpo_id, hpo_name, date, age_range, SUM(age_count) as age_count
                      FROM metrics_age_cache
                      WHERE %(filters_hpo)s
                      date_inserted=:date_inserted
                      AND date BETWEEN :start_date AND :end_date
                      AND type=:cache_type
                      GROUP BY date_inserted, hpo_id, hpo_name, date, age_range
                    ) x
                  ) a
                  GROUP BY date_inserted, hpo_id, hpo_name, date
                """ % {'filters_hpo': filters_hpo}

            params = {'start_date': start_date, 'end_date': end_date, 'date_inserted': last_inserted_date,
                      'cache_type': str(self.cache_type)}

            cursor = session.execute(sql, params)
            try:
                results = cursor.fetchall()
            finally:
                cursor.close()

            return results

    def get_latest_version_from_cache(self, start_date, end_date, hpo_ids=None,
                                      enrollment_statuses=None, participant_origins=None):
        buckets = self.get_active_buckets(start_date, end_date, hpo_ids, enrollment_statuses, participant_origins)
        if buckets is None:
            return []
        operation_funcs = {
            MetricsCacheType.PUBLIC_METRICS_EXPORT_API: self.to_public_metrics_client_json,
            MetricsCacheType.METRICS_V2_API: self.to_metrics_client_json
        }
        return operation_funcs[self.cache_type](buckets)

    def update_historical_cache_data(self, new_date_inserted_time, last_date_inserted_time, start_date, end_date):
        with self.session() as session:
            query = (
                sqlalchemy
                    .update(MetricsAgeCache)
                    .where(and_(MetricsAgeCache.dateInserted == last_date_inserted_time,
                                MetricsAgeCache.date >= start_date,
                                MetricsAgeCache.date <= end_date,
                                MetricsAgeCache.type == self.cache_type)
                           )
                    .values({MetricsAgeCache.dateInserted: new_date_inserted_time})
            )
            session.execute(query)

    def delete_old_records(self, n_days_ago=7):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is not None:
                last_date_inserted = last_inserted_record.dateInserted
                seven_days_ago = last_date_inserted - datetime.timedelta(days=n_days_ago)
                delete_sql = """
          delete from metrics_age_cache where date_inserted < :seven_days_ago and type = :type
        """
                params = {'seven_days_ago': seven_days_ago, 'type': self.cache_type}
                session.execute(delete_sql, params)

    def to_metrics_client_json(self, result_set):
        client_json = []
        for record in result_set:
            new_item = {
                'date': record.date.isoformat(),
                'hpo': record.hpo_name,
                'metrics': json.loads(record.json_result)
            }
            client_json.append(new_item)
        return client_json

    def to_public_metrics_client_json(self, result_set):
        client_json = []
        for record in result_set:
            new_item = {
                'date': record.date.isoformat(),
                'metrics': json.loads(record.json_result)
            }
            client_json.append(new_item)
        return client_json

    def get_metrics_cache_sql(self, hpo_id):
        temp_table_name = TEMP_TABLE_PREFIX + str(hpo_id)
        sql = """
          insert into metrics_age_cache
            SELECT
              :date_inserted AS date_inserted,
              'core' as enrollment_status,
              '{cache_type}' as type,
              :hpo_id AS hpo_id,
              (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
              c.day AS date,
              'UNSET' AS age_range,
              IFNULL((
                SELECT SUM(results.age_count)
                FROM
                (
                  SELECT DATE(ps.enrollment_status_core_stored_sample_time) as enrollment_status_core_stored_sample_time,
                         participant_origin,
                         count(*) age_count
                  FROM {temp_table_name} ps
                  WHERE ps.date_of_birth IS NULL
                  GROUP BY DATE(ps.enrollment_status_core_stored_sample_time), participant_origin
                ) AS results
                WHERE enrollment_status_core_stored_sample_time IS NOT NULL
                AND DATE(enrollment_status_core_stored_sample_time) <= c.day
                AND results.participant_origin = d.participant_origin
              ),0) AS age_count,
              d.participant_origin
            FROM calendar c, metrics_tmp_participant_origin d
            WHERE c.day BETWEEN :start_date AND :end_date
            UNION ALL
            SELECT
              :date_inserted AS date_inserted,
              'registered' as enrollment_status,
              '{cache_type}' as type,
              :hpo_id AS hpo_id,
              (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
              c.day AS date,
              'UNSET' AS age_range,
              IFNULL((
                SELECT SUM(results.age_count)
                FROM
                (
                  SELECT DATE(ps.sign_up_time) AS day,
                         DATE(ps.consent_for_study_enrollment_time) as consent_for_study_enrollment_time,
                         participant_origin,
                         count(*) age_count
                  FROM {temp_table_name} ps
                  WHERE ps.date_of_birth IS NULL
                  GROUP BY DATE(ps.sign_up_time), DATE(ps.consent_for_study_enrollment_time), participant_origin
                ) AS results
                WHERE results.day <= c.day
                AND (consent_for_study_enrollment_time is NULL OR DATE(consent_for_study_enrollment_time)>c.day)
                AND results.participant_origin = d.participant_origin
              ),0) AS age_count,
              d.participant_origin
            FROM calendar c, metrics_tmp_participant_origin d
            WHERE c.day BETWEEN :start_date AND :end_date
            UNION ALL
            SELECT
              :date_inserted AS date_inserted,
              'participant' as enrollment_status,
              '{cache_type}' as type,
              :hpo_id AS hpo_id,
              (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
              c.day AS date,
              'UNSET' AS age_range,
              IFNULL((
                SELECT SUM(results.age_count)
                FROM
                (
                  SELECT DATE(ps.consent_for_study_enrollment_time) AS consent_for_study_enrollment_time,
                         DATE(ps.enrollment_status_member_time) as enrollment_status_member_time,
                         participant_origin,
                         count(*) age_count
                  FROM {temp_table_name} ps
                  WHERE ps.date_of_birth IS NULL
                  GROUP BY DATE(ps.consent_for_study_enrollment_time), DATE(ps.enrollment_status_member_time), participant_origin
                ) AS results
                WHERE consent_for_study_enrollment_time IS NOT NULL
                AND c.day>=DATE(consent_for_study_enrollment_time)
                AND (enrollment_status_member_time is null or DATE(enrollment_status_member_time)>c.day)
                AND results.participant_origin = d.participant_origin
              ),0) AS age_count,
              d.participant_origin
            FROM calendar c, metrics_tmp_participant_origin d
            WHERE c.day BETWEEN :start_date AND :end_date
            UNION ALL
            SELECT
              :date_inserted AS date_inserted,
              'consented' as enrollment_status,
              '{cache_type}' as type,
              :hpo_id AS hpo_id,
              (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
              c.day AS date,
              'UNSET' AS age_range,
              IFNULL((
                SELECT SUM(results.age_count)
                FROM
                (
                  SELECT DATE(ps.enrollment_status_member_time) as enrollment_status_member_time,
                         DATE(ps.enrollment_status_core_stored_sample_time) as enrollment_status_core_stored_sample_time,
                         participant_origin,
                         count(*) age_count
                  FROM {temp_table_name} ps
                  WHERE ps.date_of_birth IS NULL
                  GROUP BY DATE(ps.enrollment_status_member_time), DATE(ps.enrollment_status_core_stored_sample_time), participant_origin
                ) AS results
                WHERE enrollment_status_member_time IS NOT NULL
                AND DATE(enrollment_status_member_time) <= c.day
                AND (enrollment_status_core_stored_sample_time is null or DATE(enrollment_status_core_stored_sample_time)>c.day)
                AND results.participant_origin = d.participant_origin
              ),0) AS age_count,
              d.participant_origin
            FROM calendar c, metrics_tmp_participant_origin d
            WHERE c.day BETWEEN :start_date AND :end_date
            UNION ALL
        """.format(cache_type=str(self.cache_type), temp_table_name=temp_table_name)

        age_ranges_conditions = []
        for age_range in self.age_ranges:
            age_borders = [_f for _f in age_range.split("-") if _f]
            if len(age_borders) == 2:
                age_ranges_conditions.append(' AND (Date_format(From_Days(To_Days(c.day) - To_Days(dob)), '
                                             '\'%Y\') + 0) BETWEEN ' + age_borders[0] + ' AND '
                                             + age_borders[1], )
            else:
                age_ranges_conditions.append(' AND (Date_format(From_Days(To_Days(c.day) - To_Days(dob)), '
                                             '\'%Y\') + 0) >= ' + age_borders[0])

        sub_queries = []
        sql_template = """
          SELECT
            :date_inserted AS date_inserted,
            'core' as enrollment_status,
            '{cache_type}' as type,
            :hpo_id AS hpo_id,
            (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
            c.day as date,
            '{age_range}' AS age_range,
            IFNULL((
              SELECT SUM(results.age_count)
              FROM
              (
                SELECT DATE(ps.date_of_birth) AS dob,
                       DATE(ps.enrollment_status_core_stored_sample_time) as enrollment_status_core_stored_sample_time,
                       participant_origin,
                       count(*) age_count
                FROM {temp_table_name} ps
                WHERE ps.date_of_birth IS NOT NULL
                GROUP BY DATE(ps.date_of_birth), DATE(ps.enrollment_status_core_stored_sample_time), participant_origin
              ) AS results
              WHERE enrollment_status_core_stored_sample_time IS NOT NULL
              AND DATE(enrollment_status_core_stored_sample_time) <= c.day
              AND results.participant_origin = d.participant_origin
              {age_range_condition}
            ),0) AS age_count,
            d.participant_origin
          FROM calendar c, metrics_tmp_participant_origin d
          WHERE c.day BETWEEN :start_date AND :end_date
          UNION ALL
          SELECT
            :date_inserted AS date_inserted,
            'registered' as enrollment_status,
            '{cache_type}' as type,
            :hpo_id AS hpo_id,
            (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
            c.day as date,
            '{age_range}' AS age_range,
            IFNULL((
              SELECT SUM(results.age_count)
              FROM
              (
                SELECT DATE(ps.sign_up_time) AS day,
                       DATE(ps.date_of_birth) AS dob,
                       DATE(ps.consent_for_study_enrollment_time) as consent_for_study_enrollment_time,
                       participant_origin,
                       count(*) age_count
                FROM {temp_table_name} ps
                WHERE ps.date_of_birth IS NOT NULL
                GROUP BY DATE(ps.sign_up_time), DATE(ps.date_of_birth), DATE(ps.consent_for_study_enrollment_time), participant_origin
              ) AS results
              WHERE results.day <= c.day AND (consent_for_study_enrollment_time is NULL OR DATE(consent_for_study_enrollment_time)>c.day)
              AND results.participant_origin = d.participant_origin
              {age_range_condition}
            ),0) AS age_count,
            d.participant_origin
          FROM calendar c, metrics_tmp_participant_origin d
          WHERE c.day BETWEEN :start_date AND :end_date
          UNION ALL
          SELECT
            :date_inserted AS date_inserted,
            'participant' as enrollment_status,
            '{cache_type}' as type,
            :hpo_id AS hpo_id,
            (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
            c.day as date,
            '{age_range}' AS age_range,
            IFNULL((
              SELECT SUM(results.age_count)
              FROM
              (
                SELECT DATE(ps.consent_for_study_enrollment_time) AS consent_for_study_enrollment_time,
                       DATE(ps.date_of_birth) AS dob,
                       DATE(ps.enrollment_status_member_time) as enrollment_status_member_time,
                       participant_origin,
                       count(*) age_count
                FROM {temp_table_name} ps
                WHERE ps.date_of_birth IS NOT NULL
                GROUP BY DATE(ps.consent_for_study_enrollment_time), DATE(ps.date_of_birth), DATE(ps.enrollment_status_member_time), participant_origin
              ) AS results
              WHERE consent_for_study_enrollment_time IS NOT NULL AND consent_for_study_enrollment_time <= c.day
              AND (enrollment_status_member_time is null or DATE(enrollment_status_member_time)>c.day)
              AND results.participant_origin = d.participant_origin
              {age_range_condition}
            ),0) AS age_count,
            d.participant_origin
          FROM calendar c, metrics_tmp_participant_origin d
          WHERE c.day BETWEEN :start_date AND :end_date
          UNION ALL
          SELECT
            :date_inserted AS date_inserted,
            'consented' as enrollment_status,
            '{cache_type}' as type,
            :hpo_id AS hpo_id,
            (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
            c.day as date,
            '{age_range}' AS age_range,
            IFNULL((
              SELECT SUM(results.age_count)
              FROM
              (
                SELECT DATE(ps.date_of_birth) AS dob,
                       DATE(ps.enrollment_status_member_time) as enrollment_status_member_time,
                       DATE(ps.enrollment_status_core_stored_sample_time) as enrollment_status_core_stored_sample_time,
                       participant_origin,
                       count(*) age_count
                FROM {temp_table_name} ps
                WHERE ps.date_of_birth IS NOT NULL
                GROUP BY DATE(ps.date_of_birth), DATE(ps.enrollment_status_member_time), DATE(ps.enrollment_status_core_stored_sample_time), participant_origin
              ) AS results
              WHERE enrollment_status_member_time IS NOT NULL
              AND DATE(enrollment_status_member_time) <= c.day
              AND (enrollment_status_core_stored_sample_time is null or DATE(enrollment_status_core_stored_sample_time)>c.day)
              AND results.participant_origin = d.participant_origin
              {age_range_condition}
            ),0) AS age_count,
            d.participant_origin
          FROM calendar c, metrics_tmp_participant_origin d
          WHERE c.day BETWEEN :start_date AND :end_date
        """

        for age_range, age_range_condition in zip(self.age_ranges, age_ranges_conditions):
            sub_query = sql_template.format(cache_type=str(self.cache_type),
                                            temp_table_name=temp_table_name,
                                            age_range=age_range,
                                            age_range_condition=age_range_condition)
            sub_queries.append(sub_query)

        sql += ' UNION ALL '.join(sub_queries)

        return [sql]


class MetricsRaceCacheDao(BaseDao):

    def __init__(self, cache_type=MetricsCacheType.METRICS_V2_API, version=None):
        super(MetricsRaceCacheDao, self).__init__(MetricsRaceCache, read_uncommitted=True)
        try:
            self.cache_type = MetricsCacheType(str(cache_type))
            self.version = version
            self.table_name = MetricsRaceCache.__tablename__
        except TypeError:
            raise TypeError("Invalid metrics cache type")

    def get_serving_version_with_session(self, session):
        if self.version == MetricsAPIVersion.V2:
            status_dao = MetricsCacheJobStatusDao()
            record = status_dao.get_last_complete_data_inserted_time(self.table_name, self.cache_type)
            if record is not None:
                return record
            else:
                return (session
                        .query(MetricsRaceCache)
                        .filter(MetricsRaceCache.type == str(self.cache_type))
                        .order_by(MetricsRaceCache.dateInserted.desc())
                        .first())
        else:
            status_dao = MetricsCacheJobStatusDao()
            record = status_dao.get_last_complete_data_inserted_time(self.table_name,
                                                                     MetricsCacheType.METRICS_V2_API)
            if record is not None:
                return record
            else:
                return (session
                        .query(MetricsRaceCache)
                        .filter(MetricsRaceCache.type == MetricsCacheType.METRICS_V2_API)
                        .order_by(MetricsRaceCache.dateInserted.desc())
                        .first())

    def get_active_buckets(self, start_date=None, end_date=None, hpo_ids=None,
                           enrollment_statuses=None, participant_origins=None):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is None:
                return None
            last_inserted_date = last_inserted_record.dateInserted
            if self.cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API:
                query = session.query(MetricsRaceCache.date,
                                      func.sum(MetricsRaceCache.americanIndianAlaskaNative)
                                      .label('americanIndianAlaskaNative'),
                                      func.sum(MetricsRaceCache.asian)
                                      .label('asian'),
                                      func.sum(MetricsRaceCache.blackAfricanAmerican)
                                      .label('blackAfricanAmerican'),
                                      func.sum(MetricsRaceCache.middleEasternNorthAfrican)
                                      .label('middleEasternNorthAfrican'),
                                      func.sum(MetricsRaceCache.nativeHawaiianOtherPacificIslander)
                                      .label('nativeHawaiianOtherPacificIslander'),
                                      func.sum(MetricsRaceCache.white)
                                      .label('white'),
                                      func.sum(MetricsRaceCache.hispanicLatinoSpanish)
                                      .label('hispanicLatinoSpanish'),
                                      func.sum(MetricsRaceCache.noneOfTheseFullyDescribeMe)
                                      .label('noneOfTheseFullyDescribeMe'),
                                      func.sum(MetricsRaceCache.preferNotToAnswer)
                                      .label('preferNotToAnswer'),
                                      func.sum(MetricsRaceCache.multiAncestry)
                                      .label('multiAncestry'),
                                      func.sum(MetricsRaceCache.noAncestryChecked)
                                      .label('noAncestryChecked'),
                                      func.sum(MetricsRaceCache.unsetNoBasics)
                                      .label('unsetNoBasics')
                                      )
                if self.version == MetricsAPIVersion.V2:
                    query = query.filter(MetricsRaceCache.dateInserted == last_inserted_date,
                                         MetricsRaceCache.type == self.cache_type)
                else:
                    query = query.filter(MetricsRaceCache.dateInserted == last_inserted_date,
                                         MetricsRaceCache.type == MetricsCacheType.METRICS_V2_API)

                if start_date:
                    query = query.filter(MetricsRaceCache.date >= start_date)
                if end_date:
                    query = query.filter(MetricsRaceCache.date <= end_date)
                if hpo_ids:
                    query = query.filter(MetricsRaceCache.hpoId.in_(hpo_ids))
                if enrollment_statuses:
                    param_list = []
                    for status in enrollment_statuses:
                        if status == str(EnrollmentStatus.INTERESTED):
                            param_list.append(MetricsRaceCache.registeredFlag == 1)
                            param_list.append(MetricsRaceCache.participantFlag == 1)
                        elif status == str(EnrollmentStatus.MEMBER):
                            param_list.append(MetricsRaceCache.consentedFlag == 1)
                        elif status == str(EnrollmentStatus.FULL_PARTICIPANT):
                            param_list.append(MetricsRaceCache.coreFlag == 1)
                    if param_list:
                        query = query.filter(or_(*param_list))

                return query.group_by(MetricsRaceCache.date).all()
            else:
                query = session.query(MetricsRaceCache.date, MetricsRaceCache.hpoName,
                                      func.sum(MetricsRaceCache.americanIndianAlaskaNative)
                                      .label('americanIndianAlaskaNative'),
                                      func.sum(MetricsRaceCache.asian)
                                      .label('asian'),
                                      func.sum(MetricsRaceCache.blackAfricanAmerican)
                                      .label('blackAfricanAmerican'),
                                      func.sum(MetricsRaceCache.middleEasternNorthAfrican)
                                      .label('middleEasternNorthAfrican'),
                                      func.sum(MetricsRaceCache.nativeHawaiianOtherPacificIslander)
                                      .label('nativeHawaiianOtherPacificIslander'),
                                      func.sum(MetricsRaceCache.white)
                                      .label('white'),
                                      func.sum(MetricsRaceCache.hispanicLatinoSpanish)
                                      .label('hispanicLatinoSpanish'),
                                      func.sum(MetricsRaceCache.noneOfTheseFullyDescribeMe)
                                      .label('noneOfTheseFullyDescribeMe'),
                                      func.sum(MetricsRaceCache.preferNotToAnswer)
                                      .label('preferNotToAnswer'),
                                      func.sum(MetricsRaceCache.multiAncestry)
                                      .label('multiAncestry'),
                                      func.sum(MetricsRaceCache.noAncestryChecked)
                                      .label('noAncestryChecked'),
                                      func.sum(MetricsRaceCache.unsetNoBasics)
                                      .label('unsetNoBasics')
                                      )
                query = query.filter(MetricsRaceCache.dateInserted == last_inserted_date,
                                     MetricsRaceCache.type == self.cache_type)
                if start_date:
                    query = query.filter(MetricsRaceCache.date >= start_date)
                if end_date:
                    query = query.filter(MetricsRaceCache.date <= end_date)
                if hpo_ids:
                    query = query.filter(MetricsRaceCache.hpoId.in_(hpo_ids))
                if participant_origins:
                    query = query.filter(MetricsRaceCache.participantOrigin.in_(participant_origins))
                if enrollment_statuses and self.version == MetricsAPIVersion.V2:
                    param_list = []
                    for status in enrollment_statuses:
                        if status == str(EnrollmentStatusV2.REGISTERED):
                            param_list.append(MetricsRaceCache.registeredFlag == 1)
                        elif status == str(EnrollmentStatusV2.PARTICIPANT):
                            param_list.append(MetricsRaceCache.participantFlag == 1)
                        elif status == str(EnrollmentStatusV2.FULLY_CONSENTED):
                            param_list.append(MetricsRaceCache.consentedFlag == 1)
                        elif status == str(EnrollmentStatusV2.CORE_PARTICIPANT):
                            param_list.append(MetricsRaceCache.coreFlag == 1)
                    if param_list:
                        query = query.filter(or_(*param_list))

                return query.group_by(MetricsRaceCache.date, MetricsRaceCache.hpoName).all()

    def get_latest_version_from_cache(self, start_date, end_date, hpo_ids=None,
                                      enrollment_statuses=None, participant_origins=None):
        buckets = self.get_active_buckets(start_date, end_date, hpo_ids, enrollment_statuses, participant_origins)
        if buckets is None:
            return []
        operation_funcs = {
            MetricsCacheType.PUBLIC_METRICS_EXPORT_API: self.to_public_metrics_client_json,
            MetricsCacheType.METRICS_V2_API: self.to_metrics_client_json
        }
        return operation_funcs[self.cache_type](buckets)

    def update_historical_cache_data(self, new_date_inserted_time, last_date_inserted_time, start_date, end_date):
        with self.session() as session:
            query = (
                sqlalchemy
                    .update(MetricsRaceCache)
                    .where(and_(MetricsRaceCache.dateInserted == last_date_inserted_time,
                                MetricsRaceCache.date >= start_date,
                                MetricsRaceCache.date <= end_date,
                                MetricsRaceCache.type == self.cache_type)
                           )
                    .values({MetricsRaceCache.dateInserted: new_date_inserted_time})
            )
            session.execute(query)

    def delete_old_records(self, n_days_ago=7):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is not None:
                last_date_inserted = last_inserted_record.dateInserted
                seven_days_ago = last_date_inserted - datetime.timedelta(days=n_days_ago)
                delete_sql = """
                  delete from metrics_race_cache where date_inserted < :seven_days_ago and type = :type
                """
                params = {'seven_days_ago': seven_days_ago, 'type': self.cache_type}
                session.execute(delete_sql, params)

    def to_metrics_client_json(self, result_set):
        client_json = []
        for record in result_set:
            new_item = {
                'date': record.date.isoformat(),
                'hpo': record.hpoName,
                'metrics': {
                    'American_Indian_Alaska_Native': int(record.americanIndianAlaskaNative),
                    'Asian': int(record.asian),
                    'Black_African_American': int(record.blackAfricanAmerican),
                    'Middle_Eastern_North_African': int(record.middleEasternNorthAfrican),
                    'Native_Hawaiian_other_Pacific_Islander': int(record.nativeHawaiianOtherPacificIslander),
                    'White': int(record.white),
                    'Hispanic_Latino_Spanish': int(record.hispanicLatinoSpanish),
                    'None_Of_These_Fully_Describe_Me': int(record.noneOfTheseFullyDescribeMe),
                    'Prefer_Not_To_Answer': int(record.preferNotToAnswer),
                    'Multi_Ancestry': int(record.multiAncestry),
                    'No_Ancestry_Checked': int(record.noAncestryChecked),
                    'Unset_No_Basics': int(record.unsetNoBasics)
                }
            }
            client_json.append(new_item)
        return client_json

    def to_public_metrics_client_json(self, result_set):
        client_json = []
        for record in result_set:
            new_item = {
                'date': record.date.isoformat(),
                'metrics': {
                    'American_Indian_Alaska_Native': int(record.americanIndianAlaskaNative),
                    'Asian': int(record.asian),
                    'Black_African_American': int(record.blackAfricanAmerican),
                    'Middle_Eastern_North_African': int(record.middleEasternNorthAfrican),
                    'Native_Hawaiian_other_Pacific_Islander': int(record.nativeHawaiianOtherPacificIslander),
                    'White': int(record.white),
                    'Hispanic_Latino_Spanish': int(record.hispanicLatinoSpanish),
                    'None_Of_These_Fully_Describe_Me': int(record.noneOfTheseFullyDescribeMe),
                    'Prefer_Not_To_Answer': int(record.preferNotToAnswer),
                    'Multi_Ancestry': int(record.multiAncestry),
                    'No_Ancestry_Checked': int(record.noAncestryChecked),
                    'Unset_No_Basics': int(record.unsetNoBasics)
                }
            }
            client_json.append(new_item)
        return client_json

    def get_metrics_cache_sql(self, hpo_id):
        temp_table_name = TEMP_TABLE_PREFIX + str(hpo_id)
        race_code_dict = {
            'Race_WhatRaceEthnicity': 193,
            'WhatRaceEthnicity_Hispanic': 207,
            'WhatRaceEthnicity_Black': 259,
            'WhatRaceEthnicity_White': 220,
            'WhatRaceEthnicity_AIAN': 252,
            'WhatRaceEthnicity_RaceEthnicityNoneOfThese': 235,
            'WhatRaceEthnicity_Asian': 194,
            'PMI_PreferNotToAnswer': 924,
            'WhatRaceEthnicity_MENA': 274,
            'PMI_Skip': 930,
            'WhatRaceEthnicity_NHPI': 237
        }

        for k in race_code_dict:
            code = CodeDao().get_code(PPI_SYSTEM, k)
            if code is not None:
                race_code_dict[k] = code.codeId
        if self.cache_type == MetricsCacheType.METRICS_V2_API:
            sql = """
            insert into metrics_race_cache
              SELECT
                :date_inserted as date_inserted,
                '{cache_type}' as type,
                registered as registered_flag,
                participant as participant_flag,
                consented as consented_flag,
                core as core_flag,
                hpo_id,
                (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
                day,
                SUM(American_Indian_Alaska_Native) AS American_Indian_Alaska_Native,
                SUM(Asian) AS Asian,
                SUM(Black_African_American) AS Black_African_American,
                SUM(Middle_Eastern_North_African) AS Middle_Eastern_North_African,
                SUM(Native_Hawaiian_other_Pacific_Islander) AS Native_Hawaiian_other_Pacific_Islander,
                SUM(White) AS White,
                SUM(Hispanic_Latino_Spanish) AS Hispanic_Latino_Spanish,
                SUM(None_Of_These_Fully_Describe_Me) AS None_Of_These_Fully_Describe_Me,
                SUM(Prefer_Not_To_Answer) AS Prefer_Not_To_Answer,
                SUM(Multi_Ancestry) AS Multi_Ancestry,
                SUM(No_Ancestry_Checked) AS No_Ancestry_Checked,
                participant_origin,
                SUM(Unset_No_Basics) AS Unset_No_Basics
                FROM
                (
                  SELECT p.hpo_id,
                         day,
                         p.participant_origin,
                         CASE WHEN DATE(sign_up_time)<=calendar.day AND (consent_for_study_enrollment_time IS NULL OR DATE(consent_for_study_enrollment_time)>calendar.day)
                                   THEN 1 ELSE 0 END AS registered,
                         CASE WHEN (consent_for_study_enrollment_time IS NOT NULL AND DATE(consent_for_study_enrollment_time)<=calendar.day) AND
                                   (enrollment_status_member_time IS NULL OR DATE(enrollment_status_member_time)>calendar.day)
                             THEN 1 ELSE 0 END AS participant,
                         CASE WHEN (enrollment_status_member_time IS NOT NULL AND DATE(enrollment_status_member_time)<=calendar.day) AND
                                   (enrollment_status_core_stored_sample_time IS NULL OR DATE(enrollment_status_core_stored_sample_time)>calendar.day)
                             THEN 1 ELSE 0 END AS consented,
                         CASE WHEN enrollment_status_core_stored_sample_time IS NOT NULL AND DATE(enrollment_status_core_stored_sample_time)<=calendar.day
                             THEN 1 ELSE 0 END AS core,
                         CASE WHEN WhatRaceEthnicity_AIAN=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS American_Indian_Alaska_Native,
                         CASE WHEN WhatRaceEthnicity_Asian=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Asian,
                         CASE WHEN WhatRaceEthnicity_Black=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Black_African_American,
                         CASE WHEN WhatRaceEthnicity_MENA=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Middle_Eastern_North_African,
                         CASE WHEN WhatRaceEthnicity_NHPI=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Native_Hawaiian_other_Pacific_Islander,
                         CASE WHEN WhatRaceEthnicity_White=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS White,
                         CASE WHEN WhatRaceEthnicity_Hispanic=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Hispanic_Latino_Spanish,
                         CASE WHEN WhatRaceEthnicity_RaceEthnicityNoneOfThese=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS None_Of_These_Fully_Describe_Me,
                         CASE WHEN PMI_PreferNotToAnswer=1 AND Number_of_Answer=1 THEN 1 ELSE 0 END AS Prefer_Not_To_Answer,
                         CASE
                           WHEN (
                                 WhatRaceEthnicity_Hispanic +
                                 WhatRaceEthnicity_Black +
                                 WhatRaceEthnicity_White +
                                 WhatRaceEthnicity_AIAN +
                                 WhatRaceEthnicity_Asian +
                                 WhatRaceEthnicity_MENA +
                                 WhatRaceEthnicity_NHPI
                                 ) > 1
                             THEN 1
                           ELSE 0
                         END AS Multi_Ancestry,
                         CASE
                           WHEN (PMI_Skip = 1 AND Number_of_Answer=1) OR (HasBasicsSurvey = 1 AND UNSET = 1)
                             THEN 1
                           ELSE 0
                         END AS No_Ancestry_Checked,
                         CASE WHEN HasBasicsSurvey = 0 THEN 1 ELSE 0 END AS Unset_No_Basics
                  FROM (
                         SELECT participant_id,
                                hpo_id,
                                sign_up_time,
                                consent_for_study_enrollment_time,
                                enrollment_status_member_time,
                                enrollment_status_core_stored_sample_time,
                                participant_origin,
                                MAX(WhatRaceEthnicity_Hispanic)                 AS WhatRaceEthnicity_Hispanic,
                                MAX(WhatRaceEthnicity_Black)                    AS WhatRaceEthnicity_Black,
                                MAX(WhatRaceEthnicity_White)                    AS WhatRaceEthnicity_White,
                                MAX(WhatRaceEthnicity_AIAN)                     AS WhatRaceEthnicity_AIAN,
                                MAX(UNSET)                                      AS UNSET,
                                MAX(WhatRaceEthnicity_RaceEthnicityNoneOfThese) AS WhatRaceEthnicity_RaceEthnicityNoneOfThese,
                                MAX(WhatRaceEthnicity_Asian)                    AS WhatRaceEthnicity_Asian,
                                MAX(PMI_PreferNotToAnswer)                      AS PMI_PreferNotToAnswer,
                                MAX(WhatRaceEthnicity_MENA)                     AS WhatRaceEthnicity_MENA,
                                MAX(PMI_Skip)                                   AS PMI_Skip,
                                MAX(WhatRaceEthnicity_NHPI)                     AS WhatRaceEthnicity_NHPI,
                                MAX(questionnaire_on_the_basics) AS HasBasicsSurvey,
                                COUNT(*) as Number_of_Answer
                         FROM (
                                SELECT ps.participant_id,
                                       ps.hpo_id,
                                       ps.sign_up_time,
                                       consent_for_study_enrollment_time,
                                       ps.enrollment_status_member_time,
                                       ps.enrollment_status_core_stored_sample_time,
                                       ps.participant_origin,
                                       ps.questionnaire_on_the_basics,
                                       CASE WHEN q.code_id = {WhatRaceEthnicity_Hispanic} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_Hispanic,
                                       CASE WHEN q.code_id = {WhatRaceEthnicity_Black} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_Black,
                                       CASE WHEN q.code_id = {WhatRaceEthnicity_White} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_White,
                                       CASE WHEN q.code_id = {WhatRaceEthnicity_AIAN} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_AIAN,
                                       CASE WHEN q.code_id IS NULL THEN 1 ELSE 0 END AS UNSET,
                                       CASE WHEN q.code_id = {WhatRaceEthnicity_RaceEthnicityNoneOfThese} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_RaceEthnicityNoneOfThese,
                                       CASE WHEN q.code_id = {WhatRaceEthnicity_Asian} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_Asian,
                                       CASE WHEN q.code_id = {PMI_PreferNotToAnswer} THEN 1 ELSE 0 END   AS PMI_PreferNotToAnswer,
                                       CASE WHEN q.code_id = {WhatRaceEthnicity_MENA} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_MENA,
                                       CASE WHEN q.code_id = {PMI_Skip} THEN 1 ELSE 0 END   AS PMI_Skip,
                                       CASE WHEN q.code_id = {WhatRaceEthnicity_NHPI} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_NHPI
                                FROM {temp_table_name} ps
                                LEFT JOIN participant_race_answers q ON ps.participant_id = q.participant_id
                              ) x
                         GROUP BY participant_id, hpo_id, sign_up_time, consent_for_study_enrollment_time, enrollment_status_member_time, enrollment_status_core_stored_sample_time, participant_origin
                       ) p,
                       calendar, metrics_tmp_participant_origin po
                  WHERE calendar.day >= :start_date
                    AND calendar.day <= :end_date
                    AND calendar.day >= Date(p.sign_up_time)
                    AND p.participant_origin = po.participant_origin
                ) y
                GROUP BY day, hpo_id, registered, participant, consented, core, participant_origin
                ;
            """.format(cache_type=self.cache_type,
                       temp_table_name=temp_table_name,
                       Race_WhatRaceEthnicity=race_code_dict['Race_WhatRaceEthnicity'],
                       WhatRaceEthnicity_Hispanic=race_code_dict['WhatRaceEthnicity_Hispanic'],
                       WhatRaceEthnicity_Black=race_code_dict['WhatRaceEthnicity_Black'],
                       WhatRaceEthnicity_White=race_code_dict['WhatRaceEthnicity_White'],
                       WhatRaceEthnicity_AIAN=race_code_dict['WhatRaceEthnicity_AIAN'],
                       WhatRaceEthnicity_RaceEthnicityNoneOfThese=
                                         race_code_dict['WhatRaceEthnicity_RaceEthnicityNoneOfThese'],
                       WhatRaceEthnicity_Asian=race_code_dict['WhatRaceEthnicity_Asian'],
                       PMI_PreferNotToAnswer=race_code_dict['PMI_PreferNotToAnswer'],
                       WhatRaceEthnicity_MENA=race_code_dict['WhatRaceEthnicity_MENA'],
                       PMI_Skip=race_code_dict['PMI_Skip'],
                       WhatRaceEthnicity_NHPI=race_code_dict['WhatRaceEthnicity_NHPI'])
        else:
            sql = """
                  insert into metrics_race_cache
                    SELECT
                      :date_inserted as date_inserted,
                      '{cache_type}' as type,
                      registered as registered_flag,
                      participant as participant_flag,
                      consented as consented_flag,
                      core as core_flag,
                      hpo_id,
                      (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
                      day,
                      SUM(American_Indian_Alaska_Native) AS American_Indian_Alaska_Native,
                      SUM(Asian) AS Asian,
                      SUM(Black_African_American) AS Black_African_American,
                      SUM(Middle_Eastern_North_African) AS Middle_Eastern_North_African,
                      SUM(Native_Hawaiian_other_Pacific_Islander) AS Native_Hawaiian_other_Pacific_Islander,
                      SUM(White) AS White,
                      SUM(Hispanic_Latino_Spanish) AS Hispanic_Latino_Spanish,
                      SUM(None_Of_These_Fully_Describe_Me) AS None_Of_These_Fully_Describe_Me,
                      SUM(Prefer_Not_To_Answer) AS Prefer_Not_To_Answer,
                      SUM(Multi_Ancestry) AS Multi_Ancestry,
                      SUM(No_Ancestry_Checked) AS No_Ancestry_Checked,
                      participant_origin,
                      SUM(Unset_No_Basics) AS Unset_No_Basics
                      FROM
                      (
                        SELECT p.hpo_id,
                               day,
                               p.participant_origin,
                               CASE WHEN DATE(sign_up_time)<=calendar.day AND (consent_for_study_enrollment_time IS NULL OR DATE(consent_for_study_enrollment_time)>calendar.day)
                                   THEN 1 ELSE 0 END AS registered,
                               CASE WHEN (consent_for_study_enrollment_time IS NOT NULL AND DATE(consent_for_study_enrollment_time)<=calendar.day) AND
                                         (enrollment_status_member_time IS NULL OR DATE(enrollment_status_member_time)>calendar.day)
                                   THEN 1 ELSE 0 END AS participant,
                               CASE WHEN (enrollment_status_member_time IS NOT NULL AND DATE(enrollment_status_member_time)<=calendar.day) AND
                                         (enrollment_status_core_stored_sample_time IS NULL OR DATE(enrollment_status_core_stored_sample_time)>calendar.day)
                                   THEN 1 ELSE 0 END AS consented,
                               CASE WHEN enrollment_status_core_stored_sample_time IS NOT NULL AND DATE(enrollment_status_core_stored_sample_time)<=calendar.day
                                   THEN 1 ELSE 0 END AS core,
                               CASE WHEN WhatRaceEthnicity_AIAN=1 THEN 1 ELSE 0 END AS American_Indian_Alaska_Native,
                               CASE WHEN WhatRaceEthnicity_Asian=1 THEN 1 ELSE 0 END AS Asian,
                               CASE WHEN WhatRaceEthnicity_Black=1 THEN 1 ELSE 0 END AS Black_African_American,
                               CASE WHEN WhatRaceEthnicity_MENA=1 THEN 1 ELSE 0 END AS Middle_Eastern_North_African,
                               CASE WHEN WhatRaceEthnicity_NHPI=1 THEN 1 ELSE 0 END AS Native_Hawaiian_other_Pacific_Islander,
                               CASE WHEN WhatRaceEthnicity_White=1 THEN 1 ELSE 0 END AS White,
                               CASE WHEN WhatRaceEthnicity_Hispanic=1 THEN 1 ELSE 0 END AS Hispanic_Latino_Spanish,
                               CASE WHEN WhatRaceEthnicity_RaceEthnicityNoneOfThese=1 THEN 1 ELSE 0 END AS None_Of_These_Fully_Describe_Me,
                               CASE WHEN PMI_PreferNotToAnswer=1 THEN 1 ELSE 0 END AS Prefer_Not_To_Answer,
                               CASE
                                 WHEN (
                                       WhatRaceEthnicity_Hispanic +
                                       WhatRaceEthnicity_Black +
                                       WhatRaceEthnicity_White +
                                       WhatRaceEthnicity_AIAN +
                                       WhatRaceEthnicity_Asian +
                                       WhatRaceEthnicity_MENA +
                                       WhatRaceEthnicity_NHPI
                                       ) > 1
                                   THEN 1
                                 ELSE 0
                               END AS Multi_Ancestry,
                               CASE
                                 WHEN (PMI_Skip = 1 AND Number_of_Answer=1) OR (HasBasicsSurvey = 1 AND UNSET = 1)
                                   THEN 1
                                 ELSE 0
                               END AS No_Ancestry_Checked,
                               CASE WHEN HasBasicsSurvey = 0 THEN 1 ELSE 0 END AS Unset_No_Basics
                        FROM (
                               SELECT participant_id,
                                      hpo_id,
                                      sign_up_time,
                                      consent_for_study_enrollment_time,
                                      enrollment_status_member_time,
                                      enrollment_status_core_stored_sample_time,
                                      participant_origin,
                                      MAX(WhatRaceEthnicity_Hispanic)                 AS WhatRaceEthnicity_Hispanic,
                                      MAX(WhatRaceEthnicity_Black)                    AS WhatRaceEthnicity_Black,
                                      MAX(WhatRaceEthnicity_White)                    AS WhatRaceEthnicity_White,
                                      MAX(WhatRaceEthnicity_AIAN)                     AS WhatRaceEthnicity_AIAN,
                                      MAX(UNSET)                                      AS UNSET,
                                      MAX(WhatRaceEthnicity_RaceEthnicityNoneOfThese) AS WhatRaceEthnicity_RaceEthnicityNoneOfThese,
                                      MAX(WhatRaceEthnicity_Asian)                    AS WhatRaceEthnicity_Asian,
                                      MAX(PMI_PreferNotToAnswer)                      AS PMI_PreferNotToAnswer,
                                      MAX(WhatRaceEthnicity_MENA)                     AS WhatRaceEthnicity_MENA,
                                      MAX(PMI_Skip)                                   AS PMI_Skip,
                                      MAX(WhatRaceEthnicity_NHPI)                     AS WhatRaceEthnicity_NHPI,
                                      MAX(questionnaire_on_the_basics) AS HasBasicsSurvey,
                                      COUNT(*) as Number_of_Answer
                               FROM (
                                      SELECT ps.participant_id,
                                             ps.hpo_id,
                                             ps.sign_up_time,
                                             consent_for_study_enrollment_time,
                                             ps.enrollment_status_member_time,
                                             ps.enrollment_status_core_stored_sample_time,
                                             ps.participant_origin,
                                             ps.questionnaire_on_the_basics,
                                             CASE WHEN q.code_id = {WhatRaceEthnicity_Hispanic} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_Hispanic,
                                             CASE WHEN q.code_id = {WhatRaceEthnicity_Black} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_Black,
                                             CASE WHEN q.code_id = {WhatRaceEthnicity_White} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_White,
                                             CASE WHEN q.code_id = {WhatRaceEthnicity_AIAN} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_AIAN,
                                             CASE WHEN q.code_id IS NULL THEN 1 ELSE 0 END AS UNSET,
                                             CASE WHEN q.code_id = {WhatRaceEthnicity_RaceEthnicityNoneOfThese} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_RaceEthnicityNoneOfThese,
                                             CASE WHEN q.code_id = {WhatRaceEthnicity_Asian} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_Asian,
                                             CASE WHEN q.code_id = {PMI_PreferNotToAnswer} THEN 1 ELSE 0 END   AS PMI_PreferNotToAnswer,
                                             CASE WHEN q.code_id = {WhatRaceEthnicity_MENA} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_MENA,
                                             CASE WHEN q.code_id = {PMI_Skip} THEN 1 ELSE 0 END   AS PMI_Skip,
                                             CASE WHEN q.code_id = {WhatRaceEthnicity_NHPI} THEN 1 ELSE 0 END   AS WhatRaceEthnicity_NHPI
                                      FROM {temp_table_name} ps
                                      LEFT JOIN participant_race_answers q ON ps.participant_id = q.participant_id
                                    ) x
                               GROUP BY participant_id, hpo_id, sign_up_time, consent_for_study_enrollment_time, enrollment_status_member_time, enrollment_status_core_stored_sample_time, participant_origin
                             ) p,
                             calendar, metrics_tmp_participant_origin po
                        WHERE calendar.day >= :start_date
                          AND calendar.day <= :end_date
                          AND calendar.day >= Date(p.sign_up_time)
                          AND p.participant_origin = po.participant_origin
                      ) y
                      GROUP BY day, hpo_id, registered, participant, consented, core, participant_origin
                      ;
                """.format(cache_type=self.cache_type,
                           temp_table_name=temp_table_name,
                           Race_WhatRaceEthnicity=race_code_dict['Race_WhatRaceEthnicity'],
                           WhatRaceEthnicity_Hispanic=race_code_dict['WhatRaceEthnicity_Hispanic'],
                           WhatRaceEthnicity_Black=race_code_dict['WhatRaceEthnicity_Black'],
                           WhatRaceEthnicity_White=race_code_dict['WhatRaceEthnicity_White'],
                           WhatRaceEthnicity_AIAN=race_code_dict['WhatRaceEthnicity_AIAN'],
                           WhatRaceEthnicity_RaceEthnicityNoneOfThese=
                                             race_code_dict['WhatRaceEthnicity_RaceEthnicityNoneOfThese'],
                           WhatRaceEthnicity_Asian=race_code_dict['WhatRaceEthnicity_Asian'],
                           PMI_PreferNotToAnswer=race_code_dict['PMI_PreferNotToAnswer'],
                           WhatRaceEthnicity_MENA=race_code_dict['WhatRaceEthnicity_MENA'],
                           PMI_Skip=race_code_dict['PMI_Skip'],
                           WhatRaceEthnicity_NHPI=race_code_dict['WhatRaceEthnicity_NHPI'])
        return [sql]


class MetricsRegionCacheDao(BaseDao):

    def __init__(self, cache_type=MetricsCacheType.METRICS_V2_API, version=None):
        super(MetricsRegionCacheDao, self).__init__(MetricsRegionCache)
        self.version = version
        self.table_name = MetricsRegionCache.__tablename__
        try:
            self.cache_type = MetricsCacheType(str(cache_type))
        except TypeError:
            raise TypeError("Invalid metrics cache type")

    def get_serving_version_with_session(self, session):
        status_dao = MetricsCacheJobStatusDao()
        record = status_dao.get_last_complete_data_inserted_time(self.table_name)
        if record is not None:
            return record
        else:
            return (session.query(MetricsRegionCache)
                    .order_by(MetricsRegionCache.dateInserted.desc())
                    .first())

    def get_active_buckets(self, cutoff, stratification, hpo_ids=None, enrollment_statuses=None,
                           participant_origins=None):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is None:
                return None
            last_inserted_date = last_inserted_record.dateInserted
            if self.cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API \
                and stratification not in [Stratifications.FULL_AWARDEE, Stratifications.GEO_AWARDEE]:
                query = session.query(MetricsRegionCache.date, MetricsRegionCache.stateName,
                                      func.sum(MetricsRegionCache.stateCount).label('total'))
                query = query.filter(MetricsRegionCache.dateInserted == last_inserted_date)
                query = query.filter(MetricsRegionCache.date == cutoff)
                if stratification in [Stratifications.FULL_STATE, Stratifications.FULL_CENSUS,
                                      Stratifications.FULL_AWARDEE]:
                    query = query.filter(MetricsRegionCache.enrollmentStatus == 'core')
                if hpo_ids:
                    query = query.filter(MetricsRegionCache.hpoId.in_(hpo_ids))
                if enrollment_statuses:
                    status_filter_list = []
                    for status in enrollment_statuses:
                        if status == str(EnrollmentStatus.INTERESTED):
                            status_filter_list.append('registered')
                            status_filter_list.append('participant')
                        elif status == str(EnrollmentStatus.MEMBER):
                            status_filter_list.append('consented')
                        elif status == str(EnrollmentStatus.FULL_PARTICIPANT):
                            status_filter_list.append('core')
                    query = query.filter(MetricsRegionCache.enrollmentStatus.in_(status_filter_list))

                return query.group_by(MetricsRegionCache.date, MetricsRegionCache.stateName).all()
            else:
                if self.version == MetricsAPIVersion.V2:
                    query = session.query(MetricsRegionCache.date, MetricsRegionCache.hpoName,
                                          MetricsRegionCache.stateName,
                                          func.sum(MetricsRegionCache.stateCount).label('total'))
                    query = query.filter(MetricsRegionCache.dateInserted == last_inserted_date)
                    query = query.filter(MetricsRegionCache.date == cutoff)
                    if stratification in [Stratifications.FULL_STATE, Stratifications.FULL_CENSUS,
                                          Stratifications.FULL_AWARDEE]:
                        query = query.filter(MetricsRegionCache.enrollmentStatus == 'core')
                    if hpo_ids:
                        query = query.filter(MetricsRegionCache.hpoId.in_(hpo_ids))
                    if participant_origins:
                        query = query.filter(MetricsRegionCache.participantOrigin.in_(participant_origins))
                    if enrollment_statuses:
                        status_filter_list = []
                        for status in enrollment_statuses:
                            if status == str(EnrollmentStatusV2.REGISTERED):
                                status_filter_list.append('registered')
                            elif status == str(EnrollmentStatusV2.PARTICIPANT):
                                status_filter_list.append('participant')
                            elif status == str(EnrollmentStatusV2.FULLY_CONSENTED):
                                status_filter_list.append('consented')
                            elif status == str(EnrollmentStatusV2.CORE_PARTICIPANT):
                                status_filter_list.append('core')
                        query = query.filter(MetricsRegionCache.enrollmentStatus.in_(status_filter_list))

                    return query.group_by(MetricsRegionCache.date, MetricsRegionCache.hpoName,
                                          MetricsRegionCache.stateName).all()
                else:
                    query = session.query(MetricsRegionCache.date, MetricsRegionCache.hpoName,
                                          MetricsRegionCache.stateName,
                                          func.sum(MetricsRegionCache.stateCount).label('total'))
                    query = query.filter(MetricsRegionCache.dateInserted == last_inserted_date)
                    query = query.filter(MetricsRegionCache.date == cutoff)
                    if stratification in [Stratifications.FULL_STATE, Stratifications.FULL_CENSUS,
                                          Stratifications.FULL_AWARDEE]:
                        query = query.filter(MetricsRegionCache.enrollmentStatus == 'core')
                    if hpo_ids:
                        query = query.filter(MetricsRegionCache.hpoId.in_(hpo_ids))
                    if enrollment_statuses:
                        status_filter_list = []
                        for status in enrollment_statuses:
                            if status == str(EnrollmentStatus.INTERESTED):
                                status_filter_list.append('registered')
                                status_filter_list.append('participant')
                            elif status == str(EnrollmentStatus.MEMBER):
                                status_filter_list.append('consented')
                            elif status == str(EnrollmentStatus.FULL_PARTICIPANT):
                                status_filter_list.append('core')
                        query = query.filter(MetricsRegionCache.enrollmentStatus.in_(status_filter_list))

                    return query.group_by(MetricsRegionCache.date, MetricsRegionCache.hpoName,
                                          MetricsRegionCache.stateName).all()

    def get_latest_version_from_cache(self, cutoff, stratification, hpo_ids=None,
                                      enrollment_statuses=None, participant_origins=None):
        stratification = Stratifications(str(stratification))
        operation_funcs = {
            Stratifications.FULL_STATE: self.to_state_client_json,
            Stratifications.FULL_CENSUS: self.to_census_client_json,
            Stratifications.FULL_AWARDEE: self.to_awardee_client_json,
            Stratifications.GEO_STATE: self.to_state_client_json,
            Stratifications.GEO_CENSUS: self.to_census_client_json,
            Stratifications.GEO_AWARDEE: self.to_awardee_client_json
        }

        buckets = self.get_active_buckets(cutoff, stratification, hpo_ids, enrollment_statuses, participant_origins)
        if buckets is None:
            return []
        return operation_funcs[stratification](buckets)

    def delete_old_records(self, n_days_ago=7):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is not None:
                last_date_inserted = last_inserted_record.dateInserted
                seven_days_ago = last_date_inserted - datetime.timedelta(days=n_days_ago)
                delete_sql = """
                  delete from metrics_region_cache where date_inserted < :seven_days_ago
                """
                params = {'seven_days_ago': seven_days_ago}
                session.execute(delete_sql, params)

    def remove_prefix(self, text, prefix):
        if text.startswith(prefix):
            return text[len(prefix):]
        return text

    def to_state_client_json(self, result_set):
        client_json = []
        if self.cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API:
            for record in result_set:
                state_name = self.remove_prefix(record.stateName, 'PIIState_')
                if state_name not in census_regions:
                    continue
                is_exist = False
                for item in client_json:
                    if item['date'] == record.date.isoformat():
                        item['metrics'][state_name] = int(record.total)
                        is_exist = True
                        break

                if not is_exist:
                    metrics = {stateName: 0 for stateName in census_regions.keys()}
                    new_item = {
                        'date': record.date.isoformat(),
                        'metrics': metrics
                    }
                    new_item['metrics'][state_name] = int(record.total)
                    client_json.append(new_item)
        else:
            for record in result_set:
                state_name = self.remove_prefix(record.stateName, 'PIIState_')
                if state_name not in census_regions:
                    continue
                is_exist = False
                for item in client_json:
                    if item['date'] == record.date.isoformat() and item['hpo'] == record.hpoName:
                        item['metrics'][state_name] = int(record.total)
                        is_exist = True
                        break

                if not is_exist:
                    metrics = {stateName: 0 for stateName in census_regions.keys()}
                    new_item = {
                        'date': record.date.isoformat(),
                        'hpo': record.hpoName,
                        'metrics': metrics
                    }
                    new_item['metrics'][state_name] = int(record.total)
                    client_json.append(new_item)

        return client_json

    def to_census_client_json(self, result_set):
        client_json = []
        if self.cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API:
            for record in result_set:
                state_name = self.remove_prefix(record.stateName, 'PIIState_')
                if state_name in census_regions:
                    census_name = census_regions[state_name]
                else:
                    continue
                is_exist = False
                for item in client_json:
                    if item['date'] == record.date.isoformat():
                        item['metrics'][census_name] += int(record.total)
                        is_exist = True
                        break

                if not is_exist:
                    new_item = {
                        'date': record.date.isoformat(),
                        'metrics': {
                            'NORTHEAST': 0,
                            'MIDWEST': 0,
                            'SOUTH': 0,
                            'WEST': 0,
                            'TERRITORIES': 0
                        }
                    }
                    new_item['metrics'][census_name] = int(record.total)
                    client_json.append(new_item)
        else:
            for record in result_set:
                state_name = self.remove_prefix(record.stateName, 'PIIState_')
                if state_name in census_regions:
                    census_name = census_regions[state_name]
                else:
                    continue
                is_exist = False
                for item in client_json:
                    if item['date'] == record.date.isoformat() and item['hpo'] == record.hpoName:
                        item['metrics'][census_name] += int(record.total)
                        is_exist = True
                        break

                if not is_exist:
                    new_item = {
                        'date': record.date.isoformat(),
                        'hpo': record.hpoName,
                        'metrics': {
                            'NORTHEAST': 0,
                            'MIDWEST': 0,
                            'SOUTH': 0,
                            'WEST': 0,
                            'TERRITORIES': 0
                        }
                    }
                    new_item['metrics'][census_name] = int(record.total)
                    client_json.append(new_item)

        return client_json

    def to_awardee_client_json(self, result_set):
        client_json = []
        for record in result_set:
            is_exist = False
            for item in client_json:
                if item['date'] == record.date.isoformat() and item['hpo'] == record.hpoName:
                    item['count'] += int(record.total)
                    is_exist = True
                    break

            if not is_exist:
                new_item = {
                    'date': record.date.isoformat(),
                    'hpo': record.hpoName,
                    'count': int(record.total)
                }
                client_json.append(new_item)
        return client_json

    def get_metrics_cache_sql(self, hpo_id):
        temp_table_name = TEMP_TABLE_PREFIX + str(hpo_id)
        sql = """
          INSERT INTO metrics_region_cache
            SELECT
              :date_inserted AS date_inserted,
              'core' as enrollment_status,
              :hpo_id AS hpo_id,
              (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
              c.day,
              IFNULL(ps.value,'UNSET') AS state_name,
              count(ps.participant_id) AS state_count,
              ps.participant_origin
            FROM
              (
              SELECT participant_id, participant_origin, hpo_id, value, enrollment_status_core_stored_sample_time
              FROM {temp_table_name}, code WHERE state_id=code_id
              ) ps,
              metrics_tmp_participant_origin po,
              calendar c
            WHERE ps.participant_origin = po.participant_origin
            AND ps.enrollment_status_core_stored_sample_time IS NOT NULL
            AND DATE(ps.enrollment_status_core_stored_sample_time) <= c.day
            AND c.day BETWEEN :start_date AND :end_date
            GROUP BY c.day, ps.hpo_id, ps.value, ps.participant_origin
            UNION ALL
            SELECT
              :date_inserted AS date_inserted,
              'registered' as enrollment_status,
              :hpo_id AS hpo_id,
              (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
              c.day,
              IFNULL(ps.value,'UNSET') AS state_name,
              count(ps.participant_id) AS state_count,
              ps.participant_origin
            FROM
              (
              SELECT participant_id, participant_origin, hpo_id, value, sign_up_time, consent_for_study_enrollment_time
              FROM {temp_table_name}, code WHERE state_id=code_id
              ) ps,
              metrics_tmp_participant_origin po,
              calendar c
            WHERE ps.participant_origin = po.participant_origin
            AND ps.sign_up_time IS NOT NULL
            AND DATE(ps.sign_up_time) <= c.day
            AND (ps.consent_for_study_enrollment_time IS NULL OR DATE(ps.consent_for_study_enrollment_time)>c.day)
            AND c.day BETWEEN :start_date AND :end_date
            GROUP BY c.day, ps.hpo_id, ps.value, ps.participant_origin
            UNION ALL
            SELECT
              :date_inserted AS date_inserted,
              'participant' as enrollment_status,
              :hpo_id AS hpo_id,
              (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
              c.day,
              IFNULL(ps.value,'UNSET') AS state_name,
              count(ps.participant_id) AS state_count,
              ps.participant_origin
            FROM
              (
              SELECT participant_id, participant_origin, hpo_id, value, consent_for_study_enrollment_time, enrollment_status_member_time
              FROM {temp_table_name}, code WHERE state_id=code_id
              ) ps,
              metrics_tmp_participant_origin po,
              calendar c
            WHERE ps.participant_origin = po.participant_origin
            AND ps.consent_for_study_enrollment_time IS NOT NULL
            AND DATE(ps.consent_for_study_enrollment_time) <= c.day
            AND (ps.enrollment_status_member_time is null or DATE(ps.enrollment_status_member_time)>c.day)
            AND c.day BETWEEN :start_date AND :end_date
            GROUP BY c.day, ps.hpo_id, ps.value, ps.participant_origin
            UNION ALL
            SELECT
              :date_inserted AS date_inserted,
              'consented' as enrollment_status,
              :hpo_id AS hpo_id,
              (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
              c.day,
              IFNULL(ps.value,'UNSET') AS state_name,
              count(ps.participant_id) AS state_count,
              ps.participant_origin
            FROM
              (
              SELECT participant_id, participant_origin, hpo_id, value, enrollment_status_member_time, enrollment_status_core_stored_sample_time
              FROM {temp_table_name}, code WHERE state_id=code_id
              ) ps,
              metrics_tmp_participant_origin po,
              calendar c
            WHERE ps.participant_origin = po.participant_origin
            AND ps.enrollment_status_member_time IS NOT NULL
            AND DATE(ps.enrollment_status_member_time) <= c.day
            AND (ps.enrollment_status_core_stored_sample_time is null or DATE(ps.enrollment_status_core_stored_sample_time)>c.day)
            AND c.day BETWEEN :start_date AND :end_date
            GROUP BY c.day, ps.hpo_id, ps.value, ps.participant_origin
            ;
        """.format(temp_table_name=temp_table_name)

        return [sql]


class MetricsLifecycleCacheDao(BaseDao):

    def __init__(self, cache_type=MetricsCacheType.METRICS_V2_API, version=None):
        super(MetricsLifecycleCacheDao, self).__init__(MetricsLifecycleCache)
        try:
            self.cache_type = MetricsCacheType(str(cache_type))
            self.version = version
            self.table_name = MetricsLifecycleCache.__tablename__
        except TypeError:
            raise TypeError("Invalid metrics cache type")

    def get_serving_version_with_session(self, session):
        status_dao = MetricsCacheJobStatusDao()
        record = status_dao.get_last_complete_data_inserted_time(self.table_name, self.cache_type)
        if record is not None:
            return record
        else:
            return (session.query(MetricsLifecycleCache)
                    .order_by(MetricsLifecycleCache.dateInserted.desc())
                    .first())

    def get_active_buckets(self, cutoff, hpo_ids=None, enrollment_statuses=None, participant_origins=None):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is None:
                return None
            last_inserted_date = last_inserted_record.dateInserted
            if self.cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API:
                query = session.query(MetricsLifecycleCache.date,
                                      func.sum(MetricsLifecycleCache.registered)
                                      .label('registered'),
                                      func.sum(MetricsLifecycleCache.consentEnrollment)
                                      .label('consentEnrollment'),
                                      func.sum(MetricsLifecycleCache.consentComplete)
                                      .label('consentComplete'),
                                      func.sum(MetricsLifecycleCache.ppiBasics)
                                      .label('ppiBasics'),
                                      func.sum(MetricsLifecycleCache.ppiOverallHealth)
                                      .label('ppiOverallHealth'),
                                      func.sum(MetricsLifecycleCache.ppiLifestyle)
                                      .label('ppiLifestyle'),
                                      func.sum(MetricsLifecycleCache.ppiHealthcareAccess)
                                      .label('ppiHealthcareAccess'),
                                      func.sum(MetricsLifecycleCache.ppiMedicalHistory)
                                      .label('ppiMedicalHistory'),
                                      func.sum(MetricsLifecycleCache.ppiMedications)
                                      .label('ppiMedications'),
                                      func.sum(MetricsLifecycleCache.ppiFamilyHealth)
                                      .label('ppiFamilyHealth'),
                                      func.sum(MetricsLifecycleCache.ppiBaselineComplete)
                                      .label('ppiBaselineComplete'),
                                      func.sum(MetricsLifecycleCache.physicalMeasurement)
                                      .label('physicalMeasurement'),
                                      func.sum(MetricsLifecycleCache.sampleReceived)
                                      .label('sampleReceived'),
                                      func.sum(MetricsLifecycleCache.fullParticipant)
                                      .label('fullParticipant')
                                      )
                query = query.filter(MetricsLifecycleCache.dateInserted == last_inserted_date)
                query = query.filter(MetricsLifecycleCache.type != MetricsCacheType.METRICS_V2_API)
                query = query.filter(MetricsLifecycleCache.date == cutoff)

                if hpo_ids:
                    query = query.filter(MetricsLifecycleCache.hpoId.in_(hpo_ids))

                return query.group_by(MetricsLifecycleCache.date).all()
            else:
                query = session.query(MetricsLifecycleCache.date,
                                      MetricsLifecycleCache.hpoId,
                                      MetricsLifecycleCache.hpoName,
                                      func.sum(MetricsLifecycleCache.registered)
                                      .label('registered'),
                                      func.sum(MetricsLifecycleCache.consentEnrollment)
                                      .label('consentEnrollment'),
                                      func.sum(MetricsLifecycleCache.consentComplete)
                                      .label('consentComplete'),
                                      func.sum(MetricsLifecycleCache.ppiBasics)
                                      .label('ppiBasics'),
                                      func.sum(MetricsLifecycleCache.ppiOverallHealth)
                                      .label('ppiOverallHealth'),
                                      func.sum(MetricsLifecycleCache.ppiLifestyle)
                                      .label('ppiLifestyle'),
                                      func.sum(MetricsLifecycleCache.ppiHealthcareAccess)
                                      .label('ppiHealthcareAccess'),
                                      func.sum(MetricsLifecycleCache.ppiMedicalHistory)
                                      .label('ppiMedicalHistory'),
                                      func.sum(MetricsLifecycleCache.ppiMedications)
                                      .label('ppiMedications'),
                                      func.sum(MetricsLifecycleCache.ppiFamilyHealth)
                                      .label('ppiFamilyHealth'),
                                      func.sum(MetricsLifecycleCache.ppiBaselineComplete)
                                      .label('ppiBaselineComplete'),
                                      func.sum(MetricsLifecycleCache.physicalMeasurement)
                                      .label('physicalMeasurement'),
                                      func.sum(MetricsLifecycleCache.retentionModulesEligible)
                                      .label('retentionModulesEligible'),
                                      func.sum(MetricsLifecycleCache.retentionModulesComplete)
                                      .label('retentionModulesComplete'),
                                      func.sum(MetricsLifecycleCache.sampleReceived)
                                      .label('sampleReceived'),
                                      func.sum(MetricsLifecycleCache.fullParticipant)
                                      .label('fullParticipant')
                                      )
                query = query.filter(MetricsLifecycleCache.dateInserted == last_inserted_date)
                query = query.filter(MetricsLifecycleCache.type !=
                                     MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
                query = query.filter(MetricsLifecycleCache.date == cutoff)

                if hpo_ids:
                    query = query.filter(MetricsLifecycleCache.hpoId.in_(hpo_ids))

                if participant_origins:
                    query = query.filter(MetricsLifecycleCache.participantOrigin.in_(participant_origins))

                if enrollment_statuses:
                    status_filter_list = []
                    for status in enrollment_statuses:
                        if status == str(EnrollmentStatusV2.REGISTERED):
                            status_filter_list.append('registered')
                        elif status == str(EnrollmentStatusV2.PARTICIPANT):
                            status_filter_list.append('participant')
                        elif status == str(EnrollmentStatusV2.FULLY_CONSENTED):
                            status_filter_list.append('consented')
                        elif status == str(EnrollmentStatusV2.CORE_PARTICIPANT):
                            status_filter_list.append('core')
                    query = query.filter(MetricsLifecycleCache.enrollmentStatus.in_(status_filter_list))

                return query.group_by(MetricsLifecycleCache.hpoId, MetricsLifecycleCache.hpoName,
                                      MetricsLifecycleCache.date).all()

    def get_primary_consent_count_over_time(self, start_date, end_date, hpo_ids=None):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is None:
                return None
            last_inserted_date = last_inserted_record.dateInserted
            query = session.query(MetricsLifecycleCache.date,
                                  func.sum(MetricsLifecycleCache.consentEnrollment)
                                  .label('primaryConsent'))
            query = query.filter(MetricsLifecycleCache.dateInserted == last_inserted_date)
            query = query.filter(MetricsLifecycleCache.type == self.cache_type)
            query = query.filter(MetricsLifecycleCache.date >= start_date)
            query = query.filter(MetricsLifecycleCache.date <= end_date)

            if hpo_ids:
                query = query.filter(MetricsLifecycleCache.hpoId.in_(hpo_ids))

            result_set = query.group_by(MetricsLifecycleCache.date).all()
            client_json = []
            for record in result_set:
                new_item = {
                    'date': record.date.isoformat(),
                    'metrics': {
                        'Primary_Consent': int(record.primaryConsent)
                    }
                }
                client_json.append(new_item)
            return client_json

    def get_latest_version_from_cache(self, cutoff, hpo_ids=None, enrollment_statuses=None, participant_origins=None):
        buckets = self.get_active_buckets(cutoff, hpo_ids, enrollment_statuses, participant_origins)
        if buckets is None:
            return []
        operation_funcs = {
            MetricsCacheType.PUBLIC_METRICS_EXPORT_API: self.to_public_metrics_client_json,
            MetricsCacheType.METRICS_V2_API: self.to_metrics_client_json
        }
        return operation_funcs[self.cache_type](buckets)

    def update_historical_cache_data(self, new_date_inserted_time, last_date_inserted_time, start_date, end_date):
        with self.session() as session:
            query = (
                sqlalchemy
                    .update(MetricsLifecycleCache)
                    .where(and_(MetricsLifecycleCache.dateInserted == last_date_inserted_time,
                                MetricsLifecycleCache.date >= start_date,
                                MetricsLifecycleCache.date <= end_date,
                                MetricsLifecycleCache.type == self.cache_type)
                           )
                    .values({MetricsLifecycleCache.dateInserted: new_date_inserted_time})
            )
            session.execute(query)

    def delete_old_records(self, n_days_ago=7):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is not None:
                last_date_inserted = last_inserted_record.dateInserted
                seven_days_ago = last_date_inserted - datetime.timedelta(days=n_days_ago)
                delete_sql = """
          delete from metrics_lifecycle_cache where date_inserted < :seven_days_ago and type = :type
        """
                params = {'seven_days_ago': seven_days_ago, 'type': self.cache_type}
                session.execute(delete_sql, params)

    def to_metrics_client_json(self, result_set):
        client_json = []
        if self.version == MetricsAPIVersion.V2:
            for record in result_set:
                new_item = {
                    'date': record.date.isoformat(),
                    'hpo': record.hpoName,
                    'metrics': {
                        'completed': {
                            'Registered': int(record.registered),
                            'Consent_Enrollment': int(record.consentEnrollment),
                            'Consent_Complete': int(record.consentComplete),
                            'PPI_Module_The_Basics': int(record.ppiBasics),
                            'PPI_Module_Overall_Health': int(record.ppiOverallHealth),
                            'PPI_Module_Lifestyle': int(record.ppiLifestyle),
                            'Baseline_PPI_Modules_Complete': int(record.ppiBaselineComplete),
                            'Physical_Measurements': int(record.physicalMeasurement),
                            'PPI_Module_Healthcare_Access': int(record.ppiHealthcareAccess),
                            'PPI_Module_Family_Health': int(record.ppiFamilyHealth),
                            'PPI_Module_Medical_History': int(record.ppiMedicalHistory),
                            'PPI_Retention_Modules_Complete': int(record.retentionModulesComplete),
                            'Samples_Received': int(record.sampleReceived),
                            'Full_Participant': int(record.fullParticipant)
                        },
                        'not_completed': {
                            'Registered': 0,
                            'Consent_Enrollment': int(record.registered - record.consentEnrollment),
                            'Consent_Complete': int(record.consentEnrollment - record.consentComplete),
                            'PPI_Module_The_Basics': int(record.consentEnrollment - record.ppiBasics),
                            'PPI_Module_Overall_Health': int(record.consentEnrollment - record.ppiOverallHealth),
                            'PPI_Module_Lifestyle': int(record.consentEnrollment - record.ppiLifestyle),
                            'Baseline_PPI_Modules_Complete': int(record.consentEnrollment -
                                                                 record.ppiBaselineComplete),
                            'Physical_Measurements': int(record.consentEnrollment - record.physicalMeasurement),
                            'PPI_Module_Healthcare_Access': int(record.retentionModulesEligible -
                                                                record.ppiHealthcareAccess),
                            'PPI_Module_Family_Health': int(record.retentionModulesEligible -
                                                            record.ppiFamilyHealth),
                            'PPI_Module_Medical_History': int(record.retentionModulesEligible -
                                                              record.ppiMedicalHistory),
                            'PPI_Retention_Modules_Complete': int(record.retentionModulesEligible -
                                                                  record.retentionModulesComplete),
                            'Samples_Received': int(record.consentEnrollment - record.sampleReceived),
                            'Full_Participant': int(record.consentEnrollment - record.fullParticipant)
                        }
                    }
                }
                client_json.append(new_item)
        else:
            for record in result_set:
                new_item = {
                    'date': record.date.isoformat(),
                    'hpo': record.hpoName,
                    'metrics': {
                        'completed': {
                            'Registered': int(record.registered),
                            'Consent_Enrollment': int(record.consentEnrollment),
                            'Consent_Complete': int(record.consentComplete),
                            'PPI_Module_The_Basics': int(record.ppiBasics),
                            'PPI_Module_Overall_Health': int(record.ppiOverallHealth),
                            'PPI_Module_Lifestyle': int(record.ppiLifestyle),
                            'Baseline_PPI_Modules_Complete': int(record.ppiBaselineComplete),
                            'Physical_Measurements': int(record.physicalMeasurement),
                            'Samples_Received': int(record.sampleReceived),
                            'Full_Participant': int(record.fullParticipant)
                        },
                        'not_completed': {
                            'Registered': 0,
                            'Consent_Enrollment': int(record.registered - record.consentEnrollment),
                            'Consent_Complete': int(record.consentEnrollment - record.consentComplete),
                            'PPI_Module_The_Basics': int(record.consentEnrollment - record.ppiBasics),
                            'PPI_Module_Overall_Health': int(record.consentEnrollment - record.ppiOverallHealth),
                            'PPI_Module_Lifestyle': int(record.consentEnrollment - record.ppiLifestyle),
                            'Baseline_PPI_Modules_Complete': int(record.consentEnrollment -
                                                                 record.ppiBaselineComplete),
                            'Physical_Measurements': int(record.consentEnrollment - record.physicalMeasurement),
                            'Samples_Received': int(record.consentEnrollment - record.sampleReceived),
                            'Full_Participant': int(record.consentEnrollment - record.fullParticipant)
                        }
                    }
                }
                client_json.append(new_item)
        return client_json

    def to_public_metrics_client_json(self, result_set):
        client_json = []
        for record in result_set:
            new_item = {
                'date': record.date.isoformat(),
                'metrics': {
                    'completed': {
                        'Registered': int(record.registered),
                        'Consent_Enrollment': int(record.consentEnrollment),
                        'Consent_Complete': int(record.consentComplete),
                        'PPI_Module_The_Basics': int(record.ppiBasics),
                        'PPI_Module_Overall_Health': int(record.ppiOverallHealth),
                        'PPI_Module_Lifestyle': int(record.ppiLifestyle),
                        'PPI_Module_Healthcare_Access': int(record.ppiHealthcareAccess),
                        'PPI_Module_Medical_History': int(record.ppiMedicalHistory),
                        'PPI_Module_Medications': int(record.ppiMedications),
                        'PPI_Module_Family_Health': int(record.ppiFamilyHealth),
                        'Baseline_PPI_Modules_Complete': int(record.ppiBaselineComplete),
                        'Physical_Measurements': int(record.physicalMeasurement),
                        'Samples_Received': int(record.sampleReceived),
                        'Full_Participant': int(record.fullParticipant)
                    },
                    'not_completed': {
                        'Registered': 0,
                        'Consent_Enrollment': int(record.registered - record.consentEnrollment),
                        'Consent_Complete': int(record.consentEnrollment - record.consentComplete),
                        'PPI_Module_The_Basics': int(record.consentEnrollment - record.ppiBasics),
                        'PPI_Module_Overall_Health': int(record.consentEnrollment - record.ppiOverallHealth),
                        'PPI_Module_Lifestyle': int(record.consentEnrollment - record.ppiLifestyle),
                        'PPI_Module_Healthcare_Access': int(record.consentEnrollment -
                                                            record.ppiHealthcareAccess),
                        'PPI_Module_Medical_History': int(record.consentEnrollment - record.ppiMedicalHistory),
                        'PPI_Module_Medications': int(record.consentEnrollment - record.ppiMedications),
                        'PPI_Module_Family_Health': int(record.consentEnrollment - record.ppiFamilyHealth),
                        'Baseline_PPI_Modules_Complete': int(record.consentEnrollment -
                                                             record.ppiBaselineComplete),
                        'Physical_Measurements': int(record.consentEnrollment - record.physicalMeasurement),
                        'Samples_Received': int(record.consentEnrollment - record.sampleReceived),
                        'Full_Participant': int(record.consentEnrollment - record.fullParticipant)
                    }
                }
            }
            client_json.append(new_item)
        return client_json

    def get_metrics_cache_sql(self, hpo_id):
        temp_table_name = TEMP_TABLE_PREFIX + str(hpo_id)
        sql_arr = []
        if self.cache_type == MetricsCacheType.METRICS_V2_API:
            enrollment_status_criteria_arr = [
                ('registered', 'calendar.day>=DATE(sign_up_time) '
                               'AND (consent_for_study_enrollment_time IS NULL OR '
                               'DATE(consent_for_study_enrollment_time)>calendar.day)'),
                ('participant', 'consent_for_study_enrollment_time IS NOT NULL '
                                'AND calendar.day>=DATE(consent_for_study_enrollment_time) '
                                'AND (enrollment_status_member_time IS NULL '
                                'OR calendar.day < DATE(enrollment_status_member_time))'),
                ('consented', 'enrollment_status_member_time IS NOT NULL '
                              'AND calendar.day>=DATE(enrollment_status_member_time) '
                              'AND (enrollment_status_core_stored_sample_time IS NULL '
                              'OR calendar.day < DATE(enrollment_status_core_stored_sample_time))'),
                ('core', 'enrollment_status_core_stored_sample_time IS NOT NULL '
                         'AND calendar.day>=DATE(enrollment_status_core_stored_sample_time)')
            ]
            for item in enrollment_status_criteria_arr:
                sql = """
                insert into metrics_lifecycle_cache
                  select
                    :date_inserted AS date_inserted,
                    '{enrollment_status}' as enrollment_status,
                    '{cache_type}' as type,
                    ps.hpo_id,
                    (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
                    day,
                    SUM(CASE WHEN DATE(ps.sign_up_time) <= calendar.day THEN 1 ELSE 0 END) AS registered,
                    SUM(CASE WHEN DATE(ps.consent_for_study_enrollment_time) <= calendar.day THEN 1 ELSE 0 END) AS consent_enrollment,
                    SUM(CASE WHEN DATE(ps.enrollment_status_member_time) <= calendar.day THEN 1 ELSE 0 END) AS consent_complete,
                    SUM(CASE
                      WHEN
                        DATE(ps.questionnaire_on_the_basics_time) <= calendar.day AND
                        DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                      THEN 1 ELSE 0
                    END) AS ppi_basics,
                    SUM(CASE
                      WHEN
                        DATE(ps.questionnaire_on_overall_health_time) <= calendar.day AND
                        DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                      THEN 1 ELSE 0
                    END) AS ppi_overall_health,
                    SUM(CASE
                      WHEN
                        DATE(ps.questionnaire_on_lifestyle_time) <= calendar.day AND
                        DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                      THEN 1 ELSE 0
                    END) AS ppi_lifestyle,
                    SUM(CASE
                      WHEN
                        DATE(ps.questionnaire_on_healthcare_access_time) <= calendar.day AND
                        DATE(ps.questionnaire_on_the_basics_time) <= calendar.day AND
                        DATEDIFF(calendar.day, DATE(ps.consent_for_study_enrollment_time)) > 90
                      THEN 1 ELSE 0
                    END) AS ppi_healthcare_access,
                    SUM(CASE
                      WHEN
                        DATE(ps.questionnaire_on_medical_history_time) <= calendar.day AND
                        DATE(ps.questionnaire_on_the_basics_time) <= calendar.day AND
                        DATEDIFF(calendar.day, DATE(ps.consent_for_study_enrollment_time)) > 90
                      THEN 1 ELSE 0
                    END) AS ppi_medical_history,
                    SUM(CASE
                      WHEN
                        DATE(ps.questionnaire_on_medications_time) <= calendar.day AND
                        DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                      THEN 1 ELSE 0
                    END) AS ppi_medications,
                    SUM(CASE
                      WHEN
                        DATE(ps.questionnaire_on_family_health_time) <= calendar.day AND
                        DATE(ps.questionnaire_on_the_basics_time) <= calendar.day AND
                        DATEDIFF(calendar.day, DATE(ps.consent_for_study_enrollment_time)) > 90
                      THEN 1 ELSE 0
                    END) AS ppi_family_health,
                    SUM(CASE
                      WHEN
                        DATE(ps.questionnaire_on_lifestyle_time) <= calendar.day AND
                        DATE(ps.questionnaire_on_overall_health_time) <= calendar.day AND
                        DATE(ps.questionnaire_on_the_basics_time) <= calendar.day AND
                        DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                      THEN 1 ELSE 0
                    END) AS ppi_complete,
                    SUM(CASE
                      WHEN
                        DATE(ps.questionnaire_on_the_basics_time) <= calendar.day AND
                        DATEDIFF(calendar.day, DATE(ps.consent_for_study_enrollment_time)) > 90
                      THEN 1 ELSE 0
                    END) AS retention_modules_eligible,
                    SUM(CASE
                      WHEN
                        DATE(ps.questionnaire_on_the_basics_time) <= calendar.day AND
                        DATE(ps.questionnaire_on_healthcare_access_time) <= calendar.day AND
                        DATE(ps.questionnaire_on_family_health_time) <= calendar.day AND
                        DATE(ps.questionnaire_on_medical_history_time) <= calendar.day AND
                        DATEDIFF(calendar.day, DATE(ps.consent_for_study_enrollment_time)) > 90
                      THEN 1 ELSE 0
                    END) AS retention_modules_complete,
                    SUM(CASE
                      WHEN
                        (DATE(ps.clinic_physical_measurements_time) <= calendar.day OR
                        DATE(ps.self_reported_physical_measurements_authored) <= calendar.day) AND
                        DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                      THEN 1 ELSE 0
                    END) AS physical_measurement,
                    SUM(CASE
                      WHEN
                        DATE(ps.sample_status_1ed10_time) <= calendar.day OR
                        DATE(ps.sample_status_2ed10_time) <= calendar.day OR
                        DATE(ps.sample_status_1ed04_time) <= calendar.day OR
                        DATE(ps.sample_status_1sal_time) <= calendar.day OR
                        DATE(ps.sample_status_1sal2_time) <= calendar.day
                      THEN 1 ELSE 0
                    END) AS sample_received,
                    SUM(CASE WHEN DATE(ps.enrollment_status_core_stored_sample_time) <= calendar.day THEN 1 ELSE 0 END) AS core_participant,
                    ps.participant_origin
                  from {temp_table_name} ps,
                       calendar
                  WHERE {enrollment_status_criteria}
                  AND calendar.day BETWEEN :start_date AND :end_date
                  GROUP BY day, ps.hpo_id, ps.participant_origin;
              """.format(enrollment_status=item[0],
                         cache_type=self.cache_type,
                         temp_table_name=temp_table_name,
                         enrollment_status_criteria=item[1])
                sql_arr.append(sql)
        else:
            sql = """
            insert into metrics_lifecycle_cache
              select
                :date_inserted AS date_inserted,
                '' as enrollment_status,
                '{cache_type}' as type,
                ps.hpo_id,
                (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
                day,
                SUM(CASE WHEN DATE(ps.sign_up_time) <= calendar.day THEN 1 ELSE 0 END) AS registered,
                SUM(CASE WHEN DATE(ps.consent_for_study_enrollment_time) <= calendar.day THEN 1 ELSE 0 END) AS consent_enrollment,
                SUM(CASE WHEN DATE(ps.enrollment_status_member_time) <= calendar.day THEN 1 ELSE 0 END) AS consent_complete,
                SUM(CASE
                  WHEN
                    DATE(ps.questionnaire_on_the_basics_time) <= calendar.day AND
                    DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS ppi_basics,
                SUM(CASE
                  WHEN
                    DATE(ps.questionnaire_on_overall_health_time) <= calendar.day AND
                    DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS ppi_overall_health,
                SUM(CASE
                  WHEN
                    DATE(ps.questionnaire_on_lifestyle_time) <= calendar.day AND
                    DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS ppi_lifestyle,
                SUM(CASE
                  WHEN
                    DATE(ps.questionnaire_on_healthcare_access_time) <= calendar.day AND
                    DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS ppi_healthcare_access,
                SUM(CASE
                  WHEN
                    DATE(ps.questionnaire_on_medical_history_time) <= calendar.day AND
                    DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS ppi_medical_history,
                SUM(CASE
                  WHEN
                    DATE(ps.questionnaire_on_medications_time) <= calendar.day AND
                    DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS ppi_medications,
                SUM(CASE
                  WHEN
                    DATE(ps.questionnaire_on_family_health_time) <= calendar.day AND
                    DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS ppi_family_health,
                SUM(CASE
                  WHEN
                    DATE(ps.questionnaire_on_lifestyle_time) <= calendar.day AND
                    DATE(ps.questionnaire_on_overall_health_time) <= calendar.day AND
                    DATE(ps.questionnaire_on_the_basics_time) <= calendar.day AND
                    DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS ppi_complete,
                SUM(CASE
                  WHEN
                    DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS retention_modules_eligible,
                SUM(CASE
                  WHEN
                    DATE(ps.questionnaire_on_healthcare_access_time) <= calendar.day AND
                    DATE(ps.questionnaire_on_family_health_time) <= calendar.day AND
                    DATE(ps.questionnaire_on_medical_history_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS retention_modules_complete,
                SUM(CASE
                  WHEN
                    (DATE(ps.clinic_physical_measurements_time) <= calendar.day OR
                    DATE(ps.self_reported_physical_measurements_authored) <= calendar.day) AND
                    DATE(ps.consent_for_study_enrollment_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS physical_measurement,
                SUM(CASE
                  WHEN
                    DATE(ps.sample_status_1ed10_time) <= calendar.day OR
                    DATE(ps.sample_status_2ed10_time) <= calendar.day OR
                    DATE(ps.sample_status_1ed04_time) <= calendar.day OR
                    DATE(ps.sample_status_1sal_time) <= calendar.day OR
                    DATE(ps.sample_status_1sal2_time) <= calendar.day
                  THEN 1 ELSE 0
                END) AS sample_received,
                SUM(CASE WHEN DATE(ps.enrollment_status_core_stored_sample_time) <= calendar.day THEN 1 ELSE 0 END) AS core_participant,
                ps.participant_origin
              from {temp_table_name} ps,
                   calendar
              WHERE calendar.day BETWEEN :start_date AND :end_date
              GROUP BY day, ps.hpo_id, ps.participant_origin;
          """.format(cache_type=self.cache_type, temp_table_name=temp_table_name)
            sql_arr.append(sql)
        return sql_arr


class MetricsLanguageCacheDao(BaseDao):

    def __init__(self, cache_type=MetricsCacheType.METRICS_V2_API):
        super(MetricsLanguageCacheDao, self).__init__(MetricsLanguageCache)
        try:
            self.cache_type = MetricsCacheType(str(cache_type))
            self.table_name = MetricsLanguageCache.__tablename__
        except TypeError:
            raise TypeError("Invalid metrics cache type")

    def get_serving_version_with_session(self, session):
        status_dao = MetricsCacheJobStatusDao()
        record = status_dao.get_last_complete_data_inserted_time(self.table_name)
        if record is not None:
            return record
        else:
            return (session.query(MetricsLanguageCache)
                    .order_by(MetricsLanguageCache.dateInserted.desc())
                    .first())

    def get_active_buckets(self, start_date=None, end_date=None, hpo_ids=None,
                           enrollment_statuses=None):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is None:
                return None
            last_inserted_date = last_inserted_record.dateInserted
            if self.cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API:
                query = session.query(MetricsLanguageCache.date, MetricsLanguageCache.languageName,
                                      func.sum(MetricsLanguageCache.languageCount).label('total'))
            else:
                query = session.query(MetricsLanguageCache.date, MetricsLanguageCache.hpoName,
                                      MetricsLanguageCache.languageName,
                                      func.sum(MetricsLanguageCache.languageCount).label('total'))
            query = query.filter(MetricsLanguageCache.dateInserted == last_inserted_date)
            if start_date:
                query = query.filter(MetricsLanguageCache.date >= start_date)
            if end_date:
                query = query.filter(MetricsLanguageCache.date <= end_date)

            if hpo_ids:
                query = query.filter(MetricsLanguageCache.hpoId.in_(hpo_ids))
            if enrollment_statuses:
                status_filter_list = []
                for status in enrollment_statuses:
                    if status == str(EnrollmentStatus.INTERESTED):
                        status_filter_list.append('registered')
                    elif status == str(EnrollmentStatus.MEMBER):
                        status_filter_list.append('consented')
                    elif status == str(EnrollmentStatus.FULL_PARTICIPANT):
                        status_filter_list.append('core')
                query = query.filter(MetricsLanguageCache.enrollmentStatus.in_(status_filter_list))

            if self.cache_type == MetricsCacheType.PUBLIC_METRICS_EXPORT_API:
                return query.group_by(MetricsLanguageCache.date, MetricsLanguageCache.languageName).all()
            else:
                return query.group_by(MetricsLanguageCache.date, MetricsLanguageCache.hpoName,
                                      MetricsLanguageCache.languageName).all()

    def get_latest_version_from_cache(self, start_date, end_date, hpo_ids=None,
                                      enrollment_statuses=None):

        buckets = self.get_active_buckets(start_date, end_date, hpo_ids, enrollment_statuses)
        if buckets is None:
            return []

        operation_funcs = {
            MetricsCacheType.PUBLIC_METRICS_EXPORT_API: self.to_public_metrics_client_json,
            MetricsCacheType.METRICS_V2_API: self.to_metrics_client_json
        }
        return operation_funcs[self.cache_type](buckets)

    def delete_old_records(self, n_days_ago=7):
        with self.session() as session:
            last_inserted_record = self.get_serving_version_with_session(session)
            if last_inserted_record is not None:
                last_date_inserted = last_inserted_record.dateInserted
                seven_days_ago = last_date_inserted - datetime.timedelta(days=n_days_ago)
                delete_sql = """
          delete from metrics_language_cache where date_inserted < :seven_days_ago
        """
                params = {'seven_days_ago': seven_days_ago}
                session.execute(delete_sql, params)

    def to_metrics_client_json(self, result_set):
        client_json = []
        for record in result_set:
            language_name = record.languageName
            is_exist = False
            for item in client_json:
                if item['date'] == record.date.isoformat() and item['hpo'] == record.hpoName:
                    item['metrics'][language_name] = int(record.total)
                    is_exist = True
                    break

            if not is_exist:
                new_item = {
                    'date': record.date.isoformat(),
                    'hpo': record.hpoName,
                    'metrics': {
                        'EN': 0,
                        'ES': 0,
                        'UNSET': 0
                    }
                }
                new_item['metrics'][language_name] = int(record.total)
                client_json.append(new_item)
        return client_json

    def to_public_metrics_client_json(self, result_set):
        client_json = []
        for record in result_set:
            language_name = record.languageName
            is_exist = False
            for item in client_json:
                if item['date'] == record.date.isoformat():
                    item['metrics'][language_name] = int(record.total)
                    is_exist = True
                    break

            if not is_exist:
                new_item = {
                    'date': record.date.isoformat(),
                    'metrics': {
                        'EN': 0,
                        'ES': 0,
                        'UNSET': 0
                    }
                }
                new_item['metrics'][language_name] = int(record.total)
                client_json.append(new_item)
        return client_json

    def get_metrics_cache_sql(self, hpo_id):
        temp_table_name = TEMP_TABLE_PREFIX + str(hpo_id)
        sql = """
          insert into metrics_language_cache
        """

        enrollment_status_and_criteria_list = [
            ['registered', ' c.day>=DATE(sign_up_time) AND (enrollment_status_member_time IS NULL '
                           'OR c.day < DATE(enrollment_status_member_time)) '],
            ['consented', ' enrollment_status_member_time IS NOT NULL '
                          'AND day>=DATE(enrollment_status_member_time) '
                          'AND (enrollment_status_core_stored_sample_time IS NULL '
                          'OR day < DATE(enrollment_status_core_stored_sample_time)) '],
            ['core', ' enrollment_status_core_stored_sample_time IS NOT NULL '
                     'AND day>=DATE(enrollment_status_core_stored_sample_time) ']
        ]
        language_and_criteria_list = [
            ['EN', ' ps.primary_language like \'%en%\' '],
            ['ES', ' ps.primary_language like \'%es%\' '],
            ['UNSET', ' ps.primary_language is NULL ']
        ]

        sql_template = """
          select
          :date_inserted AS date_inserted,
          '{enrollment_status}' as enrollment_status,
          :hpo_id AS hpo_id,
          (SELECT name FROM hpo WHERE hpo_id=:hpo_id) AS hpo_name,
          c.day,
          '{language_name}' AS language_name,
          IFNULL((
              SELECT SUM(results.count)
              FROM
              (
                SELECT DATE(ps.sign_up_time) AS sign_up_time,
                       DATE(ps.enrollment_status_member_time) AS enrollment_status_member_time,
                       DATE(ps.enrollment_status_core_stored_sample_time) AS enrollment_status_core_stored_sample_time,
                       count(*) count
                FROM {temp_table_name} ps
                WHERE {language_criteria}
                GROUP BY DATE(ps.sign_up_time), DATE(ps.enrollment_status_member_time), DATE(ps.enrollment_status_core_stored_sample_time)
              ) AS results
              WHERE {enrollment_status_criteria}
            ),0) AS language_count
          FROM calendar c
          WHERE c.day BETWEEN :start_date AND :end_date
        """

        sub_queries = []

        for status_pairs in enrollment_status_and_criteria_list:
            for language_pairs in language_and_criteria_list:
                sub_query = sql_template.format(enrollment_status=status_pairs[0],
                                                language_name=language_pairs[0],
                                                language_criteria=language_pairs[1],
                                                enrollment_status_criteria=status_pairs[1],
                                                temp_table_name=temp_table_name)
                sub_queries.append(sub_query)

        sql = sql + ' UNION ALL '.join(sub_queries)

        return [sql]


class MetricsSitesCacheDao(BaseDao):
    def __init__(self):
        super(MetricsSitesCacheDao, self).__init__(Site)

    def get_sites_count(self):
        with self.session() as session:
            query = session.query(func.count(Site.googleGroup))
            query = query.filter(Site.enrollingStatus == 1)
            return {'sites_count': query.first()[0]}


class MetricsParticipantOriginCacheDao(BaseDao):
    def __init__(self):
        super(MetricsParticipantOriginCacheDao, self).__init__(Participant)

    def get_participant_origins(self):
        with self.session() as session:
            query = session.query(distinct(Participant.participantOrigin))
            result = [r[0] for r in query.all()]
            return {'participant_origins': result}
