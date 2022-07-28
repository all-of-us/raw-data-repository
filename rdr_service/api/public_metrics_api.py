import datetime

from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from rdr_service.api_util import STOREFRONT_AND_RDR, convert_to_datetime, get_awardee_id_from_name
from rdr_service.app_util import auth_required
from rdr_service.dao.calendar_dao import INTERVAL_DAY
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.metrics_cache_dao import (
    MetricsAgeCacheDao,
    MetricsEnrollmentStatusCacheDao,
    MetricsGenderCacheDao,
    MetricsLanguageCacheDao,
    MetricsLifecycleCacheDao,
    MetricsRaceCacheDao,
    MetricsRegionCacheDao,
    MetricsSitesCacheDao
)
from rdr_service.dao.metrics_ehr_service import MetricsEhrService
from rdr_service.participant_enums import EnrollmentStatus, MetricsAPIVersion, MetricsCacheType, Stratifications

DATE_FORMAT = "%Y-%m-%d"
DAYS_LIMIT_FOR_HISTORY_DATA = 600


class PublicMetricsApi(Resource):
    @auth_required(STOREFRONT_AND_RDR)
    def get(self):
        self.hpo_dao = HPODao()

        params = {
            "stratification": request.args.get("stratification"),
            "start_date": request.args.get("startDate"),
            "end_date": request.args.get("endDate"),
            "enrollment_statuses": request.args.get("enrollmentStatus"),
            "awardees": request.args.get("awardee"),
            "version": request.args.get("version"),
        }

        filters = self.validate_params(params)
        results = self.get_filtered_results(**filters)

        return results

    def get_filtered_results(self, stratification, start_date, end_date, awardee_ids, enrollment_statuses, version):
        """Queries DB, returns results in format consumed by front-end

    :param start_date: Start date object
    :param end_date: End date object
    :param awardee_ids: indicate awardee ids
    :param enrollment_statuses: indicate the enrollment status
    :param stratification: How to stratify (layer) results, as in a stacked bar chart
    :param version: indicate the version of the result filter
    :return: Filtered, stratified results by date
    """

        if stratification == Stratifications.TOTAL:
            dao = MetricsEnrollmentStatusCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
            return dao.get_total_interested_count(start_date, end_date, awardee_ids)
        elif stratification == Stratifications.ENROLLMENT_STATUS:
            dao = MetricsEnrollmentStatusCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
            return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids)
        elif stratification == Stratifications.GENDER_IDENTITY:
            dao = MetricsGenderCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API, version)
            return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids, enrollment_statuses)
        elif stratification == Stratifications.AGE_RANGE:
            dao = MetricsAgeCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
            return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids, enrollment_statuses)
        elif stratification == Stratifications.RACE:
            dao = MetricsRaceCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API, version)
            return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids, enrollment_statuses)
        elif stratification in [Stratifications.GEO_STATE, Stratifications.GEO_CENSUS, Stratifications.GEO_AWARDEE]:
            dao = MetricsRegionCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
            return dao.get_latest_version_from_cache(end_date, stratification, awardee_ids, enrollment_statuses)
        elif stratification == Stratifications.LANGUAGE:
            dao = MetricsLanguageCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
            return dao.get_latest_version_from_cache(start_date, end_date, awardee_ids, enrollment_statuses)
        elif stratification == Stratifications.LIFECYCLE:
            dao = MetricsLifecycleCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
            return dao.get_latest_version_from_cache(end_date, awardee_ids)
        elif stratification == Stratifications.PRIMARY_CONSENT:
            dao = MetricsLifecycleCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
            return dao.get_primary_consent_count_over_time(start_date, end_date, awardee_ids)
        elif stratification == Stratifications.EHR_METRICS:
            params = {
                "start_date": convert_to_datetime(start_date),
                "end_date": convert_to_datetime(end_date),
                "hpo_ids": awardee_ids,
                "interval": INTERVAL_DAY,
            }
            result_set = MetricsEhrService().get_metrics(**params)
            if "metrics_over_time" in result_set:
                return result_set["metrics_over_time"]
            else:
                return []
        elif stratification == Stratifications.SITES_COUNT:
            dao = MetricsSitesCacheDao()
            return dao.get_sites_count()
        else:
            raise BadRequest(f"Invalid stratification: {str(stratification)}")

    def validate_params(self, params):
        filters = {}
        # Validate stratifications
        try:
            filters["stratification"] = Stratifications(params["stratification"])
        except TypeError:
            raise BadRequest(f"Invalid stratification: {params['stratification']}")

        if filters["stratification"] in [
            Stratifications.FULL_STATE,
            Stratifications.FULL_CENSUS,
            Stratifications.FULL_AWARDEE,
            Stratifications.GEO_STATE,
            Stratifications.GEO_CENSUS,
            Stratifications.GEO_AWARDEE,
            Stratifications.LIFECYCLE,
        ]:
            # Validate dates
            if not params["end_date"]:
                raise BadRequest("end date should not be empty")
            try:
                end_date = datetime.datetime.strptime(params["end_date"], DATE_FORMAT).date()
                start_date = end_date
            except ValueError:
                raise BadRequest(f"Invalid end date: {params['end_date']}")

            filters["start_date"] = start_date
            filters["end_date"] = end_date
        elif filters['stratification'] == Stratifications.SITES_COUNT:
            # no date needed
            filters['start_date'] = None
            filters['end_date'] = None
        else:
            # Validate dates
            if not params["start_date"] or not params["end_date"]:
                raise BadRequest("Start date and end date should not be empty")
            try:
                start_date = datetime.datetime.strptime(params["start_date"], DATE_FORMAT).date()
            except ValueError:
                raise BadRequest(f"Invalid start date: {params['start_date']}")
            try:
                end_date = datetime.datetime.strptime(params["end_date"], DATE_FORMAT).date()
            except ValueError:
                raise BadRequest(f"Invalid end date: {params['end_date']}")
            date_diff = abs((end_date - start_date).days)
            if date_diff > DAYS_LIMIT_FOR_HISTORY_DATA:
                raise BadRequest(
                    f"Difference between start date and end date \
                    should not be greater than {DAYS_LIMIT_FOR_HISTORY_DATA} days"
                )

            filters["start_date"] = start_date
            filters["end_date"] = end_date

        # Validate awardees, get ID list
        awardee_ids = []
        if params["awardees"] is not None:
            awardees = params["awardees"].split(",")
            for awardee in awardees:
                if awardee != "":
                    awardee_id = get_awardee_id_from_name({"awardee": awardee}, self.hpo_dao)
                    if awardee_id is None:
                        raise BadRequest(f"Invalid awardee name: {awardee}")
                    awardee_ids.append(awardee_id)
        filters["awardee_ids"] = awardee_ids

        try:
            filters["version"] = MetricsAPIVersion(int(params["version"])) if params["version"] else None
        except ValueError:
            filters["version"] = None

        # Validate enrollment statuses
        enrollment_status_strs = []
        if params["enrollment_statuses"] is not None:
            enrollment_statuses = params["enrollment_statuses"].split(",")
            try:
                enrollment_status_strs = [str(EnrollmentStatus(val)) for val in enrollment_statuses]
            except TypeError:
                valid_enrollment_statuses = EnrollmentStatus.to_dict()
                for enrollment_status in enrollment_statuses:
                    if enrollment_status != "":
                        if enrollment_status not in valid_enrollment_statuses:
                            raise BadRequest(f"Invalid enrollment status: {enrollment_status}")
        filters["enrollment_statuses"] = enrollment_status_strs

        return filters
