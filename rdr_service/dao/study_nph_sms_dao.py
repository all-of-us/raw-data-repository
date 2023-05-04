from typing import List, Dict
from sqlalchemy import func

from rdr_service import clock
from rdr_service import config
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.study_nph import OrderedSample
from rdr_service.model.study_nph_sms import SmsSample, SmsBlocklist, SmsN0, SmsJobRun, SmsN1Mc1


class SmsManifestMixin:
    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(self.model_type, batch)


class SmsManifestSourceMixin:
    def get_transfer_def(self, **kwargs):
        """
        Required for manifests the RDR generates
        need keys for "file_name" and "bucket"
        "file_name" should include GCS prefix
        """
        raise NotImplementedError("This class requires a get_transfer_def method")

    def source_data(self, **kwargs):
        """
        Required for manifests the RDR generates
        This is the query that generates the data for the manifest.
        Field names returned in the query results should match output of manifest
        :return: query results list
        """
        raise NotImplementedError("This class requires a source_data method")


class SmsJobRunDao(BaseDao):
    def __init__(self):
        super().__init__(SmsJobRun)

    def get_id(self, obj):
        return obj.id

    def insert_run_record(self, job, **kwargs):
        params = {
            'job': job.name,
            'sub_process': kwargs.get('subprocess').name
        }
        job_run = self.model_type(**params)
        return self.insert(job_run)

    def update_run_record(self, run, result):
        run.result = result.name
        with self.session() as session:
            session.merge(run)


class SmsSampleDao(BaseDao, SmsManifestMixin):
    def __init__(self):
        super().__init__(SmsSample)

    def get_id(self, obj):
        return obj.id


class SmsBlocklistDao(BaseDao):
    def __init__(self):
        super().__init__(SmsBlocklist)

    def get_id(self, obj):
        return obj.id


class SmsN0Dao(BaseDao, SmsManifestMixin):
    def __init__(self):
        super().__init__(SmsN0)

    def get_id(self, obj):
        return obj.id


class SmsN1Mc1Dao(BaseDao, SmsManifestMixin, SmsManifestSourceMixin):
    def __init__(self):
        super().__init__(SmsN1Mc1)

    def get_id(self, obj):
        return obj.id

    def get_transfer_def(self, recipient: str) -> dict:
        bucket = "test-bucket-unc-meta"
        env_split = config.GAE_PROJECT.split('-')[-1]
        if env_split in ['prod', 'stable', 'sandbox']:
            bucket = config.NPH_SMS_BUCKETS.get(recipient)

        recipient_xfer_dict = {
            "bucket": bucket,
            "file_name": f"n1_mcac_manifests/{recipient}_n1_mcac_{clock.CLOCK.now().isoformat()}.csv"
        }

        return recipient_xfer_dict

    def source_data(self, **kwargs):
        if not kwargs.get('recipient'):
            raise KeyError("recipient required for N1_MC1")

        with self.session() as session:
            query = session.query(
                SmsSample.sample_id,
                SmsN0.matrix_id,
                SmsN0.biobank_id,
                SmsSample.sample_identifier,
                SmsN0.study,
                SmsN0.visit,
                SmsN0.timepoint,
                SmsN0.collection_site,
                SmsN0.collection_date_time,
                SmsN0.sample_type,
                SmsN0.additive_treatment,
                SmsN0.quantity_ml,
                SmsN0.age,
                SmsSample.sex_at_birth,
                SmsN0.package_id,
                SmsN0.storage_unit_id,
                SmsN0.well_box_position,
                SmsSample.destination,
                SmsN0.tracking_number,
                SmsN0.sample_comments,
                SmsSample.ethnicity,
                SmsSample.race,
                SmsSample.bmi,
                SmsSample.diet,
                func.json_extract(OrderedSample.supplemental_fields, "$.color").label('urine_color'),
                func.json_extract(OrderedSample.supplemental_fields, "$.clarity").label('urine_clarity'),
                func.json_extract(OrderedSample.supplemental_fields, "$.bowelMovement").label('bowel_movement'),
                func.json_extract(
                    OrderedSample.supplemental_fields, "$.bowelMovementQuality"
                ).label('bowel_movement_quality'),
            ).join(
                SmsN0,
                SmsN0.sample_id == SmsSample.sample_id
            ).outerjoin(
                OrderedSample,
                SmsSample.sample_id == OrderedSample.nph_sample_id
            )
            if kwargs.get("recipient"):
                query = query.filter(SmsSample.destination == kwargs['recipient'])

            return query.all()
