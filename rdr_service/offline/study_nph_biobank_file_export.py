# Script to create biobank file export and store the file in GCS Bucket
import logging
from datetime import datetime
from collections import defaultdict
from functools import lru_cache
from typing import List, Dict, Any, Iterable, Optional
from json import dump

from sqlalchemy import and_
from sqlalchemy.orm import joinedload, aliased
from google.cloud import storage
from rdr_service import config
from rdr_service.model.code import Code
from rdr_service.model.participant_summary import ParticipantSummary as RdrParticipantSummary
from rdr_service.model.rex import ParticipantMapping as RexParticipantMapping
from rdr_service.model.study_nph import (
    Participant as NphParticipant,
    StudyCategory,
    Order,
    OrderedSample,
    SampleUpdate,
    BiobankFileExport,
    SampleExport
)

from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao as RdrParticipantSummaryDao
from rdr_service.dao.rex_dao import RexParticipantMappingDao
from rdr_service.dao.study_nph_dao import (
    _format_timestamp,
    NphParticipantDao,
    NphStudyCategoryDao,
    NphOrderDao,
    NphOrderedSampleDao,
    NphSampleUpdateDao,
    NphBiobankFileExportDao,
    NphSampleExportDao
)
from rdr_service.storage import GoogleCloudStorageProvider


_logger = logging.getLogger("rdr_logger")


# FILE_BUFFER_SIZE_IN_BYTES = 1024 * 1024 # 1MB File Buffer
NPH_ANCILLARY_STUDY_ID = 2


def _get_nph_participant(participant_id: int) -> NphParticipant:
    nph_participant_dao = NphParticipantDao()
    with nph_participant_dao.session() as session:
        return session.query(NphParticipant).get(participant_id)


def _get_sample_updates_needing_export() -> Iterable[SampleUpdate]:
    """
    Return any SampleUpdates that haven't already been included in an export
    """
    sample_update_dao = NphSampleUpdateDao()
    with sample_update_dao.session() as session:
        return session.query(
            SampleUpdate
        ).outerjoin(
            SampleExport
        ).filter(
            SampleExport.id.is_(None)
        ).all()


def _get_orders_related_to_sample_updates(sample_updates: Iterable[SampleUpdate]) -> Iterable[Order]:
    nph_ordered_sample_dao = NphOrderedSampleDao()
    with nph_ordered_sample_dao.session() as session:
        ordered_sample_ids = [sample_update.rdr_ordered_sample_id for sample_update in sample_updates]
        ordered_samples: Iterable[OrderedSample] = session.query(OrderedSample).filter(
            OrderedSample.id.in_(ordered_sample_ids)
        ).distinct().all()

    nph_orders_dao = NphOrderDao()
    with nph_orders_dao.session() as session:
        order_ids = list(ordered_sample.order_id for ordered_sample in ordered_samples)
        orders: Iterable[Order] = session.query(Order).filter(
            Order.id.in_(order_ids),
            Order.ignore_flag == 0
        ).distinct().all()

    return orders


@lru_cache(maxsize=128, typed=False)
def _get_study_category(study_category_id: int) -> StudyCategory:
    study_category_dao = NphStudyCategoryDao()
    with study_category_dao.session() as session:
        return session.query(StudyCategory).get(study_category_id)


@lru_cache(maxsize=128, typed=False)
def _get_parent_study_category(study_category_id: int) -> StudyCategory:
    study_category = _get_study_category(study_category_id)
    return _get_study_category(study_category.parent_id)


@lru_cache(maxsize=128, typed=False)
def _get_code_obj_from_sex_id(sex_id: int) -> Code:
    rdr_code_dao = CodeDao()
    with rdr_code_dao.session() as rdr_code_session:
        return rdr_code_session.query(Code).filter(
            Code.codeId == sex_id
        ).first()


def _get_ordered_samples(order_id: int) -> List[OrderedSample]:
    ordered_sample_dao = NphOrderedSampleDao()
    child_sample = aliased(OrderedSample)
    with ordered_sample_dao.session() as session:
        query = (
            session.query(OrderedSample).outerjoin(
                child_sample,
                child_sample.parent_sample_id == OrderedSample.id
            ).filter(
                OrderedSample.order_id == order_id,
                child_sample.id.is_(None)
            ).options(
                joinedload(OrderedSample.parent)
            )
        )
        return query.all()


def _convert_ordered_samples_to_samples(
    order_id: str,
    ordered_samples: List[OrderedSample],
    notes,
    ordered_cancelled: bool = False
) -> List[Dict[str, Any]]:
    samples = []
    for ordered_sample in ordered_samples:
        processing_timestamp = ordered_sample.collected if not ordered_sample.parent is None else None
        sample_cancelled = ordered_cancelled or ordered_sample.status == 'cancelled'
        sample = {
            "sampleID": (ordered_sample.aliquot_id or ordered_sample.nph_sample_id),
            "specimenCode": (ordered_sample.identifier or ordered_sample.test),
            "kitID": order_id if (ordered_sample.identifier or ordered_sample.test).startswith("ST") else "",
            "volume": ordered_sample.volume,
            "volumeUOM": ordered_sample.volumeUnits,
            "collectionDateUTC": _format_timestamp((ordered_sample.parent or ordered_sample).collected),
            "processingDateUTC": _format_timestamp(processing_timestamp),
            "cancelledFlag": "Y" if sample_cancelled else "N",
            "notes": notes,
        }
        samples.append(sample)
    return samples


def _get_rdr_participant_summary_for_nph_participant(nph_participant_id: int) -> Optional[RdrParticipantSummary]:
    rex_participant_mapping_dao = RexParticipantMappingDao()
    with rex_participant_mapping_dao.session() as rex_sm_session:
        rex_participant_mapping: RexParticipantMapping = (
            rex_sm_session.query(RexParticipantMapping).filter(
                RexParticipantMapping.ancillary_participant_id == nph_participant_id,
                RexParticipantMapping.ancillary_study_id == NPH_ANCILLARY_STUDY_ID
            ).first()
        )

    rdr_participant_id = rex_participant_mapping.primary_participant_id
    rdr_participant_summary_dao = RdrParticipantSummaryDao()
    with rdr_participant_summary_dao.session() as rdr_ps_session:
        return rdr_ps_session.query(RdrParticipantSummary).filter(
            RdrParticipantSummary.participantId == rdr_participant_id
        ).first()


def _convert_orders_to_collections(
    orders: List[Order],
    rdr_participant_summary: RdrParticipantSummary
) -> List[Dict[str, Any]]:
    collections = []
    for order in orders:
        samples = _convert_ordered_samples_to_samples(
            order_id=order.nph_order_id,
            ordered_samples=_get_ordered_samples(order_id=order.id),
            ordered_cancelled=order.status == "cancelled",
            notes=", ".join([
                f"{key}: {value if value is not None else 'null'}"
                for key, value in order.notes.items()
                if value
            ])
        )
        parent_study_category = _get_parent_study_category(order.category_id)
        code_obj = _get_code_obj_from_sex_id(rdr_participant_summary.stateId)
        nyFlag = "N"
        if code_obj is not None:
            nyFlag = "Y" if code_obj.value.rsplit("_", 1)[1] == "NY" else "N"
        if len(samples) > 0:
            collections.append({
                "visitID": parent_study_category.name if parent_study_category else "",
                "timepointID": _get_study_category(order.category_id).name,
                "orderID": order.nph_order_id,
                "nyFlag": nyFlag,
                "samples": samples
            })
    return collections


def _get_all_ordered_samples_for_an_order(order_id: int) -> Iterable[OrderedSample]:
    nph_ordered_sample_dao = NphOrderedSampleDao()
    with nph_ordered_sample_dao.session() as session:
        return session.query(OrderedSample).filter(
            and_(
                OrderedSample.order_id == order_id,
                OrderedSample.aliquot_id.isnot(None)
            )
        ).all()


def _get_all_sample_updates_related_to_orders(orders: Iterable[Order]) -> Iterable[SampleUpdate]:
    nph_sample_update_dao = NphSampleUpdateDao()
    sample_updates = []
    for order in orders:
        ordered_samples = _get_all_ordered_samples_for_an_order(order.id)
        ordered_sample_ids = [ordered_sample.id for ordered_sample in ordered_samples]
        with nph_sample_update_dao.session() as session:
            sample_updates_ = session.query(SampleUpdate).filter(
                SampleUpdate.rdr_ordered_sample_id.in_(ordered_sample_ids)
            ).all()
            sample_updates.extend(sample_updates_)
    return sample_updates


# TODO: Move this to rdr_service.storage.LocalFilesystemStorageProvider in a later refactor
# def _compute_crc32c_checksum_using_file_buffer(json_filepath: str) -> int:
#     crc32c_checksum = 0
#     with open(json_filepath, 'r') as json_fp:
#         file_buffer = json_fp.read(FILE_BUFFER_SIZE_IN_BYTES)
#         while file_buffer:
#             crc32c_checksum = crc32(file_buffer.encode(), crc32c_checksum)
#             file_buffer = json_fp.read(FILE_BUFFER_SIZE_IN_BYTES)
#         return crc32c_checksum


def _get_crc32c_checksum_from_gcs_blob(bucket_name: str, blob_name: str):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    return blob.crc32c


def _create_biobank_file_export_reference(bucket_name: str, blob_name: str) -> BiobankFileExport:
    crc32c_checksum = _get_crc32c_checksum_from_gcs_blob(bucket_name, blob_name)
    biobank_file_export_dao = NphBiobankFileExportDao()
    return biobank_file_export_dao.insert(
        BiobankFileExport(
            file_name=f"{bucket_name}/{blob_name}",
            crc32c_checksum=crc32c_checksum
        )
    )


def _create_sample_export_references_for_sample_updates(
    biobank_file_export_id: int, sample_updates: Iterable[SampleUpdate]
):
    sample_export_dao = NphSampleExportDao()
    for sample_update in sample_updates:
        sample_export_params = {
            "export_id": biobank_file_export_id,
            "sample_update_id": sample_update.id
        }
        sample_export = SampleExport(**sample_export_params)
        sample_export_dao.insert(sample_export)


def main():
    orders_file_drop: List[Dict[str, Any]] = []
    sample_updates_for_file_export = _get_sample_updates_needing_export()
    orders = (
        _get_orders_related_to_sample_updates(sample_updates_for_file_export)
    )
    grouped_orders: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for order in orders:
        client_id = order.client_id
        nph_module_id = _get_parent_study_category(_get_parent_study_category(order.category_id).id)
        participant_id = order.participant_id
        grouped_orders[(client_id, nph_module_id.name, participant_id)].append(order)

    nph_biobank_prefix = (
        config.NPH_PROD_BIOBANK_PREFIX if config.GAE_PROJECT == "all-of-us-rdr-prod" \
            else config.NPH_TEST_BIOBANK_PREFIX
    )
    for (client_id, nph_module_id, participant_id), orders in grouped_orders.items():
        rdr_participant_summary: RdrParticipantSummary = (
            _get_rdr_participant_summary_for_nph_participant(order.participant_id)
        )
        participant_biobank_id = _get_nph_participant(participant_id).biobank_id

        sex_at_birth = ""
        if rdr_participant_summary.sexId is not None:
            code_obj = _get_code_obj_from_sex_id(rdr_participant_summary.sexId)
            if code_obj is not None:
                sex_at_birth: str = code_obj.value.rsplit("_", 1)[1]
                if sex_at_birth not in {"Male", "Female"}:
                    sex_at_birth = "Unknown"

        json_object = {
            "clientID": client_id,
            "studyID": f"NPH Module {nph_module_id}",
            "participantID": f"{nph_biobank_prefix}{participant_biobank_id}",
            "gender": sex_at_birth,
            "ai_an_flag": "Y" if rdr_participant_summary.aian else "N",
            "collections": _convert_orders_to_collections(orders, rdr_participant_summary)
        }
        orders_file_drop.append(json_object)

    today_dt_ts = datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
    bucket_name = config.getSetting(config.NPH_SAMPLE_DATA_BIOBANK_NIGHTLY_FILE_DROP)
    json_filepath = f"nph-orders/NPH_Orders_{today_dt_ts}.json"
    orders_filename = f"{bucket_name}/{json_filepath}"
    with GoogleCloudStorageProvider().open(orders_filename, mode='w') as dest:
        dump(orders_file_drop, dest, default=str)

    _logger.info(f"Created Biobank export file: '{orders_filename}'")
    biobank_file_export = _create_biobank_file_export_reference(bucket_name, json_filepath)
    _create_sample_export_references_for_sample_updates(biobank_file_export.id, sample_updates_for_file_export)
