# Script to create biobank file export and store the file in GCS Bucket
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Iterable, Optional
from zlib import crc32
from json import dump
from re import findall

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

from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao as RdrParticipantSummaryDao
from rdr_service.dao.rex_dao import RexParticipantMappingDao
from rdr_service.dao.study_nph_dao import (
    NphParticipantDao,
    NphStudyCategoryDao,
    NphOrderDao,
    NphOrderedSampleDao,
    NphSampleUpdateDao,
    NphBiobankFileExportDao,
    NphSampleExportDao
)


FILE_BUFFER_SIZE_IN_BYTES = 1024 * 1024 # 1MB File Buffer


def _filter_orders_by_modified_field(modified_ts: datetime):
    nph_orders_dao = NphOrderDao()
    with nph_orders_dao.session() as session:
        return session.query(Order).filter(
            Order.modified >= modified_ts
        ).all()


def get_orders_created_or_modified_in_last_n_hours(hours: int = 24) -> Iterable[Order]:
    modified_ts = datetime.now() - timedelta(hours=hours)
    return _filter_orders_by_modified_field(modified_ts=modified_ts)


def _get_study_category(study_category_id: int) -> StudyCategory:
    study_category_dao = NphStudyCategoryDao()
    with study_category_dao.session() as session:
        return session.query(StudyCategory).get(study_category_id)


def _get_parent_study_category(study_category_id: int) -> StudyCategory:
    study_category = _get_study_category(study_category_id)
    return _get_study_category(study_category.parent_id)


def _get_ordered_samples(order_id: int) -> List[OrderedSample]:
    ordered_sample_dao = NphOrderedSampleDao()
    with ordered_sample_dao.session() as session:
        return session.query(OrderedSample).filter(
            OrderedSample.order_id == order_id
        ).all()


def _convert_ordered_samples_to_samples(order_id: int, ordered_samples: List[OrderedSample]) -> List[Dict[str, Any]]:
    samples = []
    for ordered_sample in ordered_samples:
        supplemental_fields = ordered_sample.supplemental_fields if ordered_sample.supplemental_fields else {}
        notes = ", ".join([f"{key}: {value}" for key, value in supplemental_fields.items()])
        extract_volume_units = findall("[a-zA-Z]+", ordered_sample.volume)
        volume_units = extract_volume_units[-1] if extract_volume_units else ""
        sample = {
            "sampleID": ordered_sample.aliquot_id,
            "specimenCode": ordered_sample.identifier,
            "kitID": order_id if ordered_sample.identifier.startswith("ST") else "",
            "volume": ordered_sample.volume,
            "volumeSpecified": True,
            "volumeUOM": volume_units,
            "volumeUOMSpecified": True,
            "processingDateUTC": ordered_sample.finalized,
            "notes": notes,
        }
        samples.append(sample)
    return samples


def _get_rdr_participant_summary_for_nph_partipant(nph_participant_id: int) -> Optional[RdrParticipantSummary]:
    nph_participant_dao = NphParticipantDao()
    with nph_participant_dao.session() as nph_participant_session:
        nph_participant: NphParticipant = nph_participant_session.query(NphParticipant).get(nph_participant_id)

    rex_participant_mapping_dao = RexParticipantMappingDao()
    with rex_participant_mapping_dao.session() as rex_sm_session:
        rex_participant_mapping: RexParticipantMapping = (
            rex_sm_session.query(RexParticipantMapping).filter(
                RexParticipantMapping.ancillary_participant_id == nph_participant.id
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
            order_id=order.id,
            ordered_samples=_get_ordered_samples(order_id=order.id)
        )
        parent_study_category = _get_parent_study_category(order.category_id)
        collections.append({
            "visitID": parent_study_category.name if parent_study_category else "",
            "timepointID": _get_study_category(order.category_id).name,
            "orderID": order.id,
            "nyFlag": "Y" if rdr_participant_summary.state == "NY" else "N",
            "samples": samples
        })
    return collections


def _get_all_ordered_samples_for_an_order(order_id: int) -> Iterable[OrderedSample]:
    nph_ordered_sample_dao = NphOrderedSampleDao()
    with nph_ordered_sample_dao.session() as session:
        return session.query(OrderedSample).filter(
            OrderedSample.order_id == order_id
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


def _compute_crc32c_checksum_using_file_buffer(json_filepath: str) -> int:
    crc32c_checksum = 0
    with open(json_filepath, 'r') as json_fp:
        file_buffer = json_fp.read(FILE_BUFFER_SIZE_IN_BYTES)
        while file_buffer:
            crc32c_checksum = crc32(file_buffer.encode(), crc32c_checksum)
            file_buffer = json_fp.read(FILE_BUFFER_SIZE_IN_BYTES)
        return crc32c_checksum


def _create_biobank_file_export_reference(json_filepath: str) -> BiobankFileExport:
    crc32c_checksum = _compute_crc32c_checksum_using_file_buffer(json_filepath)
    biobank_file_export_dao = NphBiobankFileExportDao()
    biobank_file_export_params = {
        "file_name": json_filepath,
        "crc32c_checksum": str(crc32c_checksum)
    }
    biobank_file_export = BiobankFileExport(**biobank_file_export_params)
    return biobank_file_export_dao.insert(biobank_file_export)


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
    orders: Iterable[Order] = get_orders_created_or_modified_in_last_n_hours(hours=48)
    grouped_orders: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for order in orders:
        finalized_site = order.finalized_site
        nph_module_id = _get_parent_study_category(_get_parent_study_category(order.category_id).id)
        participant_id = order.participant_id
        grouped_orders[(finalized_site, nph_module_id.name, participant_id)].append(order)

    for (finalized_site, nph_module_id, participant_id), orders in grouped_orders.items():
        rdr_participant_summary: RdrParticipantSummary = (
            _get_rdr_participant_summary_for_nph_partipant(order.participant_id)
        )
        json_object = {
            "clientID": finalized_site,
            "studyID":  nph_module_id,
            "participantID": participant_id,
            "gender": rdr_participant_summary.sex or str(rdr_participant_summary.genderIdentity),
            "ai_an_flag": "Y" if rdr_participant_summary.aian else "N",
            "collections": _convert_orders_to_collections(orders, rdr_participant_summary)
        }
        orders_file_drop.append(json_object)

    today_dt_ts = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    orders_filename = f"NPH_Orders_{today_dt_ts}.json"
    with open(orders_filename, "w") as json_fp:
        dump(orders_file_drop, json_fp, default=str)

    sample_updates_for_orders = _get_all_sample_updates_related_to_orders(orders)
    biobank_file_export = _create_biobank_file_export_reference(orders_filename)
    _create_sample_export_references_for_sample_updates(biobank_file_export.id, sample_updates_for_orders)


if __name__=="__main__":
    main()
