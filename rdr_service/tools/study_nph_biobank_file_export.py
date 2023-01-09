# Script to create biobank file export and store the file in GCS Bucket
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Iterable
from json import dump
from random import choice as random_choice

from rdr_service.model.study_nph import (
    StudyCategory,
    Order,
    OrderedSample,
    # SampleUpdate,
    # BiobankFileExport,
    # SampleExport
)
from rdr_service.dao.study_nph_dao import (
    NphStudyCategoryDao,
    NphOrderDao,
    NphOrderedSampleDao,
    # NphSampleUpdateDao,
    # NphBiobankFileExportDao,
    # NphSampleExportDao
)


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
        sample = {
            "sampleID": ordered_sample.id,
            "specimenCode": ordered_sample.aliquot_id,
            "kitID": order_id if "stool" in ordered_sample.description else "N/A",
            "volume": ordered_sample.volume,
            "volumeSpecified": True,
            "volumeUOM": ordered_sample.volume.rsplit(" ", 1),
            "volumeUOMSpecified": True,
            "processingDateUTC": ordered_sample.finalized,
            "notes": notes,
        }
        samples.append(sample)
    return samples


def _convert_orders_to_collections(orders: List[Order]) -> List[Dict[str, Any]]:
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
            "nyFlag": random_choice(["N", "Y"]),
            "samples": samples
        })
    return collections


def main():
    orders_file_drop: List[Dict[str, Any]] = []
    orders: Iterable[Order] = get_orders_created_or_modified_in_last_n_hours(hours=48)
    orders_by_participant: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for order in orders:
        participant_id = order.participant_id
        orders_by_participant[participant_id].append(order)

    for participant_id, orders in orders_by_participant.items():
        json_object = {
            "clientID": "N/A",
            "studyID":  "N/A",
            "participantID": participant_id,
            "gender": random_choice(["M", "F"]),
            "ai_an_flag": "N/A",
            "collections": _convert_orders_to_collections(orders)
        }
        orders_file_drop.append(json_object)

    today_date = datetime.now().date().strftime("%Y_%m_%d")
    orders_filename = f"NPH_Orders_{today_date}.json"
    with open(orders_filename, "w") as json_fp:
        dump(orders_file_drop, json_fp, default=str)


if __name__=="__main__":
    main()
