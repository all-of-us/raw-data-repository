import logging
from datetime import datetime, timedelta
from io import StringIO
from typing import Dict, Union, Any, Iterator, Iterable
from csv import DictReader
from re import findall, search
from google.cloud._helpers import UTC

from rdr_service import config
from rdr_service.api_util import open_cloud_file, list_blobs
from rdr_service.model.study_nph_enums import StoredSampleStatus
from rdr_service.model.study_nph import StoredSample
from rdr_service.dao.study_nph_dao import NphStoredSampleDao
from rdr_service.offline.biobank_samples_pipeline import DataError


_logger = logging.getLogger("rdr_logger")


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
NPH_INVENTORY_PROCESS_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def read_nph_biobank_inventory_file(csv_filepath: str) -> Iterator[Dict[str, Any]]:

    def _decode_utf8_sig_content(csv_fp) -> StringIO:
        content = csv_fp.read()
        content = content.decode("utf-8-sig")
        return StringIO(content)

    with open_cloud_file(csv_filepath, mode="rb") as csv_fp:
        csv_file_content = _decode_utf8_sig_content(csv_fp)
        csv_dict_reader = DictReader(csv_file_content)
        for row in csv_dict_reader:
            yield row


def _get_nph_inventory_samples_csv_dropped_in_last_24_hrs(
    cloud_bucket_name: str, filename_pattern: str
) -> Iterable[Any]:
    """Returns the full path (including bucket name) of the most recently created CSV in the bucket.

  Raises:
    RuntimeError: if no CSVs are found in the cloud storage bucket.
  """

    bucket_stat_list = list_blobs(cloud_bucket_name)
    if not bucket_stat_list:
        raise DataError("No files in cloud bucket %r." % cloud_bucket_name)
    # GCS does not really have the concept of directories (it's just a filename convention), so all
    # directory listings are recursive and we must filter out subdirectory contents.
    bucket_stat_list = [s for s in bucket_stat_list
                        if s.name.lower().endswith(".csv")
                        and filename_pattern in s.name]
    if not bucket_stat_list:
        raise DataError("No CSVs in cloud bucket %r (all files: %s)." % (cloud_bucket_name, bucket_stat_list))
    # bucket_stat_list.sort(key=lambda s: s.updated)
    _day_before_ts = datetime.utcnow() - timedelta(days=1)
    _day_before_ts = _day_before_ts.replace(tzinfo=UTC)
    csv_files = list(
        map(
            lambda blob: blob.name,
            filter(lambda file: file.updated > _day_before_ts, bucket_stat_list)
        )
    )
    return csv_files


def _convert_csv_row_to_stored_sample_object(csv_obj: Dict[str, Union[str, int]]) -> StoredSample:
    stored_sample_status = {
        "Shipped": StoredSampleStatus.SHIPPED,
        "Received": StoredSampleStatus.RECEIVED,
        "Disposed": StoredSampleStatus.DISPOSED
    }
    def _parse_sample_id_field_to_int(sample_id):
        if search("[a-zA-Z]+", sample_id) is not None:
            sample_id_matches = findall("[0-9]+", sample_id or "")
            if len(sample_id_matches) > 0:
                return int(sample_id_matches[0])
        return int(sample_id)

    biobank_modified_ts = datetime.strptime(csv_obj["MOD_DATE"], NPH_INVENTORY_PROCESS_DATETIME_FORMAT)
    biobank_id = int(csv_obj["BIOBANK_ID"][1:])
    stored_sample_obj = {
        "biobank_modified": biobank_modified_ts,
        "biobank_id": biobank_id,
        "sample_id": _parse_sample_id_field_to_int(csv_obj["SAMPLE_ID"]),
        "lims_id": csv_obj["LIMS_SAMPLE_ID"],
        "status": stored_sample_status.get(csv_obj["STATUS"]),
        "disposition": stored_sample_status.get(csv_obj["DISPOSITION"])
    }
    return StoredSample(**stored_sample_obj)


def import_biobank_inventory_into_stored_samples(csv_filepath: str):
    nph_stored_sample_dao = NphStoredSampleDao()
    for row in read_nph_biobank_inventory_file(csv_filepath):
        stored_sample = _convert_csv_row_to_stored_sample_object(row)
        nph_stored_sample_dao.insert(stored_sample)


def main():
    bucket_name = config.getSetting(config.NPH_SAMPLE_DATA_BIOBANK_NIGHTLY_FILE_DROP)
    latest_csv_files = _get_nph_inventory_samples_csv_dropped_in_last_24_hrs(
        cloud_bucket_name=bucket_name, filename_pattern="nph_inventory_process"
    )
    for csv_filepath in latest_csv_files:
        _logger.info(f"Importing {csv_filepath} file")
        import_biobank_inventory_into_stored_samples(f"{bucket_name}/{csv_filepath}")
        _logger.info(f"Successfully imported biobank samples from '{csv_filepath}'")


if __name__=="__main__":
    main()
