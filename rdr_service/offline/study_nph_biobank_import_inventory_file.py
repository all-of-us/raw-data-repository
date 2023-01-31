from datetime import datetime
from io import StringIO
from typing import Dict, Union, Any, Iterator
from csv import DictReader
from re import findall, search

from rdr_service.api_util import open_cloud_file
from rdr_service.model.study_nph_enums import StoredSampleStatus
from rdr_service.model.study_nph import StoredSample
from rdr_service.dao.study_nph_dao import NphStoredSampleDao


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
NPH_INVENTORY_PROCESS_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def read_nph_biobank_inventory_file(csv_filepath: str) -> Iterator[Dict[str, Any]]:

    def _decode_utf8_sig_content(csv_fp) -> StringIO:
        content = csv_fp.read()
        content = content.decode("utf-8-sig")
        return StringIO(content)

    # bucket_name = config.getSetting(config.NPH_SAMPLE_DATA_BIOBANK_NIGHTLY_FILE_DROP)
    bucket_name = "stable-nph-sample-data-biobank"
    with open_cloud_file(f"{bucket_name}/{csv_filepath}", mode="rb") as csv_fp:
        csv_file_content = _decode_utf8_sig_content(csv_fp)
        csv_dict_reader = DictReader(csv_file_content)
        for row in csv_dict_reader:
            yield row


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
    csv_filepath = "nph_inventory_process/sample_nph_biobank_inventory_file.csv"
    import_biobank_inventory_into_stored_samples(csv_filepath)


if __name__=="__main__":
    main()
