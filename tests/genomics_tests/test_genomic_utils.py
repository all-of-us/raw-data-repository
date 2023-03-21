import os
import time

from rdr_service import storage, clock
from rdr_service.api_util import open_cloud_file
from tests.genomics_tests.test_genomic_pipeline import _FAKE_BUCKET
from tests.test_data import data_path


def create_ingestion_test_file(
    test_data_filename,
    bucket_name,
    folder=None,
    include_timestamp=True,
    include_sub_num=False,
    extension=None
):
    test_data_file = open_genomic_set_file(test_data_filename)

    input_filename = '{}{}{}{}'.format(
        test_data_filename.replace('.csv', ''),
        '_11192019' if include_timestamp else '',
        '_1' if include_sub_num else '',
        '.csv' if not extension else extension
    )
    write_cloud_csv(
        input_filename,
        test_data_file,
        folder=folder,
        bucket=bucket_name
    )

    return input_filename


def open_genomic_set_file(test_filename):
    with open(data_path(test_filename)) as f:
        lines = f.readlines()
        csv_str = ""
        for line in lines:
            csv_str += line

        return csv_str


def write_cloud_csv(
    file_name,
    contents_str,
    bucket=None,
    folder=None,
):
    bucket = _FAKE_BUCKET if bucket is None else bucket
    if folder is None:
        path = "/%s/%s" % (bucket, file_name)
    else:
        path = "/%s/%s/%s" % (bucket, folder, file_name)
    with open_cloud_file(path, mode='wb') as cloud_file:
        cloud_file.write(contents_str.encode("utf-8"))

    # handle update time of test files
    provider = storage.get_storage_provider()
    n = clock.CLOCK.now()
    ntime = time.mktime(n.timetuple())
    os.utime(provider.get_local_path(path), (ntime, ntime))
    return cloud_file
