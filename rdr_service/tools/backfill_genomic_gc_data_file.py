import csv

from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicJob
from rdr_service.tools.tool_libs import GCPProcessContext


FILES = (
    '../gc_data/bcm_all_wgs.txt',
    '../gc_data/bi_all_array.txt',
    '../gc_data/bi_all_wgs.txt',
    '../gc_data/uw_all_array.txt',
    '../gc_data/uw_all_wgs.txt'
)
PROJECT = "all-of-us-rdr-prod"
ACCOUNT = "josh.kanuch@pmi-ops.org"


def process_file_list(controller):
    for file_path in FILES:
        print(f'Processing {file_path}')
        with open(file_path, encoding='utf-8-sig') as f:
            reader = csv.reader(f)

            row_number = 1
            for row in reader:
                row = row[0]

                # skip directory-names
                if row.endswith('/'):
                    continue

                file_path = get_file_path_from_row(row)
                bucket_name = get_bucket_name_from_row(row)

                # ingest files into GenomicGcDataFile
                controller.accession_data_files(
                    file_path,
                    bucket_name
                )

                print(f'Row {row_number} completed.')
                row_number += 1


def get_file_path_from_row(row):
    return row.replace('gs://', '')


def get_bucket_name_from_row(row):
    return row.replace('gs://', '').split('/')[0]


def run():
    # open list of files
    with GCPProcessContext("cmd", PROJECT) as gcp_env:
        gcp_env.activate_sql_proxy()
        with GenomicJobController(GenomicJob.ACCESSION_DATA_FILES,
                                  bq_project_id=PROJECT) as controller:
            process_file_list(controller)


if __name__ == "__main__":
    run()
