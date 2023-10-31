import logging
from datetime import datetime
import pandas as pd
from google.cloud import storage
from io import BytesIO

from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.system_utils import list_chunks

from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'ps-data-dump'
tool_desc = 'bulk data dump of participants from the PS API for vibrent'
bucket_name = 'ptsc-metrics-all-of-us-rdr-prod'
client = storage.Client()

def upload_to_gcs(df, filename):
    bucket = client.get_bucket(bucket_name)
    filename = f'ops_data_api/{filename}'
    blob = bucket.blob(filename)
    df_csv = df.to_csv(index=False)
    print(df_csv)
    blob.upload_from_file(BytesIO(df_csv.encode('utf-8')), content_type='text/csv')

class ParticipantSummaryDataDump(ToolBase):
    logger_name = None

    def run(self):
        summary_list = []
        super(ParticipantSummaryDataDump, self).run()
        with (self.get_session() as session):
            summary_dao = ParticipantSummaryDao()
            # --id option takes precedence over --from-file option
            if self.args.id:
                participant_id_list = [int(i) for i in self.args.id.split(',')]
            elif self.args.from_file:
                participant_id_list = self.get_int_ids_from_file(self.args.from_file)
            else:
                # Default to all participant_summary ids
                participant_id_list = session.query(
                    ParticipantSummary.participantId
                ).order_by(ParticipantSummary.participantId).first()

            count = 0
            last_id = None

            chunk_size = 10000
            for id_list_subset in list_chunks(lst=participant_id_list, chunk_size=chunk_size):
                logging.info(f'{datetime.now()}: {count} of {len(participant_id_list)} (last id: {last_id})')
                count += chunk_size

                summary_list = session.query(
                    ParticipantSummary
                ).filter(
                    ParticipantSummary.participantId.in_(id_list_subset),
                    ParticipantSummary.participantOrigin == 'vibrent'
                ).all()
                total_rows = len(summary_list)
                ignored_columns = [
                    'biobankId',
                    'firstName',
                    'middleName',
                    'lastName',
                    'zipCode',
                    'state',
                    'city',
                    'streetAddress',
                    'streetAddress2',
                    'phoneNumber',
                    'loginPhoneNumber',
                    'email',
                    'dateOfBirth'
                ]
                columns = []
                for chunk_start in range(0, total_rows, chunk_size):
                    chunk_end = min(chunk_start + chunk_size, total_rows)
                    df = pd.DataFrame(summary_list)
                    values = []
                    for i in df.values[0]:
                        if i[0] not in ignored_columns:
                            columns.append(i[0])
                    for row in df.values:
                        row_vals = []
                        for i in row:
                            if i[0] not in ignored_columns:
                                row_vals.append(i[1])
                        values.append(row_vals)
                    df2 = pd.DataFrame(values, columns = columns)
                    filename = f'chunk_{chunk_start + 1}_{chunk_end}.csv'
                    # Upload the chunk to Google Cloud Storage
                    upload_to_gcs(df2, filename)
            return summary_list


def add_additional_arguments(parser):
    parser.add_argument('--id', required=False,
                        help="Single participant id or comma-separated list of id integer values to backfill")
    parser.add_argument('--from-file', required=False,
                        help="file of integer participant id values to backfill")
    parser.add_argument('--allow-downgrade', default=False, action="store_true",
                        help='Force recalculation of enrollment status, and allow status to revert to a lower status')


def run():
    return cli_run(tool_cmd, tool_desc, ParticipantSummaryDataDump, add_additional_arguments, replica=False)
