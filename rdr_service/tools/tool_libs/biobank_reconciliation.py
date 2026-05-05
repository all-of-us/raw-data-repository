import csv
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from sqlalchemy.orm import Session

from rdr_service.dao.biobank_specimen_dao import BiobankSpecimenDao
from rdr_service.model.biobank_order import *
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.services.system_utils import list_chunks
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'biobank-reconciliation'
tool_desc = 'Load a specific reconciliation file from Biobank to correct a field we have'


DRY_RUN = True


@dataclass()
class Correction:
    key: str
    correct_value: str


class BiobankReconciliation(ToolBase):

    def _process_correction(self, correction, db_object: BiobankAliquotDataset):
        if DRY_RUN:
            db_value = db_object.extractionDate                                        # (dry-run) extract db value
            if db_value == correction.correct_value:
                print(f'{correction.key}: WOULD NOT CHANGE VALUE')
            else:
                value_display = lambda value: None if value is None else f"{value}"

                print(
                    f'{correction.key}: would change '
                    f'db {value_display(db_value)} to '
                    f'{value_display(correction.correct_value)}'
                )
        else:
            # db_object.__setattr__(field_name, correction.correct_value)
            self.batch_data[db_object.id] = correction.correct_value

        if self.update_stored_samples:
            if correction.key not in self.stored_samples:
                print('NO STORED SAMPLE FOUND!!!!!')
                return
                raise Exception(
                    f'no stored sample found for {correction.key}'
                )

            stored_sample: BiobankStoredSample = self.stored_samples[
                correction.key
            ]
            if correction.correct_value is None:
                print(f'{correction.key}: skipping empty stored sample update')
                return


            sample_field = 'status'
            new_value = BiobankSpecimenDao._get_stored_status(
                status_str=db_object.status,                                # (sample update) specimen value ref
                disposal_reason_str=db_object.disposalReason
            )
            # new_value = correction.correct_value

            if DRY_RUN:
                db_stored_value = stored_sample.__getattribute__(sample_field)
                if db_stored_value == new_value:
                    print('\t not changing stored sample data')
                else:
                    print(
                        f'\t changing stored value '
                        f'from "{db_stored_value}" to '
                        f'"{new_value}"'
                    )
            else:
                stored_sample.__setattr__(sample_field, new_value)

    @classmethod
    def _get_mapped_db_objects(cls, keys, session) -> Dict:
        query_results = session.query(
            BiobankAliquotDataset                                                  # database table
        ).filter(
            BiobankAliquotDataset.rlimsId.in_(keys)                               # primary key filter
        ).all()

        return {
            record.rlimsId: record                                          # key mapping
            for record in query_results
        }

    def run(self):
        super().run()
                                                                            # file path
        file_path = 'recon/reconciliation_20260406_datasets_extractionmethodtimestamp_20260430.csv'
        batch_size = 5000

        # todo:
        #   move dry_run and stored_sample_update flags to the top
        #   when doing dry_run, print total count of changes at the end (for when verifying there are no changes)

        # specimen value changed:
        #   biobankId, orderId, testCode, confirmedDate,
        #   disposalDate, status, disposalReason
        self.update_stored_samples = False                                   # update stored samples

        correction_list = self._load_corrections(file_path)

        with self.get_session() as session:
            for correction_subset in list_chunks(
                correction_list, batch_size
            ):
                self.batch_data = {}

                self._print_chunk_stats(correction_subset)

                keys = [c.key for c in correction_subset]

                if self.update_stored_samples:
                    self._load_stored_samples(keys, session)

                mapped_objects = self._get_mapped_db_objects(
                    keys, session
                )

                for correction in correction_subset:
                    if correction.key not in mapped_objects:
                        raise Exception(
                            f'unable to find db object for {correction.key}'
                        )

                    self._process_correction(
                        correction, mapped_objects[correction.key]
                    )

                if not DRY_RUN:
                    self._run_batch_update(session, self.batch_data)
                session.commit()

    def _load_stored_samples(self, rlims_ids, session):
        query_results = session.query(BiobankStoredSample).filter(
            BiobankStoredSample.biobankStoredSampleId.in_(rlims_ids)
        ).all()

        self.stored_samples = {
            sample.biobankStoredSampleId: sample
            for sample in query_results
        }

    @classmethod
    def _load_corrections(cls, file_path: str) -> List[Correction]:
        results: List[Correction] = []

        with open(file_path) as file:
            reader = csv.DictReader(file)
            for index, row in enumerate(reader):
                key, value = row.values()
                if value == '':                                              # comment out to allow for clobbering data
                    value = None
                value = cls._parse_date(value)                                        # date parse

                if not key:
                    raise Exception(f'bad key found at {index}')

                results.append(
                    Correction(key, value)
                )

        return sorted(results, key=lambda c: c.key)

    @classmethod
    def _print_chunk_stats(cls, chunk: List[Correction]):
        print(
            f'{datetime.now()} processing '
            f'{chunk[0].key} to {chunk[-1].key}'
        )

    @classmethod
    def _parse_date(cls, date_str):
        return datetime.strptime(date_str, '%d%b%Y:%H:%M:%S')

    @classmethod
    def _run_batch_update(cls, session: Session, id_value_map):
        query = (
            'update biobank_aliquot_dataset set modified = now(), '
            'extraction_date = CASE id'
        )                                                                           # db column ref
        for _id, value in id_value_map.items():
            value_text = "null" if value is None else f'"{value}"'
            query += f' when {_id} then {value_text}'

        id_list_str = ','.join([str(_id) for _id in id_value_map.keys()])
        query += f' end where id in ({id_list_str});'

        session.execute(query)


def run():
    return cli_run(tool_cmd, tool_desc, BiobankReconciliation)
