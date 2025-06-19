from collections import defaultdict
import csv
import logging
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime
from typing import Dict, Iterable, List

from rdr_service.model import biobank_order as models
from rdr_service.services.system_utils import list_chunks
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'biospecimen-export'
tool_desc = 'Export Biospecimen data into CSV files'


@dataclass()
class Sample:
    rlims_id:                   str
    biobank_id:                 str
    order_id:                   str
    test_code:                  str
    repository_id:              str
    study_id:                   str
    cohort_id:                  str
    collection_date:            datetime
    confirmed_date:             datetime

    sample_type:                str
    status:                     str
    disposal_reason:            str
    disposal_date:              datetime
    freeze_thaw_count:          int
    location:                   str
    quantity:                   str
    quantity_units:             str
    processing_complete_date:   datetime
    deviations:                 str

    @classmethod
    def build(cls, sample: models.BiobankSpecimen) -> 'Sample':
        return Sample(
            rlims_id=sample.rlimsId,
            biobank_id=f'A{sample.biobankId}',
            order_id=sample.orderId,
            test_code=sample.testCode,
            repository_id=sample.repositoryId,
            study_id=sample.studyId,
            cohort_id=sample.cohortId,
            collection_date=sample.collectionDate,
            confirmed_date=sample.confirmedDate,
            sample_type=sample.sampleType,
            status=sample.status,
            disposal_reason=sample.disposalReason,
            disposal_date=sample.disposalDate,
            freeze_thaw_count=sample.freezeThawCount,
            location=sample.location,
            quantity=sample.quantity,
            quantity_units=sample.quantityUnits,
            processing_complete_date=sample.processingCompleteDate,
            deviations=sample.deviations,
        )


@dataclass()
class Attribute:
    sample_rlims_id:            str
    name:                       str
    value:                      str

    @classmethod
    def build(cls, attribute: models.BiobankSpecimenAttribute, sample_rlims_id: str) -> 'Attribute':
        assert attribute.specimen_rlims_id == sample_rlims_id
        return Attribute(
            sample_rlims_id=sample_rlims_id,
            name=attribute.name,
            value=attribute.value
        )


@dataclass()
class Aliquot:
    rlims_id:                   str
    sample_rlims_id:            str
    parent_aliquot_rlims_id:    str

    child_plan_service:         str
    initial_treatment:          str
    container_type_id:          str

    sample_type:                str
    status:                     str
    disposal_reason:            str
    disposal_date:              datetime
    freeze_thaw_count:          int
    location:                   str
    quantity:                   str
    quantity_units:             str
    processing_complete_date:   datetime
    deviations:                 str

    @classmethod
    def build(
        cls, aliquot: models.BiobankAliquot, sample_rlims_id: str
    ) -> 'Aliquot':
        assert aliquot.specimen_rlims_id == sample_rlims_id
        return Aliquot(
            rlims_id=aliquot.rlimsId,
            sample_rlims_id=sample_rlims_id,
            parent_aliquot_rlims_id=aliquot.parent_aliquot_rlims_id,
            child_plan_service=aliquot.childPlanService,
            initial_treatment=aliquot.initialTreatment,
            container_type_id=aliquot.containerTypeId,
            sample_type=aliquot.sampleType,
            status=aliquot.status,
            disposal_reason=aliquot.disposalReason,
            disposal_date=aliquot.disposalDate,
            freeze_thaw_count=aliquot.freezeThawCount,
            location=aliquot.location,
            quantity=aliquot.quantity,
            quantity_units=aliquot.quantityUnits,
            processing_complete_date=aliquot.processingCompleteDate,
            deviations=aliquot.deviations
        )


@dataclass()
class Dataset:
    rlims_id:                   str
    aliquot_rlims_id:           str

    name:                       str
    status:                     str

    @classmethod
    def build(cls, dataset: models.BiobankAliquotDataset) -> 'Dataset':
        assert dataset.aliquot_rlims_id  # just to be sure we have one for each of them
        return Dataset(
            rlims_id=dataset.rlimsId,
            aliquot_rlims_id=dataset.aliquot_rlims_id,
            name=dataset.name,
            status=dataset.status
        )


@dataclass()
class DatasetItem:
    dataset_rlims_id:           str

    param_id:                   str
    display_value:              str
    display_units:              str

    @classmethod
    def build(cls, item: models.BiobankAliquotDatasetItem) -> 'DatasetItem':
        assert item.dataset_rlims_id
        return DatasetItem(
            param_id=item.paramId,
            dataset_rlims_id=item.dataset_rlims_id,
            display_value=item.displayValue,
            display_units=item.displayUnits
        )


class CsvExport:
    def __init__(self, class_def, today):
        type_name = class_def.__name__
        file_name = f'{self._camel_to_snake(type_name)}s__{today.isoformat()}.csv'

        self._file = open(file_name, 'w')
        field_names = [field.name for field in fields(class_def)]
        self._writer = csv.DictWriter(self._file, field_names)
        self._writer.writeheader()

    def write(self, record):
        self._writer.writerow(asdict(record))

    def close(self):
        self._file.close()

    @classmethod
    def _camel_to_snake(cls, string):
        return ''.join(
            ['_' + char.lower() if char.isupper() else char for char in string]
        ).lstrip('_')


class BiospecimenExport(ToolBase):
    logger_name = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        today = date.today()

        self.sample_writer = CsvExport(Sample, today)
        self.attribute_writer = CsvExport(Attribute, today)
        self.aliquot_writer = CsvExport(Aliquot, today)
        self.dataset_writer = CsvExport(Dataset, today)
        self.item_writer = CsvExport(DatasetItem, today)

    def run(self):
        super().run()

        self.export_data()
        self.close_writers()

    def export_data(self):
        with self.get_session() as session:
            batch_size = 4000

            logging.info('getting id set')
            id_list = session.query(
                models.BiobankSpecimen.id
            ).all()
            id_list = [obj.id for obj in id_list]
            total_count = len(id_list)

            logging.info(f'exporting {total_count} samples')

            count = 0
            for id_subset in list_chunks(id_list, batch_size):
                count += len(id_subset)
                print(f'{datetime.now()}: retrieving batch')

                samples = self._load_samples(id_subset, session)
                sample_attribute_map = self._load_attributes(id_subset, session)

                # aliquots can be recursively nested, but every descendant aliquot
                # has a foreign key to the root sample's rlims id
                sample_rlims_id_list = [sample.rlimsId for sample in samples]
                sample_aliquot_map = self._load_aliquots(sample_rlims_id_list, session)

                aliquot_id_list = []
                for aliquot_list in sample_aliquot_map.values():
                    aliquot_id_list.extend([aliquot.id for aliquot in aliquot_list])
                aliquot_dataset_map = self._load_datasets(aliquot_id_list, session)

                written_dataset_rlims_id_list: List[str] = []
                for db_sample in samples:
                    self.sample_writer.write(
                        Sample.build(db_sample)
                    )

                    for db_attribute in sample_attribute_map[db_sample.id]:
                        self.attribute_writer.write(
                            Attribute.build(db_attribute, db_sample.rlimsId)
                        )

                    for db_aliquot in sample_aliquot_map[db_sample.rlimsId]:
                        self.aliquot_writer.write(
                            Aliquot.build(db_aliquot, db_sample.rlimsId)
                        )

                        written_dataset_rlims_id_list.extend(
                            self._process_datasets(aliquot_dataset_map[db_aliquot.id], db_aliquot.rlimsId)
                        )

                dataset_item_list = self._load_dataset_items(written_dataset_rlims_id_list, session)
                for db_dataset_item in dataset_item_list:
                    # assert db_item.dataset_rlims_id == dataset_rlims_id
                    self.item_writer.write(
                        DatasetItem.build(db_dataset_item)
                    )

                print(f'{datetime.now()}: completed {count} samples (out of {total_count})')

    @classmethod
    def _load_samples(cls, id_list, session) -> List[models.BiobankSpecimen]:
        return session.query(
            models.BiobankSpecimen
        ).filter(
            models.BiobankSpecimen.id.in_(id_list)
        ).all()

    @classmethod
    def _load_attributes(cls, sample_id_list, session) -> Dict[int, List[models.BiobankSpecimenAttribute]]:
        attribute_results: Iterable[models.BiobankSpecimenAttribute] = session.query(
            models.BiobankSpecimenAttribute
        ).filter(
            models.BiobankSpecimenAttribute.specimen_id.in_(sample_id_list)
        ).all()

        result: Dict[int, List[models.BiobankSpecimenAttribute]] = defaultdict(list)
        for attribute in attribute_results:
            result[attribute.specimen_id].append(attribute)

        return result

    @classmethod
    def _load_aliquots(cls, sample_rlims_id_list, session) -> Dict[str, List[models.BiobankAliquot]]:
        aliquot_results: Iterable[models.BiobankAliquot] = session.query(
            models.BiobankAliquot
        ).filter(
            models.BiobankAliquot.specimen_rlims_id.in_(sample_rlims_id_list)
        ).all()

        result: Dict[str, List[models.BiobankAliquot]] = defaultdict(list)
        for aliquot in aliquot_results:
            result[aliquot.specimen_rlims_id].append(aliquot)

        return result

    @classmethod
    def _load_datasets(cls, aliquot_id_list, session) -> Dict[int, List[models.BiobankAliquotDataset]]:
        dataset_results: Iterable[models.BiobankAliquotDataset] = session.query(
            models.BiobankAliquotDataset
        ).filter(
            models.BiobankAliquotDataset.aliquot_id.in_(aliquot_id_list)
        ).all()

        result: Dict[int, List[models.BiobankAliquotDataset]] = defaultdict(list)
        for dataset in dataset_results:
            result[dataset.aliquot_id].append(dataset)

        return result

    @classmethod
    def _load_dataset_items(cls, dataset_rlims_id_list, session) -> Iterable[models.BiobankAliquotDatasetItem]:
        return session.query(
            models.BiobankAliquotDatasetItem
        ).filter(
            models.BiobankAliquotDatasetItem.dataset_rlims_id.in_(dataset_rlims_id_list)
        ).all()

    def _process_datasets(self, dataset_list: Iterable[models.BiobankAliquotDataset], aliquot_rlims_id) -> List[str]:
        # there should only be one dataset for a given name, so use the latest one we have
        name_map = dict()
        final_dataset_id_list: List[str] = []
        for db_dataset in sorted(dataset_list, key=lambda dataset: dataset.id):
            name_map[db_dataset.name] = db_dataset

        for db_dataset in name_map.values():
            assert db_dataset.aliquot_rlims_id == aliquot_rlims_id
            self.dataset_writer.write(
                Dataset.build(db_dataset)
            )
            final_dataset_id_list.append(db_dataset.rlimsId)

        return final_dataset_id_list

    def close_writers(self):
        self.sample_writer.close()
        self.attribute_writer.close()
        self.aliquot_writer.close()
        self.dataset_writer.close()
        self.item_writer.close()


def run():
    return cli_run(tool_cmd, tool_desc, BiospecimenExport)
