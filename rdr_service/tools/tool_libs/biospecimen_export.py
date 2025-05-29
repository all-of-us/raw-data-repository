import csv
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime
from typing import Iterable, Optional

from rdr_service.model import biobank_order as models
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
        cls, aliquot: models.BiobankAliquot, sample_rlims_id: str, parent_aliquot_rlims_id: Optional[str]
    ) -> 'Aliquot':
        assert aliquot.specimen_rlims_id == sample_rlims_id
        if parent_aliquot_rlims_id:
            assert aliquot.parent_aliquot_rlims_id == parent_aliquot_rlims_id
        return Aliquot(
            rlims_id=aliquot.rlimsId,
            sample_rlims_id=sample_rlims_id,
            parent_aliquot_rlims_id=parent_aliquot_rlims_id,
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


class BiospecimenExport(ToolBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sample_records = []
        self.attribute_records = []
        self.aliquot_records = []
        self.dataset_records = []
        self.item_records = []

    def run(self):
        super().run()

        self.populate_record_lists()
        self.write_csv_files()

    def populate_record_lists(self):
        with self.get_session() as session:
            results: Iterable[models.BiobankSpecimen] = session.query(models.BiobankSpecimen).filter(
                models.BiobankSpecimen.biobankId.in_([
                ])
            ).all()  # todo: should do these in batches, and joinload all the nested objects

            for db_sample in results:
                self.sample_records.append(
                    Sample.build(db_sample)
                )

                for db_attribute in db_sample.attributes:
                    self.attribute_records.append(
                        Attribute.build(db_attribute, db_sample.rlimsId)
                    )

                direct_child_aliquots = [
                    aliquot for aliquot in db_sample.aliquots
                    if aliquot.parent_aliquot_rlims_id is None
                ]
                self._process_aliquots(direct_child_aliquots, db_sample.rlimsId)

    def _process_aliquots(
        self, aliquot_list: Iterable[models.BiobankAliquot], parent_sample_rlims_id, parent_aliquot_rlims_id=None
    ):
        if not aliquot_list:
            return

        for db_aliquot in aliquot_list:
            self.aliquot_records.append(
                Aliquot.build(db_aliquot, parent_sample_rlims_id, parent_aliquot_rlims_id=parent_aliquot_rlims_id)
            )
            self._process_datasets(db_aliquot.datasets, db_aliquot.rlimsId)
            self._process_aliquots(db_aliquot.aliquots, parent_sample_rlims_id, db_aliquot.rlimsId)

    def _process_datasets(self, dataset_list: Iterable[models.BiobankAliquotDataset], aliquot_rlims_id):
        # there should only be one dataset for a given name, so use the latest one we have
        name_map = dict()
        for db_dataset in sorted(dataset_list, key=lambda dataset: dataset.id):
            name_map[db_dataset.name] = db_dataset

        for db_dataset in name_map.values():
            assert db_dataset.aliquot_rlims_id == aliquot_rlims_id
            self.dataset_records.append(
                Dataset.build(db_dataset)
            )

            self._process_dataset_items(db_dataset.datasetItems, db_dataset.rlimsId)

    def _process_dataset_items(self, item_list: Iterable[models.BiobankAliquotDatasetItem], dataset_rlims_id):
        for db_item in item_list:
            assert db_item.dataset_rlims_id == dataset_rlims_id
            self.item_records.append(
                DatasetItem.build(db_item)
            )

    def write_csv_files(self):
        self.export_list(self.sample_records, Sample)
        self.export_list(self.attribute_records, Attribute)
        self.export_list(self.aliquot_records, Aliquot)
        self.export_list(self.dataset_records, Dataset)
        self.export_list(self.item_records, DatasetItem)

    @classmethod
    def export_list(cls, record_list, class_def):
        type_name = class_def.__name__
        today = date.today()
        file_name = f'{cls.camel_to_snake(type_name)}s__{today.isoformat()}.csv'

        with open(file_name, 'w') as file:
            field_names = [field.name for field in fields(class_def)]
            writer = csv.DictWriter(file, field_names)
            writer.writeheader()

            for record in record_list:
                writer.writerow(asdict(record))

    @classmethod
    def camel_to_snake(cls, string):
        return ''.join(
            ['_' + char.lower() if char.isupper() else char for char in string]
        ).lstrip('_')


def run():
    return cli_run(tool_cmd, tool_desc, BiospecimenExport)
