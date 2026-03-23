from collections import defaultdict
import csv
from dataclasses import dataclass, field
from datetime import datetime
from enum import auto, Enum
from typing import Collection, Dict, List, Optional

import sqlalchemy as sa
from sqlalchemy.orm import Session

from rdr_service.model import genomics, ppsc
from rdr_service.model.awardee_insite import AwardeeInSite
from rdr_service.model.biobank_order import (
    BiobankAliquot, BiobankAliquotDataset, BiobankAliquotDatasetItem, BiobankSpecimen, BiobankAliquotTreatment
)
from rdr_service.model.participant import Participant
from rdr_service.services.bigquery import BigQueryTable
from rdr_service.services.system_utils import list_chunks
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'sample-availability'
tool_desc = 'Generates dataset of sample availability'


SampleCollectionCutoffDate = datetime(2025, 1, 1)
BqUploadLocation = {
    'project': 'aou-warehouse-preprod',
    'dataset': 'privacy_review',
    'table': 'aou_sample_availability'
}


@dataclass
class Availability:
    is_available: bool = False
    collection_datetime: datetime = None


class SampleType(Enum):
    saliva = auto()
    blood = auto()
    edta_plasma = auto()
    pst_plasma = auto()
    serum = auto()
    ccfdna = auto()
    urine = auto()
    pxr_rna = auto()
    hep = auto()


@dataclass
class AliquotData:
    aliquot_id: int
    aliquot_rlims_id: str
    participant_id: int
    volume: float
    volume_units: str
    sample_type: SampleType
    collection_timestamp: datetime
    freeze_thaw_count: int
    first_freeze_date: datetime
    ds_dna_mass: float = None
    ds_concentration: float = None
    ds_concentration_units: str = None
    total_dna_concentration: float = None
    total_dna_concentration_units: str = None
    total_dna_mass: float = None
    a_260_230_ratio: float = None
    a_260_280_ratio: float = None
    extraction_method: str = None
    extraction_date: datetime = None
    treatment_type: str = None
    treatment_date: str = None
    wgs_sequenced: bool = False
    array_sequenced: bool = False

    def meets_quantity_reqs(self) -> bool:
        if self.sample_type in [SampleType.saliva, SampleType.blood]:
            return self.ds_dna_mass is not None and self.ds_dna_mass >= 2500
        else:
            if self.volume is None:
                return False

            match self.volume_units.lower():
                case 'ul':
                    ul_volume = self.volume
                case 'ml':
                    ul_volume = self.volume * 1000
                case _:
                    raise Exception(f'Unexpected volume units "{self.volume_units}"')
            return ul_volume >= 100

    def __str__(self):
        return f'{self.aliquot_id}: {self.volume} ({self.ds_dna_mass} ug) {self.sample_type}'


@dataclass
class ParticipantData:
    participant_id: int
    pst_plasma_availability: Availability = field(default_factory=Availability)
    edta_plasma_availability: Availability = field(default_factory=Availability)
    serum_availability: Availability = field(default_factory=Availability)
    blood_availability: Availability = field(default_factory=Availability)
    saliva_availability: Availability = field(default_factory=Availability)
    cell_free_dna_availability: Availability = field(default_factory=Availability)
    urine_availability: Availability = field(default_factory=Availability)
    pxr_rna_availability: Availability = field(default_factory=Availability)
    hep_availability: Availability = field(default_factory=Availability)
    array_status: bool = False
    array_source_blood: bool = False
    wgs_status: bool = False
    wgs_source_blood: bool = False

    def set_type_as_available(self, sample_type: SampleType, collection_date: datetime):
        field_to_set = None
        match sample_type:
            case SampleType.saliva:
                field_to_set = self.saliva_availability
            case SampleType.blood:
                field_to_set = self.blood_availability
            case SampleType.edta_plasma:
                field_to_set = self.edta_plasma_availability
            case SampleType.pst_plasma:
                field_to_set = self.pst_plasma_availability
            case SampleType.serum:
                field_to_set = self.serum_availability
            case SampleType.ccfdna:
                field_to_set = self.cell_free_dna_availability
            case SampleType.urine:
                field_to_set = self.urine_availability
            case SampleType.pxr_rna:
                field_to_set = self.pxr_rna_availability
            case SampleType.hep:
                field_to_set = self.hep_availability

        if field_to_set:
            field_to_set.is_available = True
            field_to_set.collection_datetime = collection_date


SalivaSampleCodes = [
    '1SAL',
    '1SAL2',
    '2SAL0'
]
BloodSampleCodes = [
    '1ED02',
    '1ED04',
    '1ED10',
    '2ED02',
    '2ED04'
]
PlasmaSampleCodes = [
    '1PS4A',
    '1PS4B',
    '1PST8',
    '2PS4A',
    '2PS4B',
    '2PST8'
]
SerumSampleCodes = [
    '1SST8',
    '2SST8'
]
CellFreeCodes = [
    '1CFD9'
]
UrineCodes = [
    '1UR10',
    '1UR90'
]
PxrRnaCodes = [
    '1PXR2'
]
HepCodes = [
    '1HEP4'
]
SampleCodesToProcess = [
    *SalivaSampleCodes,
    *BloodSampleCodes,
    *PlasmaSampleCodes,
    *SerumSampleCodes,
    *CellFreeCodes,
    *UrineCodes,
    *PxrRnaCodes,
    *HepCodes
]

availability_data = defaultdict(lambda participant_id: ParticipantData(participant_id))


class SampleAvailabilityDatasetTool(ToolBase):
    def run(self):
        super().run()  # 3/17 started test run at 8:10

        with self.get_session() as session:
            aliquot_list = self.retrieve_potential_aliquot_list(session)
            self.process_dataset_info(session, aliquot_list)
            self.load_treatment_data(session, aliquot_list)
            self.load_sequencing_data(session, aliquot_list)

            eligible_participant_id_list = sorted(self.retrieve_eligible_participant_ids(session))

        print(datetime.now(), 'getting end-check counts')
        counts = defaultdict(int)
        total_count = len(aliquot_list)
        blood_mass_count = 0
        saliva_mass_count = 0
        for a in aliquot_list:
            counts[a.sample_type] += 1
            if a.sample_type == SampleType.blood and a.ds_dna_mass:
                blood_mass_count += 1
            elif a.sample_type == SampleType.saliva and a.ds_dna_mass:
                saliva_mass_count += 1

        print('')
        print(str(total_count).rjust(30), 'aliquots')
        print('')
        for t in counts:
            print(str(counts[t]).rjust(30), t)
        print('')
        print(str(counts[SampleType.blood] - blood_mass_count).rjust(30), 'blood missing mass val')
        print(str(counts[SampleType.saliva] - saliva_mass_count).rjust(30), 'saliva missing mass val')

        unit_count = defaultdict(int)
        for a in aliquot_list:
            key = (a.sample_type, a.volume_units)
            unit_count[key] += 1

        print('')
        print('unit summary')
        for k in unit_count:
            print(str(unit_count[k]).rjust(30), f'{k[0]} ({k[1]})')

        print(datetime.now(), 'prepping for csv')
        aliquots_by_participant = self.organize_aliquots(aliquot_list)

        participant_data_to_export: List[ParticipantData] = []
        aliquot_data_to_export: List[AliquotData] = []
        for participant_id in eligible_participant_id_list:
            if not participant_id in aliquots_by_participant:
                continue

            participant_aliquot_data = aliquots_by_participant[participant_id]
            participant_export_data = None
            for sample_type in participant_aliquot_data:
                collection_date = None
                wgs_source = None
                array_source = None
                for aliquot_data in participant_aliquot_data[sample_type]:
                    if (
                        aliquot_data.meets_quantity_reqs()
                        and aliquot_data.collection_timestamp < SampleCollectionCutoffDate
                        and (
                            collection_date is None
                            or aliquot_data.collection_timestamp < collection_date
                        )
                    ):
                        collection_date = aliquot_data.collection_timestamp
                        aliquot_data_to_export.append(aliquot_data)

                        if aliquot_data.array_sequenced or aliquot_data.wgs_sequenced:
                            source = 'blood' if sample_type == SampleType.blood else 'saliva'
                            if aliquot_data.array_sequenced:
                                array_source = source
                            if aliquot_data.wgs_sequenced:
                                wgs_source = source

                if collection_date:
                    if participant_export_data is None:
                        participant_export_data = ParticipantData(participant_id)
                        participant_data_to_export.append(participant_export_data)

                    participant_export_data.set_type_as_available(sample_type, collection_date)
                    if not participant_export_data.wgs_status and wgs_source:
                        participant_export_data.wgs_status = True
                        participant_export_data.wgs_source_blood = wgs_source == 'blood'
                    if not participant_export_data.array_status and array_source:
                        participant_export_data.array_status = True
                        participant_export_data.array_source_blood = array_source == 'blood'

        print(datetime.now(), 'writing to csv')
        self._export_participant_data_as_csv(participant_data_to_export)
        self._export_aliquot_data_as_csv(aliquot_data_to_export)
        # self._upload_to_bq(data_to_export)

    @classmethod
    def _export_participant_data_as_csv(cls, data_to_export: List[ParticipantData]):
        with open('participant_export.csv', 'w') as file:
            writer = csv.DictWriter(file, [
                'participant_id',
                'pst_plasma_availability',
                'pst_plasma_collection_timestamp',
                'edta_plasma_availability',
                'edta_plasma_collection_timestamp',
                'serum_availability',
                'serum_collection_timestamp',
                'blood_availability',
                'blood_collection_timestamp',
                'saliva_availability',
                'saliva_collection_timestamp',
                'cell_free_dna_availability',
                'cell_free_dna_collection_timestamp',
                'urine_availability',
                'urine_collection_timestamp',
                'pxr_rna_availability',
                'pxr_rna_collection_timestamp',
                'hep_availability',
                'hep_collection_timestamp',
                'array_sequencing_status',
                'array_dna_source',
                'wgs_sequencing_status',
                'wgs_dna_source',
                'src_id'
            ])
            writer.writeheader()
            for data in data_to_export:
                array_source = ''
                if data.array_status:
                    array_source = 'Whole Blood' if data.array_source_blood else 'Saliva'
                wgs_source = ''
                if data.wgs_status:
                    wgs_source = 'Whole Blood' if data.wgs_source_blood else 'Saliva'

                writer.writerow({
                    'participant_id': data.participant_id,
                    'pst_plasma_availability': 'Y' if data.pst_plasma_availability.is_available else 'N',
                    'pst_plasma_collection_timestamp': data.pst_plasma_availability.collection_datetime,
                    'edta_plasma_availability': 'Y' if data.edta_plasma_availability.is_available else 'N',
                    'edta_plasma_collection_timestamp': data.edta_plasma_availability.collection_datetime,
                    'serum_availability': 'Y' if data.serum_availability.is_available else 'N',
                    'serum_collection_timestamp': data.serum_availability.collection_datetime,
                    'blood_availability': 'Y' if data.blood_availability.is_available else 'N',
                    'blood_collection_timestamp': data.blood_availability.collection_datetime,
                    'saliva_availability': 'Y' if data.saliva_availability.is_available else 'N',
                    'saliva_collection_timestamp': data.saliva_availability.collection_datetime,
                    'cell_free_dna_availability': 'Y' if data.cell_free_dna_availability.is_available else 'N',
                    'cell_free_dna_collection_timestamp': data.cell_free_dna_availability.collection_datetime,
                    'urine_availability': 'Y' if data.urine_availability.is_available else 'N',
                    'urine_collection_timestamp': data.urine_availability.collection_datetime,
                    'pxr_rna_availability': 'Y' if data.pxr_rna_availability.is_available else 'N',
                    'pxr_rna_collection_timestamp': data.pxr_rna_availability.collection_datetime,
                    'hep_availability': 'Y' if data.hep_availability.is_available else 'N',
                    'hep_collection_timestamp': data.hep_availability.collection_datetime,
                    'array_sequencing_status': 'Y' if data.array_status else 'N',
                    'array_dna_source': array_source,
                    'wgs_sequencing_status': 'Y' if data.wgs_status else 'N',
                    'wgs_dna_source': wgs_source,
                    'src_id': 'Staff Portal: LIMS'
                })

    @classmethod
    def _export_aliquot_data_as_csv(cls, data_to_export: List[AliquotData]):
        with open('aliquot_export.csv', 'w') as file:
            writer = csv.DictWriter(file, [
                'participant_id',
                'aliquot_rlims_id',
                'sample_type',
                'collection_timestamp',
                'volume',
                'volume_units',
                'total_mass',
                'total_mass_units',
                'ds_dna_mass',
                'ds_dna_mass_units',
                'total_dna_concentration',
                'total_dna_concentration_units',
                'ds_concentration',
                'ds_concentration_units',
                '260/230_ratio',
                '260/280_ratio',
                'freeze_thaw_count',
                'treatment_type',
                'treatment_date',
                'extraction_method',
                'extraction_date',
                'first_freeze_date',
                'src_id'
            ])
            writer.writeheader()
            for data in data_to_export:
                writer.writerow({
                    'participant_id': data.participant_id,
                    'aliquot_rlims_id': data.aliquot_rlims_id,
                    'sample_type': data.sample_type.name,
                    'collection_timestamp': data.collection_timestamp,
                    'volume': data.volume,
                    'volume_units': data.volume_units,
                    'total_mass': data.total_dna_mass,
                    'total_mass_units': 'ng',
                    'ds_dna_mass': data.ds_dna_mass,
                    'ds_dna_mass_units': 'ng',
                    'total_dna_concentration': data.total_dna_concentration,
                    'total_dna_concentration_units': data.total_dna_concentration_units,
                    'ds_concentration': data.ds_concentration,
                    'ds_concentration_units': data.ds_concentration_units,
                    '260/230_ratio': data.a_260_230_ratio,
                    '260/280_ratio': data.a_260_280_ratio,
                    'freeze_thaw_count': data.freeze_thaw_count,
                    'treatment_type': data.treatment_type,
                    'treatment_date': data.treatment_date,
                    'extraction_method': data.extraction_method,
                    'extraction_date': data.extraction_date,
                    'first_freeze_date': data.first_freeze_date,
                    'src_id': 'Staff Portal: LIMS'
                })

    @classmethod
    def _upload_to_bq(cls, data_to_export: List[ParticipantData]):
        bq_table = BigQueryTable(**BqUploadLocation)
        batch_size = 2000

        count = 0
        total = len(data_to_export)
        for batch in list_chunks(data_to_export, batch_size):
            bq_table.insert([
                {
                    'person_id': data.participant_id,
                    'pst_plasma_availability': data.pst_plasma_availability.is_available,
                    'pst_plasma_collection_timestamp': data.pst_plasma_availability.collection_datetime,
                    'edta_plasma_availability': data.edta_plasma_availability.is_available,
                    'edta_plasma_collection_timestamp': data.edta_plasma_availability.collection_datetime,
                    'serum_availability': data.serum_availability.is_available,
                    'serum_collection_timestamp': data.serum_availability.collection_datetime,
                    'blood_availability': data.blood_availability.is_available,
                    'blood_collection_timestamp': data.blood_availability.collection_datetime,
                    'saliva_availability': data.saliva_availability.is_available,
                    'saliva_collection_timestamp': data.saliva_availability.collection_datetime,
                }
                for data in batch
            ])
            count += batch_size
            print(f'{datetime.now()}: finish with {count} of {total}')

    @classmethod
    def _get_timestamp_or_none(cls, is_available, timestamp):
        return timestamp if is_available else None

    @classmethod
    def retrieve_potential_aliquot_list(cls, session: Session) -> List[AliquotData]:
        print(datetime.now(), 'retrieving aliquots...')
        query = session.query(
            BiobankAliquot.id,
            BiobankAliquot.rlimsId,
            Participant.participantId,
            BiobankAliquot.quantity,
            BiobankAliquot.quantityUnits,
            BiobankSpecimen.testCode,
            BiobankAliquot.sampleType,
            BiobankSpecimen.collectionDate,
            BiobankAliquot.freezeThawCount,
            BiobankAliquot.processingCompleteDate
        ).join(
            BiobankSpecimen, BiobankSpecimen.rlimsId == BiobankAliquot.specimen_rlims_id
        ).join(
            Participant, Participant.biobankId == BiobankSpecimen.biobankId
        ).filter(
            sa.and_(
                BiobankSpecimen.testCode.in_(SampleCodesToProcess),
                BiobankAliquot.status == 'In Circulation',
                BiobankAliquot.location == 'Mayo_MN'
            )
        )
        query_results = query.all()

        aliquots = []
        for result in query_results:
            (
                aliquot_id, aliquot_rlims_id, participant_id, volume_str, units_str,
                test_code, sample_type, collection_timestamp, freeze_count, processing_datetime
            ) = result
            sample_type = cls.determine_sample_type(test_code, sample_type)
            if not sample_type:
                continue

            volume_val = None
            if volume_str:
                volume_val = float(volume_str)
            aliquots.append(
                AliquotData(
                    aliquot_id, aliquot_rlims_id, participant_id, volume_val, units_str,
                    sample_type, collection_timestamp, freeze_count, processing_datetime
                )
            )
        return aliquots

    @classmethod
    def determine_sample_type(cls, test_code: str, sample_type: str) -> Optional[SampleType]:
        if test_code in BloodSampleCodes:
            if sample_type == 'Plasma':
                return SampleType.edta_plasma
            elif sample_type == 'DNA':
                return SampleType.blood
            else:
                return None
        elif test_code in PlasmaSampleCodes:
            return SampleType.pst_plasma
        elif test_code in SerumSampleCodes:
            return SampleType.serum
        elif test_code in SalivaSampleCodes and sample_type == 'DNA':
            return SampleType.saliva
        elif test_code in CellFreeCodes:
            return SampleType.ccfdna
        elif test_code in UrineCodes:
            return SampleType.urine
        elif test_code in PxrRnaCodes:
            return SampleType.pxr_rna
        elif test_code in HepCodes:
            return SampleType.hep

        return None

    @classmethod
    def process_dataset_info(cls, session: Session, aliquot_list: List[AliquotData]):
        batch_size = 8000
        current_count = 0
        total_count = len(aliquot_list)

        for aliquot_subset in list_chunks(aliquot_list, batch_size):
            print(datetime.now(), 'calculating mass values...')
            aliquot_id_list = []
            aliquot_map: Dict[int, AliquotData] = {}
            for aliquot in aliquot_subset:
                aliquot_map[aliquot.aliquot_id] = aliquot
                aliquot_id_list.append(aliquot.aliquot_id)

            print(datetime.now(), 'building dataset list...')
            dataset_list: Collection[BiobankAliquotDataset] = session.query(
                BiobankAliquotDataset
            ).filter(
                BiobankAliquotDataset.aliquot_id.in_(aliquot_id_list)
            ).order_by(
                BiobankAliquotDataset.id
            ).all()

            extraction_values = {}

            latest_dataset_map = {}
            dataset_to_aliquot_map: Dict[int, int] = {}
            for dataset in dataset_list:
                key_val = (dataset.aliquot_id, dataset.name)
                latest_dataset_map[key_val] = dataset.id
                dataset_to_aliquot_map[dataset.id] = dataset.aliquot_id

                if dataset.extractionDate:
                    extraction_values[dataset.aliquot_id] = (dataset.extractionMethod, dataset.extractionDate)

            print(datetime.now(), 'getting dataset items...')
            dataset_item_list = session.query(
                BiobankAliquotDatasetItem
            ).filter(
                BiobankAliquotDatasetItem.dataset_id.in_(latest_dataset_map.values())
            ).all()

            ds_conc_values: Dict[int, BiobankAliquotDatasetItem] = {}
            total_conc_values: Dict[int, BiobankAliquotDatasetItem] = {}
            a230_values: Dict[int, BiobankAliquotDatasetItem] = {}
            a280_values: Dict[int, BiobankAliquotDatasetItem] = {}
            for dataset_item in dataset_item_list:
                aliquot_id = dataset_to_aliquot_map[dataset_item.dataset_id]
                if dataset_item.paramId == 'dsDNA Conc':
                    ds_conc_values[aliquot_id] = dataset_item
                elif dataset_item.paramId == 'Total DNA Conc':
                    total_conc_values[aliquot_id] = dataset_item
                elif dataset_item.paramId == 'A260/230':
                    a230_values[aliquot_id] = dataset_item
                elif dataset_item.paramId == 'A260/280':
                    a280_values[aliquot_id] = dataset_item

            for aliquot_id in ds_conc_values:
                aliquot = aliquot_map[aliquot_id]
                conc_data = ds_conc_values[aliquot_id]
                aliquot.ds_concentration = float(conc_data.displayValue)
                aliquot.ds_concentration_units = conc_data.displayUnits
                aliquot.ds_dna_mass = aliquot.volume * aliquot.ds_concentration  # uL * ng/uL

            for aliquot_id in total_conc_values:
                aliquot = aliquot_map[aliquot_id]
                conc_data = total_conc_values[aliquot_id]
                aliquot.total_dna_concentration = float(conc_data.displayValue)
                aliquot.total_dna_concentration_units = conc_data.displayUnits
                aliquot.total_dna_mass = aliquot.volume * aliquot.total_dna_concentration

                # TODO: make sure all concentrations have the same units (ng/ul).
                #       as of 2026-03-19, they're all good. query to check:
                #                       select distinct param_id, bai.display_units, ba.quantity_units
                #                       from biobank_aliquot_dataset_item bai
                #                       inner join biobank_aliquot_dataset bad on bad.id = bai.dataset_id
                #                       inner join biobank_aliquot ba on ba.id  = bad.aliquot_id
                #                       where param_id like '%conc%';

            for aliquot_id in a230_values:
                aliquot = aliquot_map[aliquot_id]
                dataset_item = a230_values[aliquot_id]
                aliquot.a_260_230_ratio = dataset_item.displayValue

            for aliquot_id in a280_values:
                aliquot = aliquot_map[aliquot_id]
                dataset_item = a280_values[aliquot_id]
                aliquot.a_260_280_ratio = dataset_item.displayValue

            for aliquot_id in extraction_values:
                aliquot = aliquot_map[aliquot_id]
                aliquot.extraction_method, aliquot.extraction_date = extraction_values[aliquot_id]

            current_count += len(aliquot_subset)
            print(datetime.now(), f'completed {current_count} of {total_count}')

    @classmethod
    def load_treatment_data(cls, session: Session, aliquot_list: List[AliquotData]):
        batch_size = 8000
        current_count = 0
        total_count = len(aliquot_list)

        for aliquot_subset in list_chunks(aliquot_list, batch_size):
            print(datetime.now(), 'retrieving treatment data...')
            aliquot_id_list = []
            aliquot_map: Dict[str, AliquotData] = {}
            for aliquot in aliquot_subset:
                aliquot_map[aliquot.aliquot_rlims_id] = aliquot
                aliquot_id_list.append(aliquot.aliquot_rlims_id)

            treatment_data: Collection[BiobankAliquotTreatment] = session.query(
                BiobankAliquotTreatment
            ).filter(
                BiobankAliquotTreatment.aliquot_rlims_id.in_(aliquot_id_list)
            ).order_by(
                BiobankAliquotTreatment.rdr_received_timestamp
            ).all()

            aliquot_treatment_map: Dict[str, List[BiobankAliquotTreatment]] = defaultdict(list)
            for treatment in treatment_data:
                aliquot_treatment_map[treatment.aliquot_rlims_id].append(treatment)

            for aliquot_rlims_id, treatment_list in aliquot_treatment_map.items():
                aliquot = aliquot_map[aliquot_rlims_id]
                aliquot.treatment_type = ','.join([treatment.name for treatment in treatment_list])
                aliquot.treatment_date = ','.join(
                    [treatment.rdr_received_timestamp.isoformat() for treatment in treatment_list]
                )

            current_count += len(aliquot_subset)
            print(datetime.now(), f'completed {current_count} of {total_count}')

    @classmethod
    def load_sequencing_data(cls, session: Session, aliquot_list: List[AliquotData]):
        batch_size = 8000
        current_count = 0
        total_count = len(aliquot_list)

        for aliquot_subset in list_chunks(aliquot_list, batch_size):
            print(datetime.now(), 'retrieving sequencing data...')
            aliquot_id_list = []
            aliquot_map: Dict[str, AliquotData] = {}
            for aliquot in aliquot_subset:
                aliquot_map[aliquot.aliquot_rlims_id] = aliquot
                aliquot_id_list.append(aliquot.aliquot_rlims_id)

            query = session.query(
                genomics.GenomicAW4Raw.genome_type,
                genomics.GenomicAW1Raw.parent_sample_id
            ).join(
                genomics.GenomicAW1Raw,
                genomics.GenomicAW4Raw.sample_id == genomics.GenomicAW1Raw.sample_id
            ).filter(
                genomics.GenomicAW4Raw.qc_status.like('pass'),
                genomics.GenomicAW1Raw.parent_sample_id.in_(aliquot_id_list),
                genomics.GenomicAW4Raw.ignore_flag == False,
                genomics.GenomicAW1Raw.ignore_flag == False
            )
            qc_success_data: Collection[genomics.GenomicAW4Raw] = query.all()

            for genome_type, aliquot_rlims_id in qc_success_data:
                aliquot = aliquot_map[aliquot_rlims_id]
                if genome_type is None:
                    continue
                if genome_type.lower().endswith('wgs'):
                    aliquot.wgs_sequenced = True
                else:
                    aliquot.array_sequenced = True

            current_count += len(aliquot_subset)
            print(datetime.now(), f'completed {current_count} of {total_count}')

    @classmethod
    def organize_aliquots(cls, aliquot_list: List[AliquotData]) -> Dict[int, Dict[SampleType, List[AliquotData]]]:
        biobank_map = defaultdict(lambda: defaultdict(list))
        for aliquot in aliquot_list:
            participant_data = biobank_map[aliquot.participant_id]
            participant_data[aliquot.sample_type].append(aliquot)

        return biobank_map

    @classmethod
    def retrieve_eligible_participant_ids(cls, session: Session) -> List[int]:
        print(datetime.now(), 'getting participant ids')
        query = session.query(
            AwardeeInSite.participantId
        ).filter(
            sa.or_(
                AwardeeInSite.clinicPhysicalMeasurementsStatus == 'completed',
                AwardeeInSite.selfReportedPhysicalMeasurementsStatus == 'completed'
            ),
            AwardeeInSite.firstEhrReceiptTime != None
        )
        query = cls.filter_query_by_survey('The Basics', query)
        query = cls.filter_query_by_survey('Lifestyle', query)
        query = cls.filter_query_by_survey('Overall Health', query)
        result = query.distinct().all()
        participant_id_list = [record.participantId for record in result]

        return cls.filter_participants_by_genomic_data(session, participant_id_list)

    @classmethod
    def filter_query_by_survey(cls, survey_name, query: sa.orm.query) -> sa.orm.query:
        survey_event = sa.orm.aliased(ppsc.SurveyCompletionEvent)
        return query.join(
            survey_event,
            survey_event.participant_id == AwardeeInSite.participantId
        ).filter(
            sa.and_(
                survey_event.ignore_flag == 0,
                survey_event.event_type_name == survey_name
            )
        )

    @classmethod
    def filter_participants_by_genomic_data(cls, session, participant_id_list: List[int]) -> List[int]:
        print(datetime.now(), 'filtering participant level genomic data')
        result: List[int] = []
        batch_size = 2000
        for id_subset in list_chunks(participant_id_list, batch_size):
            query = session.query(
                Participant.participantId
            ).join(
                genomics.GenomicAW4Raw,
                genomics.GenomicAW4Raw.biobank_id == sa.func.concat('A', Participant.biobankId)
            ).filter(
                Participant.participantId.in_(id_subset),
                genomics.GenomicAW4Raw.qc_status.ilike('pass')
            ).distinct()
            query_result = query.all()
            result.extend([record.participantId for record in query_result])

        return result


def run():
    return cli_run(tool_cmd, tool_desc, SampleAvailabilityDatasetTool)
