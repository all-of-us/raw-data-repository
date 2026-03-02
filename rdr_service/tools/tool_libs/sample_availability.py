from collections import defaultdict
import csv
from dataclasses import dataclass, field
from datetime import datetime
from enum import auto, Enum
from typing import Dict, List, Optional

import sqlalchemy as sa
from sqlalchemy.orm import Session

from rdr_service.model import genomics, ppsc
from rdr_service.model.awardee_insite import AwardeeInSite
from rdr_service.model.biobank_order import (
    BiobankAliquot, BiobankAliquotDataset, BiobankAliquotDatasetItem, BiobankSpecimen
)
from rdr_service.model.participant import Participant
from rdr_service.services.bigquery import BigQueryTable
from rdr_service.services.system_utils import list_chunks
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'sample-availability'
tool_desc = 'Generates dataset of sample availability'


SampleCollectionCutoffDate = datetime(2023, 10, 1)
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


@dataclass
class AliquotData:
    aliquot_id: int
    participant_id: int
    volume: float
    volume_units: str
    sample_type: SampleType
    collection_timestamp: datetime
    ds_dna_mass: float = None
    total_dna_mass: float = None

    def meets_quantity_reqs(self) -> bool:
        if self.sample_type in [SampleType.saliva, SampleType.blood]:
            return self.ds_dna_mass is not None and self.ds_dna_mass >= 2500
        else:
            return self.volume >= 100

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
SampleCodesToProcess = [
    *SalivaSampleCodes,
    *BloodSampleCodes,
    *PlasmaSampleCodes,
    *SerumSampleCodes
]

availability_data = defaultdict(lambda participant_id: ParticipantData(participant_id))


class SampleAvailabilityDatasetTool(ToolBase):
    def run(self):
        super().run()

        with self.get_session() as session:
            aliquot_list = self.retrieve_potential_aliquot_list(session)
            self.calculate_dna_mass(session, aliquot_list)

            eligible_participant_id_list = sorted(self.retrieve_eligible_participant_ids(session))

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

        aliquots_by_participant = self.organize_aliquots(aliquot_list)

        data_to_export: List[ParticipantData] = []
        for participant_id in eligible_participant_id_list:
            if not participant_id in aliquots_by_participant:
                continue

            participant_aliquot_data = aliquots_by_participant[participant_id]
            participant_export_data = None
            for sample_type in participant_aliquot_data:
                collection_date = None
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

                if collection_date:
                    if participant_export_data is None:
                        participant_export_data = ParticipantData(participant_id)
                        data_to_export.append(participant_export_data)

                    participant_export_data.set_type_as_available(sample_type, collection_date)

        # self._store_as_csv(data_to_export)
        self._upload_to_bq(data_to_export)

    @classmethod
    def _store_as_csv(cls, data_to_export: List[ParticipantData]):
        with open('export.csv', 'w') as file:
            writer = csv.DictWriter(file, [
                'person_id',
                'pst_plasma_availability',
                'pst_plasma_collection_timestamp',
                'edta_plasma_availability',
                'edta_plasma_collection_timestamp',
                'serum_availability',
                'serum_collection_timestamp',
                'blood_availability',
                'blood_collection_timestamp',
                'saliva_availability',
                'saliva_collection_timestamp'
            ])
            writer.writeheader()
            for data in data_to_export:
                writer.writerow({
                    'person_id': data.participant_id,
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
                })

    @classmethod
    def _upload_to_bq(cls, data_to_export: List[ParticipantData]):
        # had about 420,000 records
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
            Participant.participantId,
            BiobankAliquot.quantity,
            BiobankAliquot.quantityUnits,
            BiobankSpecimen.testCode,
            BiobankAliquot.sampleType,
            BiobankSpecimen.collectionDate
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
                aliquot_id, participant_id, volume_str, units_str,
                test_code, sample_type, collection_timestamp
            ) = result
            sample_type = cls.determine_sample_type(test_code, sample_type)
            if not sample_type:
                continue

            volume_val = None
            if volume_str:
                volume_val = float(volume_str)
            aliquots.append(
                AliquotData(aliquot_id, participant_id, volume_val, units_str, sample_type, collection_timestamp)
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

        return None

    @classmethod
    def calculate_dna_mass(cls, session: Session, aliquot_list: List[AliquotData]):
        # only need to find mass values for blood and saliva aliquots
        dna_aliquots = [
            aliquot for aliquot in aliquot_list
            if aliquot.sample_type in [SampleType.blood, SampleType.saliva]
        ]

        batch_size = 8000
        current_count = 0
        total_count = len(dna_aliquots)

        for aliquot_subset in list_chunks(dna_aliquots, batch_size):
            print(datetime.now(), 'calculating mass values...')
            aliquot_id_list = []
            aliquot_map = {}
            for aliquot in aliquot_subset:
                aliquot_map[aliquot.aliquot_id] = aliquot
                aliquot_id_list.append(aliquot.aliquot_id)

            print(datetime.now(), 'building dataset list...')
            dataset_list = session.query(
                BiobankAliquotDataset
            ).filter(
                BiobankAliquotDataset.aliquot_id.in_(aliquot_id_list)
            ).order_by(
                BiobankAliquotDataset.id
            ).all()

            latest_dataset_map = {}
            dataset_to_aliquot_map: Dict[int, int] = {}
            for dataset in dataset_list:
                key_val = (dataset.aliquot_id, dataset.name)
                latest_dataset_map[key_val] = dataset.id
                dataset_to_aliquot_map[dataset.id] = dataset.aliquot_id

            print(datetime.now(), 'getting dataset items...')
            dataset_item_list = session.query(
                BiobankAliquotDatasetItem
            ).filter(
                BiobankAliquotDatasetItem.dataset_id.in_(latest_dataset_map.values())
            ).all()

            ds_conc_values: Dict[int, BiobankAliquotDatasetItem] = {}
            total_conc_values: Dict[int, BiobankAliquotDatasetItem] = {}
            for dataset_item in dataset_item_list:
                aliquot_id = dataset_to_aliquot_map[dataset_item.dataset_id]
                if dataset_item.paramId == 'dsDNA Conc':
                    ds_conc_values[aliquot_id] = dataset_item
                elif dataset_item.paramId == 'Total DNA Conc':
                    total_conc_values[aliquot_id] = dataset_item

            for aliquot_id in ds_conc_values:
                aliquot = aliquot_map[aliquot_id]
                conc_value = float(ds_conc_values[aliquot_id].displayValue)
                aliquot.ds_dna_mass = aliquot.volume * conc_value

            for aliquot_id in total_conc_values:
                aliquot = aliquot_map[aliquot_id]
                conc_value = float(total_conc_values[aliquot_id].displayValue)
                aliquot.total_dna_mass = aliquot.volume * conc_value

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
