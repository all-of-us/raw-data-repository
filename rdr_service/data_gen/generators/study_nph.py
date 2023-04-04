from typing import List, Dict, Any, Tuple, Optional, Iterable
from random import getrandbits, choice
from datetime import datetime, timedelta
from rdr_service.clock import FakeClock
from faker import Faker

from rdr_service.model.participant import Participant as RdrParticpant
from rdr_service.model.rex import Study as RexStudy, ParticipantMapping as RexParticipantMapping
from rdr_service.model.study_nph import (
    Participant as NphParticipant,
    StudyCategory,
    Site,
    Order,
    OrderedSample,
    SampleUpdate,
    BiobankFileExport,
    SampleExport,
    StoredSample,
    StoredSampleStatus
)
from rdr_service.dao.rex_dao import RexStudyDao, RexParticipantMappingDao
from rdr_service.dao.participant_dao import ParticipantDao as RdrParticipantDao
from rdr_service.dao.study_nph_dao import (
    NphParticipantDao,
    NphStudyCategoryDao,
    NphSiteDao,
    NphOrderDao,
    NphOrderedSampleDao,
    NphSampleUpdateDao,
    NphBiobankFileExportDao,
    NphSampleExportDao,
    NphStoredSampleDao
)


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class GenFakeParticipant:

    def __init__(self):
        self.nph_participant_dao = NphParticipantDao()

    def create_participant(
        self,
        ignore_flag: int = 0,
        disable_flag: int = 0,
        biobank_id: Optional[int] = None,
        research_id: Optional[int] = 0
    ) -> NphParticipant:
        faker_obj = Faker()
        disable_reason = ''.join(faker_obj.random_letters(length=512))
        nph_participant_params = {
            "ignore_flag": ignore_flag,
            "disable_flag": disable_flag,
            "disable_reason": disable_reason if disable_flag else "",
            "biobank_id": biobank_id or int(getrandbits(32)),
            "research_id": research_id or int(getrandbits(32))
        }
        nph_participant = NphParticipant(**nph_participant_params)
        return self.nph_participant_dao.insert(nph_participant)


class GenFakeStudyCategory:

    def __init__(self):
        self.nph_study_category_dao = NphStudyCategoryDao()
        self.faker = Faker()
        self.study_categories = [
            "module", "visitType", "timepoint"
        ]

    def create_nph_study_category(
        self,
        type_label: str,
        study_category_name: str,
        parent_id: Optional[int] = None,
    ) -> StudyCategory:
        nph_study_category_params = {
            "name": study_category_name,
            "type_label": type_label,
            "parent_id": parent_id,
        }
        nph_study_category = StudyCategory(**nph_study_category_params)
        return self.nph_study_category_dao.insert(nph_study_category)

    def create_nph_study_category_with_a_parent(
        self, parent_type_label: str, child_type_label: str, parent_sc_name: str, child_sc_name: str
    ) -> Tuple[StudyCategory, StudyCategory]:
        parent_study_category = (
            self.create_nph_study_category(type_label=parent_type_label, study_category_name=parent_sc_name)
        )
        child_study_category = self.create_nph_study_category(
            type_label=child_type_label, parent_id=parent_study_category.id, study_category_name=child_sc_name
        )
        return parent_study_category, child_study_category


class GenFakeSite:

    def __init__(self):
        self.nph_site_dao = NphSiteDao()
        self.faker = Faker()

    def create_nph_site(self) -> Site:
        external_id = ''.join(self.faker.random_letters(length=256))
        name = ''.join(self.faker.random_letters(length=512))
        awardee_external_id = ''.join(self.faker.random_letters(length=256))
        nph_site_params = {
            "external_id": external_id,
            "name": name,
            "awardee_external_id": awardee_external_id
        }
        nph_site = Site(**nph_site_params)
        return self.nph_site_dao.insert(nph_site)


class GenFakeOrder:

    def __init__(self):
        self.nph_order_dao = NphOrderDao()
        self.faker = Faker()
        self.start_date = datetime.now() - timedelta(days=2)
        self.end_date = datetime.now()

    def create_nph_order(
        self,
        study_category_id: int,
        participant_id: int,
        created_site_id: int,
        ignore_flag: int = 0,
        collected_site_id: Optional[int] = None,
        amended_site_id: Optional[int] = None,
        finalized_site_id: Optional[int] = None
    ) -> Order:
        nph_order_id = ''.join(self.faker.random_letters(length=64))
        date = self.faker.date_between_dates(self.start_date, self.end_date)
        order_created: datetime = datetime(year=date.year, month=date.month, day=date.day)
        created_author = ''.join(self.faker.random_letters(length=128))
        collected_author = ''.join(self.faker.random_letters(length=128))
        amended_author = ''.join(self.faker.random_letters(length=128))
        finalized_author = ''.join(self.faker.random_letters(length=128))
        amended_reason = ''.join(self.faker.random_letters(length=1024))
        status = ''.join(self.faker.random_letters(length=128))
        random_note = ''.join(self.faker.random_letters(length=128))

        nph_order_params = {
            "nph_order_id": nph_order_id,
            "order_created": order_created,
            "ignore_flag": ignore_flag,
            "category_id": study_category_id,
            "participant_id": participant_id,
            "created_author": created_author,
            "created_site": created_site_id,
            "collected_author": collected_author,
            "collected_site": collected_site_id,
            "finalized_author": finalized_author,
            "finalized_site": finalized_site_id,
            "amended_author": amended_author,
            "amended_site": amended_site_id,
            "amended_reason": amended_reason,
            "status": status,
            "notes": {
                "NOTE": random_note
            }
        }
        nph_order = Order(**nph_order_params)
        order_created_ts = datetime.strptime(order_created.strftime(DATETIME_FORMAT), DATETIME_FORMAT)
        with FakeClock(order_created_ts):
            return self.nph_order_dao.insert(nph_order)


class GenFakeOrderedSample:

    def __init__(self) -> None:
        self.nph_ordered_sample_dao = NphOrderedSampleDao()
        self.faker = Faker()
        self.start_date = datetime.now() - timedelta(days=2)
        self.end_date = datetime.now()

    def _get_volume_units(self, aliquot_identifier_code: str) -> str:
        blood_specimen_codes = {"SSTS1", "LHPSTP1", "P800P1", "EDTAP1"}
        saliva_specimen_codes = {"SA1", "SA2"}
        urine_specimen_codes = {"RU1", "RU2", "RU3", "TU1"}
        if aliquot_identifier_code in blood_specimen_codes:
            return "uL"
        elif (aliquot_identifier_code in saliva_specimen_codes) \
            or (aliquot_identifier_code in urine_specimen_codes):
            return "mL"
        return ""

    def create_nph_ordered_sample(self, order_id: int, parent_sample_id: Optional[int] = None) -> OrderedSample:
        specimen_identifiers = [
            "SSTS1",
            "LHPSTP1",
            "P800P1",
            "EDTAP1",
            "RU1",
            "RU2",
            "RU3",
            "TU1",
            "SA1",
            "SA2",
            "ST1",
            "ST2",
            "ST3",
            "ST4",
            "HA1",
            "NA1",
            "NA2",
        ]
        specimen_identifier = choice(specimen_identifiers)
        volume_units = self._get_volume_units(specimen_identifier)
        nph_sample_id = str(self.faker.random_int(10E5, 10E7))
        test = ''.join(self.faker.random_letters(length=40))
        description = ''.join(self.faker.random_letters(length=256))
        collected_dt = self.faker.date_between_dates(self.start_date, self.end_date)
        finalized_dt = self.faker.date_between_dates(collected_dt, collected_dt + timedelta(days=2))
        collected = datetime(day=collected_dt.day, month=collected_dt.month, year=collected_dt.year)
        finalized = datetime(day=finalized_dt.day, month=finalized_dt.month, year=finalized_dt.year)
        aliquot_id = str(self.faker.random_int(1, 10E5))
        identifier = choice(specimen_identifiers)
        container = ''.join(self.faker.random_letters(length=128))
        volume = ''.join([str(self.faker.random_int(1, 99)), volume_units])
        status = ''.join(self.faker.random_letters(length=128))

        nph_ordered_sample_params = {
            "nph_sample_id": nph_sample_id,
            "order_id": order_id,
            "parent_sample_id": parent_sample_id,
            "test": test,
            "description": description,
            "collected": collected,
            "finalized": finalized,
            "aliquot_id": aliquot_id,
            "identifier": identifier,
            "container": container,
            "volume": volume,
            "status": status,
        }
        nph_ordered_sample = OrderedSample(**nph_ordered_sample_params)
        nph_ordered_sample_dao = NphOrderedSampleDao()
        with nph_ordered_sample_dao.session() as session:
            session.add(nph_ordered_sample)
            session.commit()
            return nph_ordered_sample

    def create_nph_ordered_sample_with_parent_sample_id(self, order_id: int) -> Tuple[OrderedSample, OrderedSample]:
        parent_ordered_sample = self.create_nph_ordered_sample(order_id=order_id)
        child_ordered_sample = (
            self.create_nph_ordered_sample(
                    order_id=order_id,
                    parent_sample_id=parent_ordered_sample.id
                )
        )
        return parent_ordered_sample, child_ordered_sample


class GenFakeSampleUpdate:

    def __init__(self) -> None:
        self.nph_sample_update_dao = NphSampleUpdateDao()

    def create_nph_sample_update(
        self,
        ordered_sample: OrderedSample,
        ignore_flag: Optional[int] = 0
    ) -> SampleUpdate:
        ordered_sample_json = ordered_sample.asdict()
        ordered_sample_json["collected"] = ordered_sample_json["collected"].strftime(DATETIME_FORMAT)
        ordered_sample_json["finalized"] = ordered_sample_json["finalized"].strftime(DATETIME_FORMAT)

        nph_sample_update_params = {
            "ignore_flag": ignore_flag,
            "rdr_ordered_sample_id": ordered_sample.id,
            "ordered_sample_json": ordered_sample_json
        }
        nph_sample_update = SampleUpdate(**nph_sample_update_params)
        return self.nph_sample_update_dao.insert(nph_sample_update)


class GenFakeBiobankFileExport:

    def __init__(self) -> None:
        self.nph_biobank_file_export_dao = NphBiobankFileExportDao()

    def create_nph_biobank_file_export(self, nph_biobank_file_export_params: Dict[str, Any]) -> BiobankFileExport:
        nph_biobank_file_export = BiobankFileExport(**nph_biobank_file_export_params)
        return self.nph_biobank_file_export_dao.insert(nph_biobank_file_export)


class GenFakeSampleExport:

    def __init__(self) -> None:
        self.nph_sample_export_dao = NphSampleExportDao()

    def create_nph_sample_export(self, nph_sample_export_params: Dict[str, Any]) -> SampleExport:
        nph_sample_export = SampleExport(**nph_sample_export_params)
        return self.nph_sample_export_dao.insert(nph_sample_export)


class GenFakeStoredSample:

    def __init__(self) -> None:
        self.nph_stored_sample = NphStoredSampleDao()
        self.faker = Faker()
        self.start_date = datetime.now() - timedelta(days=2)
        self.end_date = datetime.now()

    def create_nph_stored_sample(
        self,
        nph_participant: NphParticipant,
        sample_id: str,
        ignore_flag: Optional[int] = 0,
    ) -> StoredSample:

        date = self.faker.date_between_dates(self.start_date, self.end_date)
        biobank_modified: datetime = datetime(year=date.year, month=date.month, day=date.day)
        lims_id = ''.join(self.faker.random_letters(length=64))
        status = choice([
            StoredSampleStatus.SHIPPED,
            StoredSampleStatus.RECEIVED,
            StoredSampleStatus.DISPOSED,
        ])
        disposition = ''.join(self.faker.random_letters(length=256))

        nph_stored_sample_params = {
            "biobank_modified": biobank_modified,
            "biobank_id": nph_participant.biobank_id,
            "ignore_flag": ignore_flag,
            "sample_id": sample_id,
            "lims_id": lims_id,
            "status": status,
            "disposition": disposition
        }
        nph_stored_sample = StoredSample(**nph_stored_sample_params)
        return self.nph_stored_sample.insert(nph_stored_sample)


def _get_rdr_participants() -> Iterable[RdrParticpant]:
    rdr_participant_dao = RdrParticipantDao()
    with rdr_participant_dao.session() as session:
        return session.query(RdrParticpant).all()


def _create_fake_rex_study(schema_name: str) -> RexStudy:
    fake_ancillary_study_params = {
        "ignore_flag": 0,
        "schema_name": schema_name,
        "prefix": 1E2+5E4
    }
    rex_study_dao = RexStudyDao()
    rex_study = RexStudy(**fake_ancillary_study_params)
    return rex_study_dao.insert(rex_study)


def _create_participant_mapping(
    primary_study_id: int, ancillary_study_id: int, primary_participant_id: int, ancillary_participant_id: int
):
    _time = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)
    rex_participant_mapping_params = {
            "created": _time,
            "modified": _time,
            "ignore_flag": 0,
            "primary_study_id": primary_study_id,
            "ancillary_study_id": ancillary_study_id,
            "primary_participant_id": primary_participant_id,
            "ancillary_participant_id": ancillary_participant_id,
        }
    rex_participant_mapping = RexParticipantMapping(**rex_participant_mapping_params)
    rex_participant_mapping_dao = RexParticipantMappingDao()
    return rex_participant_mapping_dao.insert(rex_participant_mapping)


def generate_fake_participants_and_participant_mappings() \
    -> Tuple[Iterable[RdrParticpant], Iterable[RexParticipantMapping]]:
    fake_primary_study = _create_fake_rex_study(schema_name="primary_rex_study")
    fake_ancillary_study = _create_fake_rex_study(schema_name="ancillary_rex_study")

    gen_fake_participant = GenFakeParticipant()
    participants: Iterable[RdrParticpant] = []
    participant_mappings: Iterable[RexParticipantMapping] = []
    rdr_participants = _get_rdr_participants()
    for rdr_participant in rdr_participants:
        ignore_flag = choice([0, 1])
        disable_flag = choice([0, 1])
        nph_participant = gen_fake_participant.create_participant(
            ignore_flag,
            disable_flag,
            biobank_id=rdr_participant.biobankId,
            research_id=rdr_participant.researchId
        )
        rex_participant_mapping = _create_participant_mapping(
            primary_study_id=fake_primary_study.id,
            ancillary_study_id=fake_ancillary_study.id,
            primary_participant_id=rdr_participant.participantId,
            ancillary_participant_id=nph_participant.id
        )
        participants.append(nph_participant)
        participant_mappings.append(rex_participant_mapping)
    return participants, participant_mappings


def generate_fake_study_categories() -> Iterable[StudyCategory]:
    gen_fake_study_category = GenFakeStudyCategory()
    study_categories: Iterable[StudyCategory] = []
    nph_modules = [
        "NPH Module 1",
        "NPH Module 2",
        "NPH Module 3"
    ]
    visit_ids = ["LMT"]
    timepoint_ids = [
        "Pre LMT",
        "Minus 15 min",
        "Minus 5 min",
        "15 min",
        "30 min",
        "60 min",
        "90 min",
        "120 min",
        "180 min",
        "240 min",
        "Post LMT"
    ]
    for nph_module_name in nph_modules:
        for visit_id in visit_ids:
            nph_module_sc, visit_type_sc = (
                gen_fake_study_category.\
                    create_nph_study_category_with_a_parent("module", "visitType", nph_module_name, visit_id)
            )
            for timepoint_id in timepoint_ids:
                timepoint_sc = (
                    gen_fake_study_category.create_nph_study_category(
                        type_label="timepoint", parent_id=visit_type_sc.id, study_category_name=timepoint_id
                    )
                )
                study_categories.extend([nph_module_sc, visit_type_sc, timepoint_sc])
    return study_categories


def generate_fake_sites() -> Iterable[Site]:
    gen_fake_sites = GenFakeSite()
    sites: Iterable[Site] = []
    for _ in range(10):
        site = gen_fake_sites.create_nph_site()
        sites.append(site)
    return sites


def generate_fake_orders(
    fake_participants: Iterable[NphParticipant],
    fake_study_categories: Iterable[StudyCategory],
    fake_sites: Iterable[Site]
) -> Iterable[Order]:

    gen_fake_orders = GenFakeOrder()
    orders: Iterable[Order] = []
    timepoint_sc = []
    for sc in fake_study_categories:
        if sc.type_label == "timepoint":
            timepoint_sc.append(sc)

    for fake_participant in fake_participants:
        for _ in range(2):
            fake_study_category: StudyCategory = choice(timepoint_sc)
            fake_created_site: Site = choice(fake_sites)
            fake_collected_site: Site = choice(fake_sites)
            fake_amended_site: Site = choice(fake_sites)
            fake_finalized_site: Site = choice(fake_sites)
            order = gen_fake_orders.create_nph_order(
                study_category_id=fake_study_category.id,
                participant_id=fake_participant.id,
                created_site_id=fake_created_site.id,
                collected_site_id=fake_collected_site.id,
                amended_site_id=fake_amended_site.id,
                finalized_site_id=fake_finalized_site.id,
            )
            orders.append(order)
    return orders


def generate_fake_ordered_samples(
    fake_orders: Iterable[Order]
) -> Iterable[OrderedSample]:
    gen_fake_ordered_samples = GenFakeOrderedSample()
    for fake_order in fake_orders:
        for _ in range(3):
            gen_fake_ordered_samples.create_nph_ordered_sample_with_parent_sample_id(
                order_id=fake_order.id
            )
    nph_ordered_sample_dao = NphOrderedSampleDao()
    return list(nph_ordered_sample_dao.get_all())


def generate_fake_sample_updates(
    fake_ordered_samples: Iterable[OrderedSample]
) -> Iterable[SampleUpdate]:
    gen_fake_sample_update = GenFakeSampleUpdate()
    for _, ordered_sample in enumerate(fake_ordered_samples):
        ignore_flag = choice([0, 1])
        gen_fake_sample_update.create_nph_sample_update(
            ordered_sample=ordered_sample, ignore_flag=ignore_flag
        )


def generate_fake_stored_samples(
    fake_participants: Iterable[NphParticipant], grouped_ordered_samples: Dict[int, List[OrderedSample]]
) -> Iterable[StoredSample]:
    stored_samples = []
    gen_fake_stored_sample = GenFakeStoredSample()
    for fake_participant in fake_participants:
        for ordered_sample in grouped_ordered_samples[fake_participant.id]:
            stored_samples.append(
                gen_fake_stored_sample.create_nph_stored_sample(
                    nph_participant=fake_participant, sample_id=ordered_sample.nph_sample_id
                )
            )
    return stored_samples


def main():
    participants, _ = generate_fake_participants_and_participant_mappings()
    study_categories = generate_fake_study_categories()
    sites = generate_fake_sites()
    orders = generate_fake_orders(
        fake_participants=participants,
        fake_study_categories=study_categories,
        fake_sites=sites,
    )
    ordered_samples = generate_fake_ordered_samples(fake_orders=orders)
    _ = generate_fake_sample_updates(fake_ordered_samples=ordered_samples)


if __name__=="__main__":
    main()
