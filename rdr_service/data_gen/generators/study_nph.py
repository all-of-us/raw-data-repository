from typing import Dict, Any, Tuple, Optional, Iterable
from random import getrandbits, choice
from datetime import datetime, timedelta
from rdr_service.clock import FakeClock
from faker import Faker

from rdr_service.model.study_nph import (
    Participant,
    StudyCategory,
    Site,
    Order,
    OrderedSample,
    SampleUpdate,
    BiobankFileExport,
    SampleExport
)
from rdr_service.dao.study_nph_dao import (
    NphParticipantDao,
    NphStudyCategoryDao,
    NphSiteDao,
    NphOrderDao,
    NphOrderedSampleDao,
    NphSampleUpdateDao,
    NphBiobankFileExportDao,
    NphSampleExportDao
)


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class GenFakeParticipant:

    def __init__(self):
        self.nph_participant_dao = NphParticipantDao()

    def create_participant(self, ignore_flag: int = 0, disable_flag: int = 0) -> Participant:
        faker_obj = Faker()
        disable_reason = ''.join(faker_obj.random_letters(length=512))
        nph_participant_params = {
            "ignore_flag": ignore_flag,
            "disable_flag": disable_flag,
            "disable_reason": disable_reason if disable_flag else "",
            "biobank_id": int(getrandbits(32)),
            "research_id": int(getrandbits(32))
        }
        nph_participant = Participant(**nph_participant_params)
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
        parent_id: Optional[int] = None,
        nph_module_number: Optional[int] = None
    ) -> StudyCategory:
        # type_label = choice(self.study_categories)
        if type_label == "module":
            if not isinstance(nph_module_number, int):
                raise ValueError(f"Need an integer value for 'nph_module_number'")
            study_category_name = f"NPH Module {nph_module_number}"
        else:
            study_category_name = ''.join(self.faker.random_letters(length=128))
        nph_study_category_params = {
            "name": study_category_name,
            "type_label": type_label,
            "parent_id": parent_id,
        }
        nph_study_category = StudyCategory(**nph_study_category_params)
        return self.nph_study_category_dao.insert(nph_study_category)

    def create_nph_study_category_with_a_parent(
        self, parent_type_label: str, child_type_label: str, nph_module_number: Optional[int] = None
    ) -> Tuple[StudyCategory, StudyCategory]:
        if nph_module_number is None:
            parent_study_category = self.create_nph_study_category(type_label=parent_type_label)
            child_study_category = self.create_nph_study_category(
                type_label=child_type_label, parent_id=parent_study_category.id
            )
        else:
            parent_study_category = self.create_nph_study_category(
                type_label=parent_type_label, nph_module_number=nph_module_number
            )
            child_study_category = self.create_nph_study_category(
                type_label=child_type_label, parent_id=parent_study_category.id
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

    def create_nph_ordered_sample(self, order_id: int, parent_sample_id: Optional[int] = None) -> OrderedSample:
        nph_sample_id = ''.join(self.faker.random_letters(length=64))
        test = ''.join(self.faker.random_letters(length=40))
        description = ''.join(self.faker.random_letters(length=256))
        collected_dt = self.faker.date_between_dates(self.start_date, self.end_date)
        finalized_dt = self.faker.date_between_dates(collected_dt, collected_dt + timedelta(days=2))
        collected = datetime(day=collected_dt.day, month=collected_dt.month, year=collected_dt.year)
        finalized = datetime(day=finalized_dt.day, month=finalized_dt.month, year=finalized_dt.year)
        aliquot_id = ''.join(self.faker.random_letters(length=128))
        identifier = ''.join(self.faker.random_letters(length=128))
        container = ''.join(self.faker.random_letters(length=128))
        volume = ''.join(self.faker.random_letters(length=128))
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
        return self.nph_ordered_sample_dao.insert(nph_ordered_sample)

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


def generate_fake_participants() -> Iterable[Participant]:
    gen_fake_participant = GenFakeParticipant()
    participants: Iterable[Participant] = []
    for _ in range(10):
        ignore_flag = choice([0, 1])
        disable_flag = choice([0, 1])
        participant = gen_fake_participant.create_participant(ignore_flag, disable_flag)
        participants.append(participant)
    return participants


def generate_fake_study_categories() -> Iterable[StudyCategory]:
    gen_fake_study_category = GenFakeStudyCategory()
    study_categories: Iterable[StudyCategory] = []
    for nph_module_number in range(10):
        nph_module, visit_type = (
            gen_fake_study_category.create_nph_study_category_with_a_parent(
                "module", "visitType", nph_module_number
            )
        )
        timepoint = gen_fake_study_category.create_nph_study_category(type_label="timepoint", parent_id=visit_type.id)
        study_categories.extend([nph_module, visit_type, timepoint])
    return study_categories


def generate_fake_sites() -> Iterable[Site]:
    gen_fake_sites = GenFakeSite()
    sites: Iterable[Site] = []
    for _ in range(10):
        site = gen_fake_sites.create_nph_site()
        sites.append(site)
    return sites


def generate_fake_orders(
    fake_participants: Iterable[Participant],
    fake_study_categories: Iterable[StudyCategory],
    fake_sites: Iterable[Site]
) -> Iterable[Order]:

    gen_fake_orders = GenFakeOrder()
    orders: Iterable[Order] = []
    timepoint_sc = []
    for sc in fake_study_categories:
        if sc.type_label == "timepoint":
            timepoint_sc.append(sc)
    for _ in range(10):
        fake_participant: Participant = choice(fake_participants)
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
    ordered_samples: Iterable[OrderedSample] = []
    for _ in range(10):
        fake_order: Order = choice(fake_orders)
        pos, cos = (
            gen_fake_ordered_samples.create_nph_ordered_sample_with_parent_sample_id(
                order_id=fake_order.id
            )
        )
        ordered_samples.extend([pos, cos])
    return ordered_samples


def generate_fake_sample_updates(
    fake_ordered_samples: Iterable[OrderedSample]
) -> Iterable[SampleUpdate]:
    gen_fake_sample_update = GenFakeSampleUpdate()
    for _ in range(10):
        ordered_sample = choice(fake_ordered_samples)
        ignore_flag = choice([0, 1])
        gen_fake_sample_update.create_nph_sample_update(
            ordered_sample=ordered_sample, ignore_flag=ignore_flag
        )


def main():
    participants = generate_fake_participants()
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
