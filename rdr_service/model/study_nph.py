from sqlalchemy import (
    Column, Integer, BigInteger, String, ForeignKey, Index, event
)
from sqlalchemy.dialects.mysql import TINYINT, JSON

from rdr_service.model.base import NphBase, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime


class Participant(NphBase):
    __tablename__ = "participant"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT)
    disable_flag = Column(TINYINT)
    disable_reason = Column(String(1024))
    biobank_id = Column(BigInteger)
    research_id = Column(BigInteger)


event.listen(Participant, "before_insert", model_insert_listener)
event.listen(Participant, "before_update", model_update_listener)


class StudyCategory(NphBase):
    __tablename__ = "study_category"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    name = Column(String(128))
    type_label = Column(String(128))
    parent_id = Column(BigInteger, ForeignKey("study_category.id"))

event.listen(StudyCategory, "before_insert", model_insert_listener)


class Site(NphBase):
    __tablename__ = "site"
    id = Column("id", Integer, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    external_id = Column(String(256))
    name = Column(String(512))
    awardee_external_id = Column(String(256))


event.listen(Site, "before_insert", model_insert_listener)
event.listen(Site, "before_update", model_update_listener)


class Order(NphBase):
    __tablename__ = "order"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    nph_order_id = Column(String(64))
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    order_created = Column(UTCDateTime)
    category_id = Column(BigInteger, ForeignKey("study_category.id"))
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    created_author = Column(String(128))
    created_site = Column(Integer, ForeignKey("site.id"))
    collected_author = Column(String(128))
    collected_site = Column(Integer, ForeignKey("site.id"))
    finalized_author = Column(String(128))
    finalized_site = Column(Integer, ForeignKey("site.id"))
    amended_author = Column(String(128))
    amended_site = Column(Integer, ForeignKey("site.id"))
    amended_reason = Column(String(1024))
    notes = Column(JSON, nullable=False)
    status = Column(String(128))


Index("order_participant_id", Order.participant_id)
Index("order_created_site", Order.created_site)
Index("order_collected_site", Order.collected_site)
Index("order_finalized_site", Order.finalized_site)
Index("order_amended_site", Order.amended_site)

event.listen(Order, "before_insert", model_insert_listener)
event.listen(Order, "before_update", model_update_listener)


class OrderedSample(NphBase):
    __tablename__ = "ordered_sample"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    nph_sample_id = Column(String(64))
    order_id = Column(BigInteger, ForeignKey("order.id"))
    parent_sample_id = Column(BigInteger, ForeignKey("ordered_sample.id"))
    test = Column(String(40))
    description = Column(String(256))
    collected = Column(UTCDateTime)
    finalized = Column(UTCDateTime)
    aliquot_id = Column(String(128))
    container = Column(String(128))
    status = Column(String(128))
    supplemental_fields = Column(JSON, nullable=True)


class SampleUpdate(NphBase):
    __tablename__ = "sample_update"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    rdr_ordered_sample_id = Column(BigInteger, ForeignKey("ordered_sample.id"))


event.listen(SampleUpdate, "before_insert", model_insert_listener)


class BiobankFileExport(NphBase):
    __tablename__ = "biobank_file_export"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    file_name = Column(String(256))


event.listen(BiobankFileExport, "before_insert", model_insert_listener)


class SampleExport(NphBase):
    __tablename__ = "sample_export"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    export_id = Column(BigInteger, ForeignKey("biobank_file_export.id"))
    sample_update_id = Column(BigInteger, ForeignKey("sample_update.id"))
