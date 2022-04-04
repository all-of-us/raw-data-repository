
from sqlalchemy import (
    Column, DateTime, ForeignKey, Integer,
    String, SmallInteger, event
)
from rdr_service.model.base import Base, model_insert_listener, model_update_listener


class DatagenBase:
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)


class GenomicDataGenRun(Base, DatagenBase):

    __tablename__ = 'genomic_datagen_run'

    project_name = Column(String(255), nullable=False)


event.listen(GenomicDataGenRun, "before_insert", model_insert_listener)
event.listen(GenomicDataGenRun, "before_update", model_update_listener)


class GenomicDatagenMemeberRun(Base, DatagenBase):

    __tablename__ = 'genomic_datagen_member_run'

    created_run_id = Column(ForeignKey('genomic_datagen_run.id'), nullable=False)
    genomic_set_member_id = Column(ForeignKey('genomic_set_member.id'), nullable=False)
    template_name = Column(String(255), nullable=False)


event.listen(GenomicDatagenMemeberRun, "before_insert", model_insert_listener)
event.listen(GenomicDatagenMemeberRun, "before_update", model_update_listener)


class GenomicDataGenCaseTemplate(Base, DatagenBase):

    __tablename__ = 'genomic_datagen_case_template'

    project_name = Column(String(255), nullable=False)
    template_name = Column(String(255), nullable=False)
    rdr_field = Column(String(255), nullable=False)
    field_source = Column(String(255), nullable=False)
    field_value = Column(String(255), nullable=True)


event.listen(GenomicDataGenCaseTemplate, "before_insert", model_insert_listener)
event.listen(GenomicDataGenCaseTemplate, "before_update", model_update_listener)


class GenomicDataGenOutputTemplate(Base, DatagenBase):

    __tablename__ = 'genomic_datagen_output_template'
    project_name = Column(String(255), nullable=False)
    template_name = Column(String(255), nullable=False)
    field_index = Column(SmallInteger, nullable=False)
    field_name = Column(String(255), nullable=False)
    source_type = Column(String(255), nullable=False)
    source_value = Column(String(255), nullable=False)


event.listen(GenomicDataGenOutputTemplate, "before_insert", model_insert_listener)
event.listen(GenomicDataGenOutputTemplate, "before_update", model_update_listener)
