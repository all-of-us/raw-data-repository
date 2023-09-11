from sqlalchemy import Column, BigInteger, String, event, ForeignKey
from sqlalchemy.dialects.mysql import TINYINT
from rdr_service.model.base import NphBase, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime


class SmsJobRun(NphBase):
    # Job executions for investigation and analytics purposes.
    __tablename__ = "sms_job_run"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(1024))
    job = Column(String(64), default="UNSET", index=True)
    sub_process = Column(String(64), default="UNSET", index=True)
    result = Column(String(64), default="UNSET")


event.listen(SmsJobRun, "before_insert", model_insert_listener)
event.listen(SmsJobRun, "before_update", model_update_listener)


class SmsBlocklist(NphBase):
    # Samples blocked from various processes in the Sample Management System
    # identifier_value is a sample_id, biobank_id, participant_id, etc.
    # identifier_type is an enum for the allowed identifier types to blocklist
    __tablename__ = "sms_blocklist"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(1024))
    identifier_value = Column(BigInteger, index=True)
    identifier_type = Column(String(128), index=True)
    block_category = Column(String(128), index=True)


event.listen(SmsBlocklist, "before_insert", model_insert_listener)
event.listen(SmsBlocklist, "before_update", model_update_listener)


class SmsSample(NphBase):
    # Source of all samples in the sample management system (sms)
    # Sourced from RTI 'pull list'
    __tablename__ = "sms_sample"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(1024))
    file_path = Column(String(255), index=True)
    job_run_id = Column(BigInteger, ForeignKey("sms_job_run.id"))

    # File Fields
    sample_id = Column(BigInteger, index=True)
    lims_sample_id = Column(String(32), index=True)
    plate_number = Column(String(32))
    position = Column(String(16))
    labware_type = Column(String(32))
    sample_identifier = Column(String(32))
    diet = Column(String(32))
    sex_at_birth = Column(String(32))
    bmi = Column(String(4))
    age = Column(String(4))
    race = Column(String(1024))
    ethnicity = Column(String(1024))
    destination = Column(String(64), index=True)


event.listen(SmsSample, "before_insert", model_insert_listener)
event.listen(SmsSample, "before_update", model_update_listener)


class SmsN0(NphBase):
    # Records from N0 manifest. Sourced from Biobank
    __tablename__ = "sms_n0"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(1024))
    file_path = Column(String(255), index=True)
    job_run_id = Column(BigInteger, ForeignKey("sms_job_run.id"))

    # File Fields
    lims_sample_id = Column(String(32))
    matrix_id = Column(String(32))
    biobank_id = Column(String(32))
    sample_id = Column(BigInteger, index=True)
    study = Column(String(64))
    visit = Column(String(64))
    timepoint = Column(String(64))
    collection_site = Column(String(512))
    collection_date_time = Column(UTCDateTime)
    sample_type = Column(String(32))
    additive_treatment = Column(String(16))
    quantity_ml = Column(String(16))
    manufacturer_lot = Column(String(32))
    well_box_position = Column(String(32), index=True)
    storage_unit_id = Column(String(32))
    package_id = Column(String(32), index=True)
    tracking_number = Column(String(32))
    shipment_storage_temperature = Column(String(16))
    sample_comments = Column(String(1024))
    age = Column(String(4))


event.listen(SmsN0, "before_insert", model_insert_listener)
event.listen(SmsN0, "before_update", model_update_listener)


class SmsN1Mc1(NphBase):
    # Records from N1_MC1 manifest. Sourced from RDR
    __tablename__ = "sms_n1_mc1"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(1024))
    file_path = Column(String(255), index=True)
    job_run_id = Column(BigInteger, ForeignKey("sms_job_run.id"))

    # Manifest Fields
    sample_id = Column(BigInteger, index=True)
    matrix_id = Column(String(32))
    biobank_id = Column(String(32))
    sample_identifier = Column(String(32))
    study = Column(String(64))
    visit = Column(String(64))
    timepoint = Column(String(64))
    collection_site = Column(String(512))
    collection_date_time = Column(UTCDateTime)
    sample_type = Column(String(32))
    additive_treatment = Column(String(16))
    quantity_ml = Column(String(16))
    age = Column(String(4))
    sex_at_birth = Column(String(16))
    package_id = Column(String(32), index=True)
    storage_unit_id = Column(String(32))
    well_box_position = Column(String(32), index=True)
    destination = Column(String(128))
    tracking_number = Column(String(32))
    manufacturer_lot = Column(String(32))
    sample_comments = Column(String(1024))
    ethnicity = Column(String(1024))
    race = Column(String(1024))
    bmi = Column(String(4))
    diet = Column(String(32))
    urine_color = Column(String(1024))
    urine_clarity = Column(String(1024))
    bowel_movement = Column(String(1024))
    bowel_movement_quality = Column(String(1024))


event.listen(SmsN1Mc1, "before_insert", model_insert_listener)
event.listen(SmsN1Mc1, "before_update", model_update_listener)
