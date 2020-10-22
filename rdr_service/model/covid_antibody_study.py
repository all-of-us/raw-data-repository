from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint, event
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime6


class BiobankCovidAntibodySample(Base):
    __tablename__ = "biobank_covid_antibody_sample"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)

    aouBiobankId = Column("aou_biobank_id", Integer, ForeignKey("participant.biobank_id"), nullable=True)
    noAouBiobankId = Column("no_aou_biobank_id", String(80), nullable=True)
    sampleId = Column("sample_id", String(80), nullable=False)
    matrixTubeId = Column("matrix_tube_id", Integer)
    sampleType = Column("sample_type", String(80))
    quantityUl = Column("quantity_ul", Integer)
    storageLocation = Column("storage_location", String(200))
    collectionDate = Column("collection_date", UTCDateTime6)
    ingestFileName = Column("ingest_file_name", String(80))
    __table_args__ = (UniqueConstraint("sample_id"),)


class QuestCovidAntibodyTest(Base):
    __tablename__ = "quest_covid_antibody_test"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)

    specimenId = Column("specimen_id", String(80))
    testCode = Column("test_code", Integer)
    testName = Column("test_name", String(200))
    runDateTime = Column("run_date_time", UTCDateTime6)
    accession = Column("accession", String(80), nullable=False)
    instrumentName = Column("instrument_name", String(200))
    position = Column("position", String(80))
    batch = Column("batch", String(80))
    ingestFileName = Column("ingest_file_name", String(80))
    __table_args__ = (UniqueConstraint("accession", "batch"),)


class QuestCovidAntibodyTestResult(Base):
    __tablename__ = "quest_covid_antibody_test_result"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)

    accession = Column("accession", String(80), nullable=False)
    resultName = Column("result_name", String(200))
    resultValue = Column("result_value", String(2000))
    batch = Column("batch", String(80))
    ingestFileName = Column("ingest_file_name", String(80))
    __table_args__ = (UniqueConstraint("accession", "result_name", "batch"),)


event.listen(BiobankCovidAntibodySample, "before_insert", model_insert_listener)
event.listen(BiobankCovidAntibodySample, "before_update", model_update_listener)
event.listen(QuestCovidAntibodyTest, "before_insert", model_insert_listener)
event.listen(QuestCovidAntibodyTest, "before_update", model_update_listener)
event.listen(QuestCovidAntibodyTestResult, "before_insert", model_insert_listener)
event.listen(QuestCovidAntibodyTestResult, "before_update", model_update_listener)
