from protorpc import messages
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, UniqueConstraint, event
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, MultiEnum


class GenomicSetStatus(messages.Enum):
    """Status of Genomic Set"""

    UNSET = 0
    VALID = 1
    INVALID = 2


class GenomicValidationStatus(messages.Enum):
    """Original Specification needed by older database migrations"""

    UNSET = 0
    VALID = 1
    INVALID_BIOBANK_ORDER = 2
    INVALID_NY_ZIPCODE = 3
    INVALID_SEX_AT_BIRTH = 4
    INVALID_GENOME_TYPE = 5
    INVALID_CONSENT = 6
    INVALID_WITHDRAW_STATUS = 7
    INVALID_AGE = 8
    INVALID_DUP_PARTICIPANT = 9


class GenomicSetMemberStatus(messages.Enum):
    """Status of Genomic Set Member"""

    UNSET = 0
    VALID = 1
    INVALID = 2


class GenomicValidationFlag(messages.Enum):
    """Validation Status Flags"""

    UNSET = 0
    # VALID = 1
    INVALID_BIOBANK_ORDER = 2
    INVALID_NY_ZIPCODE = 3
    INVALID_SEX_AT_BIRTH = 4
    INVALID_GENOME_TYPE = 5
    INVALID_CONSENT = 6
    INVALID_WITHDRAW_STATUS = 7
    INVALID_AGE = 8
    INVALID_DUP_PARTICIPANT = 9


class GenomicSubProcessStatus(messages.Enum):
    """The status of a Genomics Sub-Process"""
    QUEUED = 0
    COMPLETED = 1
    RUNNING = 2
    ABORTED = 3


class GenomicSubProcessResult(messages.Enum):
    """The result codes for a particular run of a sub-process"""
    UNSET = 0
    SUCCESS = 1
    NO_FILES = 2
    INVALID_FILE_NAME = 3
    INVALID_FILE_STRUCTURE = 4
    ERROR = 5


class GenomicSet(Base):
    """
  Genomic Set model
  """

    __tablename__ = "genomic_set"

    genomicSetMember = relationship("GenomicSetMember", cascade="all, delete-orphan")

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)

    genomicSetName = Column("genomic_set_name", String(80), nullable=False)
    genomicSetCriteria = Column("genomic_set_criteria", String(80), nullable=False)
    genomicSetVersion = Column("genomic_set_version", Integer, nullable=False)
    # genomic set file
    genomicSetFile = Column("genomic_set_file", String(250), nullable=True)
    # genomic set file timestamp
    genomicSetFileTime = Column("genomic_set_file_time", DateTime, nullable=True)

    genomicSetStatus = Column("genomic_set_status", Enum(GenomicSetStatus), default=GenomicSetStatus.UNSET)
    validatedTime = Column("validated_time", DateTime, nullable=True)

    __table_args__ = (UniqueConstraint("genomic_set_name", "genomic_set_version", name="uidx_genomic_name_version"),)


event.listen(GenomicSet, "before_insert", model_insert_listener)
event.listen(GenomicSet, "before_update", model_update_listener)


class GenomicSetMember(Base):
    """
  Genomic Set Member model
  """

    __tablename__ = "genomic_set_member"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)

    genomicSetId = Column("genomic_set_id", Integer, ForeignKey("genomic_set.id"), nullable=False)

    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"), nullable=False)
    nyFlag = Column("ny_flag", Integer, nullable=True)

    sexAtBirth = Column("sex_at_birth", String(20), nullable=True)
    genomeType = Column("genome_type", String(80), nullable=True)

    biobankOrderId = Column(
        "biobank_order_id", String(80), ForeignKey("biobank_order.biobank_order_id"), unique=False, nullable=True
    )

    biobankId = Column("biobank_id", String(80), nullable=True)

    biobankOrderClientId = Column("biobank_order_client_Id", String(80), nullable=True)

    packageId = Column("package_id", String(250), nullable=True)

    validationStatus = Column("validation_status", Enum(GenomicSetMemberStatus), default=GenomicSetStatus.UNSET)
    validationFlags = Column("validation_flags", MultiEnum(GenomicValidationFlag), nullable=True)

    validatedTime = Column("validated_time", DateTime, nullable=True)

    sampleId = Column('sample_id', String(80), nullable=True)
    sampleType = Column('sample_type', String(50), nullable=True)


event.listen(GenomicSetMember, "before_insert", model_insert_listener)
event.listen(GenomicSetMember, "before_update", model_update_listener)


class GenomicJob(Base):
    """Genomic Job model.
    The Genomics system includes several workflows that involve data transfer.
    This model represents a genomics data transfer job."""
    __tablename__ = 'genomic_job'

    # Primary Key
    id = Column('id', Integer,
                primary_key=True,
                autoincrement=True,
                nullable=False)

    # Auto-Timestamps
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    name = Column('name', String(80), nullable=False)
    activeFlag = Column('active_flag', Integer, nullable=False)


event.listen(GenomicJob, 'before_insert', model_insert_listener)
event.listen(GenomicJob, 'before_update', model_update_listener)


class GenomicJobRun(Base):
    """Genomic Job Run model.
    This model represents a 'run' of a genomics job,
    And tracks the results of a run."""
    __tablename__ = 'genomic_job_run'

    # Primary Key
    id = Column('id', Integer,
              primary_key=True,
              autoincrement=True,
              nullable=False)

    jobId = Column('job_id', Integer,
                   ForeignKey('genomic_job.id'), nullable=False)
    startTime = Column('start_time', DateTime, nullable=False)
    endTime = Column('end_time', DateTime, nullable=True)
    runStatus = Column('run_status',
                     Enum(GenomicSubProcessStatus),
                     default=GenomicSubProcessStatus.RUNNING)
    runResult = Column('run_result',
                     Enum(GenomicSubProcessResult),
                     default=GenomicSubProcessResult.UNSET)
    resultMessage = Column('result_message', String(150), nullable=True)


class GenomicFileProcessed(Base):
    """Genomic File Processed model.
    This model represents the file(s) processed during a genomics run."""
    __tablename__ = 'genomic_file_processed'

    # Primary Key
    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)

    runId = Column('run_id', Integer,
                   ForeignKey('genomic_job_run.id'), nullable=False)
    startTime = Column('start_time', DateTime, nullable=False)
    endTime = Column('end_time', DateTime, nullable=True)
    filePath = Column('file_path', String(255), nullable=False)
    bucketName = Column('bucket_name', String(128), nullable=False)
    fileName = Column('file_name', String(128), nullable=False)
    fileStatus = Column('file_status',
                        Enum(GenomicSubProcessStatus),
                        default=GenomicSubProcessStatus.QUEUED)
    fileResult = Column('file_result',
                        Enum(GenomicSubProcessResult),
                        default=GenomicSubProcessResult.UNSET)


class GenomicGCValidationMetrics(Base):
    """Genomic Sequencing Metrics model.
    This is the data ingested from
    Genome Centers' validation result metrics files."""
    __tablename__ = 'genomic_gc_validation_metrics'

    # Primary Key
    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    genomicSetMemberId = Column('genomic_set_member_id', Integer,
                                nullable=True)
    genomicFileProcessedId = Column('genomic_file_processed_id',
                                    ForeignKey('genomic_file_processed.id'))
    # Auto-Timestamps
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    # TODO: This should be removed since in genomic_set_member,
    #  but that table's pid can't be trusted yet
    participantId = Column('participant_id', Integer,
                           ForeignKey('participant.participant_id'),
                           nullable=False)
    # Ingested Data
    sampleId = Column('sample_id', String(80), nullable=True)
    limsId = Column('lims_id', String(80), nullable=True)
    callRate = Column('call_rate', Integer, nullable=True)
    meanCoverage = Column('mean_coverage', Integer, nullable=True)
    genomeCoverage = Column('genome_coverage', Integer, nullable=True)
    contamination = Column('contamination', Integer, nullable=True)
    sexConcordance = Column('sex_concordance', String(10), nullable=True)
    alignedQ20Bases = Column('aligned_q20_bases', Integer, nullable=True)
    processingStatus = Column('processing_status', String(15), nullable=True)
    notes = Column('notes', String(128), nullable=True)
    consentForRor = Column('consent_for_ror', String(10), nullable=True)
    withdrawnStatus = Column('withdrawn_status', Integer, nullable=True)
    siteId = Column('site_id', Integer, nullable=True)


event.listen(GenomicGCValidationMetrics, 'before_insert', model_insert_listener)
event.listen(GenomicGCValidationMetrics, 'before_update', model_update_listener)
