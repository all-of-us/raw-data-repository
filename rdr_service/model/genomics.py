from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, UniqueConstraint, event
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, MultiEnum
from rdr_service.participant_enums import (
    GenomicSetStatus,
    GenomicSetMemberStatus,
    GenomicValidationFlag,
    GenomicSubProcessStatus,
    GenomicSubProcessResult,
    GenomicJob
)


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

    consentForRor = Column('consent_for_ror', String(10), nullable=True)
    withdrawnStatus = Column('withdrawn_status', Integer, nullable=True)

    sequencingFileName = Column('sequencing_file_name',
                                String(128), nullable=True)

    # Reconciliation and Manifest columns
    reconcileManifestJobRunId = Column('reconcile_manifest_job_run_id',
                                       Integer, ForeignKey("genomic_job_run.id"),
                                       nullable=True)
    reconcileSequencingJobRunId = Column('reconcile_sequencing_job_run_id',
                                         Integer, ForeignKey("genomic_job_run.id"),
                                         nullable=True)
    reconcileCvlJobRunId = Column('reconcile_cvl_job_run_id',
                                  Integer, ForeignKey("genomic_job_run.id"),
                                  nullable=True)
    CvlManifestWgsJobRunId = Column('cvl_manifest_wgs_job_run_id',
                                    Integer, ForeignKey("genomic_job_run.id"),
                                    nullable=True)
    CvlManifestArrJobRunId = Column('cvl_manifest_arr_job_run_id',
                                    Integer, ForeignKey("genomic_job_run.id"),
                                    nullable=True)


event.listen(GenomicSetMember, "before_insert", model_insert_listener)
event.listen(GenomicSetMember, "before_update", model_update_listener)


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

    jobId = Column('job_id', Enum(GenomicJob),
                   default=GenomicJob.UNSET, nullable=False)
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
    genomicSetMemberId = Column('genomic_set_member_id',
                                ForeignKey('genomic_set_member.id'),
                                nullable=True)
    genomicFileProcessedId = Column('genomic_file_processed_id',
                                    ForeignKey('genomic_file_processed.id'))
    # Auto-Timestamps
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    biobankId = Column('biobank_id', String(80), nullable=False)
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
    siteId = Column('site_id', Integer, nullable=True)


event.listen(GenomicGCValidationMetrics, 'before_insert', model_insert_listener)
event.listen(GenomicGCValidationMetrics, 'before_update', model_update_listener)
