from sqlalchemy import (
    Column, DateTime, ForeignKey, Integer,
    String, SmallInteger, UniqueConstraint, event
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import JSON

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, MultiEnum
from rdr_service.participant_enums import (
    GenomicSetStatus,
    GenomicSetMemberStatus,
    GenomicValidationFlag,
    GenomicSubProcessStatus,
    GenomicSubProcessResult,
    GenomicJob,
    GenomicWorkflowState
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

    participantId = Column("participant_id", Integer, nullable=True)
    nyFlag = Column("ny_flag", Integer, nullable=True)

    sexAtBirth = Column("sex_at_birth", String(20), nullable=True)
    genomeType = Column("genome_type", String(80), nullable=True)

    # American Indian or Alaskan Native
    ai_an = Column('ai_an', String(2), nullable=True)

    biobankOrderId = Column(
        "biobank_order_id", String(80), ForeignKey("biobank_order.biobank_order_id"), unique=False, nullable=True
    )

    biobankId = Column("biobank_id", String(80), nullable=True)

    biobankOrderClientId = Column("biobank_order_client_Id", String(80), nullable=True)

    packageId = Column("package_id", String(250), nullable=True)

    validationStatus = Column("validation_status", Enum(GenomicSetMemberStatus), default=GenomicSetStatus.UNSET)
    validationFlags = Column("validation_flags", MultiEnum(GenomicValidationFlag), nullable=True)

    validatedTime = Column("validated_time", DateTime, nullable=True)

    # collectionTubeId corresponds to biobank_stored_sample_id
    collectionTubeId = Column('collection_tube_id', String(80), nullable=True)

    # sampleId is the great-grandchild aliquot of collectionTubeID
    sampleId = Column('sample_id', String(80), nullable=True)
    sampleType = Column('sample_type', String(50), nullable=True)

    sequencingFileName = Column('sequencing_file_name',
                                String(128), nullable=True)

    gcSiteId = Column('gc_site_id', String(11), nullable=True)

    # BBGC Manifest Columns; ingested from GC manifest
    gcManifestBoxStorageUnitId = Column('gc_manifest_box_storage_unit_id', String(50), nullable=True)
    gcManifestBoxPlateId = Column('gc_manifest_box_plate_id', String(50), nullable=True)
    gcManifestWellPosition = Column('gc_manifest_well_position', String(10), nullable=True)
    gcManifestParentSampleId = Column('gc_manifest_parent_sample_id', String(20), nullable=True)
    gcManifestMatrixId = Column('gc_manifest_matrix_id', String(20), nullable=True)
    gcManifestTreatments = Column('gc_manifest_treatments', String(20), nullable=True)
    gcManifestQuantity_ul = Column('gc_manifest_quantity_ul', Integer, nullable=True)
    gcManifestTotalConcentration_ng_per_ul = Column('gc_manifest_total_concentration_ng_per_ul', Integer, nullable=True)
    gcManifestTotalDNA_ng = Column('gc_manifest_total_dna_ng', Integer, nullable=True)
    gcManifestVisitDescription = Column('gc_manifest_visit_description', String(128), nullable=True)
    gcManifestSampleSource = Column('gc_manifest_sample_source', String(20), nullable=True)
    gcManifestStudy = Column('gc_manifest_study', String(50), nullable=True)
    gcManifestTrackingNumber = Column('gc_manifest_tracking_number', String(50), nullable=True)
    gcManifestContact = Column('gc_manifest_contact', String(50), nullable=True)
    gcManifestEmail = Column('gc_manifest_email', String(50), nullable=True)
    gcManifestStudyPI = Column('gc_manifest_study_pi', String(50), nullable=True)
    gcManifestTestName = Column('gc_manifest_test_name', String(50), nullable=True)
    gcManifestFailureMode = Column('gc_manifest_failure_mode', String(128), nullable=True)
    gcManifestFailureDescription = Column('gc_manifest_failure_description', String(128), nullable=True)

    # Reconciliation and Manifest columns
    # Reconciled to BB Manifest
    reconcileMetricsBBManifestJobRunId = Column('reconcile_metrics_bb_manifest_job_run_id',
                                                Integer, ForeignKey("genomic_job_run.id"),
                                                nullable=True)
    # Reconciled to GC manifest
    reconcileGCManifestJobRunId = Column('reconcile_gc_manifest_job_run_id',
                                         Integer, ForeignKey("genomic_job_run.id"),
                                         nullable=True)
    reconcileMetricsSequencingJobRunId = Column('reconcile_metrics_sequencing_job_run_id',
                                                Integer, ForeignKey("genomic_job_run.id"),
                                                nullable=True)
    reconcileCvlJobRunId = Column('reconcile_cvl_job_run_id',
                                  Integer, ForeignKey("genomic_job_run.id"),
                                  nullable=True)

    gemA1ManifestJobRunId = Column('gem_a1_manifest_job_run_id',
                                    Integer, ForeignKey("genomic_job_run.id"),
                                    nullable=True)
    gemA2ManifestJobRunId = Column('gem_a2_manifest_job_run_id',
                                    Integer, ForeignKey("genomic_job_run.id"),
                                    nullable=True)
    gemPass = Column('gem_pass', String(10), nullable=True)

    gemDateOfImport = Column("gem_date_of_import", DateTime, nullable=True)

    gemA3ManifestJobRunId = Column('gem_a3_manifest_job_run_id',
                                   Integer, ForeignKey("genomic_job_run.id"),
                                   nullable=True)

    aw3ManifestJobRunID = Column('aw3_manifest_job_run_id',
                                 Integer, ForeignKey("genomic_job_run.id"),
                                 nullable=True)

    aw4ManifestJobRunID = Column('aw4_manifest_job_run_id',
                                 Integer, ForeignKey("genomic_job_run.id"),
                                 nullable=True)

    # CVL WGS Fields
    cvlW1ManifestJobRunId = Column('cvl_w1_manifest_job_run_id',
                                   Integer, ForeignKey("genomic_job_run.id"),
                                   nullable=True)

    cvlW2ManifestJobRunID = Column('cvl_w2_manifest_job_run_id',
                                   Integer, ForeignKey("genomic_job_run.id"),
                                   nullable=True)

    cvlW3ManifestJobRunID = Column('cvl_w3_manifest_job_run_id',
                                   Integer, ForeignKey("genomic_job_run.id"),
                                   nullable=True)

    cvlW4ManifestJobRunID = Column('cvl_w4_manifest_job_run_id',
                                   Integer, ForeignKey("genomic_job_run.id"),
                                   nullable=True)

    cvlW4FManifestJobRunID = Column('cvl_w4f_manifest_job_run_id',
                                    Integer, ForeignKey("genomic_job_run.id"),
                                    nullable=True)

    cvlAW1CManifestJobRunID = Column('cvl_aw1c_manifest_job_run_id',
                                     Integer, ForeignKey("genomic_job_run.id"),
                                     nullable=True)

    cvlAW1CFManifestJobRunID = Column('cvl_aw1cf_manifest_job_run_id',
                                      Integer, ForeignKey("genomic_job_run.id"),
                                      nullable=True)

    # Genomic State Fields
    genomicWorkflowState = Column('genomic_workflow_state',
                                  Enum(GenomicWorkflowState),
                                  default=GenomicWorkflowState.UNSET)

    genomicWorkflowStateHistory = Column("genomic_workflow_state_history", JSON, nullable=True)


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

    # Ingested Data
    limsId = Column('lims_id', String(80), nullable=True)
    chipwellbarcode = Column('chipwellbarcode', String(80), nullable=True)
    callRate = Column('call_rate', String(10), nullable=True)
    meanCoverage = Column('mean_coverage', String(10), nullable=True)
    genomeCoverage = Column('genome_coverage', String(10), nullable=True)
    aouHdrCoverage = Column('aou_hdr_coverage', String(10), nullable=True)
    contamination = Column('contamination', String(10), nullable=True)
    sexConcordance = Column('sex_concordance', String(10), nullable=True)
    sexPloidy = Column('sex_ploidy', String(10), nullable=True)
    alignedQ30Bases = Column('aligned_q30_bases', Integer, nullable=True)
    arrayConcordance = Column('array_concordance', String(10), nullable=True)
    processingStatus = Column('processing_status', String(15), nullable=True)
    notes = Column('notes', String(128), nullable=True)
    siteId = Column('site_id', String(80), nullable=True)

    # Genotyping Data (Array) reconciliation
    idatRedReceived = Column('idat_red_received', SmallInteger, nullable=False, default=0)
    idatRedPath = Column('idat_red_path', String(255), nullable=True)

    idatGreenReceived = Column('idat_green_received', SmallInteger, nullable=False, default=0)
    idatGreenPath = Column('idat_green_path', String(255), nullable=True)

    idatRedMd5Received = Column('idat_red_md5_received', SmallInteger, nullable=False, default=0)
    idatRedMd5Path = Column('idat_red_md5_path', String(255), nullable=True)

    idatGreenMd5Received = Column('idat_green_md5_received', SmallInteger, nullable=False, default=0)
    idatGreenMd5Path = Column('idat_green_md5_path', String(255), nullable=True)

    vcfReceived = Column('vcf_received', SmallInteger, nullable=False, default=0)
    vcfPath = Column('vcf_path', String(255), nullable=True)

    vcfMd5Received = Column('vcf_md5_received', SmallInteger, nullable=False, default=0)
    vcfMd5Path = Column('vcf_md5_path', String(255), nullable=True)

    vcfTbiReceived = Column('vcf_tbi_received', SmallInteger, nullable=False, default=0)
    vcfTbiPath = Column('vcf_tbi_path', String(255), nullable=True)

    # Sequencing Data (WGS) reconciliation
    # Single sample VCF: Hard - filtered for clinical purpose
    hfVcfReceived = Column('hf_vcf_received', SmallInteger, nullable=False, default=0)
    hfVcfPath = Column('hf_vcf_path', String(255), nullable=True)

    hfVcfTbiReceived = Column('hf_vcf_tbi_received', SmallInteger, nullable=False, default=0)
    hfVcfTbiPath = Column('hf_vcf_tbi_path', String(255), nullable=True)

    hfVcfMd5Received = Column('hf_vcf_md5_received', SmallInteger, nullable=False, default=0)
    hfVcfMd5Path = Column('hf_vcf_md5_path', String(255), nullable=True)

    # Single sample VCF: Raw for research purpose
    rawVcfReceived = Column('raw_vcf_received', SmallInteger, nullable=False, default=0)
    rawVcfPath = Column('raw_vcf_path', String(255), nullable=True)

    rawVcfTbiReceived = Column('raw_vcf_tbi_received', SmallInteger, nullable=False, default=0)
    rawVcfTbiPath = Column('raw_vcf_tbi_path', String(255), nullable=True)

    rawVcfMd5Received = Column('raw_vcf_md5_received', SmallInteger, nullable=False, default=0)
    rawVcfMd5Path = Column('raw_vcf_md5_path', String(255), nullable=True)

    # CRAMs and CRAIs
    cramReceived = Column('cram_received', SmallInteger, nullable=False, default=0)
    cramPath = Column('cram_path', String(255), nullable=True)

    cramMd5Received = Column('cram_md5_received', SmallInteger, nullable=False, default=0)
    cramMd5Path = Column('cram_md5_path', String(255), nullable=True)

    craiReceived = Column('crai_received', SmallInteger, nullable=False, default=0)
    craiPath = Column('crai_path', String(255), nullable=True)


event.listen(GenomicGCValidationMetrics, 'before_insert', model_insert_listener)
event.listen(GenomicGCValidationMetrics, 'before_update', model_update_listener)
