from sqlalchemy import (
    Column, DateTime, ForeignKey, Integer,
    String, SmallInteger, UniqueConstraint, event,
    BigInteger)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import JSON

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, MultiEnum, UTCDateTime
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.participant_enums import (
    GenomicSetStatus,
    GenomicSetMemberStatus,
    GenomicValidationFlag,
    GenomicSubProcessStatus,
    GenomicSubProcessResult,
    GenomicJob,
    GenomicWorkflowState,
    GenomicQcStatus,
    GenomicManifestTypes,
    GenomicContaminationCategory, GenomicIncidentCode, GenomicIncidentStatus)


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

    ai_an = Column('ai_an', String(2), nullable=True)
    """Flag for if participant is American Indian/Alaska Native"""

    biobankId = Column("biobank_id", String(128), nullable=True, index=True)

    packageId = Column("package_id", String(250), nullable=True)

    validationStatus = Column("validation_status", Enum(GenomicSetMemberStatus), default=GenomicSetMemberStatus.UNSET)
    validationFlags = Column("validation_flags", MultiEnum(GenomicValidationFlag), nullable=True)

    validatedTime = Column("validated_time", DateTime, nullable=True)

    # collectionTubeId corresponds to biobank_stored_sample_id
    collectionTubeId = Column('collection_tube_id', String(80), nullable=True, index=True)

    # sampleId is the great-grandchild aliquot of collectionTubeID
    sampleId = Column('sample_id', String(80), nullable=True, index=True)
    sampleType = Column('sample_type', String(50), nullable=True)

    sequencingFileName = Column('sequencing_file_name', String(255), nullable=True)
    """Name of the csv file being used for genomics sequencing"""

    gcSiteId = Column('gc_site_id', String(11), nullable=True)

    # BBGC Manifest Columns; ingested from GC manifest
    gcManifestBoxStorageUnitId = Column('gc_manifest_box_storage_unit_id', String(255), nullable=True)
    gcManifestBoxPlateId = Column('gc_manifest_box_plate_id', String(255), nullable=True)
    gcManifestWellPosition = Column('gc_manifest_well_position', String(10), nullable=True)
    gcManifestParentSampleId = Column('gc_manifest_parent_sample_id', String(20), nullable=True)
    gcManifestMatrixId = Column('gc_manifest_matrix_id', String(20), nullable=True)
    gcManifestTreatments = Column('gc_manifest_treatments', String(20), nullable=True)
    gcManifestQuantity_ul = Column('gc_manifest_quantity_ul', Integer, nullable=True)
    gcManifestTotalConcentration_ng_per_ul = Column('gc_manifest_total_concentration_ng_per_ul', Integer, nullable=True)
    gcManifestTotalDNA_ng = Column('gc_manifest_total_dna_ng', Integer, nullable=True)
    gcManifestVisitDescription = Column('gc_manifest_visit_description', String(128), nullable=True)
    gcManifestSampleSource = Column('gc_manifest_sample_source', String(20), nullable=True)
    gcManifestStudy = Column('gc_manifest_study', String(255), nullable=True)
    gcManifestTrackingNumber = Column('gc_manifest_tracking_number', String(255), nullable=True)
    gcManifestContact = Column('gc_manifest_contact', String(255), nullable=True)
    gcManifestEmail = Column('gc_manifest_email', String(255), nullable=True)
    gcManifestStudyPI = Column('gc_manifest_study_pi', String(255), nullable=True)
    gcManifestTestName = Column('gc_manifest_test_name', String(255), nullable=True)
    gcManifestFailureMode = Column('gc_manifest_failure_mode', String(128), nullable=True)
    gcManifestFailureDescription = Column('gc_manifest_failure_description', String(255), nullable=True)

    # File Processed IDs
    aw1FileProcessedId = Column('aw1_file_processed_id',
                                Integer, ForeignKey("genomic_file_processed.id"),
                                nullable=True)
    aw2FileProcessedId = Column('aw2_file_processed_id',
                                Integer, ForeignKey("genomic_file_processed.id"),
                                nullable=True)
    aw2fFileProcessedId = Column('aw2f_file_processed_id',
                                 Integer, ForeignKey("genomic_file_processed.id"),
                                 nullable=True)

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

    colorMetricsJobRunID = Column('color_metrics_job_run_id',
                                  Integer, ForeignKey("genomic_job_run.id"),
                                  nullable=True)

    gemMetricsAncestryLoopResponse = Column('gem_metrics_ancestry_loop_response',
                                            String(10), nullable=True)

    gemMetricsAvailableResults = Column('gem_metrics_available_results',
                                        String(255), nullable=True)

    gemMetricsResultsReleasedAt = Column('gem_metrics_results_released_at',
                                         DateTime, nullable=True)

    # Genomic State Fields
    genomicWorkflowState = Column('genomic_workflow_state',
                                  Enum(GenomicWorkflowState),
                                  default=GenomicWorkflowState.UNSET)

    genomicWorkflowStateModifiedTime = Column("genomic_workflow_state_modified_time", DateTime, nullable=True)

    reportConsentRemovalDate = Column('report_consent_removal_date', DateTime(timezone=True), nullable=True)

    genomicWorkflowStateHistory = Column("genomic_workflow_state_history", JSON, nullable=True)

    # Broad QC Status
    qcStatus = Column('qc_status', Enum(GenomicQcStatus), default=GenomicQcStatus.UNSET)

    # Broad fingerprint file path
    fingerprintPath = Column('fingerprint_path', String(255), nullable=True)

    # Developer note
    devNote = Column('dev_note', String(255), nullable=True)


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
    genomicManifestFileId = Column('genomic_manifest_file_id', Integer,
                                   ForeignKey("genomic_manifest_file.id"),
                                   nullable=True)

    # TODO: file_path, bucket_name, file_name, and upload_date to be removed
    # after genomic_manifest_file created, backfilled, and downstream partners notified.
    filePath = Column('file_path', String(255), nullable=False)
    bucketName = Column('bucket_name', String(128), nullable=False)
    fileName = Column('file_name', String(128), nullable=False)
    fileStatus = Column('file_status',
                        Enum(GenomicSubProcessStatus),
                        default=GenomicSubProcessStatus.QUEUED)
    fileResult = Column('file_result',
                        Enum(GenomicSubProcessResult),
                        default=GenomicSubProcessResult.UNSET)
    uploadDate = Column('upload_date', UTCDateTime, nullable=True)


class GenomicManifestFile(Base):
    """
    Genomic manifest file model.
    This model represents a manifest file.
    This includes both RDR and externally-generated manifests.
    """
    __tablename__ = 'genomic_manifest_file'

    id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime, nullable=False)
    modified = Column("modified", UTCDateTime, nullable=False)
    uploadDate = Column('upload_date', UTCDateTime, nullable=True)
    manifestTypeId = Column('manifest_type_id', Enum(GenomicManifestTypes), nullable=True)
    filePath = Column('file_path', String(255), nullable=True, index=True)
    fileName = Column('file_name', String(255), nullable=True, index=True)
    bucketName = Column('bucket_name', String(128), nullable=True)
    recordCount = Column('record_count', Integer, nullable=False, default=0)
    rdrProcessingComplete = Column('rdr_processing_complete', SmallInteger, nullable=False, default=0)
    rdrProcessingCompleteDate = Column('rdr_processing_complete_date', UTCDateTime, nullable=True)
    # TODO: Deprecated via DA-1865, to be removed after `ignore_flag` backfilled
    ignore = Column('ignore', SmallInteger, nullable=False, default=0)
    # Replaces `ignore` DA-1865
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)

    __table_args__ = (UniqueConstraint('file_path', 'ignore', name='_file_path_ignore_uc'),)


event.listen(GenomicManifestFile, 'before_insert', model_insert_listener)
event.listen(GenomicManifestFile, 'before_update', model_update_listener)


class GenomicManifestFeedback(Base):
    """
    Genomic manifest feedback model.
    This model represents a relationship
    between two genomic_manifest_file records:
        the input file and the feedback file.
    """
    __tablename__ = 'genomic_manifest_feedback'

    id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime, nullable=False)
    modified = Column("modified", UTCDateTime, nullable=False)

    # Foreign keys to genomic_manifest_file
    # Relates two manifests: the Input manifest and the Feedback manifest
    inputManifestFileId = Column("input_manifest_file_id", Integer,
                                 ForeignKey("genomic_manifest_file.id"), nullable=False)
    feedbackManifestFileId = Column("feedback_manifest_file_id", Integer,
                                    ForeignKey("genomic_manifest_file.id"), nullable=True)

    # Records RDR has received feedback for
    feedbackRecordCount = Column('feedback_record_count', Integer, nullable=False, default=0)

    # Once feedback_record_count = genomic_manifest_file.record_count
    # feedback_complete = 1 and a feedback manifest is generated, i.e. AW2F.
    feedbackComplete = Column('feedback_complete', SmallInteger, nullable=False, default=0)
    feedbackCompleteDate = Column('feedback_complete_date', UTCDateTime, nullable=True)
    # TODO: Deprecated via DA-1865, to be removed after `ignore_flag` backfilled
    ignore = Column('ignore', SmallInteger, nullable=False, default=0)
    # Replaces `ignore` DA-1865
    ignoreFlag = Column('ignore_flag', SmallInteger, nullable=False, default=0)


event.listen(GenomicManifestFeedback, 'before_insert', model_insert_listener)
event.listen(GenomicManifestFeedback, 'before_update', model_update_listener)


class GenomicAW1Raw(Base):
    """
    Raw text data from AW1 files
    """
    __tablename__ = 'genomic_aw1_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)

    # Auto-Timestamps
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    dev_note = Column('dev_note', String(255), nullable=True)

    # Raw AW1 data
    package_id = Column("package_id", String(255), nullable=True)
    biobankid_sample_id = Column("biobankid_sample_id", String(255), nullable=True)
    box_storageunit_id = Column("box_storageunit_id", String(255), nullable=True)
    box_id_plate_id = Column("box_id_plate_id", String(255), nullable=True)
    well_position = Column("well_position", String(255), nullable=True)
    sample_id = Column("sample_id", String(255), nullable=True)
    parent_sample_id = Column("parent_sample_id", String(255), nullable=True)
    collection_tube_id = Column("collection_tube_id", String(255), nullable=True)
    matrix_id = Column("matrix_id", String(255), nullable=True)
    collection_date = Column("collection_date", String(255), nullable=True)
    biobank_id = Column("biobank_id", String(255), nullable=True)
    sex_at_birth = Column("sex_at_birth", String(255), nullable=True)
    """Assigned sex at birth"""
    age = Column("age", String(255), nullable=True)
    ny_state = Column("ny_state", String(255), nullable=True)
    sample_type = Column("sample_type", String(255), nullable=True)
    treatments = Column("treatments", String(255), nullable=True)
    quantity = Column("quantity", String(255), nullable=True)
    total_concentration = Column("total_concentration", String(255), nullable=True)
    total_dna = Column("total_dna", String(255), nullable=True)
    visit_description = Column("visit_description", String(255), nullable=True)
    sample_source = Column("sample_source", String(255), nullable=True)
    study = Column("study", String(255), nullable=True)
    tracking_number = Column("tracking_number", String(255), nullable=True)
    contact = Column("contact", String(255), nullable=True)
    email = Column("email", String(255), nullable=True)
    study_pi = Column("study_pi", String(255), nullable=True)
    test_name = Column("test_name", String(255), nullable=True)
    failure_mode = Column("failure_mode", String(255), nullable=True)
    failure_mode_desc = Column("failure_mode_desc", String(255), nullable=True)


event.listen(GenomicAW1Raw, 'before_insert', model_insert_listener)
event.listen(GenomicAW1Raw, 'before_update', model_update_listener)


class GenomicAW2Raw(Base):
    """
    Raw text data from AW2 files
    """
    __tablename__ = 'genomic_aw2_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)

    # Auto-Timestamps
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    dev_note = Column('dev_note', String(255), nullable=True)

    # Raw AW2 Data
    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    biobankidsampleid = Column(String(255), nullable=True)
    lims_id = Column(String(255), nullable=True)
    mean_coverage = Column(String(255), nullable=True)
    genome_coverage = Column(String(255), nullable=True)
    aouhdr_coverage = Column(String(255), nullable=True)
    contamination = Column(String(255), nullable=True)
    sex_concordance = Column(String(255), nullable=True)
    sex_ploidy = Column(String(255), nullable=True)
    aligned_q30_bases = Column(String(255), nullable=True)
    array_concordance = Column(String(255), nullable=True)
    processing_status = Column(String(255), nullable=True)
    notes = Column(String(255), nullable=True)
    chipwellbarcode = Column(String(255), nullable=True)
    call_rate = Column(String(255), nullable=True)


event.listen(GenomicAW2Raw, 'before_insert', model_insert_listener)
event.listen(GenomicAW2Raw, 'before_update', model_update_listener)


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
    # TODO: change datatype of contamintion to float in RDR and PDR
    contamination = Column('contamination', String(10), nullable=True)
    sexConcordance = Column('sex_concordance', String(10), nullable=True)
    sexPloidy = Column('sex_ploidy', String(10), nullable=True)
    alignedQ30Bases = Column('aligned_q30_bases', BigInteger, nullable=True)
    arrayConcordance = Column('array_concordance', String(10), nullable=True)
    processingStatus = Column('processing_status', String(15), nullable=True)
    notes = Column('notes', String(128), nullable=True)
    siteId = Column('site_id', String(80), nullable=True)

    drcSexConcordance = Column('drc_sex_concordance', String(255), nullable=True)
    drcContamination = Column('drc_contamination', String(255), nullable=True)
    drcCallRate = Column('drc_call_rate', String(255), nullable=True)
    drcMeanCoverage = Column('drc_mean_coverage', String(255), nullable=True)
    drcFpConcordance = Column('drc_fp_concordance', String(255), nullable=True)

    # Genotyping Data (Array) reconciliation
    idatRedReceived = Column('idat_red_received', SmallInteger, nullable=False, default=0)
    idatRedDeleted = Column('idat_red_deleted', SmallInteger, nullable=False, default=0)
    idatRedPath = Column('idat_red_path', String(255), nullable=True)

    idatGreenReceived = Column('idat_green_received', SmallInteger, nullable=False, default=0)
    idatGreenDeleted = Column('idat_green_deleted', SmallInteger, nullable=False, default=0)
    idatGreenPath = Column('idat_green_path', String(255), nullable=True)

    idatRedMd5Received = Column('idat_red_md5_received', SmallInteger, nullable=False, default=0)
    idatRedMd5Deleted = Column('idat_red_md5_deleted', SmallInteger, nullable=False, default=0)
    idatRedMd5Path = Column('idat_red_md5_path', String(255), nullable=True)

    idatGreenMd5Received = Column('idat_green_md5_received', SmallInteger, nullable=False, default=0)
    idatGreenMd5Deleted = Column('idat_green_md5_deleted', SmallInteger, nullable=False, default=0)
    idatGreenMd5Path = Column('idat_green_md5_path', String(255), nullable=True)

    vcfReceived = Column('vcf_received', SmallInteger, nullable=False, default=0)
    vcfDeleted = Column('vcf_deleted', SmallInteger, nullable=False, default=0)
    vcfPath = Column('vcf_path', String(255), nullable=True)

    vcfMd5Received = Column('vcf_md5_received', SmallInteger, nullable=False, default=0)
    vcfMd5Deleted = Column('vcf_md5_deleted', SmallInteger, nullable=False, default=0)
    vcfMd5Path = Column('vcf_md5_path', String(255), nullable=True)

    vcfTbiReceived = Column('vcf_tbi_received', SmallInteger, nullable=False, default=0)
    vcfTbiDeleted = Column('vcf_tbi_deleted', SmallInteger, nullable=False, default=0)
    vcfTbiPath = Column('vcf_tbi_path', String(255), nullable=True)

    # Sequencing Data (WGS) reconciliation
    # Single sample VCF: Hard - filtered for clinical purpose
    hfVcfReceived = Column('hf_vcf_received', SmallInteger, nullable=False, default=0)
    hfVcfDeleted = Column('hf_vcf_deleted', SmallInteger, nullable=False, default=0)
    hfVcfPath = Column('hf_vcf_path', String(255), nullable=True)

    hfVcfTbiReceived = Column('hf_vcf_tbi_received', SmallInteger, nullable=False, default=0)
    hfVcfTbiDeleted = Column('hf_vcf_tbi_deleted', SmallInteger, nullable=False, default=0)
    hfVcfTbiPath = Column('hf_vcf_tbi_path', String(255), nullable=True)

    hfVcfMd5Received = Column('hf_vcf_md5_received', SmallInteger, nullable=False, default=0)
    hfVcfMd5Deleted = Column('hf_vcf_md5_deleted', SmallInteger, nullable=False, default=0)
    hfVcfMd5Path = Column('hf_vcf_md5_path', String(255), nullable=True)

    # Single sample VCF: Raw for research purpose
    rawVcfReceived = Column('raw_vcf_received', SmallInteger, nullable=False, default=0)
    rawVcfDeleted = Column('raw_vcf_deleted', SmallInteger, nullable=False, default=0)
    rawVcfPath = Column('raw_vcf_path', String(255), nullable=True)

    rawVcfTbiReceived = Column('raw_vcf_tbi_received', SmallInteger, nullable=False, default=0)
    rawVcfTbiDeleted = Column('raw_vcf_tbi_deleted', SmallInteger, nullable=False, default=0)
    rawVcfTbiPath = Column('raw_vcf_tbi_path', String(255), nullable=True)

    rawVcfMd5Received = Column('raw_vcf_md5_received', SmallInteger, nullable=False, default=0)
    rawVcfMd5Deleted = Column('raw_vcf_md5_deleted', SmallInteger, nullable=False, default=0)
    rawVcfMd5Path = Column('raw_vcf_md5_path', String(255), nullable=True)

    # CRAMs and CRAIs
    cramReceived = Column('cram_received', SmallInteger, nullable=False, default=0)
    cramDeleted = Column('cram_deleted', SmallInteger, nullable=False, default=0)
    cramPath = Column('cram_path', String(255), nullable=True)

    cramMd5Received = Column('cram_md5_received', SmallInteger, nullable=False, default=0)
    cramMd5Deleted = Column('cram_md5_deleted', SmallInteger, nullable=False, default=0)
    cramMd5Path = Column('cram_md5_path', String(255), nullable=True)

    craiReceived = Column('crai_received', SmallInteger, nullable=False, default=0)
    craiDeleted = Column('crai_deleted', SmallInteger, nullable=False, default=0)
    craiPath = Column('crai_path', String(255), nullable=True)

    # Ignore Record
    ignoreFlag = Column('ignore_flag', SmallInteger, nullable=True, default=0)
    devNote = Column('dev_note', String(255), nullable=True)

    # Contamination category
    contaminationCategory = Column('contamination_category',
                                   Enum(GenomicContaminationCategory),
                                   default=GenomicSubProcessResult.UNSET)


event.listen(GenomicGCValidationMetrics, 'before_insert', model_insert_listener)
event.listen(GenomicGCValidationMetrics, 'before_update', model_update_listener)


class GenomicSampleContamination(Base):
    """A list of samples that have been found to be contaminated with
    information on what stage of the process they have been added to the table."""
    __tablename__ = 'genomic_sample_contamination'

    # Primary Key
    id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)
    # Auto-Timestamps
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    sampleId = Column('sample_id', ForeignKey(BiobankStoredSample.biobankStoredSampleId), nullable=False)
    failedInJob = Column('failed_in_job', Enum(GenomicJob), nullable=False)


event.listen(GenomicSampleContamination, 'before_insert', model_insert_listener)
event.listen(GenomicSampleContamination, 'before_update', model_update_listener)


class GenomicIncident(Base):
    """
    An incident occuring during processing of genomic records
    """
    __tablename__ = 'genomic_incident'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)

    created = Column('created', DateTime)
    modified = Column('modified', DateTime)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)
    dev_note = Column(String(255))
    code = Column(String(80), default=GenomicIncidentCode.UNSET.name)
    message = Column(String(512))
    status = Column(String(80), default=GenomicIncidentStatus.OPEN.name)
    slack_notification = Column(SmallInteger, nullable=False, default=0)
    slack_notification_date = Column(DateTime, nullable=True)
    source_job_run_id = Column(Integer, ForeignKey("genomic_job_run.id"))
    source_file_processed_id = Column(Integer, ForeignKey("genomic_file_processed.id"))
    audit_job_run_id = Column(Integer, ForeignKey("genomic_job_run.id"))
    repair_job_run_id = Column(Integer, ForeignKey("genomic_job_run.id"))
    genomic_set_member_id = Column(Integer, ForeignKey("genomic_set_member.id"))
    gc_validation_metrics_id = Column(Integer, ForeignKey("genomic_gc_validation_metrics.id"))
    biobank_id = Column(String(128), index=True)
    sample_id = Column(String(80), index=True)
    collection_tube_id = Column(String(80), index=True)


event.listen(GenomicIncident, 'before_insert', model_insert_listener)
event.listen(GenomicIncident, 'before_update', model_update_listener)
