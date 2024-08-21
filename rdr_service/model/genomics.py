from sqlalchemy import (
    Column, DateTime, ForeignKey, Integer,
    String, SmallInteger, UniqueConstraint, event,
    BigInteger)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import JSON

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, MultiEnum, UTCDateTime, UTCDateTime6
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.genomic_enums import GenomicSetStatus, GenomicSetMemberStatus, GenomicValidationFlag, GenomicJob, \
    GenomicWorkflowState, GenomicSubProcessStatus, GenomicSubProcessResult, GenomicManifestTypes, \
    GenomicContaminationCategory, GenomicQcStatus, GenomicIncidentCode, GenomicIncidentStatus, GenomicReportState, \
    GenomicSampleSwapCategory, GenomicLongReadPlatform


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

    history_table = True
    exclude_column_names_from_history = [
        'modified',
        'reconcile_metrics_bb_manifest_job_run_id',
        'reconcile_gc_manifest_job_run_id',
        'reconcile_metrics_sequencing_job_run_id',
        'reconcile_cvl_job_run_id',
        'gem_a1_manifest_job_run_id',
        'gem_a2_manifest_job_run_id',
        'gem_a3_manifest_job_run_id',
        'aw3_manifest_job_run_id',
        'aw4_manifest_job_run_id',
        'aw2f_manifest_job_run_id',
        'cvl_w1_manifest_job_run_id',
        'cvl_w2_manifest_job_run_id',
        'cvl_w3_manifest_job_run_id',
        'cvl_w4_manifest_job_run_id',
        'cvl_w4f_manifest_job_run_id',
        'cvl_aw1c_manifest_job_run_id',
        'cvl_aw1cf_manifest_job_run_id',
        'cvl_w3sr_manifest_job_run_id',
        'cvl_w2sc_manifest_job_run_id',
        'color_metrics_job_run_id',
        'cvl_w1il_pgx_job_run_id',
        'cvl_w1il_hdr_job_run_id',
        'cvl_w2w_job_run_id',
        'cvl_w4wr_pgx_manifest_job_run_id',
        'cvl_w4wr_hdr_manifest_job_run_id',
        'cvl_w3sc_manifest_job_run_id',
        'cvl_w3ns_manifest_job_run_id',
        'cvl_w5nf_pgx_manifest_job_run_id',
        'cvl_w5nf_hdr_manifest_job_run_id',
        'cvl_w3ss_manifest_job_run_id'
    ]

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

    aw2fManifestJobRunID = Column('aw2f_manifest_job_run_id',
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

    aw3ManifestFileId = Column('aw3_manifest_file_id',
                               Integer, ForeignKey("genomic_manifest_file.id"),
                               nullable=True)

    aw0ManifestFileId = Column('aw0_manifest_file_id',
                               Integer, ForeignKey("genomic_manifest_file.id"),
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
    genomicWorkflowStateStr = Column('genomic_workflow_state_str', String(64), default="UNSET")

    genomicWorkflowStateModifiedTime = Column("genomic_workflow_state_modified_time", DateTime, nullable=True)

    reportConsentRemovalDate = Column('report_consent_removal_date', DateTime(timezone=True), nullable=True)

    # Broad QC Status
    qcStatus = Column('qc_status', Enum(GenomicQcStatus), default=GenomicQcStatus.UNSET)
    qcStatusStr = Column('qc_status_str', String(64), default="UNSET")

    # Broad fingerprint file path
    fingerprintPath = Column('fingerprint_path', String(255), nullable=True)

    # Developer note
    devNote = Column('dev_note', String(255), nullable=True)

    # For tracking replates
    replatedMemberId = Column('replated_member_id',
                              ForeignKey('genomic_set_member.id'),
                              nullable=True)
    ignoreFlag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    blockResearch = Column('block_research', SmallInteger, nullable=False, default=0)
    blockResearchReason = Column('block_research_reason', String(255), nullable=True)
    blockResults = Column('block_results', SmallInteger, nullable=False, default=0)
    blockResultsReason = Column('block_results_reason', String(255), nullable=True)
    participantOrigin = Column("participant_origin", String(80), nullable=True)

    cvlW1ilPgxJobRunId = Column('cvl_w1il_pgx_job_run_id',
                                Integer, ForeignKey('genomic_job_run.id'),
                                nullable=True)
    cvlW1ilHdrJobRunId = Column('cvl_w1il_hdr_job_run_id',
                                Integer, ForeignKey('genomic_job_run.id'),
                                nullable=True)

    cvlW2wJobRunId = Column('cvl_w2w_job_run_id',
                            Integer, ForeignKey('genomic_job_run.id'),
                            nullable=True)

    cvlSecondaryConfFailure = Column('cvl_secondary_conf_failure', String(255), nullable=True)

    # PGX / HDR Run IDs
    cvlW4wrPgxManifestJobRunID = Column('cvl_w4wr_pgx_manifest_job_run_id',
                                        Integer, ForeignKey("genomic_job_run.id"),
                                        nullable=True)
    cvlW4wrHdrManifestJobRunID = Column('cvl_w4wr_hdr_manifest_job_run_id',
                                        Integer, ForeignKey("genomic_job_run.id"),
                                        nullable=True)
    cvlW5nfPgxManifestJobRunID = Column('cvl_w5nf_pgx_manifest_job_run_id',
                                     Integer, ForeignKey("genomic_job_run.id"),
                                     nullable=True)

    cvlW5nfHdrManifestJobRunID = Column('cvl_w5nf_hdr_manifest_job_run_id',
                                     Integer, ForeignKey("genomic_job_run.id"),
                                     nullable=True)

    # Only HDR Run IDs
    cvlW2scManifestJobRunID = Column('cvl_w2sc_manifest_job_run_id',
                                     Integer, ForeignKey("genomic_job_run.id"),
                                     nullable=True)
    cvlW3ssManifestJobRunID = Column('cvl_w3ss_manifest_job_run_id',
                                     Integer, ForeignKey("genomic_job_run.id"),
                                     nullable=True)
    cvlW3nsManifestJobRunID = Column('cvl_w3ns_manifest_job_run_id',
                                     Integer, ForeignKey("genomic_job_run.id"),
                                     nullable=True)
    cvlW3srManifestJobRunID = Column('cvl_w3sr_manifest_job_run_id',
                                     Integer, ForeignKey("genomic_job_run.id"),
                                     nullable=True)
    cvlW3scManifestJobRunID = Column('cvl_w3sc_manifest_job_run_id',
                                     Integer, ForeignKey("genomic_job_run.id"),
                                     nullable=True)

    diversionPouchSiteFlag = Column('diversion_pouch_site_flag', SmallInteger, nullable=False, default=0)
    informingLoopReadyFlag = Column('informing_loop_ready_flag', Integer, nullable=False, default=0)
    informingLoopReadyFlagModified = Column("informing_loop_ready_flag_modified", DateTime, nullable=True)


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
    created = Column("created", UTCDateTime, nullable=True)
    modified = Column("modified", UTCDateTime, nullable=True)
    jobId = Column('job_id', Enum(GenomicJob),
                   default=GenomicJob.UNSET, nullable=False)
    jobIdStr = Column('job_id_str', String(64), default="UNSET")
    startTime = Column('start_time', DateTime, nullable=False)
    endTime = Column('end_time', DateTime, nullable=True)
    runStatus = Column('run_status',
                       Enum(GenomicSubProcessStatus),
                       default=GenomicSubProcessStatus.RUNNING)
    runResult = Column('run_result',
                       Enum(GenomicSubProcessResult),
                       default=GenomicSubProcessResult.UNSET)
    runResultStr = Column('run_result_str', String(64), default="UNSET")
    resultMessage = Column('result_message', String(150), nullable=True)


event.listen(GenomicJobRun, 'before_insert', model_insert_listener)
event.listen(GenomicJobRun, 'before_update', model_update_listener)


class GenomicFileProcessed(Base):
    """Genomic File Processed model.
    This model represents the file(s) processed during a genomics run."""
    __tablename__ = 'genomic_file_processed'

    # Primary Key
    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime, nullable=True)
    modified = Column("modified", UTCDateTime, nullable=True)
    runId = Column('run_id', Integer,
                   ForeignKey('genomic_job_run.id'), nullable=False)
    startTime = Column('start_time', DateTime, nullable=False)
    endTime = Column('end_time', DateTime, nullable=True)
    genomicManifestFileId = Column('genomic_manifest_file_id', Integer,
                                   ForeignKey("genomic_manifest_file.id"),
                                   nullable=True)

    # TODO: file_path, bucket_name, file_name, and upload_date to be removed
    # after genomic_manifest_file created, backfilled, and downstream partners notified.
    filePath = Column('file_path', String(255), nullable=False, index=True)
    bucketName = Column('bucket_name', String(128), nullable=False)
    fileName = Column('file_name', String(128), nullable=False)
    fileStatus = Column('file_status',
                        Enum(GenomicSubProcessStatus),
                        default=GenomicSubProcessStatus.QUEUED)
    fileResult = Column('file_result',
                        Enum(GenomicSubProcessResult),
                        default=GenomicSubProcessResult.UNSET)
    uploadDate = Column('upload_date', UTCDateTime, nullable=True)


event.listen(GenomicFileProcessed, 'before_insert', model_insert_listener)
event.listen(GenomicFileProcessed, 'before_update', model_update_listener)


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
    manifestTypeIdStr = Column('manifest_type_id_str', String(64), nullable=True)
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
    version = Column(Integer, nullable=False, default=0)


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
    sample_id = Column("sample_id", String(255), nullable=True, index=True)
    parent_sample_id = Column("parent_sample_id", String(255), nullable=True, index=True)
    collection_tube_id = Column("collection_tube_id", String(255), nullable=True, index=True)
    matrix_id = Column("matrix_id", String(255), nullable=True)
    collection_date = Column("collection_date", String(255), nullable=True)
    biobank_id = Column("biobank_id", String(255), nullable=True, index=True)
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
    site_name = Column("site_name", String(255), nullable=True, index=True)
    test_name = Column("test_name", String(255), nullable=True, index=True)
    failure_mode = Column("failure_mode", String(255), nullable=True)
    failure_mode_desc = Column("failure_mode_desc", String(255), nullable=True)
    genome_type = Column(String(80), nullable=True, index=True)


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
    sample_source = Column(String(255), nullable=True)
    mapped_reads_pct = Column(String(255), nullable=True)
    sex_concordance = Column(String(255), nullable=True)
    sex_ploidy = Column(String(255), nullable=True)
    aligned_q30_bases = Column(String(255), nullable=True)
    array_concordance = Column(String(255), nullable=True)
    processing_status = Column(String(255), nullable=True)
    notes = Column(String(255), nullable=True)
    chipwellbarcode = Column(String(255), nullable=True)
    call_rate = Column(String(255), nullable=True)
    genome_type = Column(String(80), nullable=True)
    pipeline_id = Column(String(255), nullable=True, index=True)
    vcf_hf_path = Column(String(255), nullable=True)
    vcf_hf_index_path = Column(String(255), nullable=True)
    vcf_hf_md5_path = Column(String(255), nullable=True)
    cram_path = Column(String(255), nullable=True)
    cram_md5_path = Column(String(255), nullable=True)
    crai_path = Column(String(255), nullable=True)
    gvcf_path = Column(String(255), nullable=True)
    gvcf_md5_path = Column(String(255), nullable=True)


event.listen(GenomicAW2Raw, 'before_insert', model_insert_listener)
event.listen(GenomicAW2Raw, 'before_update', model_update_listener)


class GenomicAW3Raw(Base):
    """
    Raw data from AW3 files
    """
    __tablename__ = 'genomic_aw3_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)

    # Auto-Timestamps
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)
    dev_note = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True, index=True)

    # Raw AW3 Data
    chipwellbarcode = Column(String(255), nullable=True, index=True)
    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    research_id = Column(String(255), nullable=True, index=True)
    biobankidsampleid = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    site_id = Column(String(255), nullable=True, index=True)
    callrate = Column(String(255), nullable=True)
    sex_concordance = Column(String(255), nullable=True)
    contamination = Column(String(255), nullable=True)
    processing_status = Column(String(255), nullable=True)
    mean_coverage = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    pipeline_id = Column(String(255), nullable=True)
    mapped_reads_pct = Column(String(255), nullable=True)
    sex_ploidy = Column(String(255), nullable=True)
    ai_an = Column(String(255), nullable=True)
    blocklisted = Column(String(255), nullable=True, index=True)
    blocklisted_reason = Column(String(255), nullable=True)
    red_idat_path = Column(String(255), nullable=True)
    red_idat_md5_path = Column(String(255), nullable=True)
    green_idat_path = Column(String(255), nullable=True)
    green_idat_md5_path = Column(String(255), nullable=True)
    vcf_path = Column(String(255), nullable=True)
    vcf_index_path = Column(String(255), nullable=True)
    vcf_md5_path = Column(String(255), nullable=True)
    vcf_hf_path = Column(String(255), nullable=True)
    vcf_hf_index_path = Column(String(255), nullable=True)
    vcf_hf_md5_path = Column(String(255), nullable=True)
    cram_path = Column(String(255), nullable=True)
    cram_md5_path = Column(String(255), nullable=True)
    crai_path = Column(String(255), nullable=True)
    gvcf_path = Column(String(255), nullable=True)
    gvcf_md5_path = Column(String(255), nullable=True)
    processing_count = Column(String(255), nullable=True)


event.listen(GenomicAW3Raw, 'before_insert', model_insert_listener)
event.listen(GenomicAW3Raw, 'before_update', model_update_listener)


class GenomicAW4Raw(Base):
    """
    Raw data from AW4 files
    """
    __tablename__ = 'genomic_aw4_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)

    # Auto-Timestamps
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)
    dev_note = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True, index=True)

    # Raw AW4 Data
    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    sex_at_birth = Column(String(255), nullable=True)
    site_id = Column(String(255), nullable=True, index=True)
    red_idat_path = Column(String(255), nullable=True)
    red_idat_md5_path = Column(String(255), nullable=True)
    green_idat_path = Column(String(255), nullable=True)
    green_idat_md5_path = Column(String(255), nullable=True)
    vcf_path = Column(String(255), nullable=True)
    vcf_index_path = Column(String(255), nullable=True)
    vcf_hf_path = Column(String(255), nullable=True)
    vcf_hf_md5_path = Column(String(255), nullable=True)
    vcf_hf_index_path = Column(String(255), nullable=True)
    vcf_raw_path = Column(String(255), nullable=True)
    vcf_raw_md5_path = Column(String(255), nullable=True)
    vcf_raw_index_path = Column(String(255), nullable=True)
    gvcf_path = Column(String(255), nullable=True)
    gvcf_md5_path = Column(String(255), nullable=True)
    cram_path = Column(String(255), nullable=True)
    cram_md5_path = Column(String(255), nullable=True)
    crai_path = Column(String(255), nullable=True)
    research_id = Column(String(255), nullable=True, index=True)
    qc_status = Column(String(255), nullable=True)
    drc_sex_concordance = Column(String(255), nullable=True)
    drc_call_rate = Column(String(255), nullable=True)
    drc_contamination = Column(String(255), nullable=True)
    drc_mean_coverage = Column(String(255), nullable=True)
    drc_fp_concordance = Column(String(255), nullable=True)
    pass_to_research_pipeline = Column(String(255), nullable=True)
    pipeline_id = Column(String(255), nullable=True, index=True)
    processing_count = Column(String(255), nullable=True)


event.listen(GenomicAW4Raw, 'before_insert', model_insert_listener)
event.listen(GenomicAW4Raw, 'before_update', model_update_listener)


class GenomicAW5Raw(Base):
    """
    Raw data from AW5 files
    """
    __tablename__ = 'genomic_aw5_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)

    # Auto-Timestamps
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)
    dev_note = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True, index=True)

    # Raw AW5 Data
    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankidsampleid = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    site_id = Column(String(255), nullable=True, index=True)
    red_idat = Column(String(255), nullable=True)
    red_idat_md5 = Column(String(255), nullable=True)
    red_idat_basename = Column(String(255), nullable=True)
    red_idat_md5_hash = Column(String(255), nullable=True)
    green_idat = Column(String(255), nullable=True)
    green_idat_md5 = Column(String(255), nullable=True)
    green_idat_basename = Column(String(255), nullable=True)
    green_idat_md5_hash = Column(String(255), nullable=True)
    vcf = Column(String(255), nullable=True)
    vcf_index = Column(String(255), nullable=True)
    vcf_md5 = Column(String(255), nullable=True)
    vcf_basename = Column(String(255), nullable=True)
    vcf_md5_hash = Column(String(255), nullable=True)

event.listen(GenomicAW5Raw, 'before_insert', model_insert_listener)
event.listen(GenomicAW5Raw, 'before_update', model_update_listener)

class GenomicW1ILRaw(Base):
    """
    Raw data from W1IL files
    """
    __tablename__ = 'genomic_w1il_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    dev_note = Column('dev_note', String(255), nullable=True)

    cvl_site_id = Column(String(128), nullable=True, index=True)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    vcf_raw_path = Column(String(255), nullable=True)
    vcf_raw_index_path = Column(String(255), nullable=True)
    vcf_raw_md5_path = Column(String(255), nullable=True)
    gvcf_path = Column(String(255), nullable=True)
    gvcf_md5_path = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    ny_flag = Column(String(255), nullable=True)
    genome_center = Column(String(255), nullable=True)
    consent_for_gror = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)
    informing_loop_pgx = Column(String(255), nullable=True)
    informing_loop_hdr = Column(String(255), nullable=True)
    aou_hdr_coverage = Column(String(255), nullable=True)
    contamination = Column(String(255), nullable=True)
    sex_ploidy = Column(String(255), nullable=True)


event.listen(GenomicW1ILRaw, 'before_insert', model_insert_listener)
event.listen(GenomicW1ILRaw, 'before_update', model_update_listener)


class GenomicW2SCRaw(Base):
    """
    Raw data from W2SC files
    """
    __tablename__ = 'genomic_w2sc_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    dev_note = Column('dev_note', String(255), nullable=True)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)


event.listen(GenomicW2SCRaw, 'before_insert', model_insert_listener)
event.listen(GenomicW2SCRaw, 'before_update', model_update_listener)


class GenomicW2WRaw(Base):
    """
    Raw data from W2W files
    """
    __tablename__ = 'genomic_w2w_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    dev_note = Column('dev_note', String(255), nullable=True)

    cvl_site_id = Column(String(128), nullable=True, index=True)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    date_of_consent_removal = Column(String(255), nullable=True)


event.listen(GenomicW2WRaw, 'before_insert', model_insert_listener)
event.listen(GenomicW2WRaw, 'before_update', model_update_listener)


class GenomicW3NSRaw(Base):
    """
    Raw data from W3NS files
    """
    __tablename__ = 'genomic_w3ns_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    dev_note = Column('dev_note', String(255), nullable=True)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    unavailable_reason = Column(String(255), nullable=True)


event.listen(GenomicW3NSRaw, 'before_insert', model_insert_listener)
event.listen(GenomicW3NSRaw, 'before_update', model_update_listener)


class GenomicW3SRRaw(Base):
    """
    Raw data from W3SR files
    """
    __tablename__ = 'genomic_w3sr_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    dev_note = Column('dev_note', String(255), nullable=True)

    cvl_site_id = Column(String(128), nullable=True, index=True)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    parent_sample_id = Column(String(255), nullable=True)
    collection_tubeid = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    ny_flag = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)
    site_name = Column(String(255), nullable=True)
    ai_an = Column(String(255), nullable=True)


event.listen(GenomicW3SRRaw, 'before_insert', model_insert_listener)
event.listen(GenomicW3SRRaw, 'before_update', model_update_listener)


class GenomicW3SCRaw(Base):
    """
    Raw data from W3SR files
    """
    __tablename__ = 'genomic_w3sc_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    dev_note = Column('dev_note', String(255), nullable=True)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    cvl_secondary_conf_failure = Column(String(255), nullable=True)


event.listen(GenomicW3SRRaw, 'before_insert', model_insert_listener)
event.listen(GenomicW3SRRaw, 'before_update', model_update_listener)


class GenomicW3SSRaw(Base):
    """
    Raw data from W3SR files
    """
    __tablename__ = 'genomic_w3ss_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    dev_note = Column('dev_note', String(255), nullable=True)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    packageId = Column(String(250), nullable=True)
    version = Column(String(255), nullable=True)
    box_storageunit_id = Column(String(255), nullable=True)
    box_id_plate_id = Column(String(255), nullable=True)
    well_position = Column(String(255), nullable=True)
    cvl_sample_id = Column(String(255), nullable=True)
    parent_sample_id = Column(String(255), nullable=True)
    collection_tube_id = Column(String(255), nullable=True)
    matrix_id = Column(String(255), nullable=True)
    collection_date = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    age = Column(String(255), nullable=True)
    ny_state = Column(String(255), nullable=True)
    sample_type = Column(String(255), nullable=True)
    treatments = Column(String(255), nullable=True)
    quantity = Column(String(255), nullable=True)
    total_concentration = Column(String(255), nullable=True)
    total_dna = Column(String(255), nullable=True)
    visit_description = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    study = Column(String(255), nullable=True)
    tracking_number = Column(String(255), nullable=True)
    contact = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    study_pi = Column(String(255), nullable=True)
    site_name = Column(String(255), nullable=True, index=True)
    genome_type = Column(String(80), nullable=True, index=True)
    failure_mode = Column(String(255), nullable=True)
    failure_mode_desc = Column(String(255), nullable=True)


event.listen(GenomicW3SRRaw, 'before_insert', model_insert_listener)
event.listen(GenomicW3SRRaw, 'before_update', model_update_listener)


class GenomicW4WRRaw(Base):
    """
    Raw data from W4WR files
    """
    __tablename__ = 'genomic_w4wr_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    dev_note = Column('dev_note', String(255), nullable=True)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    health_related_data_file_name = Column(String(255), nullable=True)
    clinical_analysis_type = Column(String(255), nullable=True)


event.listen(GenomicW4WRRaw, 'before_insert', model_insert_listener)
event.listen(GenomicW4WRRaw, 'before_update', model_update_listener)


class GenomicW5NFRaw(Base):
    """
    Raw data from W5NF files
    """
    __tablename__ = 'genomic_w5nf_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)
    dev_note = Column('dev_note', String(255), nullable=True)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    request_reason = Column(String(255), nullable=True)
    request_reason_free = Column(String(512), nullable=True)
    health_related_data_file_name = Column(String(255), nullable=True)
    clinical_analysis_type = Column(String(255), nullable=True)


event.listen(GenomicW5NFRaw, 'before_insert', model_insert_listener)
event.listen(GenomicW5NFRaw, 'before_update', model_update_listener)


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
    mappedReadsPct = Column('mapped_reads_pct', String(10), nullable=True)
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
    idatRedDeleted = Column('idat_red_deleted', SmallInteger, nullable=False, default=0)
    idatRedPath = Column('idat_red_path', String(255), nullable=True)

    idatGreenDeleted = Column('idat_green_deleted', SmallInteger, nullable=False, default=0)
    idatGreenPath = Column('idat_green_path', String(255), nullable=True)

    idatRedMd5Deleted = Column('idat_red_md5_deleted', SmallInteger, nullable=False, default=0)
    idatRedMd5Path = Column('idat_red_md5_path', String(255), nullable=True)

    idatGreenMd5Deleted = Column('idat_green_md5_deleted', SmallInteger, nullable=False, default=0)
    idatGreenMd5Path = Column('idat_green_md5_path', String(255), nullable=True)

    vcfDeleted = Column('vcf_deleted', SmallInteger, nullable=False, default=0)
    vcfPath = Column('vcf_path', String(255), nullable=True)

    vcfMd5Deleted = Column('vcf_md5_deleted', SmallInteger, nullable=False, default=0)
    vcfMd5Path = Column('vcf_md5_path', String(255), nullable=True)

    vcfTbiDeleted = Column('vcf_tbi_deleted', SmallInteger, nullable=False, default=0)
    vcfTbiPath = Column('vcf_tbi_path', String(255), nullable=True)

    # Sequencing Data (WGS) reconciliation
    # Single sample VCF: Hard - filtered for clinical purpose
    hfVcfDeleted = Column('hf_vcf_deleted', SmallInteger, nullable=False, default=0)
    hfVcfPath = Column('hf_vcf_path', String(255), nullable=True)

    hfVcfTbiDeleted = Column('hf_vcf_tbi_deleted', SmallInteger, nullable=False, default=0)
    hfVcfTbiPath = Column('hf_vcf_tbi_path', String(255), nullable=True)

    hfVcfMd5Deleted = Column('hf_vcf_md5_deleted', SmallInteger, nullable=False, default=0)
    hfVcfMd5Path = Column('hf_vcf_md5_path', String(255), nullable=True)

    # Single sample VCF: Raw for research purpose
    rawVcfDeleted = Column('raw_vcf_deleted', SmallInteger, nullable=False, default=0)
    rawVcfPath = Column('raw_vcf_path', String(255), nullable=True)

    rawVcfTbiDeleted = Column('raw_vcf_tbi_deleted', SmallInteger, nullable=False, default=0)
    rawVcfTbiPath = Column('raw_vcf_tbi_path', String(255), nullable=True)

    rawVcfMd5Deleted = Column('raw_vcf_md5_deleted', SmallInteger, nullable=False, default=0)
    rawVcfMd5Path = Column('raw_vcf_md5_path', String(255), nullable=True)

    # CRAMs and CRAIs
    cramDeleted = Column('cram_deleted', SmallInteger, nullable=False, default=0)
    cramPath = Column('cram_path', String(255), nullable=True)

    cramMd5Deleted = Column('cram_md5_deleted', SmallInteger, nullable=False, default=0)
    cramMd5Path = Column('cram_md5_path', String(255), nullable=True)

    craiDeleted = Column('crai_deleted', SmallInteger, nullable=False, default=0)
    craiPath = Column('crai_path', String(255), nullable=True)

    gvcfDeleted = Column('gvcf_deleted', SmallInteger, nullable=False, default=0)
    gvcfPath = Column('gvcf_path', String(512), nullable=True)

    gvcfMd5Deleted = Column('gvcf_md5_deleted', SmallInteger, nullable=False, default=0)
    gvcfMd5Path = Column('gvcf_md5_path', String(255), nullable=True)

    # Ignore Record
    ignoreFlag = Column('ignore_flag', SmallInteger, nullable=True, default=0)
    devNote = Column('dev_note', String(255), nullable=True)

    # Contamination category
    contaminationCategory = Column('contamination_category',
                                   Enum(GenomicContaminationCategory),
                                   default=GenomicSubProcessResult.UNSET)
    contaminationCategoryStr = Column('contamination_category_str', String(64), default="UNSET")

    pipelineId = Column('pipeline_id', String(255), nullable=True, index=True)

    aw3ReadyFlag = Column('aw3_ready_flag', SmallInteger, nullable=False, default=0)
    aw3ManifestJobRunID = Column('aw3_manifest_job_run_id', Integer, ForeignKey('genomic_job_run.id'))
    aw3ManifestFileId = Column('aw3_manifest_file_id', Integer, ForeignKey("genomic_manifest_file.id"))
    aw4ManifestJobRunID = Column('aw4_manifest_job_run_id', Integer, ForeignKey('genomic_job_run.id'))


event.listen(GenomicGCValidationMetrics, 'before_insert', model_insert_listener)
event.listen(GenomicGCValidationMetrics, 'before_update', model_update_listener)


class GenomicCVLSecondSample(Base):
    """
    Used for storage in GHR3 of second sample records
    """

    __tablename__ = 'genomic_cvl_second_sample'

    id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)
    genomic_set_member_id = Column(ForeignKey('genomic_set_member.id'), nullable=False, index=True)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True, index=True)

    package_id = Column(String(250), nullable=True)
    version = Column(String(255), nullable=False, default=0)
    box_storageunit_id = Column(String(255), nullable=True)
    box_id_plate_id = Column(String(255), nullable=True)
    well_position = Column(String(255), nullable=True)
    cvl_sample_id = Column(String(255), nullable=True, index=True)
    parent_sample_id = Column(String(255), nullable=True)
    collection_tube_id = Column(String(255), nullable=True)
    matrix_id = Column(String(255), nullable=True)
    collection_date = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    age = Column(String(255), nullable=True)
    ny_state = Column(String(255), nullable=True)
    sample_type = Column(String(255), nullable=True)
    treatments = Column(String(255), nullable=True)
    quantity = Column(String(255), nullable=True)
    total_concentration = Column(String(255), nullable=True)
    total_dna = Column(String(255), nullable=True)
    visit_description = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    study = Column(String(255), nullable=True)
    tracking_number = Column(String(255), nullable=True)
    contact = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    study_pi = Column(String(255), nullable=True)
    site_name = Column(String(255), nullable=True, index=True)
    genome_type = Column(String(80), nullable=True)
    failure_mode = Column(String(255), nullable=True)
    failure_mode_desc = Column(String(255), nullable=True)


event.listen(GenomicCVLSecondSample, 'before_insert', model_insert_listener)
event.listen(GenomicCVLSecondSample, 'before_update', model_update_listener)


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
    participant_id = Column(String(128), index=True)
    biobank_id = Column(String(128), index=True)
    sample_id = Column(String(80), index=True)
    collection_tube_id = Column(String(80), index=True)
    data_file_path = Column(String(512))
    submitted_gc_site_id = Column(String(128), nullable=True)
    email_notification_sent = Column(SmallInteger, nullable=True, default=0)
    email_notification_sent_date = Column(DateTime, nullable=True)
    manifest_file_name = Column(String(512), nullable=True)


event.listen(GenomicIncident, 'before_insert', model_insert_listener)
event.listen(GenomicIncident, 'before_update', model_update_listener)


class GenomicCloudRequests(Base):
    """
    Used for capturing cloud requests payloads via
    Google Cloud Functions
    """
    __tablename__ = 'genomic_cloud_requests'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    event_payload = Column(JSON, nullable=False)
    topic = Column(String(255), nullable=False)
    api_route = Column(String(255), nullable=False)
    file_path = Column(String(255), nullable=False)
    task = Column(String(255), nullable=False)
    bucket_name = Column(String(255), nullable=False)


event.listen(GenomicCloudRequests, 'before_insert', model_insert_listener)
event.listen(GenomicCloudRequests, 'before_update', model_update_listener)


class GenomicMemberReportState(Base):
    """
    Used for maintaining one-to-many relationship
    from GenomicSetMember based on multiple report states
    """

    __tablename__ = 'genomic_member_report_state'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    message_record_id = Column(Integer, nullable=True)
    genomic_set_member_id = Column(ForeignKey('genomic_set_member.id'), nullable=False)
    genomic_report_state = Column(Enum(GenomicReportState), default=GenomicReportState.UNSET)
    genomic_report_state_str = Column(String(64), default="UNSET")
    participant_id = Column(Integer, ForeignKey("participant.participant_id"), nullable=True)
    event_type = Column(String(256), nullable=True)
    event_authored_time = Column(UTCDateTime6, nullable=True)
    module = Column(String(80), nullable=False)
    sample_id = Column(String(80), nullable=True, index=True)
    report_revision_number = Column(SmallInteger, nullable=True)
    created_from_metric_id = Column(Integer, ForeignKey("user_event_metrics.id"), nullable=True)


event.listen(GenomicMemberReportState, 'before_insert', model_insert_listener)
event.listen(GenomicMemberReportState, 'before_update', model_update_listener)


class GenomicInformingLoop(Base):
    """
    Used for maintaining normalized value set of
    informing_loop_decision ingested from MessageBrokerEventData
    """

    __tablename__ = 'genomic_informing_loop'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)
    message_record_id = Column(Integer, nullable=True)
    participant_id = Column(Integer, ForeignKey("participant.participant_id"), nullable=False)
    event_type = Column(String(256), nullable=False, index=True)
    event_authored_time = Column(UTCDateTime6, index=True)
    module_type = Column(String(128), index=True)
    decision_value = Column(String(128), index=True)
    sample_id = Column(String(80), nullable=True, index=True)
    created_from_metric_id = Column(Integer, ForeignKey("user_event_metrics.id"), nullable=True)


event.listen(GenomicInformingLoop, 'before_insert', model_insert_listener)
event.listen(GenomicInformingLoop, 'before_update', model_update_listener)


class GenomicResultViewed(Base):
    """
    Used for maintaining normalized value set of
    result_viewed ingested from MessageBrokerEventData
    """

    __tablename__ = 'genomic_result_viewed'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    message_record_id = Column(Integer, nullable=True)
    participant_id = Column(Integer, ForeignKey("participant.participant_id"), nullable=False)
    event_type = Column(String(256), nullable=False, index=True)
    event_authored_time = Column(UTCDateTime6, index=True)
    module_type = Column(String(128), index=True)
    first_viewed = Column(UTCDateTime6)
    last_viewed = Column(UTCDateTime6)
    sample_id = Column(String(80), nullable=True, index=True)
    created_from_metric_id = Column(Integer, ForeignKey("user_event_metrics.id"), nullable=True)


event.listen(GenomicResultViewed, 'before_insert', model_insert_listener)
event.listen(GenomicResultViewed, 'before_update', model_update_listener)


class GenomicAppointmentEvent(Base):
    """
    Used for maintaining normalized value set of
    appointment events ingested from MessageBrokerEventData
    """

    __tablename__ = 'genomic_appointment_event'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    message_record_id = Column(Integer)
    participant_id = Column(Integer, ForeignKey("participant.participant_id"), nullable=False)
    event_type = Column(String(256), nullable=False)
    event_authored_time = Column(UTCDateTime6)
    module_type = Column(String(255))
    appointment_id = Column(Integer, nullable=False)
    appointment_timestamp = Column(UTCDateTime6)
    appointment_timezone = Column(String(255))
    source = Column(String(255))
    location = Column(String(255))
    contact_number = Column(String(255))
    language = Column(String(255))
    cancellation_reason = Column(String(255))
    created_from_metric_id = Column(Integer, ForeignKey("genomic_appointment_event_metrics.id"))


event.listen(GenomicAppointmentEvent, 'before_insert', model_insert_listener)
event.listen(GenomicAppointmentEvent, 'before_update', model_update_listener)


class GenomicAppointmentEventMetrics(Base):
    """
    Used for storage GHR3 appointment metrics
    """

    __tablename__ = 'genomic_appointment_event_metrics'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    participant_id = Column(Integer, ForeignKey("participant.participant_id"), nullable=False, index=True)
    appointment_event = Column(JSON, nullable=False)
    module_type = Column(String(255), nullable=False)
    event_authored_time = Column(UTCDateTime6, nullable=False)
    event_type = Column(String(256), nullable=False)
    file_path = Column(String(512), index=True, nullable=False)
    reconcile_job_run_id = Column(Integer, ForeignKey("genomic_job_run.id"), nullable=True)


event.listen(GenomicAppointmentEventMetrics, 'before_insert', model_insert_listener)
event.listen(GenomicAppointmentEventMetrics, 'before_update', model_update_listener)


class GenomicAppointmentEventNotified(Base):
    """
    Used to record notifications sent for participants whose GRoR consent has changed
    """

    __tablename__ = 'genomic_appointment_event_notified'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    participant_id = Column(Integer, ForeignKey("participant.participant_id"), nullable=False, index=True)
    appointment_event_id = Column(Integer, ForeignKey("genomic_appointment_event.id"), nullable=False, index=True)


event.listen(GenomicAppointmentEventNotified, 'before_insert', model_insert_listener)
event.listen(GenomicAppointmentEventNotified, 'before_update', model_update_listener)


class GenomicGCROutreachEscalationNotified(Base):
    """
    Used to record notifications sent for participants when GCR outreach 14 day escalation email sent
    """

    __tablename__ = 'genomic_gcr_outreach_escalation_notified'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    participant_id = Column(Integer, ForeignKey("participant.participant_id"), nullable=False, index=True)
    message_sent = Column(SmallInteger, default=0)


event.listen(GenomicGCROutreachEscalationNotified, 'before_insert', model_insert_listener)
event.listen(GenomicGCROutreachEscalationNotified, 'before_update', model_update_listener)


class GenomicGcDataFile(Base):
    """
    Used for tracking genomic data files produced by the GCs
    """

    __tablename__ = 'genomic_gc_data_file'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    file_path = Column(String(255), nullable=False, index=True)
    gc_site_id = Column(String(64), nullable=False, index=True)
    bucket_name = Column(String(128), nullable=False, index=True)
    file_prefix = Column(String(128), nullable=True)
    file_name = Column(String(128), nullable=False)
    file_type = Column(String(128), nullable=False, index=True)  # everything after the first '.'

    # reconciliation process uses the identifier_* fields to match metrics records
    identifier_type = Column(String(128), index=True)  # sample_id for WGS; chipwellbarcode for Array
    identifier_value = Column(String(128), index=True)  # value to match the metric record
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)  # 0 is no, 1 is yes


event.listen(GenomicGcDataFile, 'before_insert', model_insert_listener)
event.listen(GenomicGcDataFile, 'before_update', model_update_listener)


class GenomicGcDataFileMissing(Base):
    """
    Used for tracking missing genomic data files produced by the GCs
    """

    __tablename__ = 'genomic_gc_data_file_missing'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    gc_site_id = Column(String(64), nullable=False, index=True)
    file_type = Column(String(128), nullable=False, index=True)  # gc data file extension
    run_id = Column(Integer, ForeignKey("genomic_job_run.id"), nullable=False)
    gc_validation_metric_id = Column(Integer, ForeignKey("genomic_gc_validation_metrics.id"), nullable=False)
    resolved = Column(SmallInteger, nullable=False, default=0)  # 0 is no, 1 is yes
    resolved_date = Column(DateTime, nullable=True)  # 0 is no, 1 is yes
    ignore_flag = Column(SmallInteger, nullable=False, default=0)  # 0 is no, 1 is yes


event.listen(GenomicGcDataFileMissing, 'before_insert', model_insert_listener)
event.listen(GenomicGcDataFileMissing, 'before_update', model_update_listener)


class GcDataFileStaging(Base):
    """
    Staging table for "GC data file reconciliation to table" job
    Cleared and reloaded every job run
    """

    __tablename__ = 'gc_data_file_staging'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    bucket_name = Column(String(128), nullable=False, index=True)
    file_path = Column(String(255), nullable=False, index=True)


class GemToGpMigration(Base):
    """
    Used for storing GEM to GP migration records
    """

    __tablename__ = 'gem_to_gp_migration'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)  # 0 is no, 1 is yes
    dev_note = Column(String(255), nullable=True)
    file_path = Column(String(255), nullable=True, index=True)
    run_id = Column(Integer, ForeignKey("genomic_job_run.id"))

    # Fields sent to GP
    participant_id = Column(Integer, nullable=True, index=True)
    informing_loop_status = Column(String(64), nullable=True)
    informing_loop_authored = Column(DateTime, index=True)
    ancestry_traits_response = Column(String(64), nullable=True, index=True)


event.listen(GemToGpMigration, 'before_insert', model_insert_listener)
event.listen(GemToGpMigration, 'before_update', model_update_listener)


class UserEventMetrics(Base):
    """
    Used for storage GHR3 user event metrics
    """

    __tablename__ = 'user_event_metrics'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    participant_id = Column(Integer, ForeignKey("participant.participant_id"), nullable=False, index=True)
    created_at = Column(String(255), index=True)
    event_name = Column(String(512), index=True)
    device = Column(String(255))
    operating_system = Column(String(255))
    browser = Column(String(255))
    file_path = Column(String(512), index=True)
    run_id = Column(Integer, ForeignKey("genomic_job_run.id"), nullable=False)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)
    reconcile_job_run_id = Column(Integer, ForeignKey("genomic_job_run.id"), nullable=True)


event.listen(UserEventMetrics, 'before_insert', model_insert_listener)
event.listen(UserEventMetrics, 'before_update', model_update_listener)


class GenomicSampleSwap(Base):
    """
    Used for storing sample swap types
    """

    __tablename__ = "genomic_sample_swap"

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    name = Column(String(255), nullable=False)
    description = Column(String(512))
    open_investigation = Column(SmallInteger, nullable=False, default=0)
    open_investigation_date = Column(DateTime, nullable=True)
    closed_investigation = Column(SmallInteger, nullable=False, default=0)
    closed_investigation_date = Column(DateTime, nullable=True)
    number = Column(SmallInteger)
    location = Column(String(512))
    ignore_flag = Column(SmallInteger, nullable=False, default=0)


event.listen(GenomicSampleSwap, 'before_insert', model_insert_listener)
event.listen(GenomicSampleSwap, 'before_update', model_update_listener)


class GenomicSampleSwapMember(Base):
    """
    Used for storing sample swap members
    """

    __tablename__ = "genomic_sample_swap_member"

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    genomic_sample_swap = Column(Integer, ForeignKey("genomic_sample_swap.id"), nullable=False)
    genomic_set_member_id = Column(Integer, ForeignKey('genomic_set_member.id'), nullable=False)
    category = Column(Enum(GenomicSampleSwapCategory), default=GenomicSampleSwapCategory.UNSET)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)


event.listen(GenomicSampleSwapMember, 'before_insert', model_insert_listener)
event.listen(GenomicSampleSwapMember, 'before_update', model_update_listener)


class GenomicCVLResultPastDue(Base):
    """
    Used for storing samples in need on reconciliation by CVLs
    """

    __tablename__ = "genomic_cvl_result_past_due"

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    genomic_set_member_id = Column(Integer, ForeignKey('genomic_set_member.id'), nullable=False)
    sample_id = Column(String(255), nullable=False, index=True)
    results_type = Column(String(128), nullable=False)
    cvl_site_id = Column(String(128), nullable=False, index=True)
    email_notification_sent = Column(SmallInteger, nullable=False, default=0)
    email_notification_sent_date = Column(DateTime, nullable=True)
    resolved = Column(SmallInteger, nullable=False, default=0)
    resolved_date = Column(DateTime, nullable=True)


event.listen(GenomicCVLResultPastDue, 'before_insert', model_insert_listener)
event.listen(GenomicCVLResultPastDue, 'before_update', model_update_listener)


class GenomicResultWithdrawals(Base):
    """
    Used for storing the samples in results pipeline
    that have been withdrawn
    """

    __tablename__ = "genomic_result_withdrawals"

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    participant_id = Column(Integer, ForeignKey("participant.participant_id"), nullable=False, index=True)
    array_results = Column(SmallInteger, nullable=False, default=0)
    cvl_results = Column(SmallInteger, nullable=False, default=0)


event.listen(GenomicResultWithdrawals, 'before_insert', model_insert_listener)
event.listen(GenomicResultWithdrawals, 'before_update', model_update_listener)


class GenomicStorageUpdate(Base):
    """
    Used for storing which genomic data files, in relation to metrics records,
    have had the storage class updated
    """

    __tablename__ = "genomic_storage_update"

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    metrics_id = Column(Integer, ForeignKey("genomic_gc_validation_metrics.id"), nullable=False, index=True)
    genome_type = Column(String(80), nullable=True)
    storage_class = Column(String(250), nullable=False)
    has_error = Column(SmallInteger, nullable=False, default=0)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)


event.listen(GenomicStorageUpdate, 'before_insert', model_insert_listener)
event.listen(GenomicStorageUpdate, 'before_update', model_update_listener)


class GenomicLongRead(Base):
    """
    Used for storing the member records that are being
    sequenced in the long read pipeline
    """

    __tablename__ = "genomic_long_read"

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    genomic_set_member_id = Column(Integer, ForeignKey("genomic_set_member.id"), nullable=True, index=True)
    biobank_id = Column(String(128), nullable=False, index=True)
    collection_tube_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(80), nullable=True, index=True)
    genome_type = Column(String(80), nullable=False, default='aou_long_read')
    lr_site_id = Column(String(11), nullable=False)
    long_read_platform = Column(Enum(GenomicLongReadPlatform), default=GenomicLongReadPlatform.UNSET)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)
    long_read_set = Column(Integer, nullable=False, default=0)
    created_job_run_id = Column(Integer, ForeignKey("genomic_job_run.id"), nullable=True)


event.listen(GenomicLongRead, 'before_insert', model_insert_listener)
event.listen(GenomicLongRead, 'before_update', model_update_listener)


class GenomicLRRaw(Base):
    """
    Raw data from LR files
    """
    __tablename__ = 'genomic_lr_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)
    parent_tube_id = Column(String(255), nullable=True)
    lr_site_id = Column(String(255), nullable=True)
    long_read_platform = Column(String(255), nullable=True)


event.listen(GenomicLRRaw, 'before_insert', model_insert_listener)
event.listen(GenomicLRRaw, 'before_update', model_update_listener)


class GenomicL0Raw(Base):
    """
    Raw data from L0 files
    """
    __tablename__ = 'genomic_l0_raw'

    id = Column('id', Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column('created', DateTime, nullable=True)
    modified = Column('modified', DateTime, nullable=True)

    file_path = Column('file_path', String(255), nullable=True, index=True)
    ignore_flag = Column('ignore_flag', SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True)
    collection_tube_id = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)
    ny_flag = Column(String(255), nullable=True)
    validation_passed = Column(String(255), nullable=True)
    ai_an = Column(String(255), nullable=True)
    parent_tube_id = Column(String(255), nullable=True)
    lr_site_id = Column(String(255), nullable=True)
    long_read_platform = Column(String(255), nullable=True)


event.listen(GenomicL0Raw, 'before_insert', model_insert_listener)
event.listen(GenomicL0Raw, 'before_update', model_update_listener)


class GenomicL1Raw(Base):
    """
    Raw Data from L1 files
    """
    __tablename__ = 'genomic_l1_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    package_id = Column(String(255), nullable=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    box_storageunit_id = Column(String(255), nullable=True)
    box_id_plate_id = Column(String(255), nullable=True)
    well_position = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True, index=True)
    parent_sample_id = Column(String(255), nullable=True, index=True)
    collection_tubeid = Column(String(255), nullable=True, index=True)
    matrix_id = Column(String(255), nullable=True)
    collection_date = Column(String(255), nullable=True)
    biobank_id = Column(String(255), nullable=True, index=True)
    sex_at_birth = Column(String(255), nullable=True)
    age = Column(String(255), nullable=True)
    ny_state_y_n = Column(String(255), nullable=True)
    sample_type = Column(String(255), nullable=True)
    treatments = Column(String(255), nullable=True)
    quantity_ul = Column(String(255), nullable=True)
    total_concentration_ng_ul = Column(String(255), nullable=True)
    total_dna_ng = Column(String(255), nullable=True)
    visit_description = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    study = Column(String(255), nullable=True)
    tracking_number = Column(String(255), nullable=True)
    contact = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    study_pi = Column(String(255), nullable=True)
    genome_type = Column(String(80), nullable=True, index=True)
    lr_site_id = Column(String(80), nullable=True, index=True)
    long_read_platform = Column(String(80), nullable=True, index=True)
    failure_mode = Column(String(255), nullable=True)
    failure_mode_desc = Column(String(255), nullable=True)


event.listen(GenomicL1Raw, 'before_insert', model_insert_listener)
event.listen(GenomicL1Raw, 'before_update', model_update_listener)


class GenomicL1FRaw(Base):
    """
    Raw Data from L1D files
    """
    __tablename__ = 'genomic_l1f_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    package_id = Column(String(255), nullable=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    box_storageunit_id = Column(String(255), nullable=True)
    box_id_plate_id = Column(String(255), nullable=True)
    well_position = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True, index=True)
    parent_sample_id = Column(String(255), nullable=True)
    collection_tubeid = Column(String(255), nullable=True)
    matrix_id = Column(String(255), nullable=True)
    collection_date = Column(String(255), nullable=True)
    biobank_id = Column(String(255), nullable=True, index=True)
    sex_at_birth = Column(String(255), nullable=True)
    age = Column(String(255), nullable=True)
    ny_state_y_n = Column(String(255), nullable=True)
    sample_type = Column(String(255), nullable=True)
    treatments = Column(String(255), nullable=True)
    quantity_ul = Column(String(255), nullable=True)
    total_concentration_ng_ul = Column(String(255), nullable=True)
    total_dna_ng = Column(String(255), nullable=True)
    visit_description = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    study = Column(String(255), nullable=True)
    tracking_number = Column(String(255), nullable=True)
    contact = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    study_pi = Column(String(255), nullable=True)
    genome_type = Column(String(80), nullable=True)
    lr_site_id = Column(String(80), nullable=True)
    long_read_platform = Column(String(80), nullable=True)
    failure_mode = Column(String(255), nullable=True)
    failure_mode_desc = Column(String(255), nullable=True)


event.listen(GenomicL1FRaw, 'before_insert', model_insert_listener)
event.listen(GenomicL1FRaw, 'before_update', model_update_listener)


class GenomicL2ONTRaw(Base):
    """
    Raw Data from L2 ONT files
    """
    __tablename__ = 'genomic_l2_ont_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    flowcell_id = Column(String(255), nullable=True)
    basecaller_version = Column(String(255), nullable=True)
    basecaller_model = Column(String(255), nullable=True)
    bam_path = Column(String(255), nullable=True)
    long_read_platform = Column(String(255), nullable=True)
    barcode = Column(String(255), nullable=True)
    lims_id = Column(String(255), nullable=True)
    processing_status = Column(String(255), nullable=True)
    translocation_speed = Column(String(255), nullable=True)
    minimum_read_length = Column(String(255), nullable=True)
    mapped_reads_pct = Column(String(255), nullable=True)
    mean_coverage = Column(String(255), nullable=True)
    genome_coverage = Column(String(255), nullable=True)
    read_error_rate = Column(String(255), nullable=True)
    read_length_n50 = Column(String(255), nullable=True)
    mean_read_quality = Column(String(255), nullable=True)
    aligned_q10_bases = Column(String(255), nullable=True)
    contamination = Column(String(255), nullable=True)
    array_concordance = Column(String(255), nullable=True)
    sex_concordance = Column(String(255), nullable=True)
    sex_ploidy = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)


event.listen(GenomicL2ONTRaw, 'before_insert', model_insert_listener)
event.listen(GenomicL2ONTRaw, 'before_update', model_update_listener)


class GenomicL2PBCCSRaw(Base):
    """
    Raw Data from L2 PB CCS files
    """
    __tablename__ = 'genomic_l2_pb_ccs_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    lims_id = Column(String(255), nullable=True)
    aggregation_level = Column(String(255), nullable=True)
    flowcell_id = Column(String(255), nullable=True)
    barcode = Column(String(255), nullable=True)
    mean_coverage = Column(String(255), nullable=True)
    genome_coverage = Column(String(255), nullable=True)
    contamination = Column(String(255), nullable=True)
    sex_concordance = Column(String(255), nullable=True)
    sex_ploidy = Column(String(255), nullable=True)
    aligned_hifi_bases = Column(String(255), nullable=True)
    read_error_rate = Column(String(255), nullable=True)
    num_hifi_reads = Column(String(255), nullable=True)
    read_length_mean = Column(String(255), nullable=True)
    array_concordance = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    mapped_reads_pct = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)
    processing_status = Column(String(255), nullable=True)
    bam_path = Column(String(255), nullable=True)
    long_read_platform = Column(String(255), nullable=True)
    instrument = Column(String(255), nullable=True)
    smrtlink_server_version = Column(String(255), nullable=True)
    instrument_ics_version = Column(String(255), nullable=True)


event.listen(GenomicL2PBCCSRaw, 'before_insert', model_insert_listener)
event.listen(GenomicL2PBCCSRaw, 'before_update', model_update_listener)


class GenomicL3Raw(Base):
    """
    Raw Data from L3 files
    """
    __tablename__ = 'genomic_l3_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    flowcell_id = Column(String(255), nullable=True)
    barcode = Column(String(255), nullable=True)
    long_read_platform = Column(String(255), nullable=True)
    bam_path = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    lr_site_id = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    gc_processing_status = Column(String(255), nullable=True)
    fragment_length = Column(String(255), nullable=True)
    pacbio_instrument_type = Column(String(255), nullable=True)
    smrtlink_server_version = Column(String(255), nullable=True)
    pacbio_instrument_ics_version = Column(String(255), nullable=True)
    gc_read_error_rate = Column(String(255), nullable=True)
    gc_mean_coverage = Column(String(255), nullable=True)
    gc_genome_coverage = Column(String(255), nullable=True)
    gc_contamination = Column(String(255), nullable=True)
    ont_basecaller_version = Column(String(255), nullable=True)
    ont_basecaller_model = Column(String(255), nullable=True)
    ont_mean_read_qual = Column(String(255), nullable=True)


event.listen(GenomicL3Raw, 'before_insert', model_insert_listener)
event.listen(GenomicL3Raw, 'before_update', model_update_listener)


class GenomicL4Raw(Base):
    """
    Raw Data from L4 files
    """
    __tablename__ = 'genomic_l4_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    aggregation_level = Column(String(255), nullable=True)
    flowcell_id = Column(String(255), nullable=True)
    barcode = Column(String(255), nullable=True)
    lr_site_id = Column(String(255), nullable=True)
    long_read_platform = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    bam_path = Column(String(255), nullable=True)
    drc_contamination = Column(String(255), nullable=True)
    drc_sex_concordance = Column(String(255), nullable=True)
    drc_array_concordance = Column(String(255), nullable=True)
    drc_mean_coverage = Column(String(255), nullable=True)
    drc_processing_status = Column(String(255), nullable=True)
    drc_failure_mode = Column(String(255), nullable=True)
    drc_failure_mode_desc = Column(String(255), nullable=True)
    drc_processing_count = Column(String(255), nullable=True)
    pass_to_research_pipeline = Column(String(255), nullable=True)


event.listen(GenomicL4Raw, 'before_insert', model_insert_listener)
event.listen(GenomicL4Raw, 'before_update', model_update_listener)


class GenomicL4FRaw(Base):
    """
    Raw Data from L4F files
    """
    __tablename__ = 'genomic_l4f_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    aggregation_level = Column(String(255), nullable=True)
    flowcell_id = Column(String(255), nullable=True)
    barcode = Column(String(255), nullable=True)
    lr_site_id = Column(String(255), nullable=True)
    long_read_platform = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    bam_path = Column(String(255), nullable=True)
    drc_contamination = Column(String(255), nullable=True)
    drc_sex_concordance = Column(String(255), nullable=True)
    drc_array_concordance = Column(String(255), nullable=True)
    drc_mean_coverage = Column(String(255), nullable=True)
    drc_processing_status = Column(String(255), nullable=True)
    drc_failure_mode = Column(String(255), nullable=True)
    drc_failure_mode_desc = Column(String(255), nullable=True)
    drc_processing_count = Column(String(255), nullable=True)
    pass_to_research_pipeline = Column(String(255), nullable=True)


event.listen(GenomicL4FRaw, 'before_insert', model_insert_listener)
event.listen(GenomicL4FRaw, 'before_update', model_update_listener)


class GenomicL5Raw(Base):
    """
    Raw Data from L5 files
    """
    __tablename__ = 'genomic_l5_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    flowcell_id = Column(String(255), nullable=True)
    barcode = Column(String(255), nullable=True)
    long_read_platform = Column(String(255), nullable=True)
    lr_site_id = Column(String(255), nullable=True)


event.listen(GenomicL5Raw, 'before_insert', model_insert_listener)
event.listen(GenomicL5Raw, 'before_update', model_update_listener)


class GenomicL6Raw(Base):
    """
    Raw Data from L6 files
    """
    __tablename__ = 'genomic_l6_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    long_read_platform = Column(String(255), nullable=True)
    lr_site_id = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    drc_contamination = Column(String(255), nullable=True)
    drc_sex_concordance = Column(String(255), nullable=True)
    drc_array_concordance = Column(String(255), nullable=True)
    drc_mean_coverage = Column(String(255), nullable=True)
    drc_processing_status = Column(String(255), nullable=True)
    drc_failure_mode = Column(String(255), nullable=True)
    drc_failure_mode_desc = Column(String(255), nullable=True)
    drc_processing_count = Column(String(255), nullable=True)
    pass_to_research_pipeline = Column(String(255), nullable=True)


event.listen(GenomicL6Raw, 'before_insert', model_insert_listener)
event.listen(GenomicL6Raw, 'before_update', model_update_listener)


class GenomicL6FRaw(Base):
    """
    Raw Data from L6F files
    """
    __tablename__ = 'genomic_l6f_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    long_read_platform = Column(String(255), nullable=True)
    lr_site_id = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    drc_contamination = Column(String(255), nullable=True)
    drc_sex_concordance = Column(String(255), nullable=True)
    drc_array_concordance = Column(String(255), nullable=True)
    drc_mean_coverage = Column(String(255), nullable=True)
    drc_processing_status = Column(String(255), nullable=True)
    drc_failure_mode = Column(String(255), nullable=True)
    drc_failure_mode_desc = Column(String(255), nullable=True)
    drc_processing_count = Column(String(255), nullable=True)
    pass_to_research_pipeline = Column(String(255), nullable=True)


event.listen(GenomicL6FRaw, 'before_insert', model_insert_listener)
event.listen(GenomicL6FRaw, 'before_update', model_update_listener)


class GenomicProteomics(Base):
    """
    Used for storing the member records that are being
    sequenced in the Proteomics pipeline
    """

    __tablename__ = "genomic_proteomics"

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    genomic_set_member_id = Column(Integer, ForeignKey("genomic_set_member.id"), nullable=False, index=True)
    biobank_id = Column(String(128), nullable=False, index=True)
    collection_tube_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(80), nullable=True, index=True)
    genome_type = Column(String(80), nullable=False)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)
    p_site_id = Column(String(255), nullable=True)
    proteomics_set = Column(Integer, nullable=False, default=0)
    created_job_run_id = Column(Integer, ForeignKey("genomic_job_run.id"), nullable=True)


event.listen(GenomicProteomics, 'before_insert', model_insert_listener)
event.listen(GenomicProteomics, 'before_update', model_update_listener)


class GenomicPRRaw(Base):
    """
    Raw data from PR files
    """
    __tablename__ = 'genomic_pr_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)
    p_site_id = Column(String(255), nullable=True)


event.listen(GenomicPRRaw, 'before_insert', model_insert_listener)
event.listen(GenomicPRRaw, 'before_update', model_update_listener)


class GenomicP0Raw(Base):
    """
    Raw data from P0 files
    """
    __tablename__ = 'genomic_p0_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True)
    collection_tube_id = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)
    ny_flag = Column(String(255), nullable=True)
    validation_passed = Column(String(255), nullable=True)
    ai_an = Column(String(255), nullable=True)
    p_site_id = Column(String(255), nullable=True)


event.listen(GenomicP0Raw, 'before_insert', model_insert_listener)
event.listen(GenomicP0Raw, 'before_update', model_update_listener)


class GenomicP1Raw(Base):
    """
    Raw Data from P1 files
    """
    __tablename__ = 'genomic_p1_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    package_id = Column(String(255), nullable=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    box_storageunit_id = Column(String(255), nullable=True)
    box_id_plate_id = Column(String(255), nullable=True)
    well_position = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True, index=True)
    parent_sample_id = Column(String(255), nullable=True, index=True)
    collection_tubeid = Column(String(255), nullable=True, index=True)
    matrix_id = Column(String(255), nullable=True)
    collection_date = Column(String(255), nullable=True)
    biobank_id = Column(String(255), nullable=True, index=True)
    sex_at_birth = Column(String(255), nullable=True)
    age = Column(String(255), nullable=True)
    ny_state_y_n = Column(String(255), nullable=True)
    sample_type = Column(String(255), nullable=True)
    treatments = Column(String(255), nullable=True)
    quantity_ul = Column(String(255), nullable=True)
    visit_description = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    study = Column(String(255), nullable=True)
    tracking_number = Column(String(255), nullable=True)
    contact = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    study_pi = Column(String(255), nullable=True)
    site_name = Column(String(255), nullable=True, index=True)
    failure_mode = Column(String(255), nullable=True)
    failure_mode_desc = Column(String(255), nullable=True)
    genome_type = Column(String(80), nullable=True, index=True)


event.listen(GenomicP1Raw, 'before_insert', model_insert_listener)
event.listen(GenomicP1Raw, 'before_update', model_update_listener)


class GenomicP1FRaw(Base):
    """
    Raw Data from P1F files
    """
    __tablename__ = 'genomic_p1f_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    package_id = Column(String(255), nullable=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    box_storageunit_id = Column(String(255), nullable=True)
    box_id_plate_id = Column(String(255), nullable=True)
    well_position = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True, index=True)
    parent_sample_id = Column(String(255), nullable=True, index=True)
    collection_tubeid = Column(String(255), nullable=True, index=True)
    matrix_id = Column(String(255), nullable=True)
    collection_date = Column(String(255), nullable=True)
    biobank_id = Column(String(255), nullable=True, index=True)
    sex_at_birth = Column(String(255), nullable=True)
    age = Column(String(255), nullable=True)
    ny_state_y_n = Column(String(255), nullable=True)
    sample_type = Column(String(255), nullable=True)
    treatments = Column(String(255), nullable=True)
    quantity_ul = Column(String(255), nullable=True)
    visit_description = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    study = Column(String(255), nullable=True)
    tracking_number = Column(String(255), nullable=True)
    contact = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    study_pi = Column(String(255), nullable=True)
    site_name = Column(String(255), nullable=True, index=True)
    failure_mode = Column(String(255), nullable=True)
    failure_mode_desc = Column(String(255), nullable=True)
    genome_type = Column(String(80), nullable=True, index=True)


event.listen(GenomicP1FRaw, 'before_insert', model_insert_listener)
event.listen(GenomicP1FRaw, 'before_update', model_update_listener)


class GenomicP2Raw(Base):
    """
    Raw Data from P2 files
    """
    __tablename__ = 'genomic_p2_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    lims_id = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)
    software_version = Column(String(255), nullable=True)
    npx_explore_path = Column(String(255), nullable=True)
    analysis_report_path = Column(String(255), nullable=True)
    kit_type = Column(String(255), nullable=True)
    notes = Column(String(1028), nullable=True)


event.listen(GenomicP2Raw, 'before_insert', model_insert_listener)
event.listen(GenomicP2Raw, 'before_update', model_update_listener)


class GenomicP3Raw(Base):
    """
    Raw Data from P3 files
    """
    __tablename__ = 'genomic_p3_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(1028), nullable=True)
    research_id = Column(String(1028), nullable=True)
    lims_id = Column(String(1028), nullable=True)
    sex_at_birth = Column(String(1028), nullable=True)
    site_id = Column(String(1028), nullable=True)
    sample_source = Column(String(1028), nullable=True)
    genome_type = Column(String(1028), nullable=True)
    ai_an = Column(String(1028), nullable=True)
    processing_count = Column(String(1028), nullable=True)
    software_version = Column(String(1028), nullable=True)
    npx_explore_path = Column(String(1028), nullable=True)
    analysis_report_path = Column(String(1028), nullable=True)
    kit_type = Column(String(1028), nullable=True)
    notes = Column(String(1028), nullable=True)


event.listen(GenomicP3Raw, 'before_insert', model_insert_listener)
event.listen(GenomicP3Raw, 'before_update', model_update_listener)


class GenomicP4Raw(Base):
    """
    Raw Data from P4 files
    """
    __tablename__ = 'genomic_p4_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    site_id = Column(String(1028), nullable=True)
    research_id = Column(String(1028), nullable=True)
    drc_qc_status = Column(String(1028), nullable=True)
    pass_to_research_pipeline = Column(String(1028), nullable=True)
    processing_count = Column(String(1028), nullable=True)
    sample_source = Column(String(1028), nullable=True)
    genome_type = Column(String(1028), nullable=True)
    software_version = Column(String(1028), nullable=True)
    npx_explore_path = Column(String(1028), nullable=True)
    analysis_report_path = Column(String(1028), nullable=True)
    kit_type = Column(String(1028), nullable=True)
    notes = Column(String(1028), nullable=True)


event.listen(GenomicP4Raw, 'before_insert', model_insert_listener)
event.listen(GenomicP4Raw, 'before_update', model_update_listener)


class GenomicP5Raw(Base):
    """
    Raw Data from P5 files
    """
    __tablename__ = 'genomic_p5_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    site_id = Column(String(255), nullable=True)
    npx_explore_path = Column(String(255), nullable=True)
    analysis_report_path = Column(String(255), nullable=True)


event.listen(GenomicP5Raw, 'before_insert', model_insert_listener)
event.listen(GenomicP5Raw, 'before_update', model_update_listener)


class GenomicRNA(Base):
    """
    Used for storing the member records that are being
    sequenced in the Proteomics pipeline
    """

    __tablename__ = "genomic_rna"

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime)
    modified = Column(DateTime)
    genomic_set_member_id = Column(Integer, ForeignKey("genomic_set_member.id"), nullable=False, index=True)
    biobank_id = Column(String(128), nullable=False, index=True)
    collection_tube_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(80), nullable=True, index=True)
    genome_type = Column(String(80), nullable=False)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)
    r_site_id = Column(String(255), nullable=True)
    rna_set = Column(Integer, nullable=False, default=0)
    created_job_run_id = Column(Integer, ForeignKey("genomic_job_run.id"), nullable=True)


event.listen(GenomicRNA, 'before_insert', model_insert_listener)
event.listen(GenomicRNA, 'before_update', model_update_listener)


class GenomicRRRaw(Base):
    """
    Raw data from RR files
    """
    __tablename__ = 'genomic_rr_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)
    r_site_id = Column(String(255), nullable=True)


event.listen(GenomicRRRaw, 'before_insert', model_insert_listener)
event.listen(GenomicRRRaw, 'before_update', model_update_listener)


class GenomicR0Raw(Base):
    """
    Raw data from R0 files
    """
    __tablename__ = 'genomic_r0_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True)
    collection_tube_id = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)
    ny_flag = Column(String(255), nullable=True)
    validation_passed = Column(String(255), nullable=True)
    ai_an = Column(String(255), nullable=True)
    r_site_id = Column(String(255), nullable=True)


event.listen(GenomicR0Raw, 'before_insert', model_insert_listener)
event.listen(GenomicR0Raw, 'before_update', model_update_listener)


class GenomicR1Raw(Base):
    """
    Raw Data from R1 files
    """
    __tablename__ = 'genomic_r1_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    package_id = Column(String(255), nullable=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    box_storageunit_id = Column(String(255), nullable=True)
    box_id_plate_id = Column(String(255), nullable=True)
    well_position = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True, index=True)
    parent_sample_id = Column(String(255), nullable=True, index=True)
    collection_tubeid = Column(String(255), nullable=True, index=True)
    matrix_id = Column(String(255), nullable=True)
    collection_date = Column(String(255), nullable=True)
    biobank_id = Column(String(255), nullable=True, index=True)
    sex_at_birth = Column(String(255), nullable=True)
    age = Column(String(255), nullable=True)
    ny_state_y_n = Column(String(255), nullable=True)
    sample_type = Column(String(255), nullable=True)
    treatments = Column(String(255), nullable=True)
    quantity_ul = Column(String(255), nullable=True)
    total_concentration_ng_ul = Column(String(255), nullable=True)
    total_yield_ng = Column(String(255), nullable=True)
    rqs = Column(String(255), nullable=True)
    two_sixty_two_thirty = Column(String(255), nullable=True)
    two_sixty_two_eighty = Column(String(255), nullable=True)
    visit_description = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    study = Column(String(255), nullable=True)
    tracking_number = Column(String(255), nullable=True)
    contact = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    study_pi = Column(String(255), nullable=True)
    site_name = Column(String(255), nullable=True, index=True)
    failure_mode = Column(String(255), nullable=True)
    failure_mode_desc = Column(String(255), nullable=True)
    genome_type = Column(String(80), nullable=True, index=True)


event.listen(GenomicR1Raw, 'before_insert', model_insert_listener)
event.listen(GenomicR1Raw, 'before_update', model_update_listener)


class GenomicR2Raw(Base):
    """
    Raw Data from R2 files
    """
    __tablename__ = 'genomic_r2_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True, index=True)
    sample_id = Column(String(255), nullable=True, index=True)
    biobankid_sampleid = Column(String(255), nullable=True)
    lims_id = Column(String(255), nullable=True)
    sample_source = Column(String(255), nullable=True)
    alignment_rate_pct = Column(String(255), nullable=True)
    duplication_pct = Column(String(255), nullable=True)
    mrna_bases_pct = Column(String(255), nullable=True)
    reads_aligned_in_pairs = Column(String(255), nullable=True)
    ribosomal_bases_pct = Column(String(255), nullable=True)
    median_cv_coverage = Column(String(255), nullable=True)
    mean_insert_size = Column(String(255), nullable=True)
    rqs = Column(String(255), nullable=True)
    genome_type = Column(String(255), nullable=True)
    processing_status = Column(String(255), nullable=True)
    pipeline_id = Column(String(255), nullable=True)
    cram_path = Column(String(255), nullable=True)
    cram_md5_path = Column(String(255), nullable=True)
    notes = Column(String(1028), nullable=True)


event.listen(GenomicR2Raw, 'before_insert', model_insert_listener)
event.listen(GenomicR2Raw, 'before_update', model_update_listener)


class GenomicA1Raw(Base):
    """
    Raw data from A1 files
    """
    __tablename__ = 'genomic_a1_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    sex_at_birth = Column(String(255), nullable=True)
    consent_for_ror = Column(String(255), nullable=True)
    chipwellbarcode = Column(String(255), nullable=True)
    genome_center = Column(String(255), nullable=True)


event.listen(GenomicA1Raw, 'before_insert', model_insert_listener)
event.listen(GenomicA1Raw, 'before_update', model_update_listener)


class GenomicA2Raw(Base):
    """
    Raw data from A2 files
    """
    __tablename__ = 'genomic_a2_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    success = Column(String(255), nullable=True)
    date_of_import = Column(String(255), nullable=True)


event.listen(GenomicA2Raw, 'before_insert', model_insert_listener)
event.listen(GenomicA2Raw, 'before_update', model_update_listener)


class GenomicA3Raw(Base):
    """
    Raw data from A3 files
    """
    __tablename__ = 'genomic_a3_raw'

    id = Column(Integer,
                primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=True)
    modified = Column(DateTime, nullable=True)

    file_path = Column(String(255), nullable=True, index=True)
    ignore_flag = Column(SmallInteger, nullable=False, default=0)

    biobank_id = Column(String(255), nullable=True)
    sample_id = Column(String(255), nullable=True)
    date_of_consent_removal = Column(String(255), nullable=True)


event.listen(GenomicA3Raw, 'before_insert', model_insert_listener)
event.listen(GenomicA3Raw, 'before_update', model_update_listener)
