#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from enum import Enum

from rdr_service.genomic_enums import GenomicSetStatus as _GenomicSetStatus, \
    GenomicSetMemberStatus as _GenomicSetMemberStatus, GenomicValidationFlag as _GenomicValidationFlag, \
    GenomicJob as _GenomicJob, GenomicWorkflowState as _GenomicWorkflowState, \
    GenomicSubProcessStatus as _GenomicSubProcessStatus, GenomicSubProcessResult as _GenomicSubProcessResult, \
    GenomicManifestTypes as _GenomicManifestTypes, GenomicContaminationCategory as _GenomicContaminationCategory, \
    GenomicQcStatus as _GenomicQcStatus
from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum

# Convert weird participant_enums to standard python enums.
GenomicSetStatusEnum = Enum('GenomicSetStatusEnum', _GenomicSetStatus.to_dict())
GenomicSetMemberStatusEnum = Enum('GenomicSetMemberStatusEnum', _GenomicSetMemberStatus.to_dict())
GenomicValidationFlag = Enum('GenomicValidationFlag', _GenomicValidationFlag.to_dict())
GenomicSubProcessStatusEnum = Enum('GenomicSubProcessStatusEnum', _GenomicSubProcessStatus.to_dict())
GenomicSubProcessResultEnum = Enum('GenomicSubProcessResultEnum', _GenomicSubProcessResult.to_dict())
GenomicJobEnum = Enum('GenomicJobEnum', _GenomicJob.to_dict())
GenomicWorkflowStateEnum = Enum('GenomicWorkflowStateEnum', _GenomicWorkflowState.to_dict())
GenomicQcStatusEnum = Enum('GenomicQcStatusEnum', _GenomicQcStatus.to_dict())
GenomicContaminationCategoryEnum = Enum('GenomicContaminationCategoryEnum', _GenomicContaminationCategory.to_dict())
GenomicManifestTypesEnum = Enum('GenomicManifestTypesEnum', _GenomicManifestTypes.to_dict())


class BQGenomicSetSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    # PDR-149:  Need to preserve the RDR table id values
    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    orig_created = BQField('orig_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    orig_modified = BQField('orig_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    genomic_set_name = BQField('genomic_set_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    genomic_set_criteria = BQField('genomic_set_criteria', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    genomic_set_version = BQField('genomic_set_version', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    genomic_set_file = BQField('genomic_set_file', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    genomic_set_file_time = BQField('genomic_set_file_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    genomic_set_status = BQField('genomic_set_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE,
                                 fld_enum=GenomicSetStatusEnum)
    genomic_set_status_id = BQField('genomic_set_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                                 fld_enum=GenomicSetStatusEnum)
    validated_time = BQField('validated_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)


class BQGenomicSet(BQTable):
    """  BigQuery Table """
    __tablename__ = 'genomic_set'
    __schema__ = BQGenomicSetSchema


class BQGenomicSetView(BQView):
    __viewname__ = 'v_genomic_set'
    __viewdescr__ = 'Genomic Set View'
    __pk_id__ = 'id'
    __table__ = BQGenomicSet


class BQGenomicSetMemberSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    # PDR-149:  Need to preserve the RDR table id values
    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    orig_created = BQField('orig_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    orig_modified = BQField('orig_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    genomic_set_id = BQField('genomic_set_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    participant_id = BQField('participant_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ny_flag = BQField('ny_flag', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    sex_at_birth = BQField('sex_at_birth', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    genome_type = BQField('genome_type', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    biobank_id = BQField('biobank_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    biobank_id_str = BQField('biobank_id_str', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    package_id = BQField('package_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    validation_status = BQField('validation_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE,
                                fld_enum=GenomicSetMemberStatusEnum)
    validation_status_id = BQField('validation_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                                fld_enum=GenomicSetMemberStatusEnum)
    # validation_flags is an array of GenomicValidationFlag Enum values.
    validation_flags = BQField('validation_flags', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    validated_time = BQField('validated_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    sample_id = BQField('sample_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sample_type = BQField('sample_type', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    reconcile_cvl_job_run_id = BQField('reconcile_cvl_job_run_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    sequencing_file_name = BQField('sequencing_file_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    reconcile_gc_manifest_job_run_id = BQField('reconcile_gc_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                               BQFieldModeEnum.NULLABLE)
    reconcile_metrics_bb_manifest_job_run_id = BQField('reconcile_metrics_bb_manifest_job_run_id',
                                                       BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    reconcile_metrics_sequencing_job_run_id = BQField('reconcile_metrics_sequencing_job_run_id',
                                                      BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ai_an = BQField('ai_an', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_manifest_box_plate_id = BQField('gc_manifest_box_plate_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_manifest_box_storage_unit_id = BQField('gc_manifest_box_storage_unit_id', BQFieldTypeEnum.STRING,
                                              BQFieldModeEnum.NULLABLE)
    gc_manifest_contact = BQField('gc_manifest_contact', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_manifest_email = BQField('gc_manifest_email', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_manifest_failure_description = BQField('gc_manifest_failure_description', BQFieldTypeEnum.STRING,
                                              BQFieldModeEnum.NULLABLE)
    gc_manifest_failure_mode = BQField('gc_manifest_failure_mode', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_manifest_matrix_id = BQField('gc_manifest_matrix_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_manifest_parent_sample_id = BQField('gc_manifest_parent_sample_id', BQFieldTypeEnum.STRING,
                                           BQFieldModeEnum.NULLABLE)
    gc_manifest_quantity_ul = BQField('gc_manifest_quantity_ul', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    gc_manifest_sample_source = BQField('gc_manifest_sample_source', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_manifest_study = BQField('gc_manifest_study', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_manifest_study_pi = BQField('gc_manifest_study_pi', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_manifest_test_name = BQField('gc_manifest_test_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_manifest_total_concentration_ng_per_ul = BQField('gc_manifest_total_concentration_ng_per_ul',
                                                        BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    gc_manifest_total_dna_ng = BQField('gc_manifest_total_dna_ng', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    gc_manifest_tracking_number = BQField('gc_manifest_tracking_number', BQFieldTypeEnum.STRING,
                                          BQFieldModeEnum.NULLABLE)
    gc_manifest_treatments = BQField('gc_manifest_treatments', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_manifest_visit_description = BQField('gc_manifest_visit_description', BQFieldTypeEnum.STRING,
                                            BQFieldModeEnum.NULLABLE)
    gc_manifest_well_position = BQField('gc_manifest_well_position', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gem_a1_manifest_job_run_id = BQField('gem_a1_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    gem_a2_manifest_job_run_id = BQField('gem_a2_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    gem_pass = BQField('gem_pass', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gem_a3_manifest_job_run_id = BQField('gem_a3_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    aw3_manifest_job_run_id = BQField('aw3_manifest_job_run_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    aw4_manifest_job_run_id = BQField('aw4_manifest_job_run_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    cvl_aw1c_manifest_job_run_id = BQField('cvl_aw1c_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                           BQFieldModeEnum.NULLABLE)
    cvl_aw1cf_manifest_job_run_id = BQField('cvl_aw1cf_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                            BQFieldModeEnum.NULLABLE)
    cvl_w1_manifest_job_run_id = BQField('cvl_w1_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    cvl_w2_manifest_job_run_id = BQField('cvl_w2_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    cvl_w3_manifest_job_run_id = BQField('cvl_w3_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    cvl_w4_manifest_job_run_id = BQField('cvl_w4_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    cvl_w4f_manifest_job_run_id = BQField('cvl_w4f_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                          BQFieldModeEnum.NULLABLE)
    genomic_workflow_state = BQField('genomic_workflow_state', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE,
                                     fld_enum=GenomicWorkflowStateEnum)
    genomic_workflow_state_id = BQField('genomic_workflow_state_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                                     fld_enum=GenomicWorkflowStateEnum)
    genomic_workflow_state_modified_time = BQField('genomic_workflow_state_modified_time', BQFieldTypeEnum.DATETIME,
                                                   BQFieldModeEnum.NULLABLE)
    collection_tube_id = BQField('collection_tube_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gc_site_id = BQField('gc_site_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    report_consent_removal_date = BQField('report_consent_removal_date',
                                          BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    qc_status = BQField('qc_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE,
                        fld_enum=GenomicQcStatusEnum)
    qc_status_id = BQField('qc_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                           fld_enum=GenomicQcStatusEnum)
    fingerprint_path = BQField('fingerprint_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    aw1_file_processed_id = BQField('aw1_file_processed_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    aw2_file_processed_id = BQField('aw2_file_processed_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    dev_note = BQField('dev_note', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    aw2f_job_run_id = BQField('aw2f_job_run_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    aw0_manifest_file_id = BQField('aw0_manifest_file_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    aw2f_manifest_job_run_id = BQField('aw2f_manifest_job_run_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    aw3_manifest_file_id = BQField('aw3_manifest_file_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    block_research = BQField('block_research', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    block_research_reason = BQField('block_research_reason', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    block_results = BQField('block_results', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    block_results_reason = BQField('block_results_reason', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    color_metrics_job_run_id = BQField('color_metrics_job_run_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    cvl_secondary_conf_failure = BQField('cvl_secondary_conf_failure', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    cvl_w1il_hdr_job_run_id = BQField('cvl_w1il_hdr_job_run_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    cvl_w1il_pgx_job_run_id = BQField('cvl_w1il_pgx_job_run_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    cvl_w2sc_manifest_job_run_id = BQField('cvl_w2sc_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                           BQFieldModeEnum.NULLABLE)
    cvl_w2w_job_run_id = BQField('cvl_w2w_job_run_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    cvl_w3ns_manifest_job_run_id = BQField('cvl_w3ns_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                           BQFieldModeEnum.NULLABLE)
    cvl_w3sc_manifest_job_run_id = BQField('cvl_w3sc_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                           BQFieldModeEnum.NULLABLE)
    cvl_w3sr_manifest_job_run_id = BQField('cvl_w3sr_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                           BQFieldModeEnum.NULLABLE)
    cvl_w3ss_manifest_job_run_id = BQField('cvl_w3ss_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                           BQFieldModeEnum.NULLABLE)
    cvl_w4wr_hdr_manifest_job_run_id = BQField('cvl_w4wr_hdr_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                               BQFieldModeEnum.NULLABLE)
    cvl_w4wr_pgx_manifest_job_run_id = BQField('cvl_w4wr_pgx_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                               BQFieldModeEnum.NULLABLE)
    cvl_w5nf_hdr_manifest_job_run_id = BQField('cvl_w5nf_hdr_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                               BQFieldModeEnum.NULLABLE)
    cvl_w5nf_pgx_manifest_job_run_id = BQField('cvl_w5nf_pgx_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                               BQFieldModeEnum.NULLABLE)
    diversion_pouch_site_flag = BQField('diversion_pouch_site_flag', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    gem_date_of_import = BQField('gem_date_of_import', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    gem_metrics_ancestry_loop_response = BQField('gem_metrics_ancestry_loop_response', BQFieldTypeEnum.STRING,
                                                 BQFieldModeEnum.NULLABLE)
    gem_metrics_available_results = BQField('gem_metrics_available_results', BQFieldTypeEnum.STRING,
                                            BQFieldModeEnum.NULLABLE)
    gem_metrics_results_released_at = BQField('gem_metrics_results_released_at', BQFieldTypeEnum.INTEGER,
                                              BQFieldModeEnum.NULLABLE)
    ignore_flag = BQField('ignore_flag', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    informing_loop_ready_flag = BQField('informing_loop_ready_flag', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    informing_loop_ready_flag_modified = BQField('informing_loop_ready_flag_modified', BQFieldTypeEnum.DATETIME,
                                                 BQFieldModeEnum.NULLABLE)
    participant_origin = BQField('participant_origin', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    replated_member_id = BQField('replated_member_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQGenomicSetMember(BQTable):
    """  BigQuery Table """
    __tablename__ = 'genomic_set_member'
    __schema__ = BQGenomicSetMemberSchema


class BQGenomicSetMemberView(BQView):
    __viewname__ = 'v_genomic_set_member'
    __viewdescr__ = 'Genomic Set Member View'
    __pk_id__ = 'id'
    __table__ = BQGenomicSetMember


class BQGenomicJobRunSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    # PDR-149:  Need to preserve RDR table id values
    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    job = BQField('job', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE, fld_enum=GenomicJobEnum)
    job_id = BQField('job_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE, fld_enum=GenomicJobEnum)
    start_time = BQField('start_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    end_time = BQField('end_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    run_status = BQField('run_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE,
                         fld_enum=GenomicSubProcessStatusEnum)
    run_status_id = BQField('run_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                         fld_enum=GenomicSubProcessStatusEnum)
    run_result = BQField('run_result', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE,
                         fld_enum=GenomicSubProcessResultEnum)
    run_result_id = BQField('run_result_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                         fld_enum=GenomicSubProcessResultEnum)
    result_message = BQField('result_message', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)


class BQGenomicJobRun(BQTable):
    """  BigQuery Table """
    __tablename__ = 'genomic_job_run'
    __schema__ = BQGenomicJobRunSchema


class BQGenomicJobRunView(BQView):
    __viewname__ = 'v_genomic_job_run'
    __viewdescr__ = 'Genomic Job Run View'
    __pk_id__ = 'id'
    __table__ = BQGenomicJobRun


class BQGenomicFileProcessedSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    # RDR fields
    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    run_id = BQField('run_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    start_time = BQField('start_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    end_time = BQField('end_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    file_path = BQField('file_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bucket_name = BQField('bucket_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    file_name = BQField('file_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    file_status_id = BQField('file_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                             fld_enum=GenomicSubProcessStatusEnum)
    file_status = BQField('file_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE,
                          fld_enum=GenomicSubProcessStatusEnum)
    file_result_id = BQField('file_result_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                             fld_enum=GenomicSubProcessResultEnum)
    file_result = BQField('file_result', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE,
                          fld_enum=GenomicSubProcessResultEnum)
    upload_date = BQField('upload_date', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    genomic_manifest_file_id = BQField('genomic_manifest_file_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQGenomicFileProcessed(BQTable):
    """  BigQuery Table """
    __tablename__ = 'genomic_file_processed'
    __schema__ = BQGenomicFileProcessedSchema


class BQGenomicFileProcessedView(BQView):
    __viewname__ = 'v_genomic_file_processed'
    __viewdescr__ = 'Genomic File Processed View'
    __pk_id__ = 'id'
    __table__ = BQGenomicFileProcessed


class BQGenomicManifestFileSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    # RDR fields
    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    upload_date = BQField('upload_date', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    manifest_type_id = BQField('manifest_type_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                               fld_enum=GenomicManifestTypesEnum)
    manifest_type = BQField('manifest_type', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE,
                            fld_enum=GenomicManifestTypesEnum)
    file_path = BQField('file_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    bucket_name = BQField('bucket_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    record_count = BQField('record_count', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    rdr_processing_complete = BQField('rdr_processing_complete', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    rdr_processing_complete_date = BQField('rdr_processing_complete_date', BQFieldTypeEnum.DATETIME,
                                           BQFieldModeEnum.NULLABLE)
    ignore_flag = BQField('ignore_flag', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    file_name = BQField('file_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)


class BQGenomicManifestFile(BQTable):
    """  BigQuery Table """
    __tablename__ = 'genomic_manifest_file'
    __schema__ = BQGenomicManifestFileSchema


class BQGenomicManifestFileView(BQView):
    __viewname__ = 'v_genomic_manifest_file'
    __viewdescr__ = 'Genomic Manifest File View'
    __pk_id__ = 'id'
    __table__ = BQGenomicManifestFile
    __sql__ = """
    SELECT gmf.*
      FROM (
         SELECT *,
          ROW_NUMBER() OVER (PARTITION BY orig_id ORDER BY modified desc) AS rn
        FROM `{project}`.{dataset}.genomic_manifest_file
      ) gmf
    WHERE gmf.rn = 1 and gmf.ignore_flag = 0
    """


class BQGenomicManifestFeedbackSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    # RDR fields
    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    input_manifest_file_id = BQField('input_manifest_file_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    feedback_manifest_file_id = BQField('feedback_manifest_file_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    feedback_record_count = BQField('feedback_record_count', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    feedback_complete = BQField('feedback_complete', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    feedback_complete_date = BQField('feedback_complete_date', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    ignore_flag = BQField('ignore_flag', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    version = BQField('version', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQGenomicManifestFeedback(BQTable):
    """  BigQuery Table """
    __tablename__ = 'genomic_manifest_feedback'
    __schema__ = BQGenomicManifestFeedbackSchema


class BQGenomicManifestFeedbackView(BQView):
    __viewname__ = 'v_genomic_manifest_feedback'
    __viewdescr__ = 'Genomic Manifest Feedback View'
    __pk_id__ = 'id'
    __table__ = BQGenomicManifestFeedback
    __sql__ = """
    SELECT gmf.*
      FROM (
         SELECT *,
          ROW_NUMBER() OVER (PARTITION BY orig_id ORDER BY modified desc) AS rn
        FROM `{project}`.{dataset}.genomic_manifest_feedback
      ) gmf
    WHERE gmf.rn = 1 and gmf.ignore_flag = 0
    """


class BQGenomicGCValidationMetricsSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    # PDR-149:  Need to preserve RDR table id values
    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    orig_created = BQField('orig_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    orig_modified = BQField('orig_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    genomic_set_member_id = BQField('genomic_set_member_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    genomic_file_processed_id = BQField('genomic_file_processed_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    lims_id = BQField('lims_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    call_rate = BQField('call_rate', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mean_coverage = BQField('mean_coverage', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    genome_coverage = BQField('genome_coverage', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    contamination = BQField('contamination', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sex_concordance = BQField('sex_concordance', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    processing_status = BQField('processing_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    notes = BQField('notes', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    site_id = BQField('site_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    chipwellbarcode = BQField('chipwellbarcode', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sex_ploidy = BQField('sex_ploidy', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    crai_path = BQField('crai_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    cram_md5_path = BQField('cram_md5_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    cram_path = BQField('cram_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    hf_vcf_md5_path = BQField('hf_vcf_md5_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    hf_vcf_path = BQField('hf_vcf_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    hf_vcf_tbi_path = BQField('hf_vcf_tbi_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    idat_green_md5_path = BQField('idat_green_md5_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    idat_green_path = BQField('idat_green_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    idat_red_md5_path = BQField('idat_red_md5_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    idat_red_path = BQField('idat_red_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    raw_vcf_md5_path = BQField('raw_vcf_md5_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    raw_vcf_path = BQField('raw_vcf_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    raw_vcf_tbi_path = BQField('raw_vcf_tbi_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    vcf_md5_path = BQField('vcf_md5_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    vcf_path = BQField('vcf_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    aligned_q30_bases = BQField('aligned_q30_bases', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    array_concordance = BQField('array_concordance', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    aou_hdr_coverage = BQField('aou_hdr_coverage', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    vcf_tbi_path = BQField('vcf_tbi_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dev_note = BQField('dev_note', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    ignore_flag = BQField('ignore_flag', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    contamination_category = BQField('contamination_category', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    crai_deleted = BQField('crai_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    cram_deleted = BQField('cram_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    cram_md5_deleted = BQField('cram_md5_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    hf_vcf_deleted = BQField('hf_vcf_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    hf_vcf_md5_deleted = BQField('hf_vcf_md5_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    hf_vcf_tbi_deleted = BQField('hf_vcf_tbi_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    idat_green_deleted = BQField('idat_green_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    idat_green_md5_deleted = BQField('idat_green_md5_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    idat_red_deleted = BQField('idat_red_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    idat_red_md5_deleted = BQField('idat_red_md5_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    raw_vcf_deleted = BQField('raw_vcf_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    raw_vcf_md5_deleted = BQField('raw_vcf_md5_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    raw_vcf_tbi_deleted = BQField('raw_vcf_tbi_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    vcf_deleted = BQField('vcf_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    vcf_md5_deleted = BQField('vcf_md5_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    vcf_tbi_deleted = BQField('vcf_tbi_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    drc_call_rate = BQField('drc_call_rate', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    drc_contamination = BQField('drc_contamination', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    drc_fp_concordance = BQField('drc_fp_concordance', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    drc_mean_coverage = BQField('drc_mean_coverage', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    drc_sex_concordance = BQField('drc_sex_concordance', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gvcf_deleted = BQField('gvcf_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    gvcf_md5_deleted = BQField('gvcf_md5_deleted', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    gvcf_md5_path = BQField('gvcf_md5_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gvcf_path = BQField('gvcf_path', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mapped_reads_pct = BQField('mapped_reads_pct', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    pipeline_id = BQField('pipeline_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    contamination_category_str = BQField('contamination_category_str', BQFieldTypeEnum.STRING,
                                         BQFieldModeEnum.NULLABLE)
    # DA-3072, PDR-1435 - WGS Reprocessing and Pipeline Upgrade columns
    processing_count = BQField('processing_count', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    aw3_ready_flag = BQField('aw3_ready_flag', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    aw3_manifest_job_run_id = BQField('aw3_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                      BQFieldModeEnum.NULLABLE)
    aw3_manifest_file_id = BQField('aw3_manifest_file_id', BQFieldTypeEnum.INTEGER,
                                   BQFieldModeEnum.NULLABLE)
    aw4_manifest_job_run_id = BQField('aw4_manifest_job_run_id', BQFieldTypeEnum.INTEGER,
                                      BQFieldModeEnum.NULLABLE)


class BQGenomicGCValidationMetrics(BQTable):
    """  BigQuery Table """
    __tablename__ = 'genomic_gc_validation_metrics'
    __schema__ = BQGenomicGCValidationMetricsSchema


class BQGenomicGCValidationMetricsView(BQView):
    __viewname__ = 'v_genomic_gc_validation_metrics'
    __viewdescr__ = 'Genomic GC Validation Metrics View'
    __pk_id__ = 'id'
    __table__ = BQGenomicGCValidationMetrics
    __sql__ = """
    SELECT gcvm.*
      FROM (
         SELECT *,
          ROW_NUMBER() OVER (PARTITION BY orig_id ORDER BY modified desc) AS rn
        FROM `{project}`.{dataset}.genomic_gc_validation_metrics
      ) gcvm
    WHERE gcvm.rn = 1 and gcvm.ignore_flag = 0
    """
