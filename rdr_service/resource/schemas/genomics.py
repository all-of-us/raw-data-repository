#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.resource import Schema, fields
from rdr_service.resource.constants import SchemaID

from rdr_service.genomic_enums import GenomicSetStatus, GenomicSetMemberStatus, GenomicJob, GenomicWorkflowState, \
    GenomicSubProcessStatus, GenomicSubProcessResult, GenomicManifestTypes, GenomicContaminationCategory, \
    GenomicQcStatus


class GenomicSetSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    genomic_set_name = fields.String(validate=validate.Length(max=80))
    genomic_set_criteria = fields.String(validate=validate.Length(max=80))
    genomic_set_version = fields.Int32()
    genomic_set_file = fields.String(validate=validate.Length(max=250))
    genomic_set_file_time = fields.DateTime()
    genomic_set_status = fields.EnumString(enum=GenomicSetStatus)
    genomic_set_status_id = fields.EnumInteger(enum=GenomicSetStatus)
    validated_time = fields.DateTime()

    class Meta:
        schema_id = SchemaID.genomic_set
        resource_uri = 'GenomicSet'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class GenomicSetMemberSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    genomic_set_id = fields.Int32()
    participant_id = fields.String(validate=validate.Length(max=10))
    ny_flag = fields.Int32()
    sex_at_birth = fields.String(validate=validate.Length(max=20))
    genome_type = fields.String(validate=validate.Length(max=80))
    biobank_id = fields.Int32()
    biobank_id_str = fields.String(validate=validate.Length(max=128))
    package_id = fields.String(validate=validate.Length(max=80))
    validation_status = fields.EnumString(enum=GenomicSetMemberStatus)
    validation_status_id = fields.EnumInteger(enum=GenomicSetMemberStatus)
    # validation_flags is an array of GenomicValidationFlag Enum values.
    validation_flags = fields.String(validate=validate.Length(max=80))
    validated_time = fields.DateTime()
    sample_id = fields.String(validate=validate.Length(max=80))
    sample_type = fields.String(validate=validate.Length(max=50))
    reconcile_cvl_job_run_id = fields.Int32()
    sequencing_file_name = fields.String(validate=validate.Length(max=128))
    reconcile_gc_manifest_job_run_id = fields.Int32()
    reconcile_metrics_bb_manifest_job_run_id = fields.Int32()
    reconcile_metrics_sequencing_job_run_id = fields.Int32()
    ai_an = fields.String(validate=validate.Length(max=2))
    gc_manifest_box_plate_id = fields.String(validate=validate.Length(max=50))
    gc_manifest_box_storage_unit_id = fields.String(validate=validate.Length(max=50))
    gc_manifest_contact = fields.String(validate=validate.Length(max=50))
    gc_manifest_email = fields.String(validate=validate.Length(max=50))
    gc_manifest_failure_description = fields.String(validate=validate.Length(max=128))
    gc_manifest_failure_mode = fields.String(validate=validate.Length(max=128))
    gc_manifest_matrix_id = fields.String(validate=validate.Length(max=20))
    gc_manifest_parent_sample_id = fields.String(validate=validate.Length(max=20))
    gc_manifest_quantity_ul = fields.Int32()
    gc_manifest_sample_source = fields.String(validate=validate.Length(max=20))
    gc_manifest_study = fields.String(validate=validate.Length(max=50))
    gc_manifest_study_pi = fields.String(validate=validate.Length(max=50))
    gc_manifest_test_name = fields.String(validate=validate.Length(max=50))
    gc_manifest_total_concentration_ng_per_ul = fields.Int32()
    gc_manifest_total_dna_ng = fields.Int32()
    gc_manifest_tracking_number = fields.String(validate=validate.Length(max=50))
    gc_manifest_treatments = fields.String(validate=validate.Length(max=20))
    gc_manifest_visit_description = fields.String(validate=validate.Length(max=128))
    gc_manifest_well_position = fields.String(validate=validate.Length(max=10))
    gem_a1_manifest_job_run_id = fields.Int32()
    gem_a2_manifest_job_run_id = fields.Int32()
    gem_pass = fields.String(validate=validate.Length(max=10))
    gem_a3_manifest_job_run_id = fields.Int32()
    aw3_manifest_job_run_id = fields.Int32()
    aw4_manifest_job_run_id = fields.Int32()
    cvl_aw1c_manifest_job_run_id = fields.Int32()
    cvl_aw1cf_manifest_job_run_id = fields.Int32()
    cvl_w1_manifest_job_run_id = fields.Int32()
    cvl_w2_manifest_job_run_id = fields.Int32()
    cvl_w3_manifest_job_run_id = fields.Int32()
    cvl_w4_manifest_job_run_id = fields.Int32()
    cvl_w4f_manifest_job_run_id = fields.Int32()
    genomic_workflow_state = fields.EnumString(enum=GenomicWorkflowState)
    genomic_workflow_state_id = fields.EnumInteger(enum=GenomicWorkflowState)
    collection_tube_id = fields.String(validate=validate.Length(max=80))
    gc_site_id = fields.String(validate=validate.Length(max=11))
    genomic_workflow_state_modified_time = fields.DateTime()
    report_consent_removal_date = fields.DateTime()
    qc_status = fields.EnumString(enum=GenomicQcStatus)
    qc_status_id = fields.EnumInteger(enum=GenomicQcStatus)
    fingerprint_path = fields.String(validate=validate.Length(max=255))
    dev_note = fields.String(validate=validate.Length(max=255))
    aw1_file_processed_id = fields.Int32()
    aw2_file_processed_id = fields.Int32()
    aw2f_job_run_id = fields.Int32()

    aw0_manifest_file_id = fields.Int32()
    aw2f_manifest_job_run_id = fields.Int32()
    aw3_manifest_file_id = fields.Int32()
    block_research = fields.Int16()
    block_research_reason = fields.String(validate=validate.Length(max=255))
    block_results = fields.Int16()
    block_results_reason = fields.String(validate=validate.Length(max=255))
    color_metrics_job_run_id = fields.Int32()
    cvl_secondary_conf_failure = fields.String(validate=validate.Length(max=255))
    cvl_w1il_hdr_job_run_id = fields.Int32()
    cvl_w1il_pgx_job_run_id = fields.Int32()
    cvl_w2sc_manifest_job_run_id = fields.Int32()
    cvl_w2w_job_run_id = fields.Int32()
    cvl_w3ns_manifest_job_run_id = fields.Int32()
    cvl_w3sc_manifest_job_run_id = fields.Int32()
    cvl_w3sr_manifest_job_run_id = fields.Int32()
    cvl_w3ss_manifest_job_run_id = fields.Int32()
    cvl_w4wr_hdr_manifest_job_run_id = fields.Int32()
    cvl_w4wr_pgx_manifest_job_run_id = fields.Int32()
    cvl_w5nf_hdr_manifest_job_run_id = fields.Int32()
    cvl_w5nf_pgx_manifest_job_run_id = fields.Int32()
    diversion_pouch_site_flag = fields.Int16()
    gem_date_of_import = fields.DateTime()
    gem_metrics_ancestry_loop_response = fields.String(validate=validate.Length(max=10))
    gem_metrics_available_results = fields.String(validate=validate.Length(max=255))
    gem_metrics_results_released_at = fields.Int32()
    ignore_flag = fields.Int16()
    informing_loop_ready_flag = fields.Int32()
    informing_loop_ready_flag_modified = fields.DateTime()
    participant_origin = fields.String(validate=validate.Length(max=80))
    replated_member_id = fields.Int32()


    class Meta:
        schema_id = SchemaID.genomic_set_member
        resource_uri = 'GenomicSetMember'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class GenomicJobRunSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    job = fields.EnumString(enum=GenomicJob)
    job_id = fields.EnumInteger(enum=GenomicJob)
    start_time = fields.DateTime()
    end_time = fields.DateTime()
    run_status = fields.EnumString(enum=GenomicSubProcessStatus)
    run_status_id = fields.EnumInteger(enum=GenomicSubProcessStatus)
    run_result = fields.EnumString(enum=GenomicSubProcessResult)
    run_result_id = fields.EnumInteger(enum=GenomicSubProcessResult)
    result_message = fields.String(validate=validate.Length(max=150))

    class Meta:
        schema_id = SchemaID.genomic_job_run
        resource_uri = 'GenomicJobRun'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).

class GenomicFileProcessedSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    run_id = fields.Int32()
    start_time = fields.DateTime()
    end_time = fields.DateTime()
    file_path = fields.String(validate=validate.Length(max=255))
    bucket_name = fields.String(validate=validate.Length(max=128))
    file_name = fields.String(validate=validate.Length(max=128))
    file_status = fields.EnumString(enum=GenomicSubProcessStatus)
    file_status_id = fields.EnumInteger(enum=GenomicSubProcessStatus)
    file_result = fields.EnumString(enum=GenomicSubProcessResult)
    file_result_id = fields.EnumInteger(enum=GenomicSubProcessResult)
    upload_date = fields.DateTime()
    genomic_manifest_file_id = fields.Int32()

    class Meta:
        schema_id = SchemaID.genomic_file_processed
        resource_uri = 'GenomicFileProcessed'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class GenomicManifestFileSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    upload_date = fields.DateTime()
    manifest_type = fields.EnumString(enum=GenomicManifestTypes)
    manifest_type_id = fields.EnumInteger(enum=GenomicManifestTypes)
    file_path = fields.String(validate=validate.Length(max=255))
    bucket_name = fields.String(validate=validate.Length(max=128))
    record_count = fields.Int32()
    rdr_processing_complete = fields.Int16()
    rdr_processing_complete_date = fields.DateTime()
    ignore_flag = fields.Int16()
    file_name = fields.String(validate=validate.Length(max=255))

    class Meta:
        schema_id = SchemaID.genomic_manifest_file
        resource_uri = 'GenomicManifestFile'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).



class GenomicManifestFeedbackSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    input_manifest_file_id = fields.Int32()
    feedback_manifest_file_id = fields.Int32()
    feedback_record_count = fields.Int32()
    feedback_complete = fields.Int16()
    feedback_complete_date = fields.DateTime()
    ignore_flag = fields.Int16()
    version = fields.Int32()

    class Meta:
        schema_id = SchemaID.genomic_manifest_feedback
        resource_uri = 'GenomicManifestFeedback'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class GenomicGCValidationMetricsSchema(Schema):

    id = fields.Int32()
    genomic_set_member_id = fields.Int32()
    genomic_file_processed_id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    lims_id = fields.String(validate=validate.Length(max=80))
    call_rate = fields.String(validate=validate.Length(max=10))
    mean_coverage = fields.String(validate=validate.Length(max=10))
    genome_coverage = fields.String(validate=validate.Length(max=10))
    contamination = fields.String(validate=validate.Length(max=10))
    sex_concordance = fields.String(validate=validate.Length(max=10))
    processing_status = fields.String(validate=validate.Length(max=15))
    notes = fields.String(validate=validate.Length(max=128))
    site_id = fields.String(validate=validate.Length(max=80))
    chipwellbarcode = fields.String(validate=validate.Length(max=80))
    idat_green_received = fields.Int16()
    idat_red_received = fields.Int16()
    vcf_received = fields.Int16()
    crai_received = fields.Int16()
    cram_md5_received = fields.Int16()
    cram_received = fields.Int16()
    hf_vcf_md5_received = fields.Int16()
    hf_vcf_received = fields.Int16()
    hf_vcf_tbi_received = fields.Int16()
    raw_vcf_md5_received = fields.Int16()
    raw_vcf_received = fields.Int16()
    raw_vcf_tbi_received = fields.Int16()
    sex_ploidy = fields.String(validate=validate.Length(max=10))
    idat_green_md5_received = fields.Int16()
    idat_red_md5_received = fields.Int16()
    vcf_md5_received = fields.Int16()
    crai_path = fields.String(validate=validate.Length(max=255))
    cram_md5_path = fields.String(validate=validate.Length(max=255))
    cram_path = fields.String(validate=validate.Length(max=255))
    hf_vcf_md5_path = fields.String(validate=validate.Length(max=255))
    hf_vcf_path = fields.String(validate=validate.Length(max=255))
    hf_vcf_tbi_path = fields.String(validate=validate.Length(max=255))
    idat_green_md5_path = fields.String(validate=validate.Length(max=255))
    idat_green_path = fields.String(validate=validate.Length(max=255))
    idat_red_md5_path = fields.String(validate=validate.Length(max=255))
    idat_red_path = fields.String(validate=validate.Length(max=255))
    raw_vcf_md5_path = fields.String(validate=validate.Length(max=255))
    raw_vcf_path = fields.String(validate=validate.Length(max=255))
    raw_vcf_tbi_path = fields.String(validate=validate.Length(max=255))
    vcf_md5_path = fields.String(validate=validate.Length(max=255))
    vcf_path = fields.String(validate=validate.Length(max=255))
    aligned_q30_bases = fields.Int32()
    array_concordance = fields.String(validate=validate.Length(max=10))
    aou_hdr_coverage = fields.String(validate=validate.Length(max=10))
    vcf_tbi_path = fields.String(validate=validate.Length(max=255))
    vcf_tbi_received = fields.Int16()
    ignore_flag = fields.Int16()
    dev_note = fields.String(validate=validate.Length(max=255))
    contamination_category = fields.EnumString(enum=GenomicContaminationCategory)
    contamination_category_id = fields.EnumInteger(enum=GenomicContaminationCategory)
    idat_green_deleted = fields.Int16()
    idat_red_deleted = fields.Int16()
    vcf_deleted = fields.Int16()
    crai_deleted = fields.Int16()
    cram_md5_deleted = fields.Int16()
    cram_deleted = fields.Int16()
    hf_vcf_md5_deleted = fields.Int16()
    hf_vcf_deleted = fields.Int16()
    hf_vcf_tbi_deleted = fields.Int16()
    raw_vcf_md5_deleted = fields.Int16()
    raw_vcf_deleted = fields.Int16()
    raw_vcf_tbi_deleted = fields.Int16()
    idat_green_md5_deleted = fields.Int16()
    idat_red_md5_deleted = fields.Int16()
    vcf_md5_deleted = fields.Int16()
    vcf_tbi_deleted = fields.Int16()
    drc_sex_concordance = fields.String(validate=validate.Length(max=128))
    drc_contamination = fields.String(validate=validate.Length(max=128))
    drc_mean_coverage = fields.String(validate=validate.Length(max=128))
    drc_fp_concordance = fields.String(validate=validate.Length(max=128))
    gvcf_path = fields.String(validate=validate.Length(max=512))
    gvcf_received = fields.Int16()
    gvcf_deleted = fields.Int16()
    gvcf_md5_path = fields.String(validate=validate.Length(max=255))
    gvcf_md5_received = fields.Int16()
    gvcf_md5_deleted = fields.Int16()
    drc_call_rate = fields.String(validate=validate.Length(max=128))
    pipeline_id = fields.String(validate=validate.Length(max=128))
    mapped_reads_pct = fields.String(validate=validate.Length(max=10))

    class Meta:
        schema_id = SchemaID.genomic_gc_validation_metrics
        resource_uri = 'GenomicGCValidationMetrics'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class GenomicUserEventMetricsSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    participant_id = fields.String(validate=validate.Length(max=10))
    created_at = fields.DateTime()
    event_name = fields.String(validate=validate.Length(max=512))
    device = fields.String(validate=validate.Length(max=255))
    operating_system = fields.String(validate=validate.Length(max=255))
    browser = fields.String(validate=validate.Length(max=255))
    file_path = fields.String(validate=validate.Length(max=512))
    run_id = fields.Int32()
    ignore_flag = fields.Int16()
    reconcile_job_run_id = fields.Int32()

    class Meta:
        schema_id = SchemaID.genomic_user_event_metrics
        resource_uri = 'GenomicUserEventMetrics'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class GenomicInformingLoopSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    message_record_id = fields.Int32()
    participant_id = fields.String(validate=validate.Length(max=10))
    event_type = fields.String(validate=validate.Length(max=256))
    event_authored_time = fields.DateTime()
    module_type = fields.String(validate=validate.Length(max=128))
    decision_value = fields.String(validate=validate.Length(max=128))
    sample_id = fields.String(validate=validate.Length(max=80))

    class Meta:
        schema_id = SchemaID.genomic_informing_loop
        resource_uri = 'GenomicInformingLoop'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).

class GenomicCVLResultPastDueSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    genomic_set_member_id = fields.Int32()
    sample_id = fields.String(validate=validate.Length(max=255))
    results_type = fields.String(validate=validate.Length(max=128))
    cvl_site_id = fields.String(validate=validate.Length(max=128))
    email_notification_sent = fields.Int16()
    email_notification_sent_date = fields.DateTime()
    resolved = fields.Int16()
    resolved_date = fields.DateTime()

    class Meta:
        schema_id = SchemaID.genomic_cvl_result_past_due
        resource_uri = 'GenomicCVLResultPastDue'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function)

class GenomicMemberReportStateSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    genomic_set_member_id = fields.Int32()
    genomic_report_state = fields.Int16()
    module = fields.String(validate=validate.Length(max=128))
    participant_id = fields.String(validate=validate.Length(max=10))
    genomic_report_state_str = fields.String(validate=validate.Length(max=128))
    event_authored_time = fields.DateTime()
    event_type = fields.String(validate=validate.Length(max=128))
    message_record_id = fields.Int16()
    sample_id = fields.String(validate=validate.Length(max=128))
    report_revision_number = fields.Int16()

    class Meta:
        schema_id = SchemaID.genomic_member_report_state
        resource_uri = 'GenomicMemberReportState'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function)

class GenomicResultViewedSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    message_record_id = fields.Int32()
    participant_id = fields.String(validate=validate.Length(max=10))
    event_type = fields.String(validate=validate.Length(max=256))
    event_authored_time = fields.DateTime()
    module_type = fields.String(validate=validate.Length(max=128))
    first_viewed = fields.DateTime()
    last_viewed = fields.DateTime()
    sample_id = fields.String(validate=validate.Length(max=80))

    class Meta:
        schema_id = SchemaID.genomic_result_viewed
        resource_uri = 'GenomicResultViewed'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function)


class GenomicAppointmentEventSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    message_record_id = fields.Int32()
    participant_id = fields.String(validate=validate.Length(max=10))
    event_type = fields.String(validate=validate.Length(max=256))
    event_authored_time = fields.DateTime()
    module_type = fields.String(validate=validate.Length(max=255))
    source = fields.String(validate=validate.Length(max=255))
    location = fields.String(validate=validate.Length(max=255))
    contact_number = fields.String(validate=validate.Length(max=255))
    language = fields.String(validate=validate.Length(max=255))
    cancellation_reason = fields.String(validate=validate.Length(max=255))
    appointment_timezone = fields.String(validate=validate.Length(max=255))
    appointment_timestamp = fields.DateTime()

    class Meta:
        schema_id = SchemaID.genomic_appointment_event
        resource_uri = 'GenomicAppointmentEvent'
        resource_pk_field = 'id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function)