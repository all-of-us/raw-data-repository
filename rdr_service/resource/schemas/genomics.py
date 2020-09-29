#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.resource import Schema, SchemaMeta, fields
from rdr_service.resource.constants import SchemaID

from rdr_service.participant_enums import (
    GenomicSetStatus,
    GenomicSetMemberStatus,
#    GenomicValidationFlag,
    GenomicSubProcessStatus,
    GenomicSubProcessResult,
    GenomicJob,
    GenomicWorkflowState,
    GenomicQcStatus
)


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
        """
        schema_meta info declares how the schema and data is stored and organized in the Resource database tables.
        """
        ordered = True
        resource_pk_field = 'id'
        # SchemaMeta (unique type id, unique type name, type URI, resource pk field, nested schemas)
        schema_meta = SchemaMeta(
            type_uid=SchemaID.genomic_set.value,
            type_name=SchemaID.genomic_set.name,
            resource_uri='GenomicSet',
            resource_pk_field='id'
        )

class GenomicSetMemberSchema(Schema):

    id = fields.Int32()
    created = fields.DateTime()
    modified = fields.DateTime()
    genomic_set_id = fields.Int32()
    participant_id = fields.Int32()
    ny_flag = fields.Int32()
    sex_at_birth = fields.String(validate=validate.Length(max=20))
    genome_type = fields.String(validate=validate.Length(max=80))
    biobank_id = fields.Int32()
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
    cvl_aw1c_manifest_job_run_id = fields.Int32()
    cvl_aw1cf_manifest_job_run_id = fields.Int32()
    cvl_w1_manifest_job_run_id = fields.Int32()
    cvl_w2_manifest_job_run_id = fields.Int32()
    cvl_w3_manifest_job_run_id = fields.Int32()
    cvl_w4_manifest_job_run_id = fields.Int32()
    cvl_w4f_manifest_job_run_id = fields.Int32()
    genomic_workflow_state = fields.EnumString(enum=GenomicWorkflowState)
    genomic_workflow_state_id = fields.EnumInteger(enum=GenomicWorkflowState)

    genomic_workflow_state_history = fields.JSON()
    collection_tube_id = fields.String(validate=validate.Length(max=80))
    gc_site_id = fields.String(validate=validate.Length(max=11))
    arr_aw3_manifest_job_run_id = fields.Int32()
    wgs_aw3_manifest_job_run_id = fields.Int32()
    genomic_workflow_state_modified_time = fields.DateTime()
    report_consent_removal_date = fields.DateTime()
    qc_status = fields.EnumString(enum=GenomicQcStatus)
    qc_status_id = fields.EnumInteger(enum=GenomicQcStatus)

    class Meta:
        """
        schema_meta info declares how the schema and data is stored and organized in the Resource database tables.
        """
        ordered = True
        resource_pk_field = 'id'
        # SchemaMeta (unique type id, unique type name, type URI, resource pk field, nested schemas)
        schema_meta = SchemaMeta(
            type_uid=SchemaID.genomic_set_member.value,
            type_name=SchemaID.genomic_set_member.name,
            resource_uri='GenomicSetMember',
            resource_pk_field='id'
        )


class GenomicJobRunSchema(Schema):

    id = fields.Int32()
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
        """
        schema_meta info declares how the schema and data is stored and organized in the Resource database tables.
        """
        ordered = True
        resource_pk_field = 'id'
        # SchemaMeta (unique type id, unique type name, type URI, resource pk field, nested schemas)
        schema_meta = SchemaMeta(
            type_uid=SchemaID.genomic_job_run.value,
            type_name=SchemaID.genomic_job_run.name,
            resource_uri='GenomicJobRun',
            resource_pk_field='id'
        )

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

    class Meta:
        """
        schema_meta info declares how the schema and data is stored and organized in the Resource database tables.
        """
        ordered = True
        resource_pk_field = 'id'
        # SchemaMeta (unique type id, unique type name, type URI, resource pk field, nested schemas)
        schema_meta = SchemaMeta(
            type_uid=SchemaID.genomic_gc_validation_metrics.value,
            type_name=SchemaID.genomic_gc_validation_metrics.name,
            resource_uri='GenomicGCValidationMetrics',
            resource_pk_field='id'
        )
