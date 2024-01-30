"""
This module provides central location for all genomics_mappings
"""
from rdr_service.genomic_enums import GenomicReportState, GenomicJob

genome_type_to_aw1_aw2_file_prefix = {
    "aou_array": "GEN",
    "aou_wgs": "SEQ",
    "aou_array_investigation": "GEN",
    "aou_wgs_investigation": "SEQ"
}

raw_aw1_to_genomic_set_member_fields = {
    "package_id": "packageId",
    "box_storageunit_id": "gcManifestBoxStorageUnitId",
    "box_id_plate_id": "gcManifestBoxPlateId",
    "well_position": "gcManifestWellPosition",
    "sample_id": "sampleId",
    "parent_sample_id": "gcManifestParentSampleId",
    "collection_tube_id": "collectionTubeId",
    "matrix_id": "gcManifestMatrixId",
    "sample_type": "sampleType",
    "treatments": "gcManifestTreatments",
    "quantity": "gcManifestQuantity_ul",
    "total_concentration": "gcManifestTotalConcentration_ng_per_ul",
    "total_dna": "gcManifestTotalDNA_ng",
    "visit_description": "gcManifestVisitDescription",
    "sample_source": "gcManifestSampleSource",
    "study": "gcManifestStudy",
    "tracking_number": "gcManifestTrackingNumber",
    "contact": "gcManifestContact",
    "email": "gcManifestEmail",
    "study_pi": "gcManifestStudyPI",
    "test_name": "gcManifestTestName",
    "failure_mode": "gcManifestFailureMode",
    "failure_mode_desc": "gcManifestFailureDescription"
}

raw_aw2_to_genomic_set_member_fields = {
    "lims_id": "limsId",
    "chipwellbarcode": "chipwellbarcode",
    "call_rate": "callRate",
    "mean_coverage": "meanCoverage",
    "genome_coverage": "genomeCoverage",
    "aouhdr_coverage": "aouHdrCoverage",
    "contamination": "contamination",
    "sex_concordance": "sexConcordance",
    "sex_ploidy": "sexPloidy",
    "aligned_q30_bases": "alignedQ30Bases",
    "array_concordance": "arrayConcordance",
    "processing_status": "processingStatus",
    "notes": "notes",
    "pipeline_id": "pipelineId"
}

genomic_data_file_mappings = {
    'idatRed': {
        'file_ext': ['_red.idat'],
        'model_attrs': ['idatRedPath', 'idatRedReceived']
    },
    'idatGreen': {
        'file_ext': ['_grn.idat'],
        'model_attrs': ['idatGreenPath', 'idatGreenReceived']
    },
    'idatRedMd5': {
        'file_ext': ['_red.idat.md5sum'],
        'model_attrs': ['idatRedMd5Path', 'idatRedMd5Received']
    },
    'idatGreenMd5': {
        'file_ext': ['_grn.idat.md5sum'],
        'model_attrs': ['idatGreenMd5Path', 'idatGreenMd5Received']
    },
    'vcf': {
        'file_ext': ['vcf.gz'],
        'model_attrs': ['vcfPath', 'vcfReceived']
    },
    'vcfTbi': {
        'file_ext': ['vcf.gz.tbi'],
        'model_attrs': ['vcfTbiPath', 'vcfTbiReceived']
    },
    'vcfMd5': {
        'file_ext': ['vcf.gz.md5sum'],
        'model_attrs': ['vcfMd5Path', 'vcfMd5Received']
    },
    'hfVcf': {
        'file_ext': ['hard-filtered.vcf.gz'],
        'model_attrs': ['hfVcfPath', 'hfVcfReceived']
    },
    'hfVcfTbi': {
        'file_ext': ['hard-filtered.vcf.gz.tbi'],
        'model_attrs': ['hfVcfTbiPath', 'hfVcfTbiReceived']
    },
    'hfVcfMd5': {
        'file_ext': ['hard-filtered.vcf.gz.md5sum'],
        'model_attrs': ['hfVcfMd5Path', 'hfVcfMd5Received']
    },
    'rawVcf': {
        'file_ext': ['vcf.gz'],
        'model_attrs': ['rawVcfPath', 'rawVcfReceived']
    },
    'rawVcfTbi': {
        'file_ext': ['vcf.gz.tbi'],
        'model_attrs': ['rawVcfTbiPath', 'rawVcfTbiReceived']
    },
    'rawVcfMd5': {
        'file_ext': ['vcf.gz.md5sum'],
        'model_attrs': ['rawVcfMd5Path', 'rawVcfMd5Received']
    },
    'cram': {
        'file_ext': ['cram'],
        'model_attrs': ['cramPath', 'cramReceived']
    },
    'cramMd5': {
        'file_ext': ['cram.md5sum'],
        'model_attrs': ['cramMd5Path', 'cramMd5Received']
    },
    'crai': {
        'file_ext': ['cram.crai'],
        'model_attrs': ['craiPath', 'craiReceived']
    },
    'gvcf': {
        'file_ext': ['hard-filtered.gvcf.gz'],
        'model_attrs': ['gvcfPath', 'gvcfReceived']
    },
    'gvcfMd5': {
        'file_ext': ['hard-filtered.gvcf.gz.md5sum'],
        'model_attrs': ['gvcfMd5Path', 'gvcfMd5Received']
    },

    'gcvf': {
        'file_ext': ['hard-filtered.gvcf.gz'],
        'model_attrs': ['gvcfPath', 'gvcfReceived']
    },
    'gcvf_md5': {
        'file_ext': ['hard-filtered.gvcf.gz.md5sum'],
        'model_attrs': ['gvcfMd5Path', 'gvcfMd5Received']
    },
}

wgs_file_types_attributes = ({'file_path_attribute': 'hfVcfPath',
                              'file_received_attribute': 'hfVcfReceived',
                              'file_type': 'hard-filtered.vcf.gz',
                              'required': True},
                             {'file_path_attribute': 'hfVcfTbiPath',
                              'file_received_attribute': 'hfVcfTbiReceived',
                              'file_type': 'hard-filtered.vcf.gz.tbi',
                              'required': True},
                             {'file_path_attribute': 'hfVcfMd5Path',
                              'file_received_attribute': 'hfVcfMd5Received',
                              'file_type': 'hard-filtered.vcf.gz.md5sum',
                              'required': True},
                             {'file_path_attribute': 'rawVcfPath',
                              'file_received_attribute': 'rawVcfReceived',
                              'file_type': 'vcf.gz',
                              'required': False},
                             {'file_path_attribute': 'rawVcfTbiPath',
                              'file_received_attribute': 'rawVcfTbiReceived',
                              'file_type': 'vcf.gz.tbi',
                              'required': False},
                             {'file_path_attribute': 'rawVcfMd5Path',
                              'file_received_attribute': 'rawVcfMd5Received',
                              'file_type': 'vcf.gz.md5sum',
                              'required': False},
                             {'file_path_attribute': 'cramPath',
                              'file_received_attribute': 'cramReceived',
                              'file_type': 'cram',
                              'required': True},
                             {'file_path_attribute': 'cramMd5Path',
                              'file_received_attribute': 'cramMd5Received',
                              'file_type': 'cram.md5sum',
                              'required': True},
                             {'file_path_attribute': 'craiPath',
                              'file_received_attribute': 'craiReceived',
                              'file_type': 'cram.crai',
                              'required': True},
                             {'file_path_attribute': 'gvcfPath',
                              'file_received_attribute': 'gvcfReceived',
                              'file_type': 'hard-filtered.gvcf.gz',
                              'required': True},
                             {'file_path_attribute': 'gvcfMd5Path',
                              'file_received_attribute': 'gvcfMd5Received',
                              'file_type': 'hard-filtered.gvcf.gz.md5sum',
                              'required': True}
                             )

array_file_types_attributes = ({'file_path_attribute': 'idatRedPath',
                                'file_received_attribute': 'idatRedReceived',
                                'file_type': 'Red.idat',
                                'required': True},
                               {'file_path_attribute': 'idatGreenPath',
                                'file_received_attribute': 'idatGreenReceived',
                                'file_type': 'Grn.idat',
                                'required': True},
                               {'file_path_attribute': 'idatRedMd5Path',
                                'file_received_attribute': 'idatRedMd5Received',
                                'file_type': 'Red.idat.md5sum',
                                'required': True},
                               {'file_path_attribute': 'idatGreenMd5Path',
                                'file_received_attribute': 'idatGreenMd5Received',
                                'file_type': 'Grn.idat.md5sum',
                                'required': True},
                               {'file_path_attribute': 'vcfPath',
                                'file_received_attribute': 'vcfReceived',
                                'file_type': 'vcf.gz',
                                'required': True},
                               {'file_path_attribute': 'vcfTbiPath',
                                'file_received_attribute': 'vcfTbiReceived',
                                'file_type': 'vcf.gz.tbi',
                                'required': True},
                               {'file_path_attribute': 'vcfMd5Path',
                                'file_received_attribute': 'vcfMd5Received',
                                'file_type': 'vcf.gz.md5sum',
                                'required': True})

genome_centers_id_from_bucket_array = {
    'baylor': 'jh',
    'broad': 'bi',
    'northwest': 'uw',
    'data': 'rdr'
}

genome_centers_id_from_bucket_wgs = {
    'baylor': 'bcm',
    'broad': 'bi',
    'northwest': 'uw'
}

informing_loop_event_mappings = {
    'gem.informing_loop_decision.no': 'gem.informing_loop.screen8_no',
    'gem.informing_loop_decision.yes': 'gem.informing_loop.screen8_yes',
    'gem.informing_loop_decision.maybe_later': 'gem.informing_loop.screen8_maybe_later',
    'pgx.informing_loop_decision.no': 'pgx.informing_loop.screen8_no',
    'pgx.informing_loop_decision.yes': 'pgx.informing_loop.screen8_yes',
    'pgx.informing_loop_decision.maybe_later': 'pgx.informing_loop.screen8_maybe_later',
    'hdr.informing_loop_decision.no': 'hdr.informing_loop.screen10_no',
    'hdr.informing_loop_decision.yes': 'hdr.informing_loop.screen10_yes',
    'hdr.informing_loop_decision.maybe_later': 'hdr.informing_loop.screen10_maybe_later'
}

cvl_result_reconciliation_modules = {
    "hdr": "hdr_v1",
    "pgx": "pgx_v1"
}

message_broker_report_ready_event_state_mappings = {
    "pgx.result_ready": GenomicReportState.PGX_RPT_READY,
    "hdr.result_ready.informative": GenomicReportState.HDR_RPT_POSITIVE,
    "hdr.result_ready.uninformative": GenomicReportState.HDR_RPT_UNINFORMATIVE
}

message_broker_report_viewed_event_state_mappings = [
    "hdr.opened_at",
    "pgx.opened_at"
]

genome_center_datafile_prefix_map = {
   'bi': {
       'cram': 'wgs_sample_raw_data/crams_crais',
       'cram.crai': 'wgs_sample_raw_data/crams_crais',
       'cram.md5sum': 'wgs_sample_raw_data/crams_crais',
       'hard-filtered.vcf.gz': 'wgs_sample_raw_data/ss_vcf_clinical',
       'hard-filtered.vcf.gz.md5sum': 'wgs_sample_raw_data/ss_vcf_clinical',
       'hard-filtered.vcf.gz.tbi': 'wgs_sample_raw_data/ss_vcf_clinical',
       'hard-filtered.gvcf.gz': 'wgs_sample_raw_data/ss_vcf_research',
       'hard-filtered.gvcf.gz.md5sum': 'wgs_sample_raw_data/ss_vcf_research',

   },
   'uw': {
       'cram': 'Wgs_sample_raw_data/CRAMs_CRAIs',
       'cram.crai': 'Wgs_sample_raw_data/CRAMs_CRAIs',
       'cram.md5sum': 'Wgs_sample_raw_data/CRAMs_CRAIs',
       'hard-filtered.vcf.gz': 'Wgs_sample_raw_data/SS_VCF_clinical',
       'hard-filtered.vcf.gz.md5sum': 'Wgs_sample_raw_data/SS_VCF_clinical',
       'hard-filtered.vcf.gz.tbi': 'Wgs_sample_raw_data/SS_VCF_clinical',
       'hard-filtered.gvcf.gz': 'Wgs_sample_raw_data/SS_VCF_research',
       'hard-filtered.gvcf.gz.md5sum': 'Wgs_sample_raw_data/SS_VCF_research'
   },
   'bcm': {
       'cram': 'Wgs_sample_raw_data/CRAMs_CRAIs',
       'cram.crai': 'Wgs_sample_raw_data/CRAMs_CRAIs',
       'cram.md5sum': 'Wgs_sample_raw_data/CRAMs_CRAIs',
       'hard-filtered.vcf.gz': 'Wgs_sample_raw_data/SS_VCF_clinical',
       'hard-filtered.vcf.gz.md5sum': 'Wgs_sample_raw_data/SS_VCF_clinical',
       'hard-filtered.vcf.gz.tbi': 'Wgs_sample_raw_data/SS_VCF_clinical',
       'hard-filtered.gvcf.gz': 'Wgs_sample_raw_data/SS_VCF_research',
       'hard-filtered.gvcf.gz.md5sum': 'Wgs_sample_raw_data/SS_VCF_research'
   },
   'rdr': {
       'cram': 'Wgs_sample_raw_data/CRAMs_CRAIs',
       'cram.crai': 'Wgs_sample_raw_data/CRAMs_CRAIs',
       'cram.md5sum': 'Wgs_sample_raw_data/CRAMs_CRAIs',
       'hard-filtered.vcf.gz': 'Wgs_sample_raw_data/SS_VCF_clinical',
       'hard-filtered.vcf.gz.md5sum': 'Wgs_sample_raw_data/SS_VCF_clinical',
       'hard-filtered.vcf.gz.tbi': 'Wgs_sample_raw_data/SS_VCF_clinical',
       'hard-filtered.gvcf.gz': 'Wgs_sample_raw_data/SS_VCF_research',
       'hard-filtered.gvcf.gz.md5sum': 'Wgs_sample_raw_data/SS_VCF_research'
   }
}

wgs_metrics_manifest_mapping = {
    'vcf_hf_path': 'hfVcfPath',
    'vcf_hf_index_path': 'hfVcfTbiPath',
    'vcf_hf_md5_path': 'hfVcfMd5Path',
    'cram_path': 'cramPath',
    'cram_md5_path': 'cramMd5Path',
    'crai_path': 'craiPath',
    'gvcf_path': 'gvcfPath',
    'gvcf_md5_path': 'gvcfMd5Path'
}

GENOMIC_SHORT_READ_INGESTION_MAP = {
    GenomicJob.AW1_MANIFEST: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.AW1F_MANIFEST: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.METRICS_INGESTION: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.AW4_ARRAY_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.AW4_WGS_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.AW5_ARRAY_MANIFEST: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.AW5_WGS_MANIFEST: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
}

GENOMIC_GEM_INGESTION_MAP = {
    GenomicJob.GEM_A2_MANIFEST: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
}

GENOMIC_CVL_INGESTION_MAP = {
    GenomicJob.CVL_W2SC_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.CVL_W3NS_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.CVL_W3SC_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.CVL_W3SS_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.CVL_W4WR_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.CVL_W5NF_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
}

GENOMIC_LONG_READ_INGESTION_MAP = {
    GenomicJob.LR_LR_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.LR_L1_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.LR_L1F_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.LR_L2_ONT_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.LR_L2_PB_CCS_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.LR_L4_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.LR_L4F_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.LR_L5_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.LR_L6_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
}

GENOMIC_PROTEOMICS_INGESTION_MAP = {
    GenomicJob.PR_PR_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.PR_P1_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.PR_P2_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    }
}

GENOMIC_RNA_INGESTION_MAP = {
    GenomicJob.RNA_RR_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.RNA_R1_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    },
    GenomicJob.RNA_R2_WORKFLOW: {
        'raw': {
            'model': '',
            'job_id': ''
        },
        'cloud_task': '',
        'ingestion_method': ''
    }
}

GENOMIC_FULL_INGESTION_MAP = {
    **GENOMIC_SHORT_READ_INGESTION_MAP,
    **GENOMIC_GEM_INGESTION_MAP,
    **GENOMIC_LONG_READ_INGESTION_MAP,
    **GENOMIC_CVL_INGESTION_MAP,
    **GENOMIC_PROTEOMICS_INGESTION_MAP,
    **GENOMIC_RNA_INGESTION_MAP
}
