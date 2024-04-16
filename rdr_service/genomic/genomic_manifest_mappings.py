from rdr_service.genomic_enums import GenomicJob
from rdr_service.model.genomics import GenomicAW4Raw, GenomicA2Raw, GenomicW2SCRaw, GenomicW4WRRaw, GenomicW5NFRaw, \
    GenomicW3SSRaw, GenomicW3SCRaw, GenomicW3NSRaw, GenomicL5Raw, GenomicL6Raw, GenomicL4FRaw, GenomicL4Raw, \
    GenomicL2PBCCSRaw, GenomicL2ONTRaw, GenomicL1FRaw, GenomicL1Raw, GenomicLRRaw, GenomicPRRaw, GenomicP1Raw, \
    GenomicP2Raw, GenomicR2Raw, GenomicR1Raw, GenomicRRRaw, GenomicL6FRaw, GenomicAW1Raw, GenomicAW2Raw

GENOMIC_SHORT_READ_INGESTION_MAP = {
    GenomicJob.AW1_MANIFEST: {
        'raw': {
            'model': GenomicAW1Raw,
            'job_id': GenomicJob.LOAD_AW1_TO_RAW_TABLE
        },
    },
    GenomicJob.AW1F_MANIFEST: {},
    GenomicJob.METRICS_INGESTION: {
        'raw': {
            'model': GenomicAW2Raw,
            'job_id': GenomicJob.LOAD_AW2_TO_RAW_TABLE
        },
    },
    GenomicJob.AW4_ARRAY_WORKFLOW: {
        'raw': {
            'model': GenomicAW4Raw,
            'job_id': GenomicJob.LOAD_AW4_TO_RAW_TABLE
        },
    },
    GenomicJob.AW4_WGS_WORKFLOW: {
        'raw': {
            'model': GenomicAW4Raw,
            'job_id': GenomicJob.LOAD_AW4_TO_RAW_TABLE
        },
    },
    GenomicJob.AW5_ARRAY_MANIFEST: {},
    GenomicJob.AW5_WGS_MANIFEST: {},
}

GENOMIC_GEM_INGESTION_MAP = {
    GenomicJob.GEM_A2_MANIFEST: {
        'raw': {
            'model': GenomicA2Raw,
            'job_id': GenomicJob.LOAD_A2_TO_RAW_TABLE
        },
    },
}

GENOMIC_CVL_INGESTION_MAP = {
    GenomicJob.CVL_W2SC_WORKFLOW: {
        'raw': {
            'model': GenomicW2SCRaw,
            'job_id': GenomicJob.LOAD_CVL_W2SC_TO_RAW_TABLE
        },
    },
    GenomicJob.CVL_W3NS_WORKFLOW: {
        'raw': {
            'model': GenomicW3NSRaw,
            'job_id': GenomicJob.LOAD_CVL_W3NS_TO_RAW_TABLE
        },
    },
    GenomicJob.CVL_W3SC_WORKFLOW: {
        'raw': {
            'model': GenomicW3SCRaw,
            'job_id':  GenomicJob.LOAD_CVL_W3SC_TO_RAW_TABLE
        },
    },
    GenomicJob.CVL_W3SS_WORKFLOW: {
        'raw': {
            'model': GenomicW3SSRaw,
            'job_id': GenomicJob.LOAD_CVL_W3SS_TO_RAW_TABLE
        },
    },
    GenomicJob.CVL_W4WR_WORKFLOW: {
        'raw': {
            'model': GenomicW4WRRaw,
            'job_id': GenomicJob.LOAD_CVL_W4WR_TO_RAW_TABLE
        },
    },
    GenomicJob.CVL_W5NF_WORKFLOW: {
        'raw': {
            'model': GenomicW5NFRaw,
            'job_id': GenomicJob.LOAD_CVL_W5NF_TO_RAW_TABLE
        },
    },
}

GENOMIC_LONG_READ_INGESTION_MAP = {
    GenomicJob.LR_LR_WORKFLOW: {
        'raw': {
            'model': GenomicLRRaw,
            'job_id': GenomicJob.LOAD_LR_TO_RAW_TABLE
        },
    },
    GenomicJob.LR_L1_WORKFLOW: {
        'raw': {
            'model': GenomicL1Raw,
            'job_id': GenomicJob.LOAD_L1_TO_RAW_TABLE
        },
    },
    GenomicJob.LR_L1F_WORKFLOW: {
        'raw': {
            'model': GenomicL1FRaw,
            'job_id': GenomicJob.LOAD_L1F_TO_RAW_TABLE
        },
    },
    GenomicJob.LR_L2_ONT_WORKFLOW: {
        'raw': {
            'model': GenomicL2ONTRaw,
            'job_id': GenomicJob.LOAD_L2_ONT_TO_RAW_TABLE
        },
    },
    GenomicJob.LR_L2_PB_CCS_WORKFLOW: {
        'raw': {
            'model': GenomicL2PBCCSRaw,
            'job_id': GenomicJob.LOAD_L2_PB_CCS_TO_RAW_TABLE
        },
    },
    GenomicJob.LR_L4_WORKFLOW: {
        'raw': {
            'model': GenomicL4Raw,
            'job_id': GenomicJob.LOAD_L4_TO_RAW_TABLE
        },
    },
    GenomicJob.LR_L4F_WORKFLOW: {
        'raw': {
            'model': GenomicL4FRaw,
            'job_id': GenomicJob.LOAD_L4F_TO_RAW_TABLE
        },
    },
    GenomicJob.LR_L5_WORKFLOW: {
        'raw': {
            'model': GenomicL5Raw,
            'job_id': GenomicJob.LOAD_L5_TO_RAW_TABLE
        },
    },
    GenomicJob.LR_L6_WORKFLOW: {
        'raw': {
            'model': GenomicL6Raw,
            'job_id':  GenomicJob.LOAD_L6_TO_RAW_TABLE
        },
    },
    GenomicJob.LR_L6F_WORKFLOW: {
        'raw': {
            'model': GenomicL6FRaw,
            'job_id': GenomicJob.LOAD_L6F_TO_RAW_TABLE
        },
    },
}

GENOMIC_PROTEOMICS_INGESTION_MAP = {
    GenomicJob.PR_PR_WORKFLOW: {
        'raw': {
            'model': GenomicPRRaw,
            'job_id':  GenomicJob.LOAD_PR_TO_RAW_TABLE
        },
    },
    GenomicJob.PR_P1_WORKFLOW: {
        'raw': {
            'model': GenomicP1Raw,
            'job_id': GenomicJob.LOAD_P1_TO_RAW_TABLE
        },
    },
    GenomicJob.PR_P2_WORKFLOW: {
        'raw': {
            'model': GenomicP2Raw,
            'job_id': GenomicJob.LOAD_P2_TO_RAW_TABLE
        },
    }
}

GENOMIC_RNA_INGESTION_MAP = {
    GenomicJob.RNA_RR_WORKFLOW: {
        'raw': {
            'model': GenomicRRRaw,
            'job_id': GenomicJob.LOAD_RR_TO_RAW_TABLE,
        },
    },
    GenomicJob.RNA_R1_WORKFLOW: {
        'raw': {
            'model': GenomicR1Raw,
            'job_id': GenomicJob.LOAD_R1_TO_RAW_TABLE,
            'special_mappings': {
                '260_230': 'two_sixty_two_thirty',
                '260_280': 'two_sixty_two_eighty'
            }
        },
    },
    GenomicJob.RNA_R2_WORKFLOW: {
        'raw': {
            'model': GenomicR2Raw,
            'job_id': GenomicJob.LOAD_R2_TO_RAW_TABLE,
        },
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
