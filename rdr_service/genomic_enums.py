from protorpc import messages


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
    INVALID_AIAN = 10
    INVALID_SUSPENSION_STATUS = 11


class GenomicJob(messages.Enum):
    """Genomic Job Definitions"""
    UNSET = 0
    METRICS_INGESTION = 1
    RECONCILE_MANIFEST = 2
    RECONCILE_ARRAY_DATA = 3
    NEW_PARTICIPANT_WORKFLOW = 4
    CVL_RECONCILIATION_REPORT = 5
    CREATE_CVL_W1_MANIFESTS = 6
    BB_RETURN_MANIFEST = 7
    AW1_MANIFEST = 8
    CVL_SEC_VAL_MAN = 9
    GEM_A1_MANIFEST = 10
    GEM_A2_MANIFEST = 11
    GEM_A3_MANIFEST = 12
    AW1F_MANIFEST = 13
    RECONCILE_WGS_DATA = 14
    W2_INGEST = 15
    W3_MANIFEST = 16
    C2_PARTICIPANT_WORKFLOW = 17
    AW1F_ALERTS = 18
    C1_PARTICIPANT_WORKFLOW = 19
    AW3_ARRAY_WORKFLOW = 20
    AW3_WGS_WORKFLOW = 21
    AW4_ARRAY_WORKFLOW = 22
    AW4_WGS_WORKFLOW = 23
    GEM_METRICS_INGEST = 24
    AW1C_INGEST = 25
    AW1CF_INGEST = 26
    AW1CF_ALERTS = 27

    GENOMIC_MANIFEST_FILE_TRIGGER = 28
    AW2F_MANIFEST = 29
    FEEDBACK_SCAN = 30
    RECALCULATE_CONTAMINATION_CATEGORY = 31

    CALCULATE_RECORD_COUNT_AW1 = 32
    CALCULATE_RECORD_COUNT_AW2 = 33  # TODO: To be implemented in future PR

    LOAD_AW1_TO_RAW_TABLE = 34
    LOAD_AW2_TO_RAW_TABLE = 35

    AW5_ARRAY_MANIFEST = 36
    AW5_WGS_MANIFEST = 37

    INGEST_DATA_FILES = 38
    INGEST_INFORMING_LOOP = 39

    # Data Quality Pipeline Jobs
    # Naming matters for reports (timeframe_level_report_target)
    DAILY_SUMMARY_REPORT_JOB_RUNS = 101
    WEEKLY_SUMMARY_REPORT_JOB_RUNS = 102
    DAILY_SUMMARY_REPORT_INGESTIONS = 103
    WEEKLY_SUMMARY_REPORT_INGESTIONS = 104
    DAILY_SUMMARY_REPORT_INCIDENTS = 105


class GenomicWorkflowState(messages.Enum):
    """Genomic State Definitions. States are not in any order. """
    UNSET = 0
    WITHDRAWN = 1
    AW0 = 2
    AW1 = 3
    AW1F_PRE = 4
    AW1F_POST = 5
    AW2 = 6
    AW2_MISSING = 7
    AW2_FAIL = 8

    # CVL Workflow only
    W1 = 9
    W2 = 10
    W3 = 11
    AW1C = 12
    AW1CF_PRE = 13
    AW1CF_POST = 14
    RHP_START = 15
    W4 = 16
    W4F = 17
    RHP_RPT_READY = 18
    RHP_RPT_PENDING_DELETE = 19
    RHP_RPT_DELETED = 20
    RHP_RPT_ACCESSED = 21
    CVL_READY = 22

    # GEM Reporting States
    GEM_RPT_READY = 23
    GEM_RPT_PENDING_DELETE = 24
    GEM_RPT_DELETED = 25
    GEM_RPT_ACCESSED = 26
    GEM_READY = 27
    A1 = 28
    A2 = 29
    A2F = 30
    A3 = 31

    # Misc States
    AW0_READY = 32
    IGNORE = 33
    CONTROL_SAMPLE = 34

    # Long Read
    LR_PENDING = 35
    LR_REJECTED = 36
    LR_ACCEPTED = 37


class GenomicReportState(messages.Enum):

    UNSET = 0

    # GEM Reporting States
    GEM_RPT_READY = 1
    GEM_RPT_PENDING_DELETE = 2
    GEM_RPT_DELETED = 3

    # PGX Reporting States
    PGX_RPT_READY = 4
    PGX_RPT_PENDING_DELETE = 5
    PGX_RPT_DELETED = 6

    # HDR Reporting States
    HDR_RPT_UNINFORMATIVE = 7
    HDR_RPT_POSITIVE = 8
    HDR_RPT_PENDING_DELETE = 9
    HDR_RPT_DELETED = 10


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


class GenomicManifestTypes(messages.Enum):
    DRC_BIOBANK = 1  # AW0
    BIOBANK_GC = 2  # AW1
    GC_DRC = 3  # AW2
    CVL_W1 = 4
    GEM_A1 = 5
    GEM_A3 = 6
    CVL_W3 = 7
    AW3_ARRAY = 8
    AW3_WGS = 9
    AW2F = 10
    GEM_A2 = 11
    AW4_ARRAY = 12
    AW4_WGS = 13
    AW1F = 14
    AW5_ARRAY = 15
    AW5_WGS = 16


class GenomicContaminationCategory(messages.Enum):
    UNSET = 0
    NO_EXTRACT = 1
    EXTRACT_WGS = 2
    EXTRACT_BOTH = 3
    TERMINAL_NO_EXTRACT = 4


class GenomicQcStatus(messages.Enum):
    UNSET = 0
    PASS = 1
    FAIL = 2


class GenomicIncidentCode(messages.Enum):
    UNSET = 0
    UNKNOWN = 1
    UNABLE_TO_FIND_MEMBER = 2
    MISSING_FILES = 3
    DATA_VALIDATION_FAILED = 4
    FILE_VALIDATION_FAILED_NAME = 5
    FILE_VALIDATION_FAILED_STRUCTURE = 6
    UNABLE_TO_FIND_METRIC = 7


class GenomicIncidentStatus(messages.Enum):
    OPEN = 0
    RESOLVED = 1
    UNABLE_TO_RESOLVE = 2
