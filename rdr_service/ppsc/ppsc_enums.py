from protorpc import messages


class AuthType(messages.Enum):
    DATA_TRANSFER = 1
    NPH_OPT_IN = 2


class DataSyncTransferType(messages.Enum):
    CORE = 1
    EHR = 2
    BIOBANK_SAMPLE = 3
    HEALTH_DATA = 4
    NPH_OPT_IN = 5


class SpecimenType(messages.Enum):
    BLOOD = 1
    SALIVA = 2
    URINE = 3


class SpecimenStatus(messages.Enum):
    RECEIVED = 1

