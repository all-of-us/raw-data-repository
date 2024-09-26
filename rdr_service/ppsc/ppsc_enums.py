from protorpc import messages


class AuthType(messages.Enum):
    DATA_TRANSFER = 1


class DataSyncTransferType(messages.Enum):
    CORE = 1
    EHR = 2
    BIOBANK_SAMPLE = 3
    HEALTH_DATA = 4
