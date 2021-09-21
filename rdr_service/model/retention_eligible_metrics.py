from sqlalchemy import Column, ForeignKey, Integer, event, Boolean
from rdr_service.model.utils import Enum
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime6
from rdr_service.participant_enums import RetentionType, RetentionStatus


class RetentionEligibleMetrics(Base):
    __tablename__ = "retention_eligible_metrics"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)

    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"), nullable=False,
                           index=True)
    retentionEligible = Column("retention_eligible", Boolean)
    retentionEligibleTime = Column("retention_eligible_time", UTCDateTime6)
    lastActiveRetentionActivityTime = Column("last_active_retention_activity_time", UTCDateTime6)

    activelyRetained = Column("actively_retained", Boolean)
    passivelyRetained = Column("passively_retained", Boolean)
    fileUploadDate = Column("file_upload_date", UTCDateTime6)
    retentionEligibleStatus = Column("retention_eligible_status", Enum(RetentionStatus))
    retentionType = Column("retention_type", Enum(RetentionType), default=RetentionType.UNSET)


event.listen(RetentionEligibleMetrics, "before_insert", model_insert_listener)
event.listen(RetentionEligibleMetrics, "before_update", model_update_listener)
