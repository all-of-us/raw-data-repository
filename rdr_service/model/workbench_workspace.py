from sqlalchemy import Column, ForeignKey, Integer, String, Boolean, UniqueConstraint, event, JSON, TEXT
from sqlalchemy.orm import relationship
from rdr_service.model.field_types import BlobUTF8
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, UTCDateTime6
from rdr_service.participant_enums import WorkbenchWorkspaceStatus, WorkbenchWorkspaceUserRole, \
    WorkbenchWorkspaceSexAtBirth, WorkbenchWorkspaceGenderIdentity, WorkbenchWorkspaceSexualOrientation, \
    WorkbenchWorkspaceGeography, WorkbenchWorkspaceDisabilityStatus, WorkbenchWorkspaceAccessToCare, \
    WorkbenchWorkspaceEducationLevel, WorkbenchWorkspaceIncomeLevel, WorkbenchAuditReviewType, \
    WorkbenchAuditWorkspaceDisplayDecision, WorkbenchAuditWorkspaceAccessDecision


class WorkbenchWorkspaceBase(object):
    workspaceSourceId = Column("workspace_source_id", Integer, nullable=False)
    """The workspace ID in RW system."""
    name = Column("name", String(1000), nullable=False)
    """The user-provided workspace name."""
    creationTime = Column("creation_time", UTCDateTime6, nullable=True)
    """Workspace creation time in RW system."""
    modifiedTime = Column("modified_time", UTCDateTime6, nullable=True)
    """The last-modified time of the workspace in RW system."""
    status = Column("status", Enum(WorkbenchWorkspaceStatus), default=WorkbenchWorkspaceStatus.UNSET)
    excludeFromPublicDirectory = Column("exclude_from_public_directory", Boolean)
    """Whether this workspace should be hidden from public view"""
    reviewRequested = Column("review_requested", Boolean)
    diseaseFocusedResearch = Column("disease_focused_research", Boolean)
    """Purpose: Disease Focused Research"""
    diseaseFocusedResearchName = Column("disease_focused_research_name", String(1000))
    """Purpose: Disease name"""
    otherPurposeDetails = Column("other_purpose_details", String(2000))
    """Purpose: Other purpose description"""
    methodsDevelopment = Column("methods_development", Boolean)
    """Purpose: Methods development/validation study"""
    controlSet = Column("control_set", Boolean)
    """Purpose: Research Control"""
    ancestry = Column("ancestry", Boolean)
    """Purpose: Genetic Research"""
    socialBehavioral = Column("social_behavioral", Boolean)
    """Purpose: Social/Behavioral Research"""
    populationHealth = Column("population_health", Boolean)
    """Purpose: Population Health/Public Health Research"""
    drugDevelopment = Column("drug_development", Boolean)
    """Purpose: Drug/Therapeutics Development Research"""
    commercialPurpose = Column("commercial_purpose", Boolean)
    """Purpose: For Profit"""
    educational = Column("educational", Boolean)
    """Purpose: Educational Purpose"""
    otherPurpose = Column("other_purpose", Boolean)
    """Purpose: Other Purpose"""
    scientificApproaches = Column("scientific_approaches", TEXT)
    intendToStudy = Column("intend_to_study", TEXT)
    findingsFromStudy = Column("findings_from_study", TEXT)
    ethicalLegalSocialImplications = Column("ethical_legal_social_implications", Boolean)
    focusOnUnderrepresentedPopulations = Column("focus_on_underrepresented_populations", Boolean)
    raceEthnicity = Column("race_ethnicity", JSON)
    age = Column("age", JSON)
    sexAtBirth = Column("sex_at_birth", Enum(WorkbenchWorkspaceSexAtBirth), default=WorkbenchWorkspaceSexAtBirth.UNSET)
    genderIdentity = Column("gender_identity", Enum(WorkbenchWorkspaceGenderIdentity),
                            default=WorkbenchWorkspaceGenderIdentity.UNSET)
    sexualOrientation = Column("sexual_orientation", Enum(WorkbenchWorkspaceSexualOrientation),
                               default=WorkbenchWorkspaceSexualOrientation.UNSET)
    geography = Column("geography", Enum(WorkbenchWorkspaceGeography),
                       default=WorkbenchWorkspaceGeography.UNSET)
    disabilityStatus = Column("disability_status", Enum(WorkbenchWorkspaceDisabilityStatus),
                              default=WorkbenchWorkspaceDisabilityStatus.UNSET)
    accessToCare = Column("access_to_care", Enum(WorkbenchWorkspaceAccessToCare),
                          default=WorkbenchWorkspaceAccessToCare.UNSET)
    educationLevel = Column("education_level", Enum(WorkbenchWorkspaceEducationLevel),
                            default=WorkbenchWorkspaceEducationLevel.UNSET)
    incomeLevel = Column("income_level", Enum(WorkbenchWorkspaceIncomeLevel),
                         default=WorkbenchWorkspaceIncomeLevel.UNSET)
    others = Column("others", String(2000))
    isReviewed = Column("is_reviewed", Boolean, default=False)
    cdrVersion = Column("cdr_version", String(200))

    resource = Column("resource", BlobUTF8, nullable=False)
    """The resource payload"""


class WorkbenchWorkspaceApproved(WorkbenchWorkspaceBase, Base):
    __tablename__ = "workbench_workspace_approved"

    workbenchWorkspaceUser = relationship("WorkbenchWorkspaceUser", cascade="all, delete-orphan")

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""

    __table_args__ = (UniqueConstraint("workspace_source_id", name="uniqe_workspace_source_id"),)


class WorkbenchWorkspaceUser(Base):
    __tablename__ = "workbench_workspace_user"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""

    workspaceId = Column("workspace_id", Integer, ForeignKey("workbench_workspace_approved.id"), nullable=False)
    researcherId = Column("researcher_Id", Integer, ForeignKey("workbench_researcher.id"), nullable=False)
    userId = Column("user_id", Integer, nullable=False)
    role = Column("role", Enum(WorkbenchWorkspaceUserRole), default=WorkbenchWorkspaceUserRole.UNSET)
    status = Column("status", Enum(WorkbenchWorkspaceStatus), default=WorkbenchWorkspaceStatus.UNSET)
    isCreator = Column("is_creator", Boolean)


class WorkbenchWorkspaceSnapshot(WorkbenchWorkspaceBase, Base):
    __tablename__ = "workbench_workspace_snapshot"

    workbenchWorkspaceUser = relationship("WorkbenchWorkspaceUserHistory", cascade="all, delete-orphan")

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""


class WorkbenchWorkspaceUserHistory(Base):
    __tablename__ = "workbench_workspace_user_history"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    """
    When that record was created in the history table specifically (if main table is updated; previous version
    if/when a record is updated; if never changed, it appears as it was originally created)
    """
    modified = Column("modified", UTCDateTime6, nullable=True)
    """
    When that record was created in the history table specifically (if main table is updated; previous version
    if/when a record is updated; if never changed, it appears as it was originally created)
    """

    workspaceId = Column("workspace_id", Integer, ForeignKey("workbench_workspace_snapshot.id"), nullable=False)
    researcherId = Column("researcher_Id", Integer, ForeignKey("workbench_researcher_history.id"), nullable=False)
    userId = Column("user_id", Integer, nullable=False)
    role = Column("role", Enum(WorkbenchWorkspaceUserRole), default=WorkbenchWorkspaceUserRole.UNSET)
    status = Column("status", Enum(WorkbenchWorkspaceStatus), default=WorkbenchWorkspaceStatus.UNSET)
    isCreator = Column("is_creator", Boolean)


class WorkbenchAudit(Base):
    __tablename__ = "workbench_audit"

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    modified = Column("modified", UTCDateTime6, nullable=True)
    workspaceSnapshotId = Column("workspace_snapshot_id", Integer, ForeignKey("workbench_workspace_snapshot.id"),
                                 nullable=False)
    auditorPmiEmail = Column("auditor_pmi_email", String(200))
    auditReviewType = Column("audit_review_type", Enum(WorkbenchAuditReviewType),
                             default=WorkbenchAuditReviewType.UNSET)
    auditWorkspaceDisplayDecision = Column("audit_workspace_display_decision",
                                           Enum(WorkbenchAuditWorkspaceDisplayDecision),
                                           default=WorkbenchAuditWorkspaceDisplayDecision.UNSET)
    auditWorkspaceAccessDecision = Column("audit_workspace_access_decision",
                                          Enum(WorkbenchAuditWorkspaceAccessDecision),
                                          default=WorkbenchAuditWorkspaceAccessDecision.UNSET)
    auditNotes = Column("audit_notes", String(1000))

    resource = Column("resource", BlobUTF8, nullable=False)


event.listen(WorkbenchWorkspaceApproved, "before_insert", model_insert_listener)
event.listen(WorkbenchWorkspaceApproved, "before_update", model_update_listener)
event.listen(WorkbenchWorkspaceUser, "before_insert", model_insert_listener)
event.listen(WorkbenchWorkspaceUser, "before_update", model_update_listener)
event.listen(WorkbenchWorkspaceSnapshot, "before_insert", model_insert_listener)
event.listen(WorkbenchWorkspaceSnapshot, "before_update", model_update_listener)
event.listen(WorkbenchWorkspaceUserHistory, "before_insert", model_insert_listener)
event.listen(WorkbenchWorkspaceUserHistory, "before_update", model_update_listener)
event.listen(WorkbenchAudit, "before_insert", model_insert_listener)
event.listen(WorkbenchAudit, "before_update", model_update_listener)
