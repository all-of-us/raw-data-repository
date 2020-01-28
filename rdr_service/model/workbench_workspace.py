from sqlalchemy import Column, ForeignKey, Integer, String, Boolean, UniqueConstraint, event, JSON
from sqlalchemy.orm import relationship
from rdr_service.model.field_types import BlobUTF8
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, UTCDateTime6
from rdr_service.participant_enums import WorkbenchWorkspaceStatus, WorkbenchWorkspaceUserRole, \
    WorkbenchWorkspaceSexAtBirth, WorkbenchWorkspaceGenderIdentity, WorkbenchWorkspaceSexualOrientation, \
    WorkbenchWorkspaceGeography, WorkbenchWorkspaceDisabilityStatus, WorkbenchWorkspaceAccessToCare, \
    WorkbenchWorkspaceEducationLevel, WorkbenchWorkspaceIncomeLevel


class WorkbenchWorkspaceBase(object):
    workspaceSourceId = Column("workspace_source_id", Integer, nullable=False)
    name = Column("name", String(250), nullable=False)
    creationTime = Column("creation_time", UTCDateTime6, nullable=True)
    modifiedTime = Column("modified_time", UTCDateTime6, nullable=True)
    status = Column("status", Enum(WorkbenchWorkspaceStatus), default=WorkbenchWorkspaceStatus.UNSET)
    excludeFromPublicDirectory = Column("exclude_from_public_directory", Boolean)
    diseaseFocusedResearch = Column("disease_focused_research", Boolean)
    diseaseFocusedResearchName = Column("disease_focused_research_name", String(250))
    otherPurposeDetails = Column("other_purpose_details", String(250))
    methodsDevelopment = Column("methods_development", Boolean)
    controlSet = Column("control_set", Boolean)
    ancestry = Column("ancestry", Boolean)
    socialBehavioral = Column("social_behavioral", Boolean)
    populationHealth = Column("population_health", Boolean)
    drugDevelopment = Column("drug_development", Boolean)
    commercialPurpose = Column("commercial_purpose", Boolean)
    educational = Column("educational", Boolean)
    otherPurpose = Column("other_purpose", Boolean)
    scientificApproaches = Column("scientific_approaches", String(500))
    intendToStudy = Column("intend_to_study", String(500))
    findingsFromStudy = Column("findings_from_study", String(500))
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
    others = Column("others", String(500))

    resource = Column("resource", BlobUTF8, nullable=False)


class WorkbenchWorkspace(WorkbenchWorkspaceBase, Base):
    __tablename__ = "workbench_workspace"

    workbenchWorkspaceUser = relationship("WorkbenchWorkspaceUser", cascade="all, delete-orphan")

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)

    __table_args__ = (UniqueConstraint("workspace_source_id", name="uniqe_workspace_source_id"),)


class WorkbenchWorkspaceUser(Base):
    __tablename__ = "workbench_workspace_user"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)

    workspaceId = Column("workspace_id", Integer, ForeignKey("workbench_workspace.id"), nullable=False)
    researcherId = Column("researcher_Id", Integer, ForeignKey("workbench_researcher.id"), nullable=False)
    userId = Column("user_id", Integer, nullable=False)
    role = Column("role", Enum(WorkbenchWorkspaceUserRole), default=WorkbenchWorkspaceUserRole.UNSET)
    status = Column("status", Enum(WorkbenchWorkspaceStatus), default=WorkbenchWorkspaceStatus.UNSET)


class WorkbenchWorkspaceHistory(WorkbenchWorkspaceBase, Base):
    __tablename__ = "workbench_workspace_history"

    workbenchWorkspaceUser = relationship("WorkbenchWorkspaceUserHistory", cascade="all, delete-orphan")

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)


class WorkbenchWorkspaceUserHistory(Base):
    __tablename__ = "workbench_workspace_user_history"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)

    workspaceId = Column("workspace_id", Integer, ForeignKey("workbench_workspace_history.id"), nullable=False)
    researcherId = Column("researcher_Id", Integer, ForeignKey("workbench_researcher_history.id"), nullable=False)
    userId = Column("user_id", Integer, nullable=False)
    role = Column("role", Enum(WorkbenchWorkspaceUserRole), default=WorkbenchWorkspaceUserRole.UNSET)
    status = Column("status", Enum(WorkbenchWorkspaceStatus), default=WorkbenchWorkspaceStatus.UNSET)


event.listen(WorkbenchWorkspace, "before_insert", model_insert_listener)
event.listen(WorkbenchWorkspace, "before_update", model_update_listener)
event.listen(WorkbenchWorkspaceUser, "before_insert", model_insert_listener)
event.listen(WorkbenchWorkspaceUser, "before_update", model_update_listener)
event.listen(WorkbenchWorkspaceHistory, "before_insert", model_insert_listener)
event.listen(WorkbenchWorkspaceHistory, "before_update", model_update_listener)
event.listen(WorkbenchWorkspaceUserHistory, "before_insert", model_insert_listener)
event.listen(WorkbenchWorkspaceUserHistory, "before_update", model_update_listener)
