from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Boolean, UniqueConstraint
from sqlalchemy.orm import relationship
from rdr_service.model.field_types import BlobUTF8
from rdr_service.model.base import Base
from rdr_service.model.utils import Enum
from rdr_service.participant_enums import WorkbenchWorkspaceStatus, WorkbenchWorkspaceUserRole


class WorkbenchWorkspaceBase(object):
    workspaceSourceId = Column("workspace_source_id", Integer, nullable=False)
    name = Column("name", String(250), nullable=False)
    creationTime = Column("creation_time", DateTime, nullable=True)
    modifiedTime = Column("modified_time", DateTime, nullable=True)
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
    resource = Column("resource", BlobUTF8, nullable=False)


class WorkbenchWorkspace(WorkbenchWorkspaceBase, Base):
    __tablename__ = "workbench_workspace"

    WorkbenchWorkspaceUser = relationship("WorkbenchWorkspaceUser", cascade="all, delete-orphan")

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)

    __table_args__ = (UniqueConstraint("workbench_workspace", "workspace_source_id"))


class WorkbenchWorkspaceUser(Base):
    __tablename__ = "workbench_workspace_user"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)

    workspaceId = Column("workspace_id", Integer, ForeignKey("workbench_workspace.id"), nullable=False)
    userId = Column("user_id", Integer, ForeignKey("workbench_researcher.id"), nullable=False)
    role = Column("role", Enum(WorkbenchWorkspaceUserRole), default=WorkbenchWorkspaceUserRole.UNSET)
    status = Column("status", Enum(WorkbenchWorkspaceStatus), default=WorkbenchWorkspaceStatus.UNSET)


class WorkbenchWorkspaceHistory(WorkbenchWorkspaceBase, Base):
    __tablename__ = "workbench_workspace_history"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)


class WorkbenchWorkspaceUserHistory(Base):
    __tablename__ = "workbench_workspace_user"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)

    workspaceId = Column("workspace_id", Integer, ForeignKey("workbench_workspace_history.id"), nullable=False)
    userId = Column("user_id", Integer, ForeignKey("workbench_researcher_history.id"), nullable=False)
    role = Column("role", Enum(WorkbenchWorkspaceUserRole), default=WorkbenchWorkspaceUserRole.UNSET)
    status = Column("status", Enum(WorkbenchWorkspaceStatus), default=WorkbenchWorkspaceStatus.UNSET)
