from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint, JSON, event, Boolean, UnicodeText
from sqlalchemy.orm import relationship
from rdr_service.model.field_types import BlobUTF8
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, UTCDateTime6
from rdr_service.participant_enums import (
    WorkbenchInstitutionNonAcademic,
    WorkbenchResearcherEducation,
    WorkbenchResearcherDisability,
    WorkbenchResearcherEthnicity,
    WorkbenchReviewStatus, WorkbenchReviewType, WorkbenchDecisionStatus, WorkspaceAccessStatus)


class WorkbenchResearcherBase(object):
    userSourceId = Column("user_source_id", Integer, nullable=False)
    creationTime = Column("creation_time", UTCDateTime6, nullable=True)
    modifiedTime = Column("modified_time", UTCDateTime6, nullable=True)
    givenName = Column("given_name", String(100))
    familyName = Column("family_name", String(100))
    email = Column("email", String(250))
    streetAddress1 = Column("street_address1", String(250))
    streetAddress2 = Column("street_address2", String(250))
    city = Column("city", String(80))
    state = Column("state", String(80))
    zipCode = Column("zip_code", String(80))
    country = Column("country", String(80))
    ethnicity = Column("ethnicity", Enum(WorkbenchResearcherEthnicity), default=WorkbenchResearcherEthnicity.UNSET)
    gender = Column("gender", JSON)
    race = Column("race", JSON)
    sexAtBirth = Column("sex_at_birth", JSON)
    education = Column("education", Enum(WorkbenchResearcherEducation), default=WorkbenchResearcherEducation.UNSET)
    degree = Column("degree", JSON)
    disability = Column("disability", Enum(WorkbenchResearcherDisability), default=WorkbenchResearcherDisability.UNSET)
    identifiesAsLgbtq = Column("identifies_as_lgbtq", Boolean)
    lgbtqIdentity = Column("lgbtq_identity", String(250))
    resource = Column("resource", BlobUTF8, nullable=False)


class WorkbenchResearcher(WorkbenchResearcherBase, Base):
    __tablename__ = "workbench_researcher"

    workbenchInstitutionalAffiliations = relationship("WorkbenchInstitutionalAffiliations",
                                                      cascade="all, delete-orphan")
    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)

    __table_args__ = (UniqueConstraint("user_source_id", name="uniqe_user_source_id"),)


class WorkbenchInstitutionalAffiliations(Base):
    __tablename__ = "workbench_institutional_affiliations"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)

    researcherId = Column("researcher_id", Integer, ForeignKey("workbench_researcher.id"), nullable=False)
    institution = Column("institution", String(250))
    role = Column("role", String(80))
    nonAcademicAffiliation = Column("non_academic_affiliation", Enum(WorkbenchInstitutionNonAcademic),
                                    default=WorkbenchInstitutionNonAcademic.UNSET)
    isVerified = Column("is_verified", Boolean)


class WorkbenchResearcherHistory(WorkbenchResearcherBase, Base):
    __tablename__ = "workbench_researcher_history"

    workbenchInstitutionalAffiliations = relationship("WorkbenchInstitutionalAffiliationsHistory",
                                                      cascade="all, delete-orphan")
    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)


class WorkbenchInstitutionalAffiliationsHistory(Base):
    __tablename__ = "workbench_institutional_affiliations_history"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)

    researcherId = Column("researcher_id", Integer, ForeignKey("workbench_researcher_history.id"), nullable=False)
    institution = Column("institution", String(250))
    role = Column("role", String(80))
    nonAcademicAffiliation = Column("non_academic_affiliation", Enum(WorkbenchInstitutionNonAcademic),
                                    default=WorkbenchInstitutionNonAcademic.UNSET)
    isVerified = Column("is_verified", Boolean)


class WorkbenchAudit(Base):
    __tablename__ = "workbench_audit"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)
    workspaceSnapshotId = Column("workspace_snapshot_id", ForeignKey("workbench_workspace_history.id"))
    auditorPmiEmail = Column("auditor_pmi_email", String(250))
    auditReviewType = Column("audit_review_type", Enum(WorkbenchReviewType))
    auditReviewStatus = Column("audit_review_status", Enum(WorkbenchReviewStatus))
    auditDecisionType = Column("audit_decision_type", Enum(WorkbenchDecisionStatus))
    auditWorkspaceAccess = Column("audit_workspace_access", Enum(WorkspaceAccessStatus))
    auditNotes = Column("audit_notes", UnicodeText)


event.listen(WorkbenchResearcher, "before_insert", model_insert_listener)
event.listen(WorkbenchResearcher, "before_update", model_update_listener)
event.listen(WorkbenchInstitutionalAffiliations, "before_insert", model_insert_listener)
event.listen(WorkbenchInstitutionalAffiliations, "before_update", model_update_listener)
event.listen(WorkbenchResearcherHistory, "before_insert", model_insert_listener)
event.listen(WorkbenchResearcherHistory, "before_update", model_update_listener)
event.listen(WorkbenchInstitutionalAffiliationsHistory, "before_insert", model_insert_listener)
event.listen(WorkbenchInstitutionalAffiliationsHistory, "before_update", model_update_listener)
