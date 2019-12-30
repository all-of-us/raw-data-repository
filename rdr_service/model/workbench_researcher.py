from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, UniqueConstraint, JSON
from sqlalchemy.orm import relationship
from rdr_service.model.field_types import BlobUTF8
from rdr_service.model.base import Base
from rdr_service.model.utils import Enum
from rdr_service.participant_enums import WorkbenchInstitutionNoAcademic

class WorkbenchResearcherBase(object):
    userSourceId = Column("user_source_id", Integer, nullable=False)
    creationTime = Column("creation_time", DateTime, nullable=True)
    modifiedTime = Column("modified_time", DateTime, nullable=True)
    givenName = Column("given_name", String(100))
    familyName = Column("family_name", String(100))
    streetAddress1 = Column("street_address1", String(250))
    streetAddress2 = Column("street_address2", String(250))
    city = Column("city", String(80))
    state = Column("state", String(80))
    zipCode = Column("zip_code", String(80))
    country = Column("country", String(80))
    ethnicity = Column("ethnicity", String(80))
    gender = Column("gender", JSON)
    race = Column("race", JSON)
    resource = Column("resource", BlobUTF8, nullable=False)


class WorkbenchResearcher(WorkbenchResearcherBase, Base):
    __tablename__ = "workbench_researcher"

    workbenchInstitutionalAffiliations = relationship("WorkbenchInstitutionalAffiliations",
                                                      cascade="all, delete-orphan")
    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)

    __table_args__ = (UniqueConstraint("user_source_id", name="uniqe_user_source_id"),)


class WorkbenchInstitutionalAffiliations(Base):
    __tablename__ = "workbench_institutional_affiliations"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)

    researcherId = Column("researcher_id", Integer, ForeignKey("workbench_researcher.id"), nullable=False)
    institution = Column("institution", String(250))
    role = Column("role", String(80))
    nonAcademicAffiliation = Column("non_academic_affiliation", Enum(WorkbenchInstitutionNoAcademic),
                                    default=WorkbenchInstitutionNoAcademic.UNSET)


class WorkbenchResearcherHistory(WorkbenchResearcherBase, Base):
    __tablename__ = "workbench_researcher_history"

    workbenchInstitutionalAffiliations = relationship("WorkbenchInstitutionalAffiliationsHistory",
                                                      cascade="all, delete-orphan")
    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)


class WorkbenchInstitutionalAffiliationsHistory(Base):
    __tablename__ = "workbench_institutional_affiliations_history"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)

    researcherId = Column("researcher_id", Integer, ForeignKey("workbench_researcher_history.id"), nullable=False)
    institution = Column("institution", String(250))
    role = Column("role", String(80))
    nonAcademicAffiliation = Column("non_academic_affiliation", Enum(WorkbenchInstitutionNoAcademic),
                                    default=WorkbenchInstitutionNoAcademic.UNSET)
