from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Boolean, UniqueConstraint
from sqlalchemy.orm import relationship
from rdr_service.model.field_types import BlobUTF8
from rdr_service.model.base import Base


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
    gender = Column("gender", String(80))
    race = Column("race", String(80))
    resource = Column("resource", BlobUTF8, nullable=False)


class WorkbenchResearcher(WorkbenchResearcherBase, Base):
    __tablename__ = "workbench_researcher"

    WorkbenchInstitutionalAffiliations = relationship("WorkbenchInstitutionalAffiliations",
                                                      cascade="all, delete-orphan")

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)

    __table_args__ = (UniqueConstraint("workbench_researcher", "user_source_id"))


class WorkbenchInstitutionalAffiliations(Base):
    __tablename__ = "workbench_institutional_affiliations"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)

    userId = Column("user_id", Integer, ForeignKey("workbench_researcher.id"), nullable=False)
    institution = Column("institution", String(250))
    role = Column("role", String(80))
    nonAcademicAffiliation = Column("non_academic_affiliation", Boolean)


class WorkbenchResearcherHistory(WorkbenchResearcherBase, Base):
    __tablename__ = "workbench_researcher_history"

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

    userId = Column("user_id", Integer, ForeignKey("workbench_researcher_history.id"), nullable=False)
    institution = Column("institution", String(250))
    role = Column("role", String(80))
    nonAcademicAffiliation = Column("non_academic_affiliation", Boolean)
