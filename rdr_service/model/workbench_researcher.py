from sqlalchemy import Column, ForeignKey, Index, Integer, String, UniqueConstraint, JSON, event, Boolean
from sqlalchemy.orm import relationship
from rdr_service.model.field_types import BlobUTF8
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, UTCDateTime6
from rdr_service.participant_enums import (
    WorkbenchInstitutionNonAcademic,
    WorkbenchResearcherEducation,
    WorkbenchResearcherDisability,
    WorkbenchResearcherEthnicity,
    WorkbenchResearcherAccessTierShortName,
    WorkbenchResearcherEducationV2,
    WorkbenchResearcherYesNoPreferNot,
    WorkbenchResearcherSexAtBirthV2
)


class WorkbenchResearcherBase(object):
    userSourceId = Column("user_source_id", Integer, nullable=False)
    """Auto increment, primary key."""
    creationTime = Column("creation_time", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modifiedTime = Column("modified_time", UTCDateTime6, nullable=True)
    """The last-modified timestamp in RW system."""
    givenName = Column("given_name", String(100))
    """The given name (e.g., first name) of the researcher"""
    familyName = Column("family_name", String(100))
    """The family name (e.g., last name) of the researcher"""
    email = Column("email", String(250))
    streetAddress1 = Column("street_address1", String(250))
    """First line of street address of the researcher"""
    streetAddress2 = Column("street_address2", String(250))
    """Second line of street address of the researcher"""
    city = Column("city", String(80))
    """The city of the address of the researcher"""
    state = Column("state", String(80))
    """The state of the address of the researcher"""
    zipCode = Column("zip_code", String(80))
    """The up to 80-character string zip code of the researcher"""
    country = Column("country", String(80))
    """The country of the researcher"""
    ethnicity = Column("ethnicity", Enum(WorkbenchResearcherEthnicity), default=WorkbenchResearcherEthnicity.UNSET)
    """The ethnicity of the researcher"""
    gender = Column("gender", JSON)
    race = Column("race", JSON)
    sexAtBirth = Column("sex_at_birth", JSON)
    education = Column("education", Enum(WorkbenchResearcherEducation), default=WorkbenchResearcherEducation.UNSET)
    degree = Column("degree", JSON)
    disability = Column("disability", Enum(WorkbenchResearcherDisability), default=WorkbenchResearcherDisability.UNSET)
    identifiesAsLgbtq = Column("identifies_as_lgbtq", Boolean)
    lgbtqIdentity = Column("lgbtq_identity", String(250))
    accessTierShortNames = Column("access_tier_short_names", JSON)
    dsv2CompletionTime = Column("dsv2_completion_time", UTCDateTime6, nullable=True)
    dsv2EthnicCategories = Column("dsv2_ethnic_categories", JSON)
    dsv2EthnicityAiAnOther = Column("dsv2_ethnicity_aian_other", String(200))
    dsv2EthnicityAsianOther = Column("dsv2_ethnicity_asian_other", String(200))
    dsv2EthnicityBlackOther = Column("dsv2_ethnicity_black_other", String(200))
    dsv2EthnicityHispanicOther = Column("dsv2_ethnicity_hispanic_other", String(200))
    dsv2EthnicityMeNaOther = Column("dsv2_ethnicity_mena_other", String(200))
    dsv2EthnicityNhPiOther = Column("dsv2_ethnicity_nhpi_other", String(200))
    dsv2EthnicityWhiteOther = Column("dsv2_ethnicity_white_other", String(200))
    dsv2EthnicityOther = Column("dsv2_ethnicity_other", String(200))
    dsv2GenderIdentities = Column("dsv2_gender_identities", JSON)
    dsv2GenderOther = Column("dsv2_gender_other", String(200))
    dsv2SexualOrientations = Column("dsv2_sexual_orientations", JSON)
    dsv2OrientationOther = Column("dsv2_orientation_other", String(200))
    dsv2SexAtBirth = Column("dsv2_sex_at_birth", Enum(WorkbenchResearcherSexAtBirthV2),
                            default=WorkbenchResearcherSexAtBirthV2.UNSET)
    dsv2SexAtBirthOther = Column("dsv2_sex_at_birth_other", String(200))
    dsv2YearOfBirth = Column("dsv2_year_of_birth", Integer, nullable=True)
    dsv2YearOfBirthPreferNot = Column("dsv2_year_of_birth_prefer_not", Boolean)
    dsv2DisabilityHearing = Column("dsv2_disability_hearing", Enum(WorkbenchResearcherYesNoPreferNot),
                                   default=WorkbenchResearcherYesNoPreferNot.UNSET)
    dsv2DisabilitySeeing = Column("dsv2_disability_seeing", Enum(WorkbenchResearcherYesNoPreferNot),
                                  default=WorkbenchResearcherYesNoPreferNot.UNSET)
    dsv2DisabilityConcentrating = Column("dsv2_disability_concentrating", Enum(WorkbenchResearcherYesNoPreferNot),
                                         default=WorkbenchResearcherYesNoPreferNot.UNSET)
    dsv2DisabilityWalking = Column("dsv2_disability_walking", Enum(WorkbenchResearcherYesNoPreferNot),
                                   default=WorkbenchResearcherYesNoPreferNot.UNSET)
    dsv2DisabilityDressing = Column("dsv2_disability_dressing", Enum(WorkbenchResearcherYesNoPreferNot),
                                    default=WorkbenchResearcherYesNoPreferNot.UNSET)
    dsv2DisabilityErrands = Column("dsv2_disability_errands", Enum(WorkbenchResearcherYesNoPreferNot),
                                   default=WorkbenchResearcherYesNoPreferNot.UNSET)
    dsv2DisabilityOther = Column("dsv2_disability_other", String(200))
    dsv2Education = Column("dsv2_education", Enum(WorkbenchResearcherEducationV2),
                           default=WorkbenchResearcherEducationV2.UNSET)
    dsv2Disadvantaged = Column("dsv2_disadvantaged", Enum(WorkbenchResearcherYesNoPreferNot),
                               default=WorkbenchResearcherYesNoPreferNot.UNSET)
    dsv2SurveyComments = Column("dsv2_survey_comments", String(1000))

    resource = Column("resource", BlobUTF8, nullable=False)
    """The resource payload"""

    def get_access_tier(self):
        access_tier_short_names = self.accessTierShortNames
        if not access_tier_short_names:
            return 'NOT_REGISTERED'
        elif len(access_tier_short_names) == 1 \
            and int(WorkbenchResearcherAccessTierShortName.REGISTERED) in access_tier_short_names:
            return 'REGISTERED'
        else:
            return 'REGISTERED_AND_CONTROLLED'


class WorkbenchResearcher(WorkbenchResearcherBase, Base):
    __tablename__ = "workbench_researcher"

    workbenchInstitutionalAffiliations = relationship("WorkbenchInstitutionalAffiliations",
                                                      cascade="all, delete-orphan")
    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""

    __table_args__ = (UniqueConstraint("user_source_id", name="uniqe_user_source_id"),)


class WorkbenchInstitutionalAffiliations(Base):
    __tablename__ = "workbench_institutional_affiliations"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""

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
    created = Column("created", UTCDateTime6, nullable=True)
    """
    When that record was created in the history table specifically (if main table is updated; previous version
    if/when a record is updated; if never changed, it appears as it was originally created)
    """
    modified = Column("modified", UTCDateTime6, nullable=True)
    """
    when that record was created in the history table specifically (if main table is updated; previous version
    if/when a record is updated; if never changed, it appears as it was originally created)
    """

    __table_args__ = (Index('idx_researcher_history_user_id', WorkbenchResearcherBase.userSourceId),)


class WorkbenchInstitutionalAffiliationsHistory(Base):
    __tablename__ = "workbench_institutional_affiliations_history"

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

    researcherId = Column("researcher_id", Integer, ForeignKey("workbench_researcher_history.id"), nullable=False)
    institution = Column("institution", String(250))
    role = Column("role", String(80))
    nonAcademicAffiliation = Column("non_academic_affiliation", Enum(WorkbenchInstitutionNonAcademic),
                                    default=WorkbenchInstitutionNonAcademic.UNSET)
    """Original if unedited; if edited, this field contains the previous value"""
    isVerified = Column("is_verified", Boolean)


event.listen(WorkbenchResearcher, "before_insert", model_insert_listener)
event.listen(WorkbenchResearcher, "before_update", model_update_listener)
event.listen(WorkbenchInstitutionalAffiliations, "before_insert", model_insert_listener)
event.listen(WorkbenchInstitutionalAffiliations, "before_update", model_update_listener)
event.listen(WorkbenchResearcherHistory, "before_insert", model_insert_listener)
event.listen(WorkbenchResearcherHistory, "before_update", model_update_listener)
event.listen(WorkbenchInstitutionalAffiliationsHistory, "before_insert", model_insert_listener)
event.listen(WorkbenchInstitutionalAffiliationsHistory, "before_update", model_update_listener)
