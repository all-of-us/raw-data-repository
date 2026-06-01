from sqlalchemy import Column, ForeignKey, Index, Integer, String, UniqueConstraint, JSON, event, Boolean, Text
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


class WorkbenchInstitutionalDura(Base):
    __tablename__ = "workbench_institutional_dura"

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column(UTCDateTime6)
    modified = Column(UTCDateTime6)
    record_id = Column(Integer, nullable=False, unique=True, index=True)
    access_method = Column(String(100))
    access_method_rt = Column(String(100))
    access_team_review_status = Column(String(100))
    additionalrequesters = Column(Text)
    additional_viewers = Column(String(100))
    agreement_auto_renew = Column(String(100))
    agreement_end_date = Column(UTCDateTime6)
    agreement_expired = Column(String(100))
    agreement_renewal_info = Column(Text)
    agreement_start_date = Column(UTCDateTime6)
    agreementstatus_id = Column(Integer)
    agreementtype_id = Column(Integer)
    ambiguous_reason = Column(Text)
    available_rh = Column(String(100))
    available_rw = Column(String(100))
    biovu_used = Column(String(100))
    carnegieclass = Column(String(100))
    closed_reason_ct = Column(Integer)
    closed_reason = Column(String(100))
    collaborator_personnel = Column(String(100))
    commercial_subcategories = Column(String(100))
    commercial_subcategory_other = Column(String(100))
    consortiummember_id = Column(Integer)
    contact_person_email = Column(String(100))
    contact_person_phone = Column(String(100))
    contact_person = Column(String(100))
    contractoutcome = Column(Text)
    country_institution_code = Column(String(100))
    country_institution_other = Column(String(100))
    ct_requester_date = Column(UTCDateTime6)
    ctrequester_email = Column(String(100))
    ctrequester = Column(String(100))
    ct_request_source = Column(Integer)
    data_description = Column(Text)
    data_exists = Column(String(100))
    data_set_transferred = Column(String(100))
    departmental_contact = Column(String(100))
    department = Column(String(100))
    description_of_the_data = Column(String(100))
    disposition_of_data = Column(Text)
    document_status___1 = Column(String(100))
    document_status___2 = Column(String(100))
    draft_contact = Column(String(100))
    edlevel = Column(String(100))
    ein_and_uei_number_complete = Column(Integer)
    ein_and_uei_number_timestamp = Column(UTCDateTime6)
    ein_first_name = Column(String(100))
    ein_institutional_role = Column(String(100))
    ein_last_name = Column(String(100))
    ein_notification = Column(String(100))
    eligibility_check1 = Column(String(100))
    eligibility_countries_of_concern = Column(String(100))
    eligibility_dura = Column(String(100))
    eligibility_idme = Column(Integer)
    eligibility_idme_persistent = Column(Integer)
    eligibility_institution_type = Column(String(100))
    eligibility_notifications = Column(String(100))
    eligible_country_concern_persistent = Column(String(100))
    email1_ct = Column(String(100))
    email1 = Column(String(100))
    email2_ct = Column(String(100))
    email2 = Column(String(100))
    email3_ct = Column(String(100))
    email3 = Column(String(100))
    email4 = Column(String(100))
    emaildomainlist_ct = Column(String(100))
    emaildomainlist = Column(String(100))
    email_notification = Column(String(100))
    era_commons = Column(Text)
    executed_dura_acceptable_domains_user_reporting_complete = Column(Integer)
    executeddura = Column(String(100))
    external_party = Column(String(100))
    family_name = Column(String(100))
    file = Column(String(100))
    finalization_formdate = Column(UTCDateTime6)
    first_individual = Column(String(100))
    foreign_interaction = Column(String(100))
    forwarded_request_ct = Column(Integer)
    fqhc = Column(String(100))
    given_name = Column(String(100))
    hbcu = Column(String(100))
    how_learn_aou_website = Column(Integer)
    how_learn_describe = Column(Text)
    how_learn_friends = Column(Integer)
    how_learn_journal_news = Column(Integer)
    how_learn_other = Column(Integer)
    how_learn_other_website = Column(Integer)
    how_learn_presentation_demonstration = Column(Integer)
    how_learn_social_media = Column(Integer)
    hsi = Column(String(100))
    human_subjects_originate = Column(String(100))
    idua_internal_tracking_complete = Column(String(100))
    individual_name = Column(String(100))
    information_outside_us = Column(String(100))
    informed_consent = Column(String(100))
    inst_email = Column(String(100))
    institutional_dua_request_for_the_all_of_us_resear_comple = Column(Integer)
    institutioncategory = Column(Integer)
    institution_website = Column(String(100))
    inst_name = Column(String(100))
    intro_letter = Column(String(100))
    location_external_party = Column(String(100))
    medical_diagnostics_institution = Column(Integer)
    method_data_transfer = Column(Text)
    multi_national_internal = Column(String(100))
    multi_national_request_form = Column(String(100))
    nih_funding = Column(String(100))
    ocmcontact_ct = Column(Integer)
    ocm_draft_template = Column(String(100))
    ocmsubmission_ct = Column(Integer)
    ocmsubmissiondate_ct = Column(UTCDateTime6)
    opeid = Column(String(100))
    original_contact_email = Column(String(100))
    original_contact_name = Column(String(100))
    original_peerconfirmation = Column(String(100))
    originalrequestinfo = Column(Text)
    originalrequest = Column(String(100))
    other_materials = Column(String(100))
    other_party_address = Column(Text)
    other_party_email = Column(String(100))
    other_party_name = Column(String(100))
    other_party_phone = Column(String(100))
    other_reason_ct = Column(String(100))
    other_reason = Column(String(100))
    partner_other = Column(Text)
    peerconfirmationdate = Column(UTCDateTime6)
    peerconfirmation = Column(String(100))
    peer_integration_complete = Column(String(100))
    peersubmissionstatusdate = Column(UTCDateTime6)
    peersubmissionstatus = Column(String(100))
    phone_number = Column(String(100))
    preapproval_discussion_date = Column(UTCDateTime6)
    pre_approval_notes = Column(Text)
    preapproval_projectcreation_date = Column(UTCDateTime6)
    preapprovalregistration2 = Column(String(100))
    preapprovalregistration = Column(String(100))
    preapproval_requestform_date = Column(UTCDateTime6)
    preapproval_reviewer_email_2 = Column(String(100))
    preapproval_reviewer_email_3 = Column(String(100))
    preapproval_reviewer_email_4 = Column(String(100))
    preapproval_reviewer_email = Column(String(100))
    preapproval_reviewer = Column(String(100))
    preapproval_tier___1 = Column(String(100))
    preapproval_tier___2 = Column(String(100))
    pre_approval_type = Column(String(100))
    presentation_date = Column(UTCDateTime6)
    presentation_source_aahd = Column(Integer)
    presentation_source_ahc = Column(Integer)
    presentation_source_baylor_college_of_medicine = Column(Integer)
    presentation_source_ctsa_pacer_community_network = Column(Integer)
    presentation_source_dref = Column(Integer)
    presentation_source_fiftyforward = Column(Integer)
    presentation_source_hunter_college_cuny = Column(Integer)
    presentation_source_nahh = Column(Integer)
    presentation_source_nnlm = Column(Integer)
    presentation_source_other = Column(Integer)
    presentation_source_pridenet = Column(Integer)
    presentation_source_pyxis_partners = Column(Integer)
    presentation_source_rti_international = Column(Integer)
    presentation_text = Column(String(100))
    principal_investigator = Column(String(100))
    program_partners_aahd = Column(Integer)
    program_partners_ahc = Column(Integer)
    program_partners_aises = Column(Integer)
    program_partners_arizona = Column(Integer)
    program_partners_ctsa_pacer_community_network = Column(Integer)
    program_partners_drc = Column(Integer)
    program_partners_dref = Column(Integer)
    program_partners_evenings_with_grp = Column(Integer)
    program_partners_fiftyforward = Column(Integer)
    program_partners_ignite = Column(Integer)
    program_partners_marshfield = Column(Integer)
    program_partners_nahh = Column(Integer)
    program_partners_nln = Column(Integer)
    program_partners_nbc = Column(Integer)
    program_partners_nnlm = Column(Integer)
    program_partners_none_of_above = Column(Integer)
    program_partners_other = Column(Integer)
    program_partners_pridenet = Column(Integer)
    program_partners_program_staff = Column(Integer)
    program_partners_pyxis_partners = Column(Integer)
    program_partners_rti_international = Column(Integer)
    program_partners_scripps = Column(Integer)
    program_partners_utah = Column(Integer)
    project_title_peer = Column(String(100))
    provided_ein_number = Column(String(100))
    provided_uei_number = Column(String(100))
    registrationformsame = Column(String(100))
    related_to_existing_contract = Column(Integer)
    reportingemail = Column(String(100))
    reportingemail_2 = Column(String(100))
    reportingemail_3 = Column(String(100))
    reportingemail_4 = Column(String(100))
    reportingname = Column(String(100))
    requestcompletion_ct = Column(Integer)
    requestcompletion = Column(String(100))
    request_ct = Column(Integer)
    request_type = Column(String(100))
    reviewconfirmationdate = Column(UTCDateTime6)
    reviewconfirmation = Column(String(100))
    reviewrequest_ct = Column(Integer)
    reviewrequest_ct_2 = Column(Integer)
    reviewrequestdate_ct = Column(UTCDateTime6)
    reviewrequestdate_ct_2 = Column(UTCDateTime6)
    riderstatus_ct_id = Column(Integer)
    role_for_dura = Column(String(100))
    ruralhealth = Column(String(100))
    separate_domains_ct = Column(Integer)
    signedriderdate_ct = Column(UTCDateTime6)
    signing_family_name = Column(String(100))
    signing_given_name = Column(String(100))
    signing_inst_email = Column(String(100))
    signing_official_contact_date = Column(UTCDateTime6)
    signing_phone_number = Column(String(100))
    signing_role = Column(String(100))
    source_of_data = Column(String(100))
    state_initials_code = Column(String(100))
    submit_peer = Column(String(100))
    tier_access___1 = Column(String(100))
    tier_access___2 = Column(String(100))
    typeoforganization_academic = Column(Integer)
    typeoforganization_for_profit = Column(Integer)
    typeoforganization_government = Column(Integer)
    typeoforganization_hci = Column(Integer)
    typeoforganization_non_profit = Column(Integer)
    typeoforganization_other_desc = Column(String(100))
    typeoforganization_other = Column(Integer)
    type = Column(String(100))
    us_ein_number = Column(String(100))
    website_url = Column(String(100))
    whitelisted_emails_ct = Column(String(100))
    whitelisted_emails_rt = Column(String(100))
    whose_data_shared = Column(String(100))
    zip = Column(String(100))


event.listen(WorkbenchResearcher, "before_insert", model_insert_listener)
event.listen(WorkbenchResearcher, "before_update", model_update_listener)
event.listen(WorkbenchInstitutionalAffiliations, "before_insert", model_insert_listener)
event.listen(WorkbenchInstitutionalAffiliations, "before_update", model_update_listener)
event.listen(WorkbenchResearcherHistory, "before_insert", model_insert_listener)
event.listen(WorkbenchResearcherHistory, "before_update", model_update_listener)
event.listen(WorkbenchInstitutionalAffiliationsHistory, "before_insert", model_insert_listener)
event.listen(WorkbenchInstitutionalAffiliationsHistory, "before_update", model_update_listener)
event.listen(WorkbenchInstitutionalDura, "before_insert", model_insert_listener)
event.listen(WorkbenchInstitutionalDura, "before_update", model_update_listener)
