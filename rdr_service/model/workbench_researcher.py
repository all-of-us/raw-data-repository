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
    record_id = Column(Integer, nullable=False)
    access_method = Column(Text)
    access_method_rt = Column(Text)
    access_team_review_status = Column(Text)
    additionalrequesters = Column(Text)
    additional_viewers = Column(Text)
    agreement_auto_renew = Column(Text)
    agreement_end_date = Column(UTCDateTime6)
    agreement_expired = Column(Text)
    agreement_renewal_info = Column(Text)
    agreement_start_date = Column(UTCDateTime6)
    agreementstatus_id = Column(Integer)
    agreementtype_id = Column(Integer)
    ambiguous_reason = Column(Text)
    available_rh = Column(Text)
    available_rw = Column(Text)
    biovu_used = Column(Text)
    carnegieclass = Column(Text)
    closed_reason_ct = Column(Integer)
    closed_reason = Column(Text)
    collaborator_personnel = Column(Text)
    commercial_subcategories = Column(Text)
    commercial_subcategory_other = Column(Text)
    consortiummember_id = Column(Integer)
    contact_person_email = Column(Text)
    contact_person_phone = Column(Text)
    contact_person = Column(Text)
    contractoutcome = Column(Text)
    country_institution_code = Column(Text)
    country_institution_other = Column(Text)
    ct_requester_date = Column(UTCDateTime6)
    ctrequester_email = Column(Text)
    ctrequester = Column(Text)
    ct_request_source = Column(Integer)
    data_description = Column(Text)
    data_exists = Column(Text)
    data_set_transferred = Column(Text)
    departmental_contact = Column(Text)
    department = Column(Text)
    description_of_the_data = Column(Text)
    disposition_of_data = Column(Text)
    document_status___1 = Column(Text)
    document_status___2 = Column(Text)
    draft_contact = Column(Text)
    edlevel = Column(Text)
    ein_and_uei_number_complete = Column(Integer)
    ein_and_uei_number_timestamp = Column(UTCDateTime6)
    ein_first_name = Column(Text)
    ein_institutional_role = Column(Text)
    ein_last_name = Column(Text)
    ein_notification = Column(Text)
    eligibility_check1 = Column(Text)
    eligibility_countries_of_concern = Column(Text)
    eligibility_dura = Column(Text)
    eligibility_idme = Column(Integer)
    eligibility_idme_persistent = Column(Integer)
    eligibility_institution_type = Column(Text)
    eligibility_notifications = Column(Text)
    eligible_country_concern_persistent = Column(Text)
    email1_ct = Column(Text)
    email1 = Column(Text)
    email2_ct = Column(Text)
    email2 = Column(Text)
    email3_ct = Column(Text)
    email3 = Column(Text)
    email4 = Column(Text)
    emaildomainlist_ct = Column(Text)
    emaildomainlist = Column(Text)
    email_notification = Column(Text)
    era_commons = Column(Text)
    executed_dura_acceptable_domains_user_reporting_complete = Column(Integer)
    executeddura = Column(Text)
    external_party = Column(Text)
    family_name = Column(Text)
    file = Column(Text)
    finalization_formdate = Column(UTCDateTime6)
    first_individual = Column(Text)
    foreign_interaction = Column(Text)
    forwarded_request_ct = Column(Integer)
    fqhc = Column(Text)
    given_name = Column(Text)
    hbcu = Column(Text)
    how_learn_aou_website = Column(Integer)
    how_learn_describe = Column(Text)
    how_learn_friends = Column(Integer)
    how_learn_journal_news = Column(Integer)
    how_learn_other = Column(Integer)
    how_learn_other_website = Column(Integer)
    how_learn_presentation_demonstration = Column(Integer)
    how_learn_social_media = Column(Integer)
    hsi = Column(Text)
    human_subjects_originate = Column(Text)
    idua_internal_tracking_complete = Column(Text)
    individual_name = Column(Text)
    information_outside_us = Column(Text)
    informed_consent = Column(Text)
    inst_email = Column(Text)
    institutional_dua_request_for_the_all_of_us_resear_comple = Column(Integer)
    institutioncategory = Column(Integer)
    institution_website = Column(Text)
    inst_name = Column(Text)
    intro_letter = Column(Text)
    location_external_party = Column(Text)
    medical_diagnostics_institution = Column(Integer)
    method_data_transfer = Column(Text)
    multi_national_internal = Column(Text)
    multi_national_request_form = Column(Text)
    nih_funding = Column(Text)
    ocmcontact_ct = Column(Integer)
    ocm_draft_template = Column(Text)
    ocmsubmission_ct = Column(Integer)
    ocmsubmissiondate_ct = Column(UTCDateTime6)
    opeid = Column(Text)
    original_contact_email = Column(Text)
    original_contact_name = Column(Text)
    original_peerconfirmation = Column(Text)
    originalrequestinfo = Column(Text)
    originalrequest = Column(Text)
    other_materials = Column(Text)
    other_party_address = Column(Text)
    other_party_email = Column(Text)
    other_party_name = Column(Text)
    other_party_phone = Column(Text)
    other_reason_ct = Column(Text)
    other_reason = Column(Text)
    partner_other = Column(Text)
    peerconfirmationdate = Column(UTCDateTime6)
    peerconfirmation = Column(Text)
    peer_integration_complete = Column(Text)
    peersubmissionstatusdate = Column(UTCDateTime6)
    peersubmissionstatus = Column(Text)
    phone_number = Column(Text)
    preapproval_discussion_date = Column(UTCDateTime6)
    pre_approval_notes = Column(Text)
    preapproval_projectcreation_date = Column(UTCDateTime6)
    preapprovalregistration2 = Column(Text)
    preapprovalregistration = Column(Text)
    preapproval_requestform_date = Column(UTCDateTime6)
    preapproval_reviewer_email_2 = Column(Text)
    preapproval_reviewer_email_3 = Column(Text)
    preapproval_reviewer_email_4 = Column(Text)
    preapproval_reviewer_email = Column(Text)
    preapproval_reviewer = Column(Text)
    preapproval_tier___1 = Column(Text)
    preapproval_tier___2 = Column(Text)
    pre_approval_type = Column(Text)
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
    presentation_text = Column(Text)
    principal_investigator = Column(Text)
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
    project_title_peer = Column(Text)
    provided_ein_number = Column(Text)
    provided_uei_number = Column(Text)
    registrationformsame = Column(Text)
    related_to_existing_contract = Column(Integer)
    reportingemail = Column(Text)
    reportingemail_2 = Column(Text)
    reportingemail_3 = Column(Text)
    reportingemail_4 = Column(Text)
    reportingname = Column(Text)
    requestcompletion_ct = Column(Integer)
    requestcompletion = Column(Text)
    request_ct = Column(Integer)
    request_type = Column(Text)
    reviewconfirmationdate = Column(UTCDateTime6)
    reviewconfirmation = Column(Text)
    reviewrequest_ct = Column(Integer)
    reviewrequest_ct_2 = Column(Integer)
    reviewrequestdate_ct = Column(UTCDateTime6)
    reviewrequestdate_ct_2 = Column(UTCDateTime6)
    riderstatus_ct_id = Column(Integer)
    role_for_dura = Column(Text)
    ruralhealth = Column(Text)
    separate_domains_ct = Column(Integer)
    signedriderdate_ct = Column(UTCDateTime6)
    signing_family_name = Column(Text)
    signing_given_name = Column(Text)
    signing_inst_email = Column(Text)
    signing_official_contact_date = Column(UTCDateTime6)
    signing_phone_number = Column(Text)
    signing_role = Column(Text)
    source_of_data = Column(Text)
    state_initials_code = Column(Text)
    submit_peer = Column(Text)
    tier_access___1 = Column(Text)
    tier_access___2 = Column(Text)
    typeoforganization_academic = Column(Integer)
    typeoforganization_for_profit = Column(Integer)
    typeoforganization_government = Column(Integer)
    typeoforganization_hci = Column(Integer)
    typeoforganization_non_profit = Column(Integer)
    typeoforganization_other_desc = Column(Text)
    typeoforganization_other = Column(Integer)
    type = Column(Text)
    us_ein_number = Column(Text)
    website_url = Column(Text)
    whitelisted_emails_ct = Column(Text)
    whitelisted_emails_rt = Column(Text)
    whose_data_shared = Column(Text)
    zip = Column(Text)


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
