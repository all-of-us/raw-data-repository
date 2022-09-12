#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.participant_enums import WorkbenchResearcherEthnicity, WorkbenchResearcherDisability, \
    WorkbenchResearcherEducation, WorkbenchInstitutionNonAcademic, WorkbenchResearcherDegree, \
    WorkbenchResearcherSexAtBirth, WorkbenchResearcherGender, WorkbenchResearcherRace, \
    WorkbenchResearcherAccessTierShortName, WorkbenchResearcherYesNoPreferNot, \
    WorkbenchResearcherEthnicCategory, WorkbenchResearcherGenderIdentity, WorkbenchResearcherSexAtBirthV2, \
    WorkbenchResearcherEducationV2, WorkbenchResearcherSexualOrientationV2
from rdr_service.resource import Schema, fields
from rdr_service.resource.constants import SchemaID


class DegreeSchema(Schema):
    degree = fields.EnumString(enum=WorkbenchResearcherDegree)
    degree_id = fields.EnumInteger(enum=WorkbenchResearcherDegree)

    class Meta:
        schema_id = SchemaID.workbench_researcher_degree
        resource_uri = 'WorkbenchResearcher/{id}/Degrees'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class SexAtBirthSchema(Schema):
    sex_at_birth = fields.EnumString(enum=WorkbenchResearcherSexAtBirth)
    sex_at_birth_id = fields.EnumInteger(enum=WorkbenchResearcherSexAtBirth)

    class Meta:
        schema_id = SchemaID.workbench_researcher_sex_at_birth
        resource_uri = 'WorkbenchResearcher/{id}/SexAtBirth'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class WorkbenchGenderSchema(Schema):
    gender = fields.EnumString(enum=WorkbenchResearcherGender)
    gender_id = fields.EnumInteger(enum=WorkbenchResearcherGender)

    class Meta:
        schema_id = SchemaID.workbench_researcher_gender
        resource_uri = 'WorkbenchResearcher/{id}/Genders'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class WorkbenchRaceSchema(Schema):
    race = fields.EnumString(enum=WorkbenchResearcherRace)
    race_id = fields.EnumInteger(enum=WorkbenchResearcherRace)

    class Meta:
        schema_id = SchemaID.workbench_researcher_race
        resource_uri = 'WorkbenchResearcher/{id}/Races'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class AccessTierShortNameSchema(Schema):
    access_tier_short_name = fields.EnumString(enum=WorkbenchResearcherAccessTierShortName)
    access_tier_short_name_id = fields.EnumInteger(enum=WorkbenchResearcherAccessTierShortName)

    class Meta:
        schema_id = SchemaID.workbench_researcher_short_tier_names
        resource_uri = 'WorkbenchResearcher/{id}/ShortTierNames'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class DSV2EthnicCategorySchema(Schema):
    dsv2_ethnic_category = fields.EnumString(enum=WorkbenchResearcherEthnicCategory)
    dsv2_ethnic_category_id = fields.EnumInteger(enum=WorkbenchResearcherEthnicCategory)

    class Meta:
        schema_id = SchemaID.workbench_researcher_dsv2_ethnic_category
        resource_uri = 'WorkbenchResearcher/{id}/DSV2EthnicCategory'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class DSV2GenderIdentitySchema(Schema):
    dsv2_gender_identity = fields.EnumString(enum=WorkbenchResearcherGenderIdentity)
    dsv2_gender_identity_id = fields.EnumInteger(enum=WorkbenchResearcherGenderIdentity)

    class Meta:
        schema_id = SchemaID.workbench_researcher_dsv2_gender_identity
        resource_uri = 'WorkbenchResearcher/{id}/DSV2GenderIdentity'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class DSV2SexualOrientationSchema(Schema):
    dsv2_sexual_orientation = fields.EnumString(enum=WorkbenchResearcherSexualOrientationV2)
    dsv2_sexual_orientation_id = fields.EnumInteger(enum=WorkbenchResearcherSexualOrientationV2)

    class Meta:
        schema_id = SchemaID.workbench_researcher_dsv2_sexual_orientation
        resource_uri = 'WorkbenchResearcher/{id}/DSV2GenderIdentity'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class WorkbenchResearcherSchema(Schema):
    """ Workbench Researcher """
    user_source_id = fields.Int32(required=True)
    creation_time = fields.DateTime()
    modified_time = fields.DateTime()

    # Start PII Fields
    given_name = fields.String(validate=validate.Length(max=100))
    family_name = fields.String(validate=validate.Length(max=100))
    email = fields.String(validate=validate.Length(max=250))
    state = fields.String(validate=validate.Length(max=80))
    zip_code = fields.String(validate=validate.Length(max=80))
    country = fields.String(validate=validate.Length(max=80))
    # End PII Fields

    ethnicity = fields.EnumString(enum=WorkbenchResearcherEthnicity, required=True)
    ethnicity_id = fields.EnumInteger(enum=WorkbenchResearcherEthnicity, required=True)

    genders = fields.Nested(WorkbenchGenderSchema, many=True)
    races = fields.Nested(WorkbenchRaceSchema, many=True)
    sex_at_birth = fields.Nested(SexAtBirthSchema, many=True)

    education = fields.EnumString(enum=WorkbenchResearcherEducation, required=True)
    education_id = fields.EnumInteger(enum=WorkbenchResearcherEducation, required=True)
    degrees = fields.Nested(DegreeSchema, many=True)
    disability = fields.EnumString(enum=WorkbenchResearcherDisability, required=True)
    disability_id = fields.EnumInteger(enum=WorkbenchResearcherDisability, required=True)

    # New fields and sub-tables for PDR-826
    identifies_as_lgbtq = fields.Boolean()
    lgbtq_identity = fields.Boolean()

    access_tier_short_name = fields.Nested(AccessTierShortNameSchema, many=True)
    dsv2_completion_time = fields.DateTime()

    dsv2_disability_concentrating = fields.EnumString(enum=WorkbenchResearcherYesNoPreferNot)
    dsv2_disability_concentrating_id = fields.EnumInteger(enum=WorkbenchResearcherYesNoPreferNot)

    dsv2_disability_dressing = fields.EnumString(enum=WorkbenchResearcherYesNoPreferNot)
    dsv2_disability_dressing_id = fields.EnumInteger(enum=WorkbenchResearcherYesNoPreferNot)

    dsv2_disability_errands = fields.EnumString(enum=WorkbenchResearcherYesNoPreferNot)
    dsv2_disability_errands_id = fields.EnumInteger(enum=WorkbenchResearcherYesNoPreferNot)

    dsv2_disability_hearing = fields.EnumString(enum=WorkbenchResearcherYesNoPreferNot)
    dsv2_disability_hearing_id = fields.EnumInteger(enum=WorkbenchResearcherYesNoPreferNot)

    dsv2_disability_other = fields.Boolean()

    dsv2_disability_seeing = fields.EnumString(enum=WorkbenchResearcherYesNoPreferNot)
    dsv2_disability_seeing_id = fields.EnumInteger(enum=WorkbenchResearcherYesNoPreferNot)

    dsv2_disability_walking = fields.EnumString(enum=WorkbenchResearcherYesNoPreferNot)
    dsv2_disability_walking_id = fields.EnumInteger(enum=WorkbenchResearcherYesNoPreferNot)

    dsv2_disadvantaged = fields.EnumString(enum=WorkbenchResearcherYesNoPreferNot)
    dsv2_disadvantaged_id = fields.EnumInteger(enum=WorkbenchResearcherYesNoPreferNot)

    dsv2_education = fields.EnumString(enum=WorkbenchResearcherEducationV2)
    dsv2_education_id = fields.EnumInteger(enum=WorkbenchResearcherEducationV2)

    dsv2_ethnic_category = fields.Nested(DSV2EthnicCategorySchema, many=True)

    dsv2_ethnicity_aian_other = fields.Boolean()
    dsv2_ethnicity_asian_other = fields.Boolean()
    dsv2_ethnicity_other = fields.Boolean()

    dsv2_gender_identity = fields.Nested(DSV2GenderIdentitySchema, many=True)

    dsv2_gender_other = fields.Boolean()
    dsv2_orientation_other = fields.Boolean()

    dsv2_sex_at_birth = fields.EnumString(enum=WorkbenchResearcherSexAtBirthV2)
    dsv2_sex_at_birth_id = fields.EnumInteger(enum=WorkbenchResearcherSexAtBirthV2)

    dsv2_sex_at_birth_other = fields.Boolean()

    dsv2_sexual_orientation = fields.Nested(DSV2SexualOrientationSchema, many=True)

    dsv2_year_of_birth = fields.Int16()
    dsv2_year_of_birth_prefer_not = fields.Boolean()

    dsv2_ethnicity_black_other = fields.Boolean()
    dsv2_ethnicity_hispanic_other = fields.Boolean()
    dsv2_ethnicity_mena_other = fields.Boolean()
    dsv2_ethnicity_nhpi_other = fields.Boolean()
    dsv2_ethnicity_white_other = fields.Boolean()
    dsv2_survey_comments = fields.Boolean()

    orig_id = fields.Int32()
    orig_created = fields.DateTime()
    orig_modified = fields.DateTime()

    class Meta:
        schema_id = SchemaID.workbench_researcher
        resource_uri = 'WorkbenchResearcher'
        resource_pk_field = 'user_source_id'
        # Exclude fields and/or functions to strip PII information from fields
        pii_fields = ('email', 'family_name', 'given_name') # List fields that contain PII data
        pii_filter = {}  # dict(field: lambda function).


class WorkbenchInstitutionalAffiliationsSchema(Schema):
    """ Institutional Affiliations """
    researcher_id = fields.Int32(required=True)
    institution = fields.String(validate=validate.Length(max=250))
    role = fields.String(validate=validate.Length(max=80))
    non_academic_affiliation = fields.EnumString(enum=WorkbenchInstitutionNonAcademic, required=True)
    non_academic_affiliation_id = fields.EnumInteger(enum=WorkbenchInstitutionNonAcademic, required=True)
    is_verified = fields.Boolean()
    modified_time = fields.DateTime()

    orig_id = fields.Int32()
    orig_created = fields.DateTime()
    orig_modified = fields.DateTime()

    class Meta:
        schema_id = SchemaID.workbench_institutional_affiliation
        resource_uri = 'WorkbenchInstitutionalAffiliation'
        resource_pk_field = 'researcher_id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).

