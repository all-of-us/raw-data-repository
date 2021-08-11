#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.participant_enums import WorkbenchResearcherEthnicity, WorkbenchResearcherDisability, \
    WorkbenchResearcherEducation, WorkbenchInstitutionNonAcademic, WorkbenchResearcherDegree, \
    WorkbenchResearcherSexAtBirth, WorkbenchResearcherGender, WorkbenchResearcherRace
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


class WorkbenchResearcherSchema(Schema):
    """ Workbench Researcher """
    user_source_id = fields.Int32(required=True)
    creation_time = fields.DateTime()
    modified_time = fields.DateTime()

    # Start PII Fields
    given_name = fields.String(validate=validate.Length(max=100))
    family_name = fields.String(validate=validate.Length(max=100))
    email = fields.String(validate=validate.Length(max=250))
    city = fields.String(validate=validate.Length(max=80))
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

    identifies_as_lgbtq = fields.Boolean()
    lgbtq_identity = fields.String(validate=validate.Length(max=250))

    class Meta:
        schema_id = SchemaID.workbench_researcher
        resource_uri = 'WorkbenchResearcher'
        resource_pk_field = 'user_source_id'
        # Exclude fields and/or functions to strip PII information from fields
        # TODO:  Confirm if we should be deleting country, zip, state from current BQ schemas as PII?
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

    class Meta:
        schema_id = SchemaID.workbench_institutional_affiliation
        resource_uri = 'WorkbenchInstitutionalAffiliation'
        resource_pk_field = 'researcher_id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).

