#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate
from rdr_service.resource.constants import SchemaID

from rdr_service.participant_enums import WorkbenchResearcherEthnicity, WorkbenchResearcherDisability, \
    WorkbenchResearcherEducation, WorkbenchInstitutionNonAcademic, WorkbenchResearcherDegree, \
    WorkbenchResearcherSexAtBirth, WorkbenchResearcherGender, WorkbenchResearcherRace
from rdr_service.resource import Schema, SchemaMeta, fields


class DegreeSchema(Schema):
    degree = fields.EnumString(enum=WorkbenchResearcherDegree)
    degree_id = fields.EnumInteger(enum=WorkbenchResearcherDegree)


class SexAtBirthSchema(Schema):
    sex_at_birth = fields.EnumString(enum=WorkbenchResearcherSexAtBirth)
    sex_at_birth_id = fields.EnumInteger(enum=WorkbenchResearcherSexAtBirth)


class WorkbenchGenderSchema(Schema):
    gender = fields.EnumString(enum=WorkbenchResearcherGender)
    gender_id = fields.EnumString(enum=WorkbenchResearcherGender)


class WorkbenchRaceSchema(Schema):
    race = fields.EnumString(enum=WorkbenchResearcherRace)
    race_id = fields.EnumString(enum=WorkbenchResearcherRace)


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

    genders = fields.Nested(WorkbenchGenderSchema)
    races = fields.Nested(WorkbenchRaceSchema)
    sex_at_birth = fields.Nested(SexAtBirthSchema)

    education = fields.EnumString(enum=WorkbenchResearcherEducation, required=True)
    education_id = fields.EnumInteger(enum=WorkbenchResearcherEducation, required=True)
    degrees = fields.Nested(DegreeSchema)
    disability = fields.EnumString(enum=WorkbenchResearcherDisability, required=True)
    disability_id = fields.EnumInteger(enum=WorkbenchResearcherDisability, required=True)

    identifies_as_lgbtq = fields.Boolean()
    lgbtq_identity = fields.String(validate=validate.Length(max=250))

    class Meta:
        """
        schema_meta info declares how the schema and data is stored and organized in the Resource database tables.
        """
        ordered = True
        resource_pk_field = 'user_source_id'
        # SchemaMeta (unique type id, unique type name, type URI, resource pk field, nested schemas)
        schema_meta = SchemaMeta(
            type_uid=SchemaID.workbench_researcher.value,
            type_name=SchemaID.workbench_researcher.name,
            resource_uri='WorkbenchResearcher',
            resource_pk_field='user_source_id'
        )


class WorkbenchInstitutionalAffiliationsSchema(Schema):
    """ Institutional Affiliations """
    researcher_id = fields.Int32(required=True)
    institution = fields.String(validate=validate.Length(max=250))
    role = fields.String(validate=validate.Length(max=80))
    non_academic_affiliation = fields.EnumString(enum=WorkbenchInstitutionNonAcademic, required=True)
    non_academic_affiliation_id = fields.EnumInteger(enum=WorkbenchInstitutionNonAcademic, required=True)
    is_verified = fields.Boolean()

    class Meta:
        """
        schema_meta info declares how the schema and data is stored and organized in the Resource database tables.
        """
        ordered = True
        resource_pk_field = 'researcher_id'
        # SchemaMeta (unique type id, unique type name, type URI, resource pk field, nested schemas)
        schema_meta = SchemaMeta(
            type_uid=SchemaID.workbench_institutional_affiliation.value,
            type_name=SchemaID.workbench_institutional_affiliation.name,
            resource_uri='WorkbenchInstitutionalAffiliation',
            resource_pk_field='researcher_id'
        )
