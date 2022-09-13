#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.participant_enums import WorkbenchResearcherEthnicity, \
    WorkbenchWorkspaceAge, WorkbenchWorkspaceStatus, WorkbenchWorkspaceSexAtBirth, WorkbenchWorkspaceGenderIdentity, \
    WorkbenchWorkspaceSexualOrientation, WorkbenchWorkspaceGeography, WorkbenchWorkspaceDisabilityStatus, \
    WorkbenchWorkspaceAccessToCare, WorkbenchWorkspaceEducationLevel, WorkbenchWorkspaceIncomeLevel, \
    WorkbenchWorkspaceUserRole, WorkbenchWorkspaceAccessTier
from rdr_service.resource import Schema, fields
from rdr_service.resource.constants import SchemaID


class WorkspaceRaceEthnicitySchema(Schema):
    race_ethnicity = fields.EnumString(enum=WorkbenchResearcherEthnicity, required=True)
    race_ethnicity_id = fields.EnumInteger(enum=WorkbenchResearcherEthnicity, required=True)

    class Meta:
        schema_id = SchemaID.workbench_workspace_ethnicity
        resource_uri = 'WorkbenchWorkspace/{id}/Ethnicities'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class WorkspaceAgeSchema(Schema):
    age = fields.EnumString(enum=WorkbenchWorkspaceAge, required=True)
    age_id = fields.EnumInteger(enum=WorkbenchWorkspaceAge, required=True)

    class Meta:
        schema_id = SchemaID.workbench_workspace_age
        resource_uri = 'WorkbenchWorkspace/{id}/Ages'
        # Exclude fields and/or functions to strip PII information from fields.
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).


class WorkbenchWorkspaceSchema(Schema):
    """
    Represents the workbench_workspace_snapshot table.
    """
    workspace_source_id = fields.Int32(required=True)
    name = fields.String(validate=validate.Length(max=1000), required=True)
    creation_time = fields.DateTime(required=True)
    modified_time = fields.DateTime(required=True)
    status = fields.EnumString(enum=WorkbenchWorkspaceStatus, required=True)
    status_id = fields.EnumInteger(enum=WorkbenchWorkspaceStatus, required=True)
    exclude_from_public_directory = fields.Boolean()
    review_requested = fields.Boolean()

    disease_focused_research = fields.Boolean()
    disease_focused_research_name = fields.String(validate=validate.Length(max=1000))
    other_purpose_details = fields.String(validate=validate.Length(max=1000))

    methods_development = fields.Boolean()
    control_set = fields.Boolean()
    ancestry = fields.Boolean()
    social_behavioral = fields.Boolean()
    population_health = fields.Boolean()
    drug_development = fields.Boolean()
    commercial_purpose = fields.Boolean()
    educational = fields.Boolean()
    other_purpose = fields.Boolean()
    ethical_legal_social_implications = fields.Boolean()

    scientific_approaches = fields.Text()
    intend_to_study = fields.Text()
    findings_from_study = fields.Text()

    focus_on_underrepresented_populations = fields.Boolean()

    race_ethnicities = fields.Nested(WorkspaceRaceEthnicitySchema, many=True)
    ages = fields.Nested(WorkspaceAgeSchema, many=True)

    sex_at_birth = fields.EnumString(enum=WorkbenchWorkspaceSexAtBirth, required=True)
    sex_at_birth_id = fields.EnumInteger(enum=WorkbenchWorkspaceSexAtBirth, required=True)
    gender_identity = fields.EnumString(enum=WorkbenchWorkspaceGenderIdentity, required=True)
    gender_identity_id = fields.EnumInteger(enum=WorkbenchWorkspaceGenderIdentity, required=True)
    sexual_orientation = fields.EnumString(enum=WorkbenchWorkspaceSexualOrientation, required=True)
    sexual_orientation_id = fields.EnumInteger(enum=WorkbenchWorkspaceSexualOrientation, required=True)
    geography = fields.EnumString(enum=WorkbenchWorkspaceGeography, required=True)
    geography_id = fields.EnumInteger(enum=WorkbenchWorkspaceGeography, required=True)
    disability_status = fields.EnumString(enum=WorkbenchWorkspaceDisabilityStatus, required=True)
    disability_status_id = fields.EnumInteger(enum=WorkbenchWorkspaceDisabilityStatus, required=True)
    access_to_care = fields.EnumString(enum=WorkbenchWorkspaceAccessToCare, required=True)
    access_to_care_id = fields.EnumInteger(enum=WorkbenchWorkspaceAccessToCare, required=True)
    education_level = fields.EnumString(enum=WorkbenchWorkspaceEducationLevel, required=True)
    education_level_id = fields.EnumInteger(enum=WorkbenchWorkspaceEducationLevel, required=True)
    income_level = fields.EnumString(enum=WorkbenchWorkspaceIncomeLevel, required=True)
    income_level_id = fields.EnumInteger(enum=WorkbenchWorkspaceIncomeLevel, required=True)

    others = fields.String(validate=validate.Length(max=2000))
    is_reviewed = fields.Boolean()

    cdr_version = fields.String(validate=validate.Length(max=200))
    access_tier = fields.EnumString(enum=WorkbenchWorkspaceAccessTier)
    access_tier_id = fields.EnumInteger(enum=WorkbenchWorkspaceAccessTier)

    orig_id = fields.Int32()
    orig_created = fields.DateTime()
    orig_modified = fields.DateTime()

    class Meta:
        schema_id = SchemaID.workbench_workspace
        resource_uri = 'WorkbenchWorkspace'
        resource_pk_field = 'workspace_source_id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function)


class WorkbenchWorkspaceUsersSchema(Schema):

    workspace_id = fields.Int32(required=True)
    researcher_id = fields.Int32(required=True)
    user_id = fields.Int32(required=True)

    role = fields.EnumString(enum=WorkbenchWorkspaceUserRole, required=True)
    role_id = fields.EnumInteger(enum=WorkbenchWorkspaceUserRole, required=True)
    status = fields.EnumString(enum=WorkbenchWorkspaceStatus, required=True)
    status_id = fields.EnumInteger(enum=WorkbenchWorkspaceStatus, required=True)

    is_creator = fields.Boolean()

    orig_id = fields.Int32()
    orig_created = fields.DateTime()
    orig_modified = fields.DateTime()

    class Meta:
        schema_id = SchemaID.workbench_workspace_users
        resource_uri = 'WorkbenchWorkspaceUsers'
        resource_pk_field = 'user_id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).
