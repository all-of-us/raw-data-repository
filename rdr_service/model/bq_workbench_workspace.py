
from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum, \
    BQRecordField


class BQWorkspaceRaceEthnicitySchema(BQSchema):
    race_ethnicity = BQField('race_ethnicity', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    race_ethnicity_id = BQField('race_ethnicity_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQWorkspaceAgeSchema(BQSchema):
    age = BQField('age', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    age_id = BQField('age_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQRWBWorkspaceSchema(BQSchema):
    """
    Represents the workbench_workspace_snapshot table.
    """
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    workspace_source_id = BQField('workspace_source_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    name = BQField('name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    creation_time = BQField('creation_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified_time = BQField('modified_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    status = BQField('status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    status_id = BQField('status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    exclude_from_public_directory = BQField('exclude_from_public_directory', BQFieldTypeEnum.INTEGER,
                                            BQFieldModeEnum.NULLABLE)
    disease_focused_research = BQField('disease_focused_research', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    disease_focused_research_name = BQField('disease_focused_research_name', BQFieldTypeEnum.STRING,
                                            BQFieldModeEnum.NULLABLE)
    other_purpose_details = BQField('other_purpose_details', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    methods_development = BQField('methods_development', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    control_set = BQField('control_set', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ancestry = BQField('ancestry', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    social_behavioral = BQField('social_behavioral', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    population_health = BQField('population_health', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    drug_development = BQField('drug_development', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    commercial_purpose = BQField('commercial_purpose', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    educational = BQField('educational', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    other_purpose = BQField('other_purpose', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    ethical_legal_social_implications = BQField('ethical_legal_social_implications', BQFieldTypeEnum.INTEGER,
                                                BQFieldModeEnum.NULLABLE)

    scientific_approaches = BQField('scientific_approaches', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    intend_to_study = BQField('intend_to_study', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    findings_from_study = BQField('findings_from_study', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    focus_on_underrepresented_populations = BQField('focus_on_underrepresented_populations', BQFieldTypeEnum.INTEGER,
                                                     BQFieldModeEnum.NULLABLE)

    race_ethnicities = BQRecordField('race_ethnicities', BQWorkspaceRaceEthnicitySchema)
    ages = BQRecordField('ages', BQWorkspaceAgeSchema)

    sex_at_birth = BQField('sex_at_birth', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sex_at_birth_id = BQField('sex_at_birth_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    gender_identity = BQField('gender_identity', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    gender_identity_id = BQField('gender_identity_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    sexual_orientation = BQField('sexual_orientation', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sexual_orientation_id = BQField('sexual_orientation_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    geography = BQField('geography', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    geography_id = BQField('geography_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    disability_status = BQField('disability_status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    disability_status_id = BQField('disability_status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    access_to_care = BQField('access_to_care', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    access_to_care_id = BQField('access_to_care_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    education_level = BQField('education_level', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    education_level_id = BQField('education_level_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    income_level = BQField('income_level', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    income_level_id = BQField('income_level_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    others = BQField('others', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    review_requested = BQField('review_requested', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    is_reviewed = BQField('is_reviewed', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    cdr_version = BQField('cdr_version', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    access_tier = BQField('access_tier', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    access_tier_id = BQField('access_tier_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    orig_created = BQField('orig_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    orig_modified = BQField('orig_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)


class BQRWBWorkspace(BQTable):
    """ Research Workbench Workspace BigQuery Table """
    __tablename__ = 'rwb_workspace'
    __schema__ = BQRWBWorkspaceSchema


class BQRWBWorkspaceView(BQView):
    __viewname__ = 'v_rwb_workspace'
    __viewdescr__ = 'Research Workbench Workspace View'
    __pk_id__ = 'workspace_source_id'
    __table__ = BQRWBWorkspace
    # We need to build a SQL statement with all fields except sub-tables and remove duplicates.
    __sql__ = """
            SELECT
                %%FIELD_LIST%%
            FROM (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified) AS rn
                  FROM `{project}`.{dataset}.rwb_workspace
              ) t
              WHERE t.rn = 1
        """.replace('%%FIELD_LIST%%', BQRWBWorkspaceSchema.get_sql_field_names(
            exclude_fields=[
                'race_ethnicities',
                'ages'
            ])
        )


class BQRWBWorkspaceRaceEthnicityView(BQView):
    __viewname__ = 'v_rwb_workspace_race_ethnicity'
    __viewdescr__ = 'Research Workbench Workspace Race Ethnicity View'
    __pk_id__ = 'workspace_source_id'
    __table__ = BQRWBWorkspace
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.orig_id, t.orig_created, t.orig_modified, 
            t.workspace_source_id, t.modified_time, nt.*
          FROM (
            SELECT *, 
                   ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified) AS rn
              FROM `{project}`.{dataset}.rwb_workspace
          ) t cross join unnest(race_ethnicities) as nt
          WHERE t.rn = 1
    """


class BQRWBWorkspaceAgeView(BQView):
    __viewname__ = 'v_rwb_workspace_age'
    __viewdescr__ = 'Research Workbench Workspace Age View'
    __pk_id__ = 'workspace_source_id'
    __table__ = BQRWBWorkspace
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.orig_id, t.orig_created, t.orig_modified, 
            t.workspace_source_id, t.modified_time, nt.*
          FROM (
            SELECT *, 
                   ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified) AS rn
              FROM `{project}`.{dataset}.rwb_workspace
          ) t cross join unnest(ages) as nt
          WHERE t.rn = 1
    """


class BQRWBWorkspaceUsersSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    workspace_id = BQField('workspace_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    user_id = BQField('user_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)

    role = BQField('role', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    role_id = BQField('role_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    status = BQField('status', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    status_id = BQField('status_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    researcher_id = BQField('researcher_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    is_creator = BQField('is_creator', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    orig_created = BQField('orig_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    orig_modified = BQField('orig_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)


class BQRWBWorkspaceUsers(BQTable):
    """ Research Workbench Workspace Users BigQuery Table """
    __tablename__ = 'rwb_workspace_users'
    __schema__ = BQRWBWorkspaceUsersSchema


class BQRWBWorkspaceUsersView(BQView):
    __viewname__ = 'v_rwb_workspace_users'
    __viewdescr__ = 'Research Workbench Workspace Users View'
    __pk_id__ = 'id'
    __table__ = BQRWBWorkspaceUsers


class BQRWBAuditSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    workspace_snapshot_id = BQField('workspace_snapshot_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    auditor_pmi_email = BQField('auditor_pmi_email', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    audit_review_type = BQField('audit_review_type', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    audit_workspace_display_decision = BQField('audit_workspace_display_decision', BQFieldTypeEnum.INTEGER,
                                               BQFieldModeEnum.REQUIRED)
    audit_workspace_access_decision = BQField('audit_workspace_access_decision', BQFieldTypeEnum.INTEGER,
                                              BQFieldModeEnum.REQUIRED)
    audit_notes = BQField('audit_notes', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)

    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    orig_created = BQField('orig_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    orig_modified = BQField('orig_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)


class BQRWBAudit(BQTable):
    """ Research Workbench Audit BigQuery Table """
    __tablename__ = 'rwb_audit'
    __schema__ = BQRWBAuditSchema


class BQRWBAuditView(BQView):
    __viewname__ = 'v_rwb_audit'
    __viewdescr__ = 'Research Workbench Audit View'
    __pk_id__ = 'id'
    __table__ = BQRWBAudit
