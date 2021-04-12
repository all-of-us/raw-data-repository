from rdr_service.model.bq_participant_summary import BQGenderSchema, BQRaceSchema
from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum, \
    BQRecordField


class BQDegreeSchema(BQSchema):
    degree = BQField('degree', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    degree_id = BQField('degree_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

class BQSexAtBirthSchema(BQSchema):
    sex_at_birth = BQField('sex_at_birth', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sex_at_birth_id = BQField('sex_at_birth_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQRWBResearcherSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    user_source_id = BQField('user_source_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    modified_time = BQField('modified_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    state = BQField('state', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    zip_code = BQField('zip_code', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    country = BQField('country', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    ethnicity = BQField('ethnicity', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    ethnicity_id = BQField('ethnicity_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    genders = BQRecordField('genders', schema=BQGenderSchema)
    races = BQRecordField('races', schema=BQRaceSchema)
    sex_at_birth = BQRecordField('sex_at_birth', schema=BQSexAtBirthSchema)

    identifies_as_lgbtq = BQField('identifies_as_lgbtq', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    lgbtq_identity = BQField('lgbtq_identity', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)

    education = BQField('education', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    education_id = BQField('education_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    degrees = BQRecordField('degrees', schema=BQDegreeSchema)

    disability = BQField('disability', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    disability_id = BQField('disability_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    creation_time = BQField('creation_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)


class BQRWBResearcher(BQTable):
    """ Code BigQuery Table """
    __tablename__ = 'rwb_researcher'
    __schema__ = BQRWBResearcherSchema


class BQRWBResearcherView(BQView):
    __viewname__ = 'v_rwb_researcher'
    __viewdescr__ = 'Research Workbench Researcher View'
    __pk_id__ = 'user_source_id'
    __table__ = BQRWBResearcher
    # We need to build a SQL statement with all fields except sub-tables and remove duplicates.
    __sql__ = """
            SELECT
                %%FIELD_LIST%%
            FROM (
                SELECT *, 
                    ROW_NUMBER() OVER (PARTITION BY user_source_id ORDER BY modified desc) AS rn
                  FROM `{project}`.{dataset}.rwb_researcher 
              ) t
              WHERE t.rn = 1
        """.replace('%%FIELD_LIST%%', BQRWBResearcherSchema.get_sql_field_names(
        exclude_fields=[
            'genders',
            'races',
            'sex_at_birth',
            'degrees'
        ])
    )


class BQRWBResearcherGenderView(BQView):
    __viewname__ = 'v_rwb_researcher_gender'
    __viewdescr__ = 'Research Workbench Researcher Gender View'
    __pk_id__ = 'user_source_id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.user_source_id, nt.*
          FROM (
            SELECT *, 
                ROW_NUMBER() OVER (PARTITION BY user_source_id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher 
          ) t cross join unnest(genders) as nt
          WHERE t.rn = 1
    """


class BQRWBResearcherRaceView(BQView):
    __viewname__ = 'v_rwb_researcher_race'
    __viewdescr__ = 'Research Workbench Researcher Race View'
    __pk_id__ = 'user_source_id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.user_source_id, nt.*
          FROM (
            SELECT *, 
                ROW_NUMBER() OVER (PARTITION BY user_source_id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher 
          ) t cross join unnest(races) as nt
          WHERE t.rn = 1
    """


class BQRWBResearcherSexAtBirthView(BQView):
    __viewname__ = 'v_rwb_researcher_sex_at_birth'
    __viewdescr__ = 'Research Workbench Researcher Sex at Birth View'
    __pk_id__ = 'user_source_id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.user_source_id, nt.*
          FROM (
            SELECT *, 
                ROW_NUMBER() OVER (PARTITION BY user_source_id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher 
          ) t cross join unnest(sex_at_birth) as nt
          WHERE t.rn = 1
    """


class BQRWBResearcherDegreeView(BQView):
    __viewname__ = 'v_rwb_researcher_degree'
    __viewdescr__ = 'Research Workbench Researcher Degree View'
    __pk_id__ = 'user_source_id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.user_source_id, nt.*
          FROM (
            SELECT *, 
                ROW_NUMBER() OVER (PARTITION BY user_source_id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher 
          ) t cross join unnest(degrees) as nt
          WHERE t.rn = 1
    """


class BQRWBInstitutionalAffiliationsSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    researcher_id = BQField('researcher_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    institution = BQField('institution', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    role = BQField('role', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    non_academic_affiliation = BQField('non_academic_affiliation', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    non_academic_affiliation_id = BQField('non_academic_affiliation_id', BQFieldTypeEnum.INTEGER,
                                          BQFieldModeEnum.NULLABLE)
    is_verified = BQField('is_verified', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQRWBInstitutionalAffiliations(BQTable):
    """ Research Workbench Institutional Affiliations BigQuery Table """
    __tablename__ = 'rwb_institutional_affiliations'
    __schema__ = BQRWBInstitutionalAffiliationsSchema


class BQRWBInstitutionalAffiliationsView(BQView):
    __viewname__ = 'v_rwb_institutional_affiliations'
    __viewdescr__ = 'Research Workbench Institutional Affiliations View'
    __pk_id__ = 'id'
    __table__ = BQRWBInstitutionalAffiliations
