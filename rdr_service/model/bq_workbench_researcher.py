from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum, \
    BQRecordField
from rdr_service.model.bq_participant_summary import BQGenderSchema, BQRaceSchema


class BQDegreeSchema(BQSchema):
    degree = BQField('degree', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    degree_id = BQField('degree_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

class BQSexAtBirthSchema(BQSchema):
    sex_at_birth = BQField('sex_at_birth', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    sex_at_birth_id = BQField('sex_at_birth_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQAccessTierShortNameSchema(BQSchema):
    access_tier_short_name = BQField('access_tier_short_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    access_tier_short_name_id = BQField('access_tier_short_name_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQDSV2EthnicCategorySchema(BQSchema):
    dsv2_ethnic_category = BQField('dsv2_ethnic_category', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dsv2_ethnic_category_id = BQField('dsv2_ethnic_category_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQDSV2GenderIdentitySchema(BQSchema):
    dsv2_gender_identity = BQField('dsv2_gender_identity', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dsv2_gender_identity_id = BQField('dsv2_gender_identity_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)


class BQDSV2SexualOrientationSchema(BQSchema):
    dsv2_sexual_orientation = BQField('dsv2_sexual_orientation', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dsv2_sexual_orientation_id = BQField('dsv2_sexual_orientation_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)


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

    # New fields and sub-tables for PDR-826
    access_tier_short_name = BQRecordField('access_tier_short_name', schema=BQAccessTierShortNameSchema)
    dsv2_completion_time = BQField('dsv2_completion_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

    dsv2_disability_concentrating = BQField('dsv2_disability_concentrating', BQFieldTypeEnum.STRING,
                                            BQFieldModeEnum.NULLABLE)
    dsv2_disability_concentrating_id = BQField('dsv2_disability_concentrating_id', BQFieldTypeEnum.INTEGER,
                                               BQFieldModeEnum.NULLABLE)

    dsv2_disability_dressing = BQField('dsv2_disability_dressing', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dsv2_disability_dressing_id = BQField('dsv2_disability_dressing_id', BQFieldTypeEnum.INTEGER,
                                          BQFieldModeEnum.NULLABLE)

    dsv2_disability_errands = BQField('dsv2_disability_errands', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dsv2_disability_errands_id = BQField('dsv2_disability_errands_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)

    dsv2_disability_hearing = BQField('dsv2_disability_hearing', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dsv2_disability_hearing_id = BQField('dsv2_disability_hearing_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)

    dsv2_disability_other = BQField('dsv2_disability_other', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    dsv2_disability_seeing = BQField('dsv2_disability_seeing', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dsv2_disability_seeing_id = BQField('dsv2_disability_seeing_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    dsv2_disability_walking = BQField('dsv2_disability_walking', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dsv2_disability_walking_id = BQField('dsv2_disability_walking_id', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)

    dsv2_disadvantaged = BQField('dsv2_disadvantaged', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dsv2_disadvantaged_id = BQField('dsv2_disadvantaged_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    dsv2_education = BQField('dsv2_education', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dsv2_education_id = BQField('dsv2_education_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    dsv2_ethnic_category = BQRecordField('dsv2_ethnic_category', schema=BQDSV2EthnicCategorySchema)

    dsv2_ethnicity_aian_other = BQField('dsv2_ethnicity_aian_other', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    dsv2_ethnicity_asian_other = BQField('dsv2_ethnicity_asian_other', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    dsv2_ethnicity_other = BQField('dsv2_ethnicity_other', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    dsv2_gender_identity = BQRecordField('dsv2_gender_identity', schema=BQDSV2GenderIdentitySchema)

    dsv2_gender_other = BQField('dsv2_gender_other', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    dsv2_orientation_other = BQField('dsv2_orientation_other', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    dsv2_sex_at_birth = BQField('dsv2_sex_at_birth', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    dsv2_sex_at_birth_id = BQField('dsv2_sex_at_birth_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    dsv2_sex_at_birth_other = BQField('dsv2_sex_at_birth_other', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    dsv2_sexual_orientation = BQRecordField('dsv2_sexual_orientation', schema=BQDSV2SexualOrientationSchema)

    dsv2_year_of_birth = BQField('dsv2_year_of_birth', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    dsv2_year_of_birth_prefer_not = BQField('dsv2_year_of_birth_prefer_not', BQFieldTypeEnum.INTEGER,
                                            BQFieldModeEnum.NULLABLE)

    dsv2_ethnicity_black_other = BQField('dsv2_ethnicity_black_other', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    dsv2_ethnicity_hispanic_other = BQField('dsv2_ethnicity_hispanic_other', BQFieldTypeEnum.INTEGER,
                                            BQFieldModeEnum.NULLABLE)
    dsv2_ethnicity_mena_other = BQField('dsv2_ethnicity_mena_other', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    dsv2_ethnicity_nhpi_other = BQField('dsv2_ethnicity_nhpi_other', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    dsv2_ethnicity_white_other = BQField('dsv2_ethnicity_white_other', BQFieldTypeEnum.INTEGER,
                                         BQFieldModeEnum.NULLABLE)
    dsv2_survey_comments = BQField('dsv2_survey_comments', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    orig_created = BQField('orig_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    orig_modified = BQField('orig_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)


class BQRWBResearcher(BQTable):
    """ Code BigQuery Table """
    __tablename__ = 'rwb_researcher'
    __schema__ = BQRWBResearcherSchema


class BQRWBResearcherView(BQView):
    __viewname__ = 'v_rwb_researcher'
    __viewdescr__ = 'Research Workbench Researcher View'
    __pk_id__ = 'id'
    __table__ = BQRWBResearcher
    # We need to build a SQL statement with all fields except sub-tables and remove duplicates.
    __sql__ = """
            SELECT
                %%FIELD_LIST%%
            FROM (
                SELECT *, 
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified desc) AS rn
                  FROM `{project}`.{dataset}.rwb_researcher 
              ) t
              WHERE t.rn = 1
        """.replace('%%FIELD_LIST%%', BQRWBResearcherSchema.get_sql_field_names(
        exclude_fields=[
            'genders',
            'races',
            'sex_at_birth',
            'degrees',
            'access_tier_short_name',
            'dsv2_ethnic_category',
            'dsv2_gender_identity',
            'dsv2_sexual_orientation'
        ])
    )


class BQRWBResearcherGenderView(BQView):
    __viewname__ = 'v_rwb_researcher_gender'
    __viewdescr__ = 'Research Workbench Researcher Gender View'
    __pk_id__ = 'id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.orig_id, t.orig_created, t.orig_modified, 
            t.user_source_id, t.modified_time, nt.*
          FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher
          ) t cross join unnest(genders) as nt
          WHERE t.rn = 1
    """


class BQRWBResearcherRaceView(BQView):
    __viewname__ = 'v_rwb_researcher_race'
    __viewdescr__ = 'Research Workbench Researcher Race View'
    __pk_id__ = 'id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.orig_id, t.orig_created, t.orig_modified, 
            t.user_source_id, t.modified_time, nt.*
          FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher
          ) t cross join unnest(races) as nt
          WHERE t.rn = 1
    """


class BQRWBResearcherSexAtBirthView(BQView):
    __viewname__ = 'v_rwb_researcher_sex_at_birth'
    __viewdescr__ = 'Research Workbench Researcher Sex at Birth View'
    __pk_id__ = 'id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.orig_id, t.orig_created, t.orig_modified, 
            t.user_source_id, t.modified_time, nt.*
          FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher
          ) t cross join unnest(sex_at_birth) as nt
          WHERE t.rn = 1
    """


class BQRWBResearcherDegreeView(BQView):
    __viewname__ = 'v_rwb_researcher_degree'
    __viewdescr__ = 'Research Workbench Researcher Degree View'
    __pk_id__ = 'id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.orig_id, t.orig_created, t.orig_modified, 
            t.user_source_id, t.modified_time, nt.*
          FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher
          ) t cross join unnest(degrees) as nt
          WHERE t.rn = 1
    """

class BQAccessTierShortNameView(BQView):
    __viewname__ = 'v_rwb_researcher_access_tier_short_name'
    __viewdescr__ = 'Research Workbench Access Tier Short Name View'
    __pk_id__ = 'id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.orig_id, t.orig_created, t.orig_modified, 
            t.user_source_id, t.modified_time, nt.*
          FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher
          ) t cross join unnest(access_tier_short_name) as nt
          WHERE t.rn = 1
    """


class BQDSV2EthnicCategoryView(BQView):
    dsv2_ethnic_category = BQField('dsv2_ethnic_category', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    __viewname__ = 'v_rwb_researcher_dsv2_ethnic_category'
    __viewdescr__ = 'Research Workbench DSV2 Ethnic Category Answer View'
    __pk_id__ = 'id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.orig_id, t.orig_created, t.orig_modified, 
            t.user_source_id, t.modified_time, nt.*
          FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher
          ) t cross join unnest(dsv2_ethnic_category) as nt
          WHERE t.rn = 1
    """


class BQDSV2GenderIdentityView(BQView):
    dsv2_gender_identity = BQField('dsv2_gender_identity', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    __viewname__ = 'v_rwb_researcher_dsv2_gender_identity'
    __viewdescr__ = 'Research Workbench SV2 Gender Identity Answer View'
    __pk_id__ = 'id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.orig_id, t.orig_created, t.orig_modified, 
            t.user_source_id, t.modified_time, nt.*
          FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher
          ) t cross join unnest(dsv2_gender_identity) as nt
          WHERE t.rn = 1
    """


class BQDSV2SexualOrientationView(BQView):
    dsv2_sexual_orientation = BQField('dsv2_sexual_orientation', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    __viewname__ = 'v_rwb_researcher_dsv2_sexual_orientation'
    __viewdescr__ = 'Research Workbench SV2 Sexual Orientation Answers View'
    __pk_id__ = 'id'
    __table__ = BQRWBResearcher
    __sql__ = """
        SELECT t.id, t.created, t.modified, t.orig_id, t.orig_created, t.orig_modified, 
            t.user_source_id, t.modified_time, nt.*
          FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY modified desc) AS rn
              FROM `{project}`.{dataset}.rwb_researcher
          ) t cross join unnest(dsv2_sexual_orientation) as nt
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
    modified_time = BQField('modified_time', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)

    orig_id = BQField('orig_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    orig_created = BQField('orig_created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)
    orig_modified = BQField('orig_modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.NULLABLE)


class BQRWBInstitutionalAffiliations(BQTable):
    """ Research Workbench Institutional Affiliations BigQuery Table """
    __tablename__ = 'rwb_institutional_affiliations'
    __schema__ = BQRWBInstitutionalAffiliationsSchema


class BQRWBInstitutionalAffiliationsView(BQView):
    __viewname__ = 'v_rwb_institutional_affiliations'
    __viewdescr__ = 'Research Workbench Institutional Affiliations View'
    __pk_id__ = 'id'
    __table__ = BQRWBInstitutionalAffiliations
