from rdr_service import config

# def test_insert_workbench_workspaces_staging_data(
#     project: str, destination_dataset: str
# ) -> str:
#     """Test inserting data into `datafeed_input_awardee_insite` table. Also takes care of withdrawn participants"""
#
#     return f"""
#     INSERT INTO `all-of-us-rdr-stable.rdr_workbench_multi_region.test_workbench_data_transfer_eas`(
#         workspace_source_id, name, creation_time, modified_time
#     )
#     SELECT DISTINCT
#         ww.workspace_id AS workspace_source_id,
#         ww.workspace_display_name AS name,
#         ww.created_date AS creationTime,
#         CURRENT_TIMESTAMP() AS modifiedTime
#     FROM `workbench-bq-log-sink.workbench_monitoring_org_logs_prod.wsm_workspaces` AS ww
#     LIMIT 1;
#     """


# def test_insert_workbench_workspaces_staging_data(project: str, destination_dataset: str, last_job_run_date: str) -> str:
#     """
#     """
#
#     verily_workbench_project = "workbench-bq-log-sink"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
#     verily_workbench_org_dataset = "workbench_monitoring_org_logs_prod"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
#     verily_workbench_data_collection_dataset = "workbench_monitoring_data_collection_logs_prod"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
#     verily_workbench_workspaces_table = "wsm_workspaces"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
#     verily_workbench_users_table = "sam_workspace_users"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
#     verily_workbench_workspace_activity_logs_table = "wsm_workspace_activity_logs"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
#     verily_workbench_data_collection_activity_logs_table = "data_collection_activity_clone_logs"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
#
#     workbench_project = "all-of-us-rw-stable"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
#     workbench_reporting_dataset = "reporting_stable"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
#     workbench_user_table = "user"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
#
#
#     return f"""
#         INSERT INTO `{project}.{destination_dataset}.datafeed_input_workbench_workspaces`
#         (
#           workspace_source_id
#           , name
#           , creation_time
#           , modified_time
#           , status
#           , exclude_from_public_directory
#           , ethical_legal_social_implications
#           , review_requested
#           , disease_focused_research
#           , disease_focused_research_name
#           , other_purpose_details
#           , methods_development
#           , control_set
#           , ancestry
#           , social_behavioral
#           , population_health
#           , drug_development
#           , commercial_purpose
#           , educational
#           , other_purpose
#           , scientific_approaches
#           , intend_to_study
#           , findings_from_study
#           , focus_on_underrepresented_populations
#           , sex_at_birth
#           , gender_identity
#           , sexual_orientation
#           , geography
#           , disability_status
#           , access_to_care
#           , education_level
#           , income_level
#           , race_ethnicity
#           , age
#           , others
#           , workspace_users
#           , creator
#           , cdr_version
#           , access_tier
#           , aian_research_type
#           , aian_research_details
#         )
#     # Get all active workspace data
#         WITH users AS (
#             SELECT DISTINCT
#                 workspace_id,
#                 ARRAY_AGG(STRUCT(role, user_email, "ACTIVE" AS status)) AS workspace_users
#             FROM
#                 `{verily_workbench_project}.{verily_workbench_org_dataset}.{verily_workbench_users_table}`
#             GROUP BY workspace_id
#         )
#         SELECT DISTINCT
#             ww.workspace_id AS workspace_source_id,
#             ww.workspace_display_name AS name,
#             ww.created_date AS creation_time,
#             wwal.change_date AS modified_time,
#             "ACTIVE" AS status, # Hardcoded Value - if a workspace is in the workspaces table, it is ACTIVE
#             IF(rwu.institution_id = 1, "true", "false") AS exclude_from_public_directory,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeElsi") AS ethical_legal_social_implications,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.requestReviewByRab") AS review_requested,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDisease") AS disease_focused_research,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDiseaseText") AS disease_focused_research_name,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeOtherText") AS other_purpose_details,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeMethods") AS methods_development,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeResearchControl") AS control_set,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeGeneticResearch") AS ancestry,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeSocialResearch") AS social_behavioral,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposePopulationResearch") AS population_health,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDrug") AS drug_development,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.form_data.purposeForProfit") AS commercial_purpose,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeEducation") AS educational,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeOther") AS other_purpose,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.scientificApproaches") AS scientific_approaches,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.scientificQuestions") AS intend_to_study,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.anticipatedFindings") AS findings_from_study,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationYesNo") AS focus_on_underrepresented_populations,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationSexOther") AS sex_at_birth,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationGenderIdentity") AS gender_identity,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationSexualOrientation") AS sexual_orientation,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationGeography") AS geography,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationDisability") AS disability_status,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationCare") AS access_to_care,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationEducation") AS education_level,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationIncome") AS income_level,
#             ARRAY(
#                 SELECT value
#                 FROM UNNEST([
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAsian") = "true", "ASIAN", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationBlackAfricanAfricanAmerican") = "true", "AA", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationHispanicLatinoSpanish") = "true", "HISPANIC", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAian") = "true", "AIAN", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationNhpi") = "true", "NHPI", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationMena") = "true", "MENA", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationMultiAncestry") = "true", "MULTI", NULL)
#                 ]) AS value
#                 WHERE value IS NOT NULL
#             ) AS race_ethnicity,
#             ARRAY(
#                 SELECT value FROM UNNEST([
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationChildren") = "true", "AGE_0_11", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAdolescents") = "true", "AGE_12_17", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOlderAdults65") = "true", "AGE_65_74", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOlderAdults75") = "true", "AGE_75_AND_MORE", NULL)
#                 ]) AS value
#                 WHERE value IS NOT NULL
#             ) AS age,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOtherText") AS others,
#             vwbu.workspace_users AS workspace_users,
#             STRUCT(rwu.user_id AS userId, rwu.given_name AS givenName, rwu.family_name AS familyName) AS creator,
#             "v8" AS cdr_version, -- This value will be hardcoded for now b/c there are no other versions
#             -- CASE
#               -- WHEN dcal.data_collection_user_facing_id = "aou-registered-tier"
#               -- THEN "REGISTERED"
#               -- WHEN dcal.data_collection_user_facing_id = "aou-controlled-tier"
#               -- THEN "CONTROLLED"
#               -- ELSE ""
#             -- END AS access_tier
#             NULL AS AS access_tier, -- TODO: Temporary value while waiting on access to this table
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.aiAnPlanType") AS aian_research_type,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.aiAnPlanExplanation") AS aian_research_details
#         FROM `{verily_workbench_project}.{verily_workbench_org_dataset}.{verily_workbench_workspaces_table}` AS ww
#         JOIN
#             (
#                 SELECT workspace_id, MAX(change_date) AS change_date
#                 FROM `{verily_workbench_project}.{verily_workbench_org_dataset}.{verily_workbench_workspace_activity_logs_table}`
#                 WHERE change_date > "{last_job_run_date}"
#                 GROUP BY workspace_id
#             ) AS wwal ON ww.workspace_id = wwal.workspace_id
#         JOIN `{workbench_project}.{workbench_reporting_dataset}.{workbench_user_table}` AS rwu ON ww.created_by_email = rwu.username
#         JOIN users AS vwbu ON ww.workspace_id = vwbu.workspace_id
#         -- JOIN `{verily_workbench_project}.{verily_workbench_data_collection_dataset}.{verily_workbench_data_collection_activity_logs_table}` AS dcal ON ww.workspace_id = dcal.workspace_id
#         UNION ALL
#     # Get all deleted workspaces - SELECT fields workspaceSourceId, modifiedTime, and status.
#     # All other fields are set to null. Null data will be filled in through the RDR MySQL table
#         SELECT DISTINCT
#             wa.workspace_id AS workspaceSourceId,
#             wa.change_date AS modifiedTime,
#             "INACTIVE" AS status, # If a workspace is in the activity_logs table as DELETE, it is INACTIVE
#             ww.workspace_display_name AS name,
#             ww.created_date AS creationTime,
#             "" AS excludeFromPublicDirectory,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeElsi") AS ethicalLegalSocialImplications,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.requestReviewByRab") AS reviewRequested,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDisease") AS diseaseFocusedResearch,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDiseaseText") AS diseaseFocusedResearchName,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeOtherText") AS otherPurposeDetails,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeMethods") AS methodsDevelopment,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeResearchControl") AS controlSet,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeGeneticResearch") AS ancestry,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeSocialResearch") AS socialBehavioral,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposePopulationResearch") AS populationHealth,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDrug") AS drugDevelopment,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.form_data.purposeForProfit") AS commercialPurpose,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeEducation") AS educational,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeOther") AS otherPurpose,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.scientificApproaches") AS scientificApproaches,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.scientificQuestions") AS intendToStudy,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.anticipatedFindings") AS findingsFromStudy,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationYesNo") AS focusOnUnderrepresentedPopulations,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationSexOther") AS sexAtBirth,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationGenderIdentity") AS genderIdentity,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationSexualOrientation") AS sexualOrientation,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationGeography") AS geography,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationDisability") AS disabilityStatus,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationCare") AS accessToCare,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationEducation") AS educationLevel,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationIncome") AS incomeLevel,
#             ARRAY(
#                 SELECT value
#                 FROM UNNEST([
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAsian") = "true", "ASIAN", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationBlackAfricanAfricanAmerican") = "true", "AA", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationHispanicLatinoSpanish") = "true", "HISPANIC", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAian") = "true", "AIAN", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationNhpi") = "true", "NHPI", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationMena") = "true", "MENA", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationMultiAncestry") = "true", "MULTI", NULL)
#                 ]) AS value
#                 WHERE value IS NOT NULL
#             ) AS raceEthnicity,
#             ARRAY(
#                 SELECT value FROM UNNEST([
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationChildren") = "true", "AGE_0_11", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAdolescents") = "true", "AGE_12_17", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOlderAdults65") = "true", "AGE_65_74", NULL),
#                     IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOlderAdults75") = "true", "AGE_75_AND_MORE", NULL)
#                 ]) AS value
#                 WHERE value IS NOT NULL
#             ) AS age,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOtherText") AS others,
#             [STRUCT("" AS role, "" AS user_email, "" AS status)] AS workspace_users,
#             STRUCT(NULL AS userId, "" AS givenName, "" AS familyName) AS creator,
#             "" AS cdrVersion, -- This value will be hardcoded for now b/c there are no other versions
#             NULL AS accessTier,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.aiAnPlanType") AS aianResearchType,
#             JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.aiAnPlanExplanation") AS aianResearchDetails
#         FROM `{verily_workbench_project}.{verily_workbench_org_dataset}.{verily_workbench_workspaces_table}` AS ww
#         FULL OUTER JOIN `{verily_workbench_project}.{verily_workbench_org_dataset}.{verily_workbench_workspace_activity_logs_table}` AS wa ON ww.workspace_id = wa.workspace_id
#         WHERE wa.change_type="DELETE" AND wa.change_subject_type = "WORKSPACE"
#     """
#

def insert_workbench_workspaces_staging_data(project: str, destination_dataset: str, last_job_run_date: str) -> str:
    """
    Insert workspace staging data into `datafeed_input_workbench_workspaces` table.
    """

    verily_workbench_project = "workbench-bq-log-sink"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
    verily_workbench_org_dataset = "workbench_monitoring_org_logs_prod"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
    verily_workbench_data_collection_dataset = "workbench_monitoring_data_collection_logs_prod"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
    verily_workbench_workspaces_table = "wsm_workspaces"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
    verily_workbench_users_table = "sam_workspace_users"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
    verily_workbench_workspace_activity_logs_table = "wsm_workspace_activity_logs"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
    verily_workbench_data_collection_activity_logs_table = "data_collection_activity_clone_logs"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]

    workbench_project = "all-of-us-rw-stable"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
    workbench_reporting_dataset = "reporting_stable"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]
    workbench_user_table = "user"  # config.getSettingJson(config.CURATION_PROD_PROJECT)[0]

    return f"""
        INSERT INTO `{project}.{destination_dataset}.datafeed_input_workbench_workspaces`
        (
          workspace_source_id
          , name
          , creation_time
          , modified_time
          , status
          , exclude_from_public_directory
          , ethical_legal_social_implications
          , review_requested
          , disease_focused_research
          , disease_focused_research_name
          , other_purpose_details
          , methods_development
          , control_set
          , ancestry
          , social_behavioral
          , population_health
          , drug_development
          , commercial_purpose
          , educational
          , other_purpose
          , scientific_approaches
          , intend_to_study
          , findings_from_study
          , focus_on_underrepresented_populations
          , sex_at_birth
          , gender_identity
          , sexual_orientation
          , geography
          , disability_status
          , access_to_care
          , education_level
          , income_level
          , race_ethnicity
          , age
          , others
          , workspace_users
          , creator
          , cdr_version
          , access_tier
          , aian_research_type
          , aian_research_details
        )
    # Get all active workspace data
        WITH users AS (
            SELECT DISTINCT
                workspace_id,
                ARRAY_AGG(STRUCT(role, user_email, "ACTIVE" AS status)) AS workspace_users
            FROM
                `{verily_workbench_project}.{verily_workbench_org_dataset}.{verily_workbench_users_table}`
            GROUP BY workspace_id
        )
        SELECT DISTINCT
            ww.workspace_id AS workspace_source_id,
            ww.workspace_display_name AS name,
            ww.created_date AS creation_time,
            wwal.change_date AS modified_time,
            "ACTIVE" AS status, # Hardcoded Value - if a workspace is in the workspaces table, it is ACTIVE
            IF(rwu.institution_id = 1, TRUE, FALSE) AS exclude_from_public_directory,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeElsi") AS ethical_legal_social_implications,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.requestReviewByRab") AS review_requested,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDisease") AS disease_focused_research,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDiseaseText") AS disease_focused_research_name,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeOtherText") AS other_purpose_details,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeMethods") AS methods_development,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeResearchControl") AS control_set,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeGeneticResearch") AS ancestry,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeSocialResearch") AS social_behavioral,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposePopulationResearch") AS population_health,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDrug") AS drug_development,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.form_data.purposeForProfit") AS commercial_purpose,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeEducation") AS educational,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeOther") AS other_purpose,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.scientificApproaches") AS scientific_approaches,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.scientificQuestions") AS intend_to_study,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.anticipatedFindings") AS findings_from_study,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationYesNo") AS focus_on_underrepresented_populations,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationSexOther") AS sex_at_birth,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationGenderIdentity") AS gender_identity,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationSexualOrientation") AS sexual_orientation,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationGeography") AS geography,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationDisability") AS disability_status,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationCare") AS access_to_care,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationEducation") AS education_level,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationIncome") AS income_level,
            ARRAY(
                SELECT value
                FROM UNNEST([
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAsian") = "true", "ASIAN", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationBlackAfricanAfricanAmerican") = "true", "AA", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationHispanicLatinoSpanish") = "true", "HISPANIC", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAian") = "true", "AIAN", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationNhpi") = "true", "NHPI", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationMena") = "true", "MENA", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationMultiAncestry") = "true", "MULTI", NULL)
                ]) AS value
                WHERE value IS NOT NULL
            ) AS race_ethnicity,
            ARRAY(
                SELECT value FROM UNNEST([
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationChildren") = "true", "AGE_0_11", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAdolescents") = "true", "AGE_12_17", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOlderAdults65") = "true", "AGE_65_74", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOlderAdults75") = "true", "AGE_75_AND_MORE", NULL)
                ]) AS value
                WHERE value IS NOT NULL
            ) AS age,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOtherText") AS others,
            vwbu.workspace_users AS workspace_users,
            STRUCT(rwu.user_id AS userId, rwu.given_name AS givenName, rwu.family_name AS familyName) AS creator,
            "v8" AS cdr_version, -- This value will be hardcoded for now b/c there are no other versions
            -- CASE
              -- WHEN dcal.data_collection_user_facing_id = "aou-registered-tier"
              -- THEN "REGISTERED"
              -- WHEN dcal.data_collection_user_facing_id = "aou-controlled-tier"
              -- THEN "CONTROLLED"
              -- ELSE ""
            -- END AS access_tier
            NULL AS access_tier, -- TODO: Temporary value while waiting on access to this table
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.aiAnPlanType") AS aian_research_type,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.aiAnPlanExplanation") AS aian_research_details
        FROM `{verily_workbench_project}.{verily_workbench_org_dataset}.{verily_workbench_workspaces_table}` AS ww
        JOIN
            (
                SELECT workspace_id, MAX(change_date) AS change_date
                FROM `{verily_workbench_project}.{verily_workbench_org_dataset}.{verily_workbench_workspace_activity_logs_table}`
                WHERE change_date > "{last_job_run_date}"
                GROUP BY workspace_id
            ) AS wwal ON ww.workspace_id = wwal.workspace_id
        JOIN `{workbench_project}.{workbench_reporting_dataset}.{workbench_user_table}` AS rwu ON ww.created_by_email = rwu.username
        JOIN users AS vwbu ON ww.workspace_id = vwbu.workspace_id
        -- JOIN `{verily_workbench_project}.{verily_workbench_data_collection_dataset}.{verily_workbench_data_collection_activity_logs_table}` AS dcal ON ww.workspace_id = dcal.workspace_id
        UNION ALL
    # Get all deleted workspaces - SELECT fields workspaceSourceId, modifiedTime, and status.
    # All other fields are set to null. Null data will be filled in through the RDR MySQL table
        SELECT DISTINCT
            wa.workspace_id AS workspaceSourceId,
            ww.workspace_display_name AS name,
            ww.created_date AS creationTime,
            wa.change_date AS modifiedTime,
            "INACTIVE" AS status, # If a workspace is in the activity_logs table as DELETE, it is INACTIVE
            NULL AS excludeFromPublicDirectory,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeElsi") AS ethicalLegalSocialImplications,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.requestReviewByRab") AS reviewRequested,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDisease") AS diseaseFocusedResearch,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDiseaseText") AS diseaseFocusedResearchName,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeOtherText") AS otherPurposeDetails,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeMethods") AS methodsDevelopment,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeResearchControl") AS controlSet,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeGeneticResearch") AS ancestry,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeSocialResearch") AS socialBehavioral,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposePopulationResearch") AS populationHealth,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeDrug") AS drugDevelopment,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.form_data.purposeForProfit") AS commercialPurpose,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeEducation") AS educational,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.purposeOther") AS otherPurpose,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.scientificApproaches") AS scientificApproaches,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.scientificQuestions") AS intendToStudy,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.anticipatedFindings") AS findingsFromStudy,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationYesNo") AS focusOnUnderrepresentedPopulations,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationSexOther") AS sexAtBirth,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationGenderIdentity") AS genderIdentity,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationSexualOrientation") AS sexualOrientation,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationGeography") AS geography,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationDisability") AS disabilityStatus,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationCare") AS accessToCare,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationEducation") AS educationLevel,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationIncome") AS incomeLevel,
            ARRAY(
                SELECT value
                FROM UNNEST([
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAsian") = "true", "ASIAN", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationBlackAfricanAfricanAmerican") = "true", "AA", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationHispanicLatinoSpanish") = "true", "HISPANIC", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAian") = "true", "AIAN", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationNhpi") = "true", "NHPI", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationMena") = "true", "MENA", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationMultiAncestry") = "true", "MULTI", NULL)
                ]) AS value
                WHERE value IS NOT NULL
            ) AS raceEthnicity,
            ARRAY(
                SELECT value FROM UNNEST([
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationChildren") = "true", "AGE_0_11", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationAdolescents") = "true", "AGE_12_17", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOlderAdults65") = "true", "AGE_65_74", NULL),
                    IF(JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOlderAdults75") = "true", "AGE_75_AND_MORE", NULL)
                ]) AS value
                WHERE value IS NOT NULL
            ) AS age,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.populationOtherText") AS others,
            [STRUCT("" AS role, "" AS user_email, "" AS status)] AS workspace_users,
            STRUCT(NULL AS userId, "" AS givenName, "" AS familyName) AS creator,
            "" AS cdrVersion, -- This value will be hardcoded for now b/c there are no other versions
            NULL AS accessTier,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.aiAnPlanType") AS aianResearchType,
            JSON_EXTRACT_SCALAR(workspace_metadata_policy, "$[0].form_data.aiAnPlanExplanation") AS aianResearchDetails
        FROM `{verily_workbench_project}.{verily_workbench_org_dataset}.{verily_workbench_workspaces_table}` AS ww
        FULL OUTER JOIN `{verily_workbench_project}.{verily_workbench_org_dataset}.{verily_workbench_workspace_activity_logs_table}` AS wa ON ww.workspace_id = wa.workspace_id
        WHERE wa.change_type="DELETE" AND wa.change_subject_type = "WORKSPACE"
        -- Just for testing
        LIMIT 1
    """

def get_workbench_workspaces_data_to_stream(project: str, destination_dataset: str, last_job_run_date: str) -> str:
    """Get data for Workbench Workspaces to stream to MySQL. The SQL will return any records that are in
    the staging table but not in MySQL
    """

    # TODO: Comparison cannot be based on the workspace_source_id
    # TODO: Better approach: Create new Unique ID in the staging table. When data is inserted into the workspace table, also insert the BQ_Unique_ID

    return f"""
         SELECT *
         FROM `{project}.{destination_dataset}.datafeed_input_workbench_workspaces` diww
         WHERE NOT EXISTS (
            SELECT 1
            FROM `rdr_operational_datastream.rdr_workbench_workspace_snapshot` rwws
            WHERE rwws.workspace_source_id = diww.workspace_source_id
            AND rwws.modified_time = diww.modified_time
         )
    """
