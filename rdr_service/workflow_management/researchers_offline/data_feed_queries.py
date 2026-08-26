def create_workspace_source_id_mapping(project: str, dataset: str, mapping_table: str, source_table: str,
                                       wb_source_table: str) -> str:
    """
    """
    return f"""
        INSERT INTO `{project}.{dataset}.{mapping_table}`
            (workspace_source_id, legacy_workspace_source_id)
        SELECT
            wdte.workspace_source_id AS workspace_source_id,
            # Generate new integer ID
            (
                SELECT COALESCE(MAX(legacy_workspace_source_id), 1000000)
                FROM `{project}.{dataset}.{mapping_table}` wsim
            ) + ROW_NUMBER() OVER (ORDER BY workspace_source_id) AS legacy_workspace_source_id
        FROM `{project}.{dataset}.{source_table}` wdte
        WHERE NOT EXISTS (
            # If the string ID doesn't already exist in the mapping table
            SELECT 1
            FROM `{project}.{dataset}.{mapping_table}` wsim
            WHERE wsim.workspace_source_id = wdte.workspace_source_id
            AND wsim.ignore_flag = false
        )
        AND wdte.workspace_source_id NOT IN (
            # If the string ID doesn't already exist in the legacy table
            SELECT DISTINCT workspace_source_id_v2
            FROM `{project}.{dataset}.{wb_source_table}` lwb
            WHERE workspace_source_id_v2 IS NOT NULL
        )
    """


def get_workbench_workspaces_data_to_stream(project: str, dataset: str, mapping_table: str, source_table: str,
                                            wb_source_table: str) -> str:
    """Get 2.0 data for Workbench Workspaces to stream to MySQL. The SQL will return any records that are in
    the staging table but not in MySQL
    """

    return f"""
        SELECT
            st.* EXCEPT (workspace_source_id, creation_time, modified_time),
            IF(
                lwb.workspace_source_id IS NULL, mt.legacy_workspace_source_id, lwb.workspace_source_id
            ) AS workspace_source_id,
            DATETIME(st.creation_time, 'UTC') AS creation_time,
            DATETIME(st.modified_time, 'UTC') AS modified_time
        FROM `{project}.{dataset}.{source_table}` st
        LEFT JOIN `{project}.{dataset}.{mapping_table}` mt ON (mt.workspace_source_id = st.workspace_source_id
                                                              AND mt.ignore_flag = false)
        LEFT JOIN (
            SELECT workspace_source_id, workspace_source_id_v2
            FROM `{project}.{dataset}.{wb_source_table}`
            WHERE workspace_source_id_v2 IS NOT NULL
            AND (migration_state = 'FINISHED' OR recovery_state = 'RECOVERED')
        ) AS lwb ON lwb.workspace_source_id_v2 = st.workspace_source_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM `rdr_operational_datastream.rdr_workbench_workspace_snapshot` rwws
            WHERE rwws.workspace_source_id = mt.legacy_workspace_source_id
            AND rwws.modified_time = DATETIME(st.modified_time, 'UTC')
        )
        AND (mt.legacy_workspace_source_id IS NOT NULL OR lwb.workspace_source_id IS NOT NULL)
    """


def get_workbench_researchers_data_to_stream(project: str, dataset: str, source_table: str) -> str:
    """Get data for Workbench Researchers to stream to MySQL. The SQL will return any records that are in
    the staging table but not in MySQL
    """
    return f"""
        SELECT
            st.* EXCEPT (creation_time, modified_time),
            DATETIME(st.creation_time, 'UTC') AS creation_time,
            DATETIME(st.modified_time, 'UTC') AS modified_time
        FROM `{project}.{dataset}.{source_table}` st
        WHERE NOT EXISTS (
            SELECT 1
            FROM `rdr_operational_datastream.rdr_workbench_researcher` rwr
            WHERE rwr.user_source_id = st.user_id
            AND rwr.modified_time = DATETIME(st.modified_time, 'UTC')
        )
    """


def get_legacy_workbench_workspaces_data_to_stream(project: str, dataset: str, wb_source_table: str) -> str:
    """Get legacy 1.0 data for Workbench Workspaces to stream to MySQL. The SQL will return any new or modified records
    in the Workbench 1.0 workspaces table that have not been migrated to 2.0
    """
    return f"""
        SELECT *
        FROM `{project}.{dataset}.{wb_source_table}` ws
        WHERE NOT EXISTS (
            SELECT 1
            FROM `rdr_operational_datastream.rdr_workbench_workspace_snapshot` rwws
            WHERE rwws.workspace_source_id = ws.workspace_source_id
            AND rwws.modified_time = DATETIME(ws.modified_time, 'UTC')
        )
        AND workspace_source_id_v2 IS NULL
        AND migration_state != 'FINISHED'
        AND recovery_state != 'RECOVERED'
        AND status != 'INACTIVE'
        """
