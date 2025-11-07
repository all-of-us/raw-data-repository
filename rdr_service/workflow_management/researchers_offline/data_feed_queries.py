def create_workspace_source_id_mapping(project: str, dataset: str, mapping_table: str, source_table: str) -> str:
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
            SELECT 1
            FROM `{project}.{dataset}.{mapping_table}` wsim
            WHERE wsim.workspace_source_id = wdte.workspace_source_id
        )
    """


def get_workbench_workspaces_data_to_stream(project: str, dataset: str, mapping_table: str, source_table: str) -> str:
    """Get data for Workbench Workspaces to stream to MySQL. The SQL will return any records that are in
    the staging table but not in MySQL
    """

    return f"""
        SELECT
            st.* EXCEPT (workspace_source_id, creation_time, modified_time),
            mt.legacy_workspace_source_id AS workspace_source_id,
            DATETIME(st.creation_time, 'UTC') AS creation_time,
            DATETIME(st.modified_time, 'UTC') AS modified_time
        FROM `{project}.{dataset}.{source_table}` st
        JOIN `{project}.{dataset}.{mapping_table}` mt ON mt.workspace_source_id = st.workspace_source_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM `rdr_operational_datastream.rdr_workbench_workspace_snapshot` rwws
            WHERE rwws.workspace_source_id = mt.legacy_workspace_source_id
            AND rwws.modified_time = DATETIME(st.modified_time, 'UTC')
        )
    """
