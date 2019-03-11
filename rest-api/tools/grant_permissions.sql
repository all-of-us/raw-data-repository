GRANT SELECT ON ${db_name}.* TO '${READONLY_DB_USER}'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE TEMPORARY TABLES, EXECUTE ON ${db_name}.* TO '${RDR_DB_USER}'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, INDEX, REFERENCES,
	CREATE TEMPORARY TABLES, CREATE VIEW, CREATE ROUTINE, ALTER ROUTINE,
  EXECUTE ON ${db_name}.* TO '${ALEMBIC_DB_USER}'@'%';
