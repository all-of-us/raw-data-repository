# Tools for the RDR API

## Scripts to run directly

### generate-schema.sh

Generates an Alembic schema migration in alembic/versions after altering the SQLAlchemy
schema in model/.

### install_config.sh

Populates configuration JSON in Datastore, for use by the AppEngine app.

### setup_database.sh

Sets up a Cloud SQL database instance, sets the root password, creates an rdr database in
it, and populates database configuration in Datastore.

### setup_env.sh

Sets up your workspace after checking out the code, or when something changes in 
requirements.txt.

### setup_local_database.sh

Sets up an rdr database on your local MySQL instance, for use when running a local 
server.

### upgrade_database.sh

Upgrades a database (either local or in Cloud SQL) to the latest or a specified Alembic revision, 
applying schema migrations.

## Included scripts (use with "source &lt;script name&gt;")

### set_path.sh

Sets PYTHONPATH to include everything in libs and the AppEngine SDK. For use when running python
scripts that rely on these libraries.

### utils.sh

Creates credentials in a temp file, exports run_cloud_sql_proxy function used to run Cloud SQL.



