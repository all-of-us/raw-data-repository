#! /bin/env python
import argparse
# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import json
import logging
import sys
import os

from pprint import pprint
#from rdr_service.dao.code_dao import CodeBookDao
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.tools.tool_libs.alembic import AlembicManagerClass
from rdr_service.services.system_utils import git_project_root, which, run_external_program

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "setup-local-db"
tool_desc = "sets up the local database for development."

PROJECT_DIR = os.environ.get('RDR_PROJECT')
if not PROJECT_DIR:
    PROJECT_DIR = git_project_root()


TMP_CREATE_DB_FILE = '/tmp/create_db.sql'


class SetupLocalDB:  # pylint: disable=too-many-instance-attributes
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.readonly_db_user = 'readonly'
        self.rdr_db_user = 'rdr'
        self.alembic_db_user = 'alembic'
        self.root_db_user = 'root'
        self.db_name = 'rdr'
        self.rdr_password = 'rdr!pwd'
        self.root_password = os.environ.get('MYSQL_ROOT_PASSWORD', 'root')
        self.root_password_args = f'-p{self.root_password}'
        self.db_connection_string = None
        self.db_user = self.root_db_user
        self.revision = 'head'

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        clr = self.gcp_env.terminal_colors
        _logger.info(clr.fmt('Setting database configuration...', clr.fg_bright_blue))

        self.set_local_vars()
        connection_info = self.set_connection_info()
        self.write_connection_to_config(connection_info)
        # self.create_db_file()
        for db in ('rdr', 'metrics', 'rdr_tasks'):
            self.create_db(db)
        # TODO:
        # set local db connection string alembic
        self.set_local_vars('alembic')
        # upgrade database
        _logger.info('Updating schema to latest')
        # TODO: need to replicate auth_setup.sh and upgrade_database.sh
        # alembic upgrade revision
        self.alembic_upgrade(self.revision)
        # import data
        self.import_data()
        # TODO: import codebook
        # with open(codebookfile) as f:
        #     codebook_json = json.load(f)
        #     CodeBookDao().import_codebook(codebook_json)

        # TODO: import questionnaires
        # TODO: import participants
        return 0

    def import_data(self):
        data_dir = PROJECT_DIR + '/rdr_service/data/'
        from rdr_service.tools import import_organizations as orgs
        orgs.HPOImporter().run(f'{data_dir}awardees.csv', False)
        orgs.HPODao()._invalidate_cache()
        orgs.OrganizationImporter().run(f'{data_dir}organizations.csv', False)
        orgs.HPODao()._invalidate_cache()
        orgs.SiteImporter().run(f'{data_dir}sites.csv', False)

    def set_local_vars(self, user=None):
        if user:
            self.db_user = user

        self.db_connection_string = f"mysql+mysqldb://{self.db_user}:{self.rdr_password}@127.0.0.1/?charset=utf8mb4"
        os.environ['DB_CONNECTION_STRING'] = self.db_connection_string

    def set_connection_info(self):
        return {
            "db_connection_string": self.db_connection_string,
            "backup_db_connection_string": self.db_connection_string,
            "unittest_db_connection_string": "<overridden in tests>",
            "db_password": self.rdr_password,
            "db_connection_name": "",
            "db_user": self.rdr_db_user,
            "db_name": self.db_name
            }

    def write_connection_to_config(self, connection_info):
        config_dir = PROJECT_DIR + '/rdr_service/.configs/'
        with open(config_dir + 'db_config.json', 'r+') as db_file:
            db_file.seek(0)
            json.dump(connection_info, db_file)
            db_file.truncate()

        _logger.info('connection info loaded to db_config.json')
        pprint(connection_info)


    def create_db_file(self, db):

        drop_db_sql = f"""
        DROP DATABASE IF EXISTS {db};
        """

        # CREATE USER IF NOT EXISTS '{self.readonly_db_user}'@'%' IDENTIFIED BY '{self.readonly_password}';
        create_db_sql = f"""
                CREATE DATABASE IF NOT EXISTS {db} CHARACTER SET utf8 COLLATE utf8_general_ci;
                CREATE USER IF NOT EXISTS '{self.rdr_db_user}'@'%' IDENTIFIED BY '{self.rdr_password}';
                CREATE USER IF NOT EXISTS '{self.alembic_db_user}'@'%' IDENTIFIED BY '{self.rdr_password}';
                """

        grant_permissions_sql = f"""
        GRANT SELECT ON {db}.* TO '{self.readonly_db_user}'@'%';
        GRANT SELECT, INSERT, UPDATE, DELETE, CREATE TEMPORARY TABLES, EXECUTE ON {db}.* TO '{self.rdr_db_user}'@'%';
        GRANT SELECT, INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, INDEX, REFERENCES,
        CREATE TEMPORARY TABLES, CREATE VIEW, CREATE ROUTINE, ALTER ROUTINE,
        EXECUTE, TRIGGER ON {db}.* TO '{self.alembic_db_user}'@'%';
        """
        if not self.args.upgrade:
            with open(TMP_CREATE_DB_FILE, 'w+') as tmp_file:
                for sql in (drop_db_sql, create_db_sql, grant_permissions_sql):
                    tmp_file.write(sql)

            return tmp_file

    def create_db(self, db):
        # from rdr_service.dao.database_factory import _SqlDatabase
        _logger.info(f'Creating empty database {db}.')
        # database = _SqlDatabase(db)
        # database.create_schema()
        tmp_file = self.create_db_file(db)

        args = [which('mysql'), '-h 127.0.0.1',
                f'-u {self.root_db_user}', self.root_password_args, '<', tmp_file.name]

        code, so, se = run_external_program(' '.join(args), env=None, shell=True)

        if code != 0:
            _logger.error(f'{se}')
        else:
            _logger.error(f'{so}')


    def alembic_upgrade(self, revision):
        _logger.info('Applying database migrations...')
        alembic = AlembicManagerClass(self.args, self.gcp_env, ['upgrade', revision])
        alembic.args.quiet = True
        if alembic.run() != 0:
            _logger.warning('Deploy process stopped.')
            return 1


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--upgrade", help="gcp iam service account", default=None)  # noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = SetupLocalDB(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
