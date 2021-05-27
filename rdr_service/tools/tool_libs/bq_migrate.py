#! /bin/env python
#
# Template for RDR tool python program.
#

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import importlib
import json
import logging
import os
import sys
import tempfile

import argparse

from rdr_service.model import BQ_TABLES, BQ_VIEWS
from rdr_service.model.bq_base import BQDuplicateFieldException, BQInvalidSchemaException, BQInvalidModeException, \
    BQSchemaStructureException, BQException, BQSchema
from rdr_service.services.gcp_utils import gcp_bq_command
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger('rdr_logger')

# tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = 'migrate-bq'
tool_desc = 'bigquery schema migration tool'

LJUST_WIDTH = 75


class BQMigration(object):
    _db_config = None

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env

    def create_table(self, bq_table, project_id, dataset_id, table_id):
        """
        Create a table with the given schema in BigQuery.
        :param bq_table: BQTable object
        :param project_id: project id
        :param dataset_id: dataset id
        :param table_id: table id
        :return: True if successful otherwise False
        """
        bq_schema = bq_table.get_schema()
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.write(str.encode(bq_schema.to_json()))
        tf.close()

        # bq mk --table --expiration [INTEGER] --description [DESCRIPTION]
        #             --label [KEY:VALUE, KEY:VALUE] [PROJECT_ID]:[DATASET].[TABLE] [SCHEMA]
        args = '{0}:{1}.{2} {3}'.format(project_id, dataset_id, table_id, tf.name)
        cflags = '--table --label organization:rdr'
        pcode, so, se = gcp_bq_command('mk', args=args, command_flags=cflags)  # pylint: disable=unused-variable

        os.unlink(tf.name)

        if pcode != 0:
            if 'parsing error in row starting at position' in so:
                raise BQInvalidSchemaException(so)
            else:
                raise BQException(se if se else so)
        _logger.info('  {0}: {1}'.format('{0}.{1}.{2}'.
                                         format(project_id, dataset_id, table_id).ljust(LJUST_WIDTH, '.'), 'created'))

        return True

    def create_view(self, bq_view, project_id, dataset_id, view_id):
        """
        Create a view
        :param bq_view: BQView object
        :param project_id: project id
        :param dataset_id: dataset id
        :param view_id: table id
        :return: True if successful otherwise False
        """
        view_desc = bq_view.get_descr()
        view_sql = bq_view.get_sql()
        bq_table = bq_view.get_table()
        if not bq_table:
            raise ValueError('BQView {0} does not have a BQTable object configured.')

        tmp_sql = view_sql.format(project=project_id, dataset=dataset_id)
        args = '{0}:{1}.{2}'.format(project_id, dataset_id, view_id)

        # Try to update
        cflags = "--description '{0}' --view '{1}'".format(view_desc, tmp_sql)
        pcode, so, se = gcp_bq_command('update', args=args, command_flags=cflags)  # pylint: disable=unused-variable

        if pcode == 0:
            _logger.info('  {0}: {1}'.format('{0}.{1}.{2}'.
                                             format(project_id, dataset_id, view_id).ljust(LJUST_WIDTH, '.'),
                                             'updated'))
        else:
            cflags = "--use_legacy_sql=false --label organization:rdr --description '{0}' --view '{1}'". \
                format(view_desc, tmp_sql)
            pcode, so, se = gcp_bq_command('mk', args=args, command_flags=cflags)  # pylint: disable=unused-variable
            if pcode != 0:
                raise BQException(se if se else so)
            _logger.info('  {0}: {1}'.format('{0}.{1}.{2}'.
                                             format(project_id, dataset_id, view_id).ljust(LJUST_WIDTH, '.'),
                                             'created'))

        return True

    def delete_view(self, bq_view, project_id, dataset_id, view_id):
        """
        Delete the view from BigQuery
        :param bq_view: BQView object
        :param project_id: project id
        :param dataset_id: dataset id
        :param view_id: table id
        :return: string
        """
        bq_table = bq_view.get_table()
        if not bq_table:
            raise ValueError('BQView {0} does not have a BQTable object configured.')

        # bq rm --force --table [PROJECT_ID]:[DATASET].[TABLE]
        args = '{0}:{1}.{2}'.format(project_id, dataset_id, view_id)
        pcode, so, se = gcp_bq_command('rm', args=args,
                                       command_flags='--force --table')  # pylint: disable=unused-variable

        if pcode != 0:
            raise BQException(se if se else so)
        _logger.info(
            '  {0}: {1}'.format('{0}.{1}.{2}'.format(project_id, dataset_id, view_id).ljust(75, '.'), 'deleted'))

        return so

    def modify_table(self, bq_table, project_id, dataset_id, table_id):
        """
        Modify the schema of a table in BigQuery.
        :param bq_table: BQTable object
        :param project_id: project id
        :param dataset_id: dataset id
        :param table_id: table id
        :return: True if successful otherwise False
        """
        bq_schema = bq_table.get_schema()
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.write(str.encode(bq_schema.to_json()))
        tf.close()

        # bq update [PROJECT_ID]:[DATASET].[TABLE] [SCHEMA]
        args = '{0}:{1}.{2} {3}'.format(project_id, dataset_id, table_id, tf.name)
        pcode, so, se = gcp_bq_command('update', args=args)  # pylint: disable=unused-variable

        os.unlink(tf.name)

        if pcode == 0:
            _logger.info('  {0}: {1}'.format('{0}.{1}.{2}'.
                                             format(project_id, dataset_id, table_id).ljust(LJUST_WIDTH, '.'),
                                             'updated'))
        else:
            if 'already exists in schema' in so:
                raise BQDuplicateFieldException(so)
            elif 'parsing error in row starting at position' in so:
                raise BQInvalidSchemaException(so)
            elif 'add required columns to an existing schema' in so:
                raise BQInvalidModeException(so)
            elif 'Precondition Failed' in so:
                raise BQSchemaStructureException(so)
            else:
                raise BQException(so)

        return True

    def delete_table(self, project_id, dataset_id, table_id):
        """
        Delete the table from BigQuery
        :param project_id: project id
        :param dataset_id: dataset id
        :param table_id: table id
        :return: String
        """
        # bq rm --force --table [PROJECT_ID]:[DATASET].[TABLE]
        args = '{0}:{1}.{2}'.format(project_id, dataset_id, table_id)
        pcode, so, se = gcp_bq_command('rm', args=args,
                                       command_flags='--force --table')  # pylint: disable=unused-variable

        if pcode != 0:
            raise BQException(se if se else so)
        _logger.info('  {0}: {1}'.format('{0}.{1}.{2}'.
                                         format(project_id, dataset_id, table_id).ljust(LJUST_WIDTH, '.'), 'deleted'))

        return so

    def get_table_schema(self, project_id, dataset_id, table_id):
        """
        Retrieve the table schema from BigQuery
        :param project_id: project id
        :param dataset_id: dataset id
        :param table_id: table id
        :return: string
        """
        # bq show --schema --format=prettyjson [PROJECT_ID]:[DATASET].[TABLE]
        args = '{0}:{1}.{2}'.format(project_id, dataset_id, table_id)
        pcode, so, se = gcp_bq_command('show', args=args,
                                       command_flags='--schema --format=prettyjson')  # pylint: disable=unused-variable

        if pcode != 0:
            if 'Not found' in so:
                return None
            if 'Authorization error' in so:
                _logger.error('** BigQuery returned an authorization error, please check the following: **')
                _logger.error('   * Service account has correct permissions.')
                _logger.error('   * Timezone and time on computer match PMI account settings.')
                # for more suggestions look at:
                #    https://blog.timekit.io/google-oauth-invalid-grant-nightmare-and-how-to-fix-it-9f4efaf1da35
            raise BQException(se if se else so)

        return so

    def compare_table_schemas (self, table_id, local_schema, remote_schema):
        local = {}
        remote = {}

        # For selectively overriding default colorization of info logging in schema comparison output
        BLUE = '\033[94m'
        END_COLOR = '\033[0m'

        # Must at least have a local schema to compare
        if not local_schema:
            _logger.error(f'Missing local schema definition for {table_id}')
            return
        else:
            for field in json.loads(local_schema.to_json()):
                local[field['name']] = (field['type'], field['mode'])

        if remote_schema:
            for field in json.loads(remote_schema.to_json()):
                remote[field['name']] = (field['type'], field['mode'])
        else:
            # Remote schema doesn't exist; assume it's a new table.  List the column definitions and return
            _logger.info(f'{BLUE}New table: {table_id}{END_COLOR}')
            for col in local.keys():
                _logger.info(f'\t{BLUE} {col} {local[col]}{END_COLOR}')
            return

        # Make a merged list of all the column keys from both local and remote schemas and compare column definitions
        columns = sorted(local.keys() | remote.keys())
        for col in columns:
            if col in local.keys() and col in remote.keys():
                if local[col] != remote[col]:
                    # The field type or mode must have been altered in the latest local schema definition;
                    # Will require dropping/recreating the BigQuery table
                    _logger.error(f'\t{table_id} column {col} changed from {remote[col]} to {local[col]}')
            elif col in local.keys() and col not in remote.keys():
                # A new column in the local schema definition; a "safe" update to apply to existing BigQuery table
                _logger.info(f'\t{BLUE}{table_id} column {col} {local[col]} added{END_COLOR}')
            else:
                # A column from the existing BigQuery table no longer exists in the local schema definition;
                # Will require dropping/recreating the BigQuery table
                _logger.error(f'\t{table_id} column {col} {remote[col]} removed')


    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        if not self.args.delete and not self.gcp_env.activate_sql_proxy():
            return 1

        migrate_all = self.args.names.lower() == 'all'
        if not migrate_all:
            migrate_list = self.args.names.lower().split(",")

        # TODO: Validate dataset name exists in BigQuery
        # Loop through table schemas
        for path, var_name in BQ_TABLES:
            mod = importlib.import_module(path, var_name)
            mod_class = getattr(mod, var_name)
            bq_table = mod_class()
            # See if we need to skip this table
            if not migrate_all and bq_table.get_name().lower() not in migrate_list:
                continue

            ls_obj = bq_table.get_schema()
            if self.args.show_schemas:
                print('Schema: {0}\n'.format(bq_table.get_name()))
                print(ls_obj.to_json())
                print('\n\n')
                continue

            # Loop through the mappings.
            mappings = bq_table.get_project_map(self.args.project)
            for project_id, dataset_id, table_id in mappings:

                if dataset_id is None:
                    _logger.info('  {0}: {1}'.format('{0}.{1}.{2}'.
                                                     format(project_id, dataset_id, table_id).ljust(LJUST_WIDTH, '.'),
                                                     'disabled'))
                    continue

                if self.args.delete:
                    self.delete_table(project_id, dataset_id, table_id)
                    continue

                # _logger.info(' schema: {0}'.format(instance.to_json()))
                rs_json = self.get_table_schema(project_id, dataset_id, table_id)

                if not rs_json:
                    if self.args.dry_run:
                        # A new table;  compare_table_schemas() will display the new schema details
                        self.compare_table_schemas(table_id, ls_obj, None)
                        continue
                    else:
                        self.create_table(bq_table, project_id, dataset_id, table_id)

                else:
                    try:
                        rs_obj = BQSchema(json.loads(rs_json))
                        if self.args.dry_run:
                            # New/added columns can be handled by re-running the command without --dry-run
                            # Changed or removed columns mean the table should be dropped (run with --delete) first
                            _logger.info(f'Comparing {project_id}:{dataset_id}.{table_id} schema changes...')
                            self.compare_table_schemas(table_id, ls_obj, rs_obj)
                            continue
                    except ValueError:
                        # Something is there in BigQuery for this schema, but it is bad.
                        # If this happens, the table can be reset by deleting it
                        # and then creating again it using this tool
                        _logger.info('  {0}: {1}'.format('{0}.{1}.{2}'.
                                                         format(project_id, dataset_id, table_id).ljust(LJUST_WIDTH,
                                                                                                        '.'),
                                                         '!!! corrupt !!!'))
                        continue

                    if rs_obj == ls_obj:
                        _logger.info('  {0}: {1}'.format('{0}.{1}.{2}'.
                                                         format(project_id, dataset_id, table_id).ljust(LJUST_WIDTH,
                                                                                                        '.'),
                                                         'unchanged'))
                    else:
                        self.modify_table(bq_table, project_id, dataset_id, table_id)

        if self.args.dry_run:
            return 0

        # Loop through view schemas
        for path, var_name in BQ_VIEWS:
            mod = importlib.import_module(path, var_name)
            mod_class = getattr(mod, var_name)
            bq_view = mod_class()
            if not bq_view:
                raise ValueError('Invalid BQ View object [{0}.{1}]'.format(path, var_name))
            view_id = bq_view.get_name()

            # See if we need to skip this view
            if not migrate_all and view_id.lower() not in migrate_list:
                continue

            bq_table = bq_view.get_table()

            if self.args.show_schemas:
                print('View: {0}\n'.format(view_id))
                print(bq_view.get_sql())
                print('\n\n')
                continue

            # Loop through the mappings.
            mappings = bq_table.get_project_map(self.args.project)
            for project_id, dataset_id, table_id in mappings:
                if dataset_id is None:
                    _logger.info('  {0}: {1}'.format('{0}.{1}.{2}'.
                                                     format(project_id, dataset_id, view_id).ljust(LJUST_WIDTH, '.'),
                                                     'disabled'))
                    continue

                if self.args.delete:
                    self.delete_view(bq_view, project_id, dataset_id, view_id)
                    continue

                self.create_view(bq_view, project_id, dataset_id, view_id)

        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(_logger, tool_cmd,
                  '--debug' in sys.argv, '{0}.log'.format(tool_cmd) if '--log-file' in sys.argv else None)
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument('--debug', help='enable debug output', default=False, action='store_true')  # noqa
    parser.add_argument('--log-file', help='write output to a log file', default=False, action='store_true')  # noqa
    parser.add_argument('--project', help='gcp project name', default='localhost')  # noqa
    parser.add_argument('--account', help='pmi-ops account', default=None)  # noqa
    parser.add_argument('--service-account', help='gcp iam service account', required=False)  # noqa
    parser.add_argument('--delete', help="delete schemas from BigQuery", default=False, action='store_true')  # noqa
    parser.add_argument('--show-schemas', help='print schemas to stdout', default=False, action='store_true')  # noqa
    parser.add_argument('--dry-run',
                        help='(tables only) Compare local schema to existing BigQuery table schema without migrating',
                        default=False, action='store_true')  # noqa
    parser.add_argument('--names', help="a comma delimited list of table/view names.",
                        default='all', metavar='[TABLE|VIEW]')  # noqa
    args = parser.parse_args()

    envs = ['localhost', 'pmi-drc-api-test', 'all-of-us-rdr-sandbox', 'all-of-us-rdr-stable', 'all-of-us-rdr-prod']
    if args.project not in envs:
        _logger.warning(f'BigQuery migration not supported for {args.project}, aborting.')
        return 0

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = BQMigration(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == '__main__':
    sys.exit(run())
