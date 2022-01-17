#
# MySQL Unit Test Helper
#
# The goal is to setup a MySQL instance once for all tests, reset the database any number of times and
# finally do any clean up when Python is ready to exit.
#
#
#
import atexit
import csv
import inspect
import io
import os
import random
import shlex
import shutil
import signal
import subprocess
import tempfile
from time import sleep
from types import ModuleType
import warnings

from alembic.script import ScriptDirectory
from sqlalchemy import event

from rdr_service import config, model, singletons
from rdr_service.dao import database_factory
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model import compiler  # pylint: disable=unused-import
from rdr_service.model.base import Base
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.site import Site
from rdr_service.model.site_enums import ObsoleteStatus
from rdr_service.participant_enums import OrganizationType, UNSET_HPO_ID
from rdr_service.services.system_utils import find_mysqld_executable, pid_is_running, \
    run_external_program, which
from tests.helpers import temporary_sys_path
from tests.helpers.mysql_helper_data import AZ_HPO_ID, AZ_ORG_ID, PITT_HPO_ID, PITT_ORG_ID

BASE_PATH = '{0}/rdr-mysqld'.format(tempfile.gettempdir())
MYSQL_HOST = "127.0.0.1"  # Do not use 'localhost', we want to force using an IP socket.
MYSQL_PORT = os.getenv('RDR_UNITTEST_SQL_SERVER_PORT', 10010)


def configure_unittest_connection_string():
    db_conn_str = f'mysql+mysqldb://root@{MYSQL_HOST}:{MYSQL_PORT}/?charset=utf8mb4'
    print(f'attempting connection to {db_conn_str}')
    config.override_setting('unittest_db_connection_string', db_conn_str)


def start_mysql_instance():
    """
    Start a new mysqld service instance for running unittests.  If there is
    already one running, do nothing.
    """
    # Check for a running instance of mysqld
    pid_file = os.path.join(BASE_PATH, 'mysqld.pid')
    if os.path.exists(pid_file):
        with open(pid_file, 'r') as handle:
            pid = int(handle.read())
        if pid_is_running(pid):
            return

    if os.path.exists(BASE_PATH):
        shutil.rmtree(BASE_PATH)
    os.mkdir(BASE_PATH)

    mysqld = find_mysqld_executable()
    if not mysqld:
        raise FileNotFoundError('mysqld executable not found')

    code, so, se = run_external_program([which("mysql"), "-V"])
    mariadb_install = (code == 0 and 'MariaDB' in so)

    data_dir = os.path.join(BASE_PATH, 'data')
    log_file = os.path.join(BASE_PATH, 'mysqld.log')
    tmp_dir = os.path.join(BASE_PATH, 'tmp')
    sock_file = os.path.join(BASE_PATH, 'mysqld.sock')
    os.mkdir(tmp_dir)
    if mariadb_install:
        cmd = '{0} --datadir={1} --auth-root-authentication-method=normal'.format(which('mysql_install_db'), data_dir)
    else:
        cmd = '{0} --initialize-insecure --datadir={1}'.format(mysqld, data_dir)

    # Initialize data directory
    code, so, se = run_external_program(shlex.split(cmd))
    if code != 0:
        raise SystemError(se)

    # Start mysqld service
    cmd = '{0} --port={1} --datadir={2} --tmpdir={3} --pid-file={4} --log-error={5} --socket={6} ' \
          '--log-bin-trust-function-creators=1'.format(mysqld, MYSQL_PORT, data_dir, tmp_dir, pid_file, log_file,
                                                       sock_file)
    proc = subprocess.Popen(shlex.split(cmd), stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            stdout=subprocess.DEVNULL, start_new_session=True)

    sleep(1.5)
    if not proc or not pid_is_running(proc.pid):
        raise OSError('new instance of mysqld service did not start.')
    # Register the stop method
    atexit.register(stop_mysql_instance)


initialize = True
table_changed = {}


def get_table_change_listener(table_name):
    def change_listener(*_):
        table_changed[table_name] = True

    return change_listener


def _track_database_changes():
    for module in [member for _, member in inspect.getmembers(model) if isinstance(member, ModuleType)]:
        for _, model_class in inspect.getmembers(module):
            if inspect.isclass(model_class) and issubclass(model_class, Base) and model_class != Base:
                table_name = model_class.__tablename__
                table_changed[table_name] = False

                event.listen(model_class, 'before_insert', get_table_change_listener(table_name))
                event.listen(model_class, 'before_update', get_table_change_listener(table_name))


def clear_table_on_next_reset(table_name):
    table_changed[table_name] = True


def _clear_data(engine):
    engine.execute("set foreign_key_checks = 0")
    for table_name, is_dirty in table_changed.items():
        if is_dirty:
            engine.execute(f'truncate table {table_name}')
            table_changed[table_name] = False

    engine.execute("set foreign_key_checks = 1")


def _initialize_database(with_data=True, with_consent_codes=False):
    """
    Initialize a clean RDR database for unit tests.
    :param with_data: Populate with basic data
    :param with_consent_codes: Populate with consent codes.
    """

    # Set these so the database factory knows to connect to the unittest mysql server instance.
    os.environ["UNITTEST_FLAG"] = "1"
    configure_unittest_connection_string()

    database = database_factory.get_database(db_name=None)
    engine = database.get_engine()

    with engine.begin():
        global initialize
        if initialize:
            engine.execute("DROP DATABASE IF EXISTS rdr")
            engine.execute("DROP DATABASE IF EXISTS metrics")
            engine.execute("DROP DATABASE IF EXISTS cdm")
            # Keep in sync with tools/setup_local_database.sh.
            engine.execute("CREATE DATABASE rdr CHARACTER SET utf8 COLLATE utf8_general_ci")
            engine.execute("CREATE DATABASE metrics CHARACTER SET utf8 COLLATE utf8_general_ci")
            engine.execute("CREATE DATABASE cdm CHARACTER SET utf8 COLLATE utf8_general_ci")

            engine.execute("USE metrics")
            database.create_metrics_schema()

            engine.execute("USE rdr")
            database.create_schema()
            # alter table charset like what db migration do
            engine.execute("ALTER TABLE `questionnaire_response_answer` CONVERT TO CHARACTER SET utf8mb4 COLLATE "
                           "utf8mb4_unicode_ci")
            engine.execute("ALTER TABLE `participant_summary` CONVERT TO CHARACTER SET utf8mb4 COLLATE "
                           "utf8mb4_unicode_ci")
            _run_unit_test_migrations(engine)

            _track_database_changes()
            initialize = False
        else:
            session = database.make_session()
            session.execute('USE rdr')
            _clear_data(session)
            session.commit()
            session.close()

    engine.dispose()
    singletons.reset_for_tests()

    if with_data:
        _setup_hpos()

    if with_consent_codes:
        _setup_consent_codes()


def reset_mysql_instance(with_data=True, with_consent_codes=False):

    # with warnings.catch_warnings():
    #     warnings.simplefilter("ignore", ResourceWarning)
    #     start_mysql_instance()
    # setup the initial database structure
    _initialize_database(with_data, with_consent_codes)

    # TODO: Decide how we are going to reset data in the database.


def _run_unit_test_migrations(engine):
    """
    Load all Views and Functions from Alembic migration files into schema.
    Note: I was able to switch to Alembic to create the full schema, but it was
    4 times as slow as using sqlalchemy. Loading DB Functions and Views this
    way into the schema works much faster.
    :param engine: Database engine object
    """
    with temporary_sys_path('rdr_service'):  # the revision files need to find modules (like model) in rdr_service
        migrations_directory = os.path.join(os.getcwd(), "rdr_service", "alembic")
        migrations_api = ScriptDirectory(migrations_directory)

        # walk_revisions returns revisions in order of newest to oldest,
        # reversing to start with the first and work up to the latest
        for revision in reversed(list(migrations_api.walk_revisions())):
            with warnings.catch_warnings():  # Ignore warnings from 'DROP IF EXISTS' sql statements
                warnings.simplefilter("ignore")

                if hasattr(revision.module, 'unittest_schemas'):
                    for operation in revision.module.unittest_schemas():
                        engine.execute(operation)


def _setup_consent_codes():
    """
    Proactively setup Codebook entries in the Code table so parent/child relationship is
    created. Matches 'test_data/study_consent.json`.
    """
    from rdr_service.model.code import Code, CodeType
    from rdr_service.code_constants import PPI_SYSTEM
    from rdr_service.dao.code_dao import CodeDao
    import datetime

    def create_code(topic, name, code_type, parent):
        code = Code(
            system=PPI_SYSTEM,
            topic=topic,
            value=name,
            display=name,
            codeType=code_type,
            mapped=True,
            shortValue=name,
            created=datetime.datetime.utcnow(),
        )
        if parent:
            parent.children.append(code)

        return code

    with CodeDao(silent=True).session() as session:
        module = create_code("Module Name", "ConsentPII", CodeType.MODULE, None)
        session.add(module)

        topic = create_code("Language", "ConsentPII_Language", CodeType.TOPIC, module)
        session.add(topic)

        qn = create_code("Language", "Language_SpokenWrittenLanguage", CodeType.QUESTION, topic)
        session.add(qn)
        session.add(create_code("Language", "SpokenWrittenLanguage_English", CodeType.ANSWER, qn))
        session.add(create_code("Language", "SpokenWrittenLanguage_ChineseChina", CodeType.ANSWER, qn))
        session.add(create_code("Language", "SpokenWrittenLanguage_French", CodeType.ANSWER, qn))

        topic = create_code("Address", "ConsentPII_PIIAddress", CodeType.TOPIC, module)
        session.add(topic)

        session.add(create_code("Address", "PIIAddress_StreetAddress", CodeType.QUESTION, topic))
        session.add(create_code("Address", "PIIAddress_StreetAddress2", CodeType.QUESTION, topic))

        topic = create_code("Name", "ConsentPII_PIIName", CodeType.TOPIC, module)
        session.add(topic)

        session.add(create_code("Name", "PIIName_First", CodeType.QUESTION, topic))
        session.add(create_code("Name", "PIIName_Middle", CodeType.QUESTION, topic))
        session.add(create_code("Name", "PIIName_Last", CodeType.QUESTION, topic))

        session.add(create_code("Email Address", "ConsentPII_EmailAddress", CodeType.QUESTION, module))

        topic = create_code("Extra Consent Items", "ConsentPII_ExtraConsent", CodeType.TOPIC, module)
        session.add(create_code("Extra Consent Items", "ExtraConsent_CABoRSignature", CodeType.QUESTION, topic))

        module = create_code("Module Name", "OverallHealth", CodeType.MODULE, None)
        session.add(module)

        session.commit()


def _convert_csv_column_to_dict(csv_data, column):
    """
    Return a dictionary object with keys from the first column and values from the specified
    column.
    :param csv_data: File-like CSV text downloaded from Google spreadsheets. (See main doc.)
    :return: dict of fields and values for given column
    """
    results = dict()

    for row in csv_data:
        key = row[0]
        data = row[1:][column]

        if data:
            if key not in results:
                results[key] = data.strip() if data else ""
            else:
                # append multiple choice questions
                results[key] += "|{0}".format(data.strip())

    return results


def _prep_awardee_csv_data(filename):
    """

    :param filename: csv file to load and transform.
    :return: dict
    """
    csv_data = list()
    with open(filename, 'rb') as h:
        data = h.read().decode('utf-8')

    # Convert csv file to a list of row data
    with io.StringIO(data) as handle:
        for row in csv.reader(handle, delimiter=','):
            csv_data.append(row)

    # Pivot the csv data
    csv_data = list(zip(*csv_data))
    return csv_data


def _setup_all_awardees():
    """
    Import all the awardee data found in tests/test-data/fixtures.
    """
    hpo_data = _prep_awardee_csv_data('tests/test-data/fixtures/awardees.csv')
    org_data = _prep_awardee_csv_data('tests/test-data/fixtures/organizations.csv')
    site_data = _prep_awardee_csv_data('tests/test-data/fixtures/sites.csv')
    dao = HPODao()
    #
    # Import HPO records
    #
    for column in range(0, len(hpo_data[0]) - 1):
        data = _convert_csv_column_to_dict(hpo_data, column)
        dao.insert(HPO(hpoId=column+1, displayName=data['Name'], name=data['Awardee ID'],
                       organizationType=OrganizationType(data['Type']), isObsolete=ObsoleteStatus.ACTIVE))
    #
    # Import Organization records
    #
    with dao.session() as session:
        for column in range(0, len(org_data[0]) - 1):
            data = _convert_csv_column_to_dict(org_data, column)
            result = session.query(HPO.hpoId).filter(HPO.name == data['Awardee ID']).first()
            dao.insert(Organization(externalId=data['Organization ID'], displayName=data['Name'], hpoId=result.hpoId))
    #
    # Import Site records
    #
    with dao.session() as session:
        for column in range(0, len(site_data[0]) - 1):
            data = _convert_csv_column_to_dict(site_data, column)
            result = session.query(Organization.hpoId, Organization.organizationId).\
                            filter(Organization.externalId == data['Organization ID']).first()
            try:
                mayo_link_id = data['MayoLINK Client #']
            except KeyError:
                mayo_link_id = str(random.randint(7040000, 7999999))
            dao.insert(Site(siteName=data['Site'], googleGroup=data['Site ID / Google Group'].lower(),
                        mayolinkClientNumber=mayo_link_id, hpoId=result.hpoId,
                        organizationId=result.organizationId))


def _setup_hpos():
    """
    Insert a basic set of Organizational data into the database.
    """
    hpo_dao = HPODao()
    hpo_dao.insert(
        HPO(hpoId=UNSET_HPO_ID, name="UNSET", displayName="Unset", organizationType=OrganizationType.UNSET)
    )
    hpo_dao.insert(
        HPO(hpoId=PITT_HPO_ID, name="PITT", displayName="Pittsburgh", organizationType=OrganizationType.HPO)
    )
    hpo_dao.insert(
        HPO(hpoId=AZ_HPO_ID, name="AZ_TUCSON", displayName="Arizona", organizationType=OrganizationType.HPO)
    )

    org_dao = OrganizationDao()
    org_dao.insert(
        Organization(
            organizationId=AZ_ORG_ID,
            externalId="AZ_TUCSON_BANNER_HEALTH",
            displayName="Banner Health",
            hpoId=AZ_HPO_ID,
        )
    )
    org_dao.insert(
        Organization(
            organizationId=PITT_ORG_ID,
            externalId="PITT_BANNER_HEALTH",
            displayName="PITT display Banner Health",
            hpoId=PITT_HPO_ID,
        )
    )

    site_dao = SiteDao()
    site_dao.insert(
        Site(
            siteName="Monroeville Urgent Care Center",
            googleGroup="hpo-site-monroeville",
            mayolinkClientNumber=7035769,
            organizationId=PITT_ORG_ID,
            hpoId=PITT_HPO_ID,
        )
    )
    site_dao.insert(
        Site(
            siteName="Phoenix Urgent Care Center",
            googleGroup="hpo-site-bannerphoenix",
            mayolinkClientNumber=7035770,
            organizationId=PITT_ORG_ID,
            hpoId=PITT_HPO_ID,
        )
    )
    site_dao.insert(
        Site(
            siteName="Phoenix clinic",
            googleGroup="hpo-site-clinic-phoenix",
            mayolinkClientNumber=7035770,
            organizationId=AZ_ORG_ID,
            hpoId=AZ_HPO_ID,
        )
    )


def stop_mysql_instance():
    """
    Stop the rdr unittest mysqld service instance.
    """
    pid_file = os.path.join(BASE_PATH, 'mysqld.pid')
    if os.path.exists(pid_file):
        with open(pid_file, 'r') as handle:
            pid = int(handle.read())
        if pid_is_running(pid):
            os.kill(pid, signal.SIGTERM)


