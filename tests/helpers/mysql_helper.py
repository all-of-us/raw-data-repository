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
import importlib
import io
import os
import random
import re
import shlex
import shutil
import signal
import subprocess
import tempfile
import warnings
from glob import glob
from time import sleep

from rdr_service import config
from rdr_service import singletons
from rdr_service.dao import database_factory
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model import compiler  # pylint: disable=unused-import
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.site import Site
from rdr_service.model.site_enums import ObsoleteStatus
from rdr_service.participant_enums import UNSET_HPO_ID, OrganizationType
from rdr_service.services.system_utils import find_mysqld_executable, pid_is_running, which, \
    run_external_program
from tests.helpers import temporary_sys_path
from tests.helpers.mysql_helper_data import PITT_HPO_ID, PITT_ORG_ID, AZ_HPO_ID, AZ_ORG_ID

BASE_PATH = '{0}/rdr-mysqld'.format(tempfile.gettempdir())
MYSQL_PORT = 9306


def start_mysql_instance():
    """
    Start a new mysqld service instance for running unittests.  If there is
    already one running, do nothing.
    """
    # Check for a running instance of mysqld
    # pids = get_process_pids(['/mysqld', '--port=9306', '--basedir={0}'.format(BASE_PATH)])
    # if len(pids) == 0:
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


def _initialize_database(with_data=True, with_consent_codes=False):
    """
    Initialize a clean RDR database for unit tests.
    :param with_data: Populate with basic data
    :param with_consent_codes: Populate with consent codes.
    """
    # Set this so the database factory knows to use the unittest connection string from the config.
    os.environ["UNITTEST_FLAG"] = "1"
    mysql_host = "127.0.0.1"  # Do not use 'localhost', we want to force using an IP socket.

    if "CIRCLECI" in os.environ:
        # Default no-pw login, according to https://circleci.com/docs/1.0/manually/#databases .
        mysql_login = "root"
    else:
        mysql_login = "root"

    database = database_factory.get_database(db_name=None)
    engine = database.get_engine()

    with engine.begin():
        engine.execute("DROP DATABASE IF EXISTS rdr")
        engine.execute("DROP DATABASE IF EXISTS metrics")
        # Keep in sync with tools/setup_local_database.sh.
        engine.execute("CREATE DATABASE rdr CHARACTER SET utf8 COLLATE utf8_general_ci")
        engine.execute("CREATE DATABASE metrics CHARACTER SET utf8 COLLATE utf8_general_ci")

        engine.execute("USE metrics")
        database.create_metrics_schema()

        engine.execute("USE rdr")
        database.create_schema()
        _load_views_and_functions(engine)


    engine.dispose()
    database = None
    singletons.reset_for_tests()

    db_conn_str = "mysql+mysqldb://{0}@{1}:{2}/rdr?charset=utf8".format(mysql_login, mysql_host, MYSQL_PORT)
    config.override_setting('unittest_db_connection_string', db_conn_str)

    if with_data:
        _setup_hpos()

    if with_consent_codes:
        _setup_consent_codes()


def reset_mysql_instance(with_data=True, with_consent_codes=False):

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ResourceWarning)
        start_mysql_instance()
    # setup the initial database structure
    _initialize_database(with_data, with_consent_codes)

    # TODO: Decide how we are going to reset data in the database.


def _load_views_and_functions(engine):
    """
    Load all Views and Functions from Alembic migration files into schema.
    Note: I was able to switch to Alembic to create the full schema, but it was
    4 times as slow as using sqlalchemy. Loading DB Functions and Views this
    way into the schema works much faster.
    :param engine: Database engine object
    """
    alembic_path = os.path.join(os.getcwd(), "rdr_service", "alembic", "versions")
    if not os.path.exists(alembic_path):
        raise OSError("alembic migrations path not found.")

    migrations = glob(os.path.join(alembic_path, "*.py"))

    steps = list()
    initial = None


    # Load all the migration step files into a unsorted list of tuples.
    # ( current_step, prev_step, has unittest func )
    for migration in migrations:
        module = os.path.basename(migration)
        rev = module.split("_")[0]
        prev_rev = None

        #contents = open(migration).read()
        with open(migration) as f:
            contents = f.read()

        result = re.search("^down_revision = ['|\"](.*?)['|\"]", contents, re.MULTILINE)
        if result:
            prev_rev = result.group(1)
        else:
            initial = (module, rev, None, False, 0)
        # Look for unittest functions in migration file
        result = re.search("^def unittest_schemas", contents, re.MULTILINE)

        steps.append((module, rev, prev_rev, (not result is None)))

    # Put the migration steps in order
    ord_steps = list()
    ord_steps.append(initial)

    def _find_next_step(c):
        for s in steps:
            if c[1] == s[2]:
                return s
        return None

    c_step = initial
    while c_step:
        n_step = _find_next_step(c_step)
        c_step = n_step
        if n_step:
            ord_steps.append(n_step)

    # Ignore warnings from 'DROP IF EXISTS' sql statements
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        # Load any schemas marked with unittests in order.
        for step in ord_steps:
            # Skip non-unittest enabled migrations
            if step[3] is False:
                continue

            # https://stackoverflow.com/questions/67631/how-to-import-a-module-given-the-full-path
            with temporary_sys_path(alembic_path):
                mod = importlib.import_module(step[0].replace(".py", ""))

            items = mod.unittest_schemas()

            for item in items:
                engine.execute(item)

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


