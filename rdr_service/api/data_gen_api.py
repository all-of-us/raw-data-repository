import csv
import datetime
import json
import logging
import random

from dateutil.parser import parse
from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest, Forbidden

from rdr_service import app_util, config, clock
from rdr_service.api_util import HEALTHPRO, open_cloud_file
from rdr_service.app_util import get_validated_user_info, nonprod
from rdr_service.code_constants import BIOBANK_TESTS
from rdr_service.config import GAE_PROJECT
from rdr_service.config_api import is_config_admin
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.data_gen.fake_participant_generator import FakeParticipantGenerator
from rdr_service.data_gen.in_process_client import InProcessClient
from rdr_service.model.config_utils import to_client_biobank_id
from rdr_service.offline.biobank_samples_pipeline import CsvColumns, INPUT_CSV_TIME_FORMAT
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask

# 10% of individual stored samples are missing by default.
# 1% of participants have samples with no associated order
_SAMPLES_MISSING_FRACTION = 0.1

_PARTICIPANTS_WITH_ORPHAN_SAMPLES = 0.01
# Max amount of time between collected ordered samples and confirmed biobank stored samples.
_MAX_MINUTES_BETWEEN_SAMPLE_COLLECTED_AND_CONFIRMED = 72 * 60
# Max amount of time between confirmed samples and disposal.
_MAX_MINUTES_BETWEEN_SAMPLE_CONFIRMED_AND_DISPOSED = 72 * 60
# Max amount of time between creating a participant and orphaned biobank samples
_MAX_MINUTES_BETWEEN_PARTICIPANT_CREATED_AND_CONFIRMED = 30 * 24 * 60

_TIME_FORMAT = "%Y/%m/%d %H:%M:%S"
_BATCH_SIZE = 1000

# _GET_ORDERED_SAMPLES_SQL = """
#  SELECT participant.biobank_id, sample.collected, sample.test
#    FROM participant, biobank_ordered_sample sample, biobank_order order
#   WHERE participant.participant_id = biobank_order.participant_id
#     AND biobank_ordered_sample.biobank_order_id = biobank_order.biobank_order_id
#     AND participant.biobank_id %% 100 > %s
# """ % _NO_SAMPLES_FOR_ORDER

def _auth_required_healthpro_or_config_admin(func):
    """A decorator that checks that the caller is a config admin for the app."""

    def wrapped(*args, **kwargs):
        if not is_config_admin(app_util.get_oauth_id()):
            _, user_info = get_validated_user_info()
            if not HEALTHPRO in user_info.get("roles", []):
                logging.warning("User has roles {}, but HEALTHPRO or admin is required".format(user_info.get("roles")))
                raise Forbidden()
        return func(*args, **kwargs)

    return wrapped

def _new_row(sample_id, biobank_id, test, confirmed_time):
    row = []
    disposed_time = confirmed_time + datetime.timedelta(
        minutes=random.randint(0, _MAX_MINUTES_BETWEEN_SAMPLE_CONFIRMED_AND_DISPOSED)
    )
    for col in CsvColumns.ALL:
        if col == CsvColumns.SAMPLE_ID:
            row.append(sample_id)
        elif col == CsvColumns.PARENT_ID:
            row.append(None)
        elif col == CsvColumns.CONFIRMED_DATE:
            row.append(confirmed_time.strftime(_TIME_FORMAT))
        elif col == CsvColumns.EXTERNAL_PARTICIPANT_ID:
            row.append(to_client_biobank_id(biobank_id))
        elif col == CsvColumns.BIOBANK_ORDER_IDENTIFIER:
            row.append("KIT")
        elif col == CsvColumns.TEST_CODE:
            row.append(test)
        elif col == CsvColumns.CREATE_DATE:
            row.append(confirmed_time.strftime(_TIME_FORMAT))
        elif col == CsvColumns.STATUS:
            # TODO: Do we want a distribution of statuses here?
            row.append("consumed")
        elif col == CsvColumns.DISPOSAL_DATE:
            row.append(disposed_time.strftime(_TIME_FORMAT))
        elif col == CsvColumns.SAMPLE_FAMILY:
            # TODO: Is there a need for a more realistic value here?
            row.append("family_id")
        else:
            raise ValueError("unsupported biobank CSV column: '{}'".format(col))
    return row


def generate_samples_task(fraction_missing):
    """
    Cloud Task: Creates fake sample CSV data in GCS.
    :param fraction_missing:
    """
    bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)
    now = clock.CLOCK.now()
    file_name = "/%s/fake_%s.csv" % (bucket_name, now.strftime(INPUT_CSV_TIME_FORMAT))
    num_rows = 0
    sample_id_start = random.randint(1000000, 10000000)

    with open_cloud_file(file_name, mode='w') as dest:
        writer = csv.writer(dest, delimiter="\t")
        writer.writerow(CsvColumns.ALL)

        # Generate orders that we were expecting.
        biobank_order_dao = BiobankOrderDao()
        with biobank_order_dao.session() as session:
            rows = biobank_order_dao.get_ordered_samples_sample(session, 1 - fraction_missing, _BATCH_SIZE)
            for biobank_id, collected_time, test in rows:
                if collected_time is None:
                    logging.warning(f"biobank_id={biobank_id} test={test} skipped (collected={collected_time})")
                    continue
                sample_id = sample_id_start + num_rows
                minutes_delta = random.randint(0, _MAX_MINUTES_BETWEEN_SAMPLE_COLLECTED_AND_CONFIRMED)
                confirmed_time = collected_time + datetime.timedelta(minutes=minutes_delta)
                writer.writerow(_new_row(sample_id, biobank_id, test, confirmed_time))
                num_rows += 1

        # Generate some orders that we weren't expecting.
        participant_dao = ParticipantDao()
        with participant_dao.session() as session:
            rows = participant_dao.get_biobank_ids_sample(session, _PARTICIPANTS_WITH_ORPHAN_SAMPLES, _BATCH_SIZE)
            for biobank_id, sign_up_time in rows:
                minutes_delta = random.randint(0, _MAX_MINUTES_BETWEEN_PARTICIPANT_CREATED_AND_CONFIRMED)
                confirmed_time = sign_up_time + datetime.timedelta(minutes=minutes_delta)
                tests = random.sample(BIOBANK_TESTS, random.randint(1, len(BIOBANK_TESTS)))
                for test in tests:
                    sample_id = sample_id_start + num_rows
                    writer.writerow(_new_row(sample_id, biobank_id, test, confirmed_time))
                    num_rows += 1

    logging.info(f"Generated {num_rows} samples in {file_name}.")


class DataGenApi(Resource):

    method_decorators = [_auth_required_healthpro_or_config_admin]
    _task = GCPCloudTask()

    @nonprod
    def post(self):
        resource = request.get_data()
        resource_json = json.loads(resource)
        num_participants = int(resource_json.get("num_participants", 0))
        include_physical_measurements = bool(resource_json.get("include_physical_measurements", False))
        include_biobank_orders = bool(resource_json.get("include_biobank_orders", False))
        requested_hpo = resource_json.get("hpo", None)
        if num_participants > 0:
            participant_generator = FakeParticipantGenerator(InProcessClient(
                headers={'Authorization': request.headers.get('Authorization', '')}
            ))
            for _ in range(0, num_participants):
                participant_generator.generate_participant(
                    include_physical_measurements, include_biobank_orders, requested_hpo
                )
        if resource_json.get("create_biobank_samples"):
            fraction = resource_json.get("samples_missing_fraction", _SAMPLES_MISSING_FRACTION)
            if GAE_PROJECT == 'localhost':
                generate_samples_task(fraction)
            else:
                params = {'fraction': fraction}
                self._task.execute('generate_bio_samples_task', payload=params)

    @nonprod
    def put(self):
        resource = request.get_data()
        p_id = json.loads(resource)
        participant_generator = FakeParticipantGenerator(InProcessClient(
            headers={'Authorization': request.headers.get('Authorization', '')}
        ), withdrawn_percent=0, suspended_percent=0)

        participant_generator.add_pm_and_biospecimens_to_participants(p_id)


class SpecDataGenApi(Resource):
    """
  API for creating specific fake participant data. Only works with one fake
  participant at a time.
  """

    @nonprod
    def post(self):
        req = json.loads(request.get_data())

        target = req.get("api", None)
        data = req.get("data", None)
        timestamp = req.get("timestamp", None)
        method = req.get("method", "POST")

        if method not in ["POST", "PUT", "GET", "PATCH"]:
            raise BadRequest({"status": "error", "error": "target method invalid"})
        if timestamp:
            timestamp = parse(timestamp)
        if not target:
            raise BadRequest({"status": "error", "error": "target api invalid"})

        result = InProcessClient().request_json(target, method, body=data, pretend_date=timestamp,
                                                headers={'Authorization': request.headers.get('Authorization', ''),
                                                         'If-Match': request.headers.get('If-Match', '')})
        return result
