#! /bin/env python
#
# Generate specific fake participant data
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import csv
import datetime
import io
import logging
import os
import random
import sys
import time
import urllib.request, urllib.error, urllib.parse

from rdr_service import clock
from rdr_service.data_gen.generators import (
    BioBankOrderGen,
    CodeBook,
    ParticipantGen,
    PhysicalMeasurementsGen,
    QuestionnaireGen,
    StoredSampleGen,
)
from rdr_service.data_gen.generators.hpo import HPOGen
from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.services.gcp_utils import gcp_get_app_access_token, gcp_get_app_host_name, gcp_make_auth_header
from rdr_service.services.system_utils import make_api_request, setup_logging, setup_i18n

_logger = logging.getLogger("rdr_logger")

tool_cmd = "spec-gen"
tool_desc = "specific participant data generator"


class DataGeneratorClass(object):

    _gen_url = "rdr/v1/SpecDataGen"
    _host = None
    _oauth_token = None

    _cb = None
    _p_gen = None
    _pm_gen = None
    _qn_gen = None
    _bio_gen = None
    _ss_gen = None

    def __init__(self, args, gcp_env):
        self.args = args
        self.gcp_env = gcp_env

        if args:
            self._host = gcp_get_app_host_name(self.args.project)
            if self.args.port:
                self._host = "{0}:{1}".format(self._host, self.args.port)
            else:
                if self._host in ["127.0.0.1", "localhost"]:
                    self._host = "{0}:{1}".format(self._host, 8080)

            if "127.0.0.1" not in self._host and "localhost" not in self._host:
                self._oauth_token = gcp_get_app_access_token()

    def _gdoc_csv_data(self, doc_id, gid):
        """
        Fetch a google doc spreadsheet in CSV format
        :param doc_id: google document id
        :param gid: google doc gid
        :return: A list object with rows from spreadsheet
        """
        url = f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv"
        if gid:
            url += f"&gid={gid}"
        response = urllib.request.urlopen(url)
        if response.code != 200:  # urllib2 already raises urllib2.HTTPError for some of these.
            return None

        csv_data = list()
        data = response.read().decode('utf-8')
        if '<html' in data:
            return None

        # Convert csv file to a list of row data
        with io.StringIO(data) as handle:
            for row in csv.reader(handle, delimiter=','):
                csv_data.append(row)

        return csv_data

    def _local_csv_data(self, filename):
        """
        Read local spreadsheet csv
        :param filename:
        :return:
        """
        if not os.path.exists(filename):
            return None

        csv_data = list()

        # read source spreadsheet into p_data
        with open(filename) as handle:
            reader = csv.reader(handle, delimiter=",")
            for row in reader:
                csv_data.append(row)

        return csv_data

    def _convert_csv_column_to_dict(self, csv_data, column):
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

    def _random_date(self, start=None, max_delta=None):
        """
    Choose a random date for participant start
    :param start: specific start date.
    :param max_delta: maximum delta from start to use for range.
    :return: datetime
    """
        if not start:
            # set a start date in the past and an end date 40 days in the past
            start = datetime.datetime.now() - datetime.timedelta(weeks=102)
        if max_delta:
            end = start + max_delta
            # don't allow future dates.
            if end > datetime.datetime.now():
                end = self._random_date(start, (datetime.datetime.now() - start))
        else:
            end = datetime.datetime.now() - datetime.timedelta(days=40)
            # if our start is close to now(), just use now().
            if end < start:
                end = datetime.datetime.now()

        # convert to floats and add a random amount of time.
        stime = time.mktime(start.timetuple())
        etime = time.mktime(end.timetuple())
        ptime = stime + (random.random() * (etime - stime))

        # convert to datetime
        ts = time.localtime(ptime)
        dt = datetime.datetime.fromtimestamp(time.mktime(ts))
        # Choose a time somewhere in regular business hours, +0:00 timezone.
        dt = dt.replace(hour=int((4 + random.random() * 12)))
        dt = dt.replace(microsecond=int(random.random() * 999999))

        return dt

    def _increment_date(self, dt, minute_range=None, day_range=None):
        """
    Increment the timestamp a bit.
    :param dt: datetime value to use for incrementing.
    :param minute_range: range to choose random minute value from.
    :param day_range: range to choose random day value from.
    :return: datetime
    """
        if minute_range:
            dt += datetime.timedelta(minutes=int(random.random() * minute_range))
        else:
            dt += datetime.timedelta(minutes=int(random.random() * 20))

        if day_range:
            dt += datetime.timedelta(days=int(random.random() * day_range))
            # Choose a time somewhere in regular business hours, +0:00 timezone.
            dt = dt.replace(hour=int((4 + random.random() * 12)))
            dt = dt.replace(microsecond=int(random.random() * 999999))

        return dt

    def create_participant(self, site_id=None, hpo_id=None):
        """
    Create a new participant with a random or specific hpo or site id
    :param site_id: name of specific hpo site
    :param hpo_id: name of hpo
    :return: participant object
    """
        hpo_site = None
        hpo_gen = HPOGen()

        if site_id:
            # if site_id is given, it also returns the HPO the site is matched with.
            hpo_site = hpo_gen.get_site(site_id)
        if hpo_id and not hpo_site:
            # if hpo is given, select a random site within the hpo.
            hpo_site = hpo_gen.get_hpo(hpo_id).get_random_site()
        if not hpo_site:
            # choose a random hpo and site.
            hpo_site = hpo_gen.get_random_site()
        # initialize participant generator.
        if not self._p_gen:
            self._p_gen = ParticipantGen()

        # make a new participant.
        p_obj = self._p_gen.new(hpo_site)

        data = dict()
        data["api"] = "Participant"
        data["data"] = p_obj.to_dict()
        data["timestamp"] = clock.CLOCK.now().isoformat()

        code, resp = make_api_request(
            self._host, self._gen_url, req_type="POST", json_data=data, headers=gcp_make_auth_header()
        )
        if not resp or code not in [200, 201]:
            _logger.error("create participant failure: [Http {0}: {1}].".format(code, resp))
            return None

        p_obj.update(resp)

        if hpo_site.id:
            resp['site'] = hpo_site.id
            headers = gcp_make_auth_header()
            headers['If-Match'] = resp['meta']['versionId']

            data["api"] = f"Participant/{p_obj.participantId}"
            data["data"] = resp
            data["timestamp"] = clock.CLOCK.now().isoformat()
            data["method"] = 'PUT'

            code, resp = make_api_request(
                self._host, self._gen_url, req_type="POST", json_data=data, headers=headers)

        if not resp or code not in [200, 201]:
            _logger.error("update participant failure: [Http {0}: {1}].".format(code, resp))
            return None

        return p_obj, hpo_site



    def submit_physical_measurements(self, participant_id, site):
        """
    Create a physical measurements response for the participant
    :param participant_id: participant id
    :param site: HPOSiteGen object
    :return: True if POST request is successful otherwise False.
    """
        if not self._pm_gen:
            self._pm_gen = PhysicalMeasurementsGen()

        pm_obj = self._pm_gen.new(participant_id, site)

        data = dict()
        data["api"] = "Participant/{0}/PhysicalMeasurements".format(participant_id)
        data["data"] = pm_obj.make_fhir_document()
        # make the submit time a little later than the authored timestamp.
        data["timestamp"] = clock.CLOCK.now().isoformat()

        code, resp = make_api_request(
            self._host, self._gen_url, req_type="POST", json_data=data, headers=gcp_make_auth_header()
        )

        if code == 200:
            pm_obj.update(resp)
            return pm_obj

        _logger.error("physical measurements response failure: [Http {0}: {1}].".format(code, resp))
        return None

    def submit_biobank_order(self, participant_id, sample_test, site, to_mayo=False):
        """
    Create a biobank order response for the participant
    :param participant_id: participant id
    :param sample_test: sample test code
    :param site: HPOSiteGen object
    :param to_mayo: if True, also send order to Mayolink.
    :return: True if POST request is successful otherwise False.
    """
        if not sample_test:
            return None

        if not self._bio_gen:
            self._bio_gen = BioBankOrderGen()

        bio_obj = self._bio_gen.new(participant_id, sample_test, site)

        data = dict()
        data["api"] = "Participant/{0}/BiobankOrder".format(participant_id)
        data["data"], finalized = bio_obj.make_fhir_document()
        # make the submit time a little later than the finalized timestamp.
        data["timestamp"] = self._increment_date(finalized, minute_range=15).isoformat()
        data["mayolink"] = to_mayo

        code, resp = make_api_request(
            self._host, self._gen_url, req_type="POST", json_data=data, headers=gcp_make_auth_header()
        )
        if code == 200:
            bio_obj.update(resp)
            return bio_obj

        _logger.error("biobank order response failure: [Http {0}: {1}].".format(code, resp))
        return None

    def submit_module_response(self, module_id, participant_id, overrides=None):
        """
    Create a questionnaire response for the given module.
    :param module_id: questionnaire module name
    :param participant_id: participant id
    :param overrides: list of tuples giving answers to specific questions.
    :return: True if POST request is successful otherwise False.
    """
        if not module_id or not isinstance(module_id, str):
            raise ValueError("invalid module id.")
        if not participant_id or not isinstance(str(participant_id), str):
            raise ValueError("invalid participant id.")

        if not self._cb:
            # We only want to create these once, because they download data from github.
            self._cb = CodeBook()
            self._qn_gen = QuestionnaireGen(self._cb, self._host)

        qn_obj = self._qn_gen.new(module_id, participant_id, overrides)

        data = dict()
        data["api"] = "Participant/{0}/QuestionnaireResponse".format(participant_id)
        data["data"] = qn_obj.make_fhir_document()
        # make the submit time a little later than the authored timestamp.
        data["timestamp"] = clock.CLOCK.now().isoformat()

        code, resp = make_api_request(
            self._host, self._gen_url, req_type="POST", json_data=data, headers=gcp_make_auth_header()
        )
        if code == 200:
            qn_obj.update(resp)
            return qn_obj

        _logger.error("module response failure: [Http {0}: {1}].".format(code, resp))
        return None

    def run(self):
        """
    Main program process
    :param args: program arguments
    :return: Exit code value
    """
        # load participant spreadsheet from bucket or local file.
        csv_data = self._local_csv_data(self.args.src_csv) or \
                                self._gdoc_csv_data(self.args.src_csv, self.args.google_gid)
        if not csv_data:
            _logger.error("unable to fetch participant source spreadsheet [{0}].".format(self.args.src_csv))
            return 1

        _logger.info("processing source data.")
        count = 0

        # see if we need to rotate the csv data
        if self.args.horiz is True:
            csv_data = list(zip(*csv_data))

        # Loop through each column and generate data.
        for column in range(0, len(csv_data[0]) - 1):

            p_data = self._convert_csv_column_to_dict(csv_data, column)

            hpo = p_data.get("_HPO", None)
            pm = p_data.get("_PM", None)
            site_id = p_data.get("_HPOSite", None)
            bio_orders = p_data.get("_BIOOrder", None)
            bio_orders_mayo = p_data.get("_BIOOrderMayo", None)
            ppi_modules = p_data.get("_PPIModule", "ConsentPII|TheBasics")
            stored_sample = p_data.get("_StoredSample", None)
            # TODO: add genomic member and manifest states

            # choose a random starting date, timestamps of all other activities feed off this value.
            start_dt = self._random_date()
            #
            # Create a new participant
            #
            count += 1
            _logger.info("participant [{0}].".format(count))
            with clock.FakeClock(start_dt):
                p_obj, hpo_site = self.create_participant(site_id=site_id, hpo_id=hpo)

                if not p_obj or "participantId" not in p_obj.__dict__:
                    _logger.error("failed to create participant.")
                    continue

                _logger.info("  created [{0}].".format(p_obj.participantId))
            #
            # process any questionnaire modules
            #
            if ppi_modules:

                # submit the first module pretty close to the start date. Assumes the first
                # module is ConsentPII.
                mod_dt = self._increment_date(start_dt, minute_range=60)

                modules = ppi_modules.split("|")
                for module in modules:
                    with clock.FakeClock(mod_dt):
                        mod_obj = self.submit_module_response(module, p_obj.participantId, list(p_data.items()))
                        if mod_obj:
                            _logger.info("  module: [{0}]: submitted.".format(module))
                        else:
                            _logger.info("  module: [{0}]: failed.".format(module))
                    #
                    # see if we need to submit physical measurements.
                    #
                    if module == "ConsentPII" and pm and pm.lower() == "yes":

                        mod_dt = self._random_date(mod_dt, datetime.timedelta(minutes=90))
                        with clock.FakeClock(mod_dt):
                            pm_obj = self.submit_physical_measurements(p_obj.participantId, hpo_site)
                            if pm_obj:
                                _logger.info("  pm: submitted.")
                            else:
                                _logger.info("  pm: failed.")
                    # choose a new random date between mod_dt and mod_dt + 15 days.
                    mod_dt = self._random_date(mod_dt, datetime.timedelta(days=15))
            #
            # process biobank samples
            #
            if bio_orders:
                sample_dt = self._increment_date(start_dt, day_range=10)

                samples = bio_orders.split("|")
                for sample in samples:
                    with clock.FakeClock(sample_dt):
                        bio_obj = self.submit_biobank_order(p_obj.participantId, sample, hpo_site)
                        if bio_obj:
                            _logger.info("  biobank order: [{0}] submitted.".format(sample))
                        else:
                            _logger.info("  biobank order: [{0}] failed.".format(sample))

                    sample_dt = self._random_date(sample_dt, datetime.timedelta(days=30))
                    # Add stored samples if sample marked as 'yes'
                    if stored_sample.lower().strip() in ["yes", 'y']:
                        self._ss_gen = StoredSampleGen()
                        ss = self._ss_gen.make_stored_sample_for_participant(int(p_obj.participantId[1:]))
                        if ss is not None:
                            _logger.info(f"  stored sample: [{ss.biobankStoredSampleId}] created.")
                        else:
                            _logger.error(f"  stored sample for [{p_obj.participantId}] failed.")
            #
            # process biobank samples that also need to be sent to Mayolink.
            #
            if bio_orders_mayo:
                sample_dt = self._increment_date(start_dt, day_range=10)

                samples = bio_orders_mayo.split("|")
                for sample in samples:
                    with clock.FakeClock(sample_dt):
                        bio_obj = self.submit_biobank_order(p_obj.participantId, sample, hpo_site, to_mayo=True)
                        if bio_obj:
                            _logger.info("  biobank order w/mayo: {0} submitted.".format(sample))
                        else:
                            _logger.info("  biobank order w/mayo: {0} failed.".format(sample))

                    sample_dt = self._random_date(sample_dt, datetime.timedelta(days=30))

                # TODO: Add code for sending orders to mayo here, in a new ticket.

        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="Enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--port", help="alternate ip port to connect to", default=None)  # noqa
    parser.add_argument(
        "--horiz", help="participant data is horizontal in the spreadsheet", default=False, action="store_true"
    )  # noqa
    parser.add_argument("--src-csv", help="participant list csv (file/google doc id)", required=True)  # noqa
    parser.add_argument("--google-gid", help="google doc gid", required=False)  #noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        # verify we're not getting pointed to production.
        if gcp_env.project == "all-of-us-rdr-prod":
            _logger.error("using spec generator in production is not allowed.")
            return 1

        process = DataGeneratorClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
