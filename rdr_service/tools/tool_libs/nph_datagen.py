#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import csv
import logging
import os
import sys

import faker

from rdr_service import clock
from rdr_service.dao import database_factory
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.rex_dao import RexParticipantMappingDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.study_nph_dao import NphSiteDao
from rdr_service.data_gen.generators.data_generator import DataGenerator
from rdr_service.data_gen.generators.nph import NphDataGenerator
from rdr_service.model.rex import ParticipantMapping
from rdr_service.services.system_utils import setup_logging, setup_i18n

from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.tools.tool_libs.tool_base import ToolBase

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "nph_datagen"
tool_desc = "NPH test data generator tool"


class ParticipantGeneratorTool(ToolBase):

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super().__init__(args, gcp_env)
        self.gcp_env.activate_sql_proxy()

        self.aou_generator = DataGenerator(
            session=database_factory.get_database().make_session(),
            faker=faker.Faker())

        self.nph_generator = NphDataGenerator()

        self.aou_participant_dao = ParticipantDao()

        self.rex_mapping_dao = RexParticipantMappingDao()
        self.code_dao = CodeDao()
        self.rdr_code_sys = "http://terminology.pmi-ops.org/CodeSystem/ppi"

        self.aou_site_dao = SiteDao()
        self.nph_site_dao = NphSiteDao()

    def run(self):
        if self.args.project == 'all-of-us-rdr-prod':
            _logger.error(f'Participant generator cannot be used on project: {self.args.project}')
            return 1

        if self.args.spec_path:
            if not os.path.exists(self.args.spec_path):
                _logger.error(f'File {self.args.spec_path} was not found.')
                return 1

            with open(self.args.spec_path, encoding='utf-8-sig') as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    self.run_participant_creation(row)

            return 0

    def run_participant_creation(self, row):
        # Insert AOU Data
        # Get data from spec
        if int(row['ny_flag']):
            state_code = self.code_dao.get_code(self.rdr_code_sys, "PIIState_NY")
        else:
            state_code = self.code_dao.get_code(self.rdr_code_sys, "PIIState_AZ")

        if row['sex'] == "m":
            sex_code = self.code_dao.get_code(self.rdr_code_sys, "SexAtBirth_Male")
        else:
            sex_code = self.code_dao.get_code(self.rdr_code_sys, "SexAtBirth_Female")

        aou_site = self.aou_site_dao.get_by_google_group(row['aou_site'])

        # Create participant
        aou_participant = self.aou_generator.create_database_participant(
            participantId=self.aou_participant_dao.get_random_id(),
            biobankId=self.aou_participant_dao.get_random_id(),
            researchId=self.aou_participant_dao.get_random_id()
        )

        # Create Summary record
        self.aou_generator.create_database_participant_summary(
            participant=aou_participant,
            stateId=state_code.codeId,
            sexId=sex_code.codeId,
            siteId=aou_site.siteId,
            aian=int(row['aian_flag'])
        )

        # Insert NPH Data
        # NPH Participant
        nph_participant = self.nph_generator.create_database_participant(
            id=row['nph_participant_id'],
            biobank_id=f"1{row['nph_participant_id']}",
            research_id=row['nph_participant_id'],
        )

        nph_site = self.nph_site_dao.get_site_from_external_id(row['nph_site'])

        # NPH Pairing
        self.nph_generator.create_database_pairing_event(
            participant_id=nph_participant.id,
            site_id=nph_site.id,
            event_authored_time=clock.CLOCK.now()
        )

        # NPH Consent
        self.nph_generator.create_database_consent_event(
            participant_id=nph_participant.id,
            event_authored_time=clock.CLOCK.now()
        )

        # Insert Rex Mapping
        rex_mapping_obj = ParticipantMapping(
            primary_study_id=1,
            ancillary_study_id=2,
            primary_participant_id=aou_participant.participantId,
            ancillary_participant_id=nph_participant.id
        )

        self.rex_mapping_dao.insert(rex_mapping_obj)


def get_datagen_process_for_run(args, gcp_env):
    datagen_map = {
        'participant_generator': ParticipantGeneratorTool(args, gcp_env),
    }
    return datagen_map.get(args.process)


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
    parser.add_argument("--account", help="pmi-ops account", default=None)
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa

    subparser = parser.add_subparsers(help='', dest='process')

    participants = subparser.add_parser("participant_generator")
    participants.add_argument("--spec-path", help="path to the request form", default=None)  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        try:
            datagen_process = get_datagen_process_for_run(args, gcp_env)
            exit_code = datagen_process.run()
        # pylint: disable=broad-except
        except Exception as e:
            _logger.info(f'Error has occured, {e}. For help use "nph_datagen --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
