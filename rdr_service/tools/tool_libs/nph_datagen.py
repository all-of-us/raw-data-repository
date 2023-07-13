#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import csv
import logging
import os
import sys
from typing import Dict, Any, Union

import faker

from rdr_service import clock
from rdr_service.dao import database_factory
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.rex_dao import RexParticipantMappingDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.study_nph_dao import NphSiteDao, NphConsentEventTypeDao, NphEnrollmentEventTypeDao
from rdr_service.data_gen.generators.data_generator import DataGenerator
from rdr_service.data_gen.generators.nph import NphDataGenerator
from rdr_service.model.rex import ParticipantMapping
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.services.system_utils import setup_logging, setup_i18n

from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.tools.tool_libs.tool_base import ToolBase

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "nph_datagen"
tool_desc = "NPH test data generator tool"


class ParticipantGeneratorTool(ToolBase):
    """
    Creates NPH participants based on an input CSV
    Required fields in CSV:
        nph_participant_id
        ny_flag (Y/N)
        sex (M/F)
        aian_flag (Y/N)
        tissue_optin (Y/N)
        aou_site: ex. hpo-site-bannerphoenix
        nph_site: ex. nph-site-nphpbrcbatonrouge
        consent_module: ex. Module 2
        nph_enrollment_status: ex. module2_eligibilityConfirmed
    """

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
        self.consent_event_type_dao = NphConsentEventTypeDao()
        self.enrollment_event_type_dao = NphEnrollmentEventTypeDao()

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

    def run_participant_creation(self, row: Dict[str, Union[str, Any]]):
        # Insert AOU Data
        # Get data from spec
        if row['ny_flag'].upper() == "Y":
            state_code = self.code_dao.get_code(self.rdr_code_sys, "PIIState_NY")
        else:
            state_code = self.code_dao.get_code(self.rdr_code_sys, "PIIState_AZ")

        if row['sex'].upper() == "M":
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
            aian=1 if row['aian_flag'].upper() == "Y" else 0,
            email=f'nph.{aou_participant.participantId}@test.com',
            phoneNumber=f"5{aou_participant.participantId}",
            questionnaireOnTheBasics=QuestionnaireStatus.SUBMITTED,
            questionnaireOnTheBasicsTime=clock.CLOCK.now(),
            questionnaireOnTheBasicsAuthored=clock.CLOCK.now(),
            questionnaireOnOverallHealth=QuestionnaireStatus.SUBMITTED,
            questionnaireOnOverallHealthTime=clock.CLOCK.now(),
            questionnaireOnOverallHealthAuthored=clock.CLOCK.now(),
            questionnaireOnLifestyle=QuestionnaireStatus.SUBMITTED,
            questionnaireOnLifestyleTime=clock.CLOCK.now(),
            questionnaireOnLifestyleAuthored=clock.CLOCK.now(),
            questionnaireOnSocialDeterminantsOfHealth=QuestionnaireStatus.SUBMITTED,
            questionnaireOnSocialDeterminantsOfHealthTime=clock.CLOCK.now(),
            questionnaireOnSocialDeterminantsOfHealthAuthored=clock.CLOCK.now(),
        )

        # Insert NPH Data
        # NPH Participant
        nph_participant = self.nph_generator.create_database_participant(
            id=row['nph_participant_id'],
            biobank_id=f"1{row['nph_participant_id'][4:]}",
            research_id=row['nph_participant_id'][4:],
        )

        nph_site = self.nph_site_dao.get_site_from_external_id(row['nph_site'])

        # NPH Pairing
        self.nph_generator.create_database_pairing_event(
            participant_id=nph_participant.id,
            site_id=nph_site.id,
            event_authored_time=clock.CLOCK.now()
        )

        # NPH Consent (module 1)
        self.nph_generator.create_database_consent_event(
            participant_id=nph_participant.id,
            event_authored_time=clock.CLOCK.now()
        )

        # Consent, other Modules
        if row['consent_module'] != 'Module 1':
            consent_event_type = self.consent_event_type_dao.get_from_name(f"{row['consent_module']} Consent")
            self.nph_generator.create_database_consent_event(
                participant_id=nph_participant.id,
                event_authored_time=clock.CLOCK.now(),
                event_type_id=consent_event_type.id
            )

        # Consent, tissue
        if row['tissue_optin'].upper() == 'Y':
            consent_event_type = self.consent_event_type_dao.get_from_name(f"Module 1 Consent Tissue")
            self.nph_generator.create_database_consent_event(
                participant_id=nph_participant.id,
                event_authored_time=clock.CLOCK.now(),
                event_type_id=consent_event_type.id,
                opt_in=1
            )

        # NPH Enrollment Event REFERRED
        self.nph_generator.create_database_enrollment_event(
            participant_id=nph_participant.id,
            event_authored_time=clock.CLOCK.now()
        )

        if row['nph_enrollment_status'] != 'module1_referred':
            enrollment_event_type = self.enrollment_event_type_dao.get_from_source_name(row['nph_enrollment_status'])
            self.nph_generator.create_database_enrollment_event(
                participant_id=nph_participant.id,
                event_authored_time=clock.CLOCK.now(),
                event_type_id=enrollment_event_type.id

            )

        # Insert Rex Mapping
        rex_mapping_obj = ParticipantMapping(
            primary_study_id=1,
            ancillary_study_id=2,
            primary_participant_id=aou_participant.participantId,
            ancillary_participant_id=nph_participant.id
        )

        self.rex_mapping_dao.insert(rex_mapping_obj)
        _logger.info(f'Participant Created (aou, nph): {aou_participant.participantId}, {nph_participant.id}')


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
