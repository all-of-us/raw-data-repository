#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import csv
import logging
import os
import sys

from rdr_service.dao.genomic_datagen_dao import GenomicDataGenRunDao
from rdr_service.services.genomic_datagen import ParticipantGenerator, GeneratorOutputTemplate
from rdr_service.services.system_utils import setup_logging, setup_i18n

from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.tools.tool_libs.tool_base import ToolBase

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "genomic_datagen"
tool_desc = ""


class ParticipantGeneratorTool(ToolBase):

    def run(self):
        if self.args.project == 'all-of-us-rdr-prod':
            _logger.error(f'Participant generator cannot be used on project: {self.args.project}')
            return 1

        self.gcp_env.activate_sql_proxy()
        datagen_run_dao = GenomicDataGenRunDao()

        def _generate_file_name():
            pass

        def _build_external_values(row_dict):
            excluded_keys = ['participant_count', 'template_name']
            cleaned_row = dict(zip([key for key in row_dict if key not in excluded_keys], row_dict.values()))
            return cleaned_row

        if self.args.output_only_run_id:

            template_output = GeneratorOutputTemplate(
                output_template_name=self.args.output_template_name,
                output_run_id=self.args.output_only_run_id
            )
            generator_output = template_output.run_output_creation()

            output_local_csv(
                filename=_generate_file_name(),
                data=generator_output['data']
            )
            return 0  # bypass generator

        if self.args.output_only_sample_ids:

            samples_id_list = []
            for sample in self.args.output_only_sample_ids.split(','):
                samples_id_list.append(sample.strip())

            template_output = GeneratorOutputTemplate(
                output_template_name=self.args.output_template_name,
                output_sample_ids=samples_id_list
            )
            generator_output = template_output.run_output_creation()

            output_local_csv(
                filename=_generate_file_name(),
                data=generator_output['data']
            )

            return 0  # bypass generator

        if self.args.spec_path:

            if not os.path.exists(self.args.spec_path):
                _logger.error(f'File {self.args.spec_path} was not found.')
                return 1

            with ParticipantGenerator() as participant_generator:
                with open(self.args.spec_path, encoding='utf-8-sig') as file:
                    csv_reader = csv.DictReader(file)
                    for row in csv_reader:
                        participant_generator.run_participant_creation(
                            num_participants=row['participant_count'],
                            template_type=row['template_name'],
                            external_values=_build_external_values(row)
                        )

            template_output = GeneratorOutputTemplate(
                output_template_name=self.args.output_template_name,
                output_run_id=datagen_run_dao.get_max_run_id()[0]
            )
            generator_output = template_output.run_output_creation()

            output_local_csv(
                filename=_generate_file_name(),
                data=generator_output['data']
            )
            return 0


def output_local_csv(*, filename, data):
    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[k for k in data[0]])
        writer.writeheader()
        writer.writerows(data)

    _logger.info(f'Generated output template csv: {os.getcwd()}/{filename}')


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
    participants.add_argument("--output-only-run-id", help="outputs only members associated with run id in "
                                                           "datagen_run table", default=None)  # noqa
    participants.add_argument("--output-only-sample-ids", help="outputs only members with sample ids attached to "
                                                               "members in the datagen_member_run table",
                              default=None)  # noqa
    participants.add_argument("--spec-path", help="path to the request form", default=None)  # noqa
    participants.add_argument("--test-project", help="type of project being tested ie. 'cvl'", default='cvl',
                              required=True)  # noqa
    participants.add_argument("--output-template-name", help="template name for output type, "
                                                             "specified in datagen_output_template",
                              default='default', required=True)  # noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        try:
            datagen_process = get_datagen_process_for_run(args, gcp_env)
            exit_code = datagen_process.run()
        # pylint: disable=broad-except
        except Exception as e:
            _logger.info(f'Error has occured, {e}. For help use "genomic --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
