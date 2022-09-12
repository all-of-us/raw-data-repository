#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import csv
import logging
import os
import sys

from rdr_service import clock
from rdr_service.dao.genomic_datagen_dao import GenomicDataGenRunDao
from rdr_service.services.genomic_datagen import ParticipantGenerator, GeneratorOutputTemplate, ManifestGenerator
from rdr_service.services.system_utils import setup_logging, setup_i18n

from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.tools.tool_libs.tool_base import ToolBase

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "genomic_datagen"
tool_desc = "Genomic participant/manifest generator tool"


class ParticipantGeneratorTool(ToolBase):

    def run(self):
        if self.args.project == 'all-of-us-rdr-prod':
            _logger.error(f'Participant generator cannot be used on project: {self.args.project}')
            return 1

        self.gcp_env.activate_sql_proxy()

        now_formatted = clock.CLOCK.now().strftime("%Y-%m-%d-%H-%M-%S")
        datagen_run_dao = GenomicDataGenRunDao()

        def _build_external_values(row_dict):
            excluded_keys = ['participant_count', 'end_to_end_start', 'template_name']
            for key in excluded_keys:
                del row_dict[key]
            for key, value in row_dict.items():
                if value.isnumeric():
                    row_dict[key] = int(value)
            return row_dict

        if self.args.output_only_run_id:

            template_output = GeneratorOutputTemplate(
                output_template_name=self.args.output_template_name,
                output_run_id=self.args.output_only_run_id
            )
            generator_output = template_output.run_output_creation()

            file_name = f'datagen_run_id_{self.args.output_only_run_id}_{now_formatted}.csv'

            output_local_csv(
                filename=file_name,
                data=generator_output
            )

            output_path = f'{os.getcwd()}/{file_name}'
            _logger.info("File Created: " + output_path)

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

            file_name = f'datagen_sample_ids_{now_formatted}.csv'
            output_local_csv(
                filename=file_name,
                data=generator_output
            )

            output_path = f'{os.getcwd()}/{file_name}'
            _logger.info("File Created: " + output_path)

            return 0  # bypass generator

        if self.args.spec_path:
            if not os.path.exists(self.args.spec_path):
                _logger.error(f'File {self.args.spec_path} was not found.')
                return 1

            with ParticipantGenerator(
                logger=_logger
            ) as participant_generator:
                with open(self.args.spec_path, encoding='utf-8-sig') as file:
                    csv_reader = csv.DictReader(file)
                    for row in csv_reader:
                        participant_generator.run_participant_creation(
                            num_participants=int(row['participant_count']),
                            template_type=row['template_name'],
                            external_values=_build_external_values(row)
                        )

            current_run_id = datagen_run_dao.get_max_run_id()[0]

            template_output = GeneratorOutputTemplate(
                output_template_name=self.args.output_template_name,
                output_run_id=current_run_id
            )
            generator_output = template_output.run_output_creation()

            file_name = f'datagen_run_id_{current_run_id}_{now_formatted}.csv'
            output_local_csv(
                filename=file_name,
                data=generator_output
            )

            output_path = f'{os.getcwd()}/{file_name}'
            _logger.info("File Created: " + output_path)

            return 0


class ManifestGeneratorTool(ToolBase):

    def run(self):
        if self.args.project == 'all-of-us-rdr-prod':
            _logger.error(f'Manifest generator cannot be used on project: {self.args.project}')
            return 1

        self.gcp_env.activate_sql_proxy()
        server_config = self.get_server_config()

        manifest_params = {
            "template_name": None,
            "sample_ids": None,
            "cvl_site_id": None,
            "update_samples": self.args.update_samples,
            "logger": _logger,
        }

        if self.args.manifest_template:
            manifest_params["template_name"] = self.args.manifest_template

        if self.args.sample_id_file:
            if not os.path.exists(self.args.sample_id_file):
                _logger.error(f'File {self.args.sample_id_file} was not found.')
                return 1

            with open(self.args.sample_id_file, encoding='utf-8-sig') as file:
                csv_reader = csv.reader(file)
                sample_ids = []

                for row in csv_reader:
                    sample_ids.append(row[0])

                manifest_params["sample_ids"] = sample_ids

        if self.args.cvl_site_id:
            manifest_params["cvl_site_id"] = self.args.cvl_site_id

        if server_config.get('biobank_id_prefix'):
            manifest_params['biobank_id_prefix'] = server_config.get('biobank_id_prefix')[0]

        # Execute the manifest generator process or the job controller
        with ManifestGenerator(**manifest_params) as manifest_generator:
            _logger.info("Running Manifest Generator...")
            results = manifest_generator.generate_manifest_data()
            _logger.info(results['status'])
            _logger.info(results['message'])

            if results['manifest_data']:

                if self.args.output_manifest_directory:
                    output_path = self.args.output_manifest_directory + "/"
                else:
                    output_path = os.getcwd() + "/"

                if self.args.output_manifest_filename:
                    output_path += self.args.output_manifest_filename
                else:
                    output_path += results['output_filename']

                _logger.info("Output path: " + output_path)

                # write file
                output_local_csv(output_path, results['manifest_data'])

                _logger.info("File Created: " + output_path)

                return 0

            return 1


def output_local_csv(filename, data):
    # Create output path if it doesn't exist
    if os.path.dirname(filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[k for k in data[0]])
        writer.writeheader()
        writer.writerows(data)


def get_datagen_process_for_run(args, gcp_env):
    datagen_map = {
        'participant_generator': ParticipantGeneratorTool(args, gcp_env),
        'manifest_generator': ManifestGeneratorTool(args, gcp_env),
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

    manifest = subparser.add_parser("manifest_generator")
    manifest.add_argument("--manifest-template", help="which manifest to generate",
                          default=None,
                          required=True)  # noqa
    manifest.add_argument("--sample-id-file", help="path to the list of sample_ids to include in manifest. "
                                                   "Leave blank for End-to-End manifest (pulls all eligible samples)",
                              default=None)  # noqa
    manifest.add_argument("--update-samples",
                          help="update the result state and manifest job run id field on completion",
                          default=False, required=False, action="store_true")  # noqa
    manifest.add_argument("--output-manifest-directory", help="local output directory for the generated manifest"
                                                       , default=None)  # noqa
    manifest.add_argument("--output-manifest-filename", help="what to name the output file",
                              default=None, required=False)  # noqa
    manifest.add_argument("--cvl-site-id", help="cvl site to pass to manifest query",
                          default=None, required=False)  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        try:
            datagen_process = get_datagen_process_for_run(args, gcp_env)
            exit_code = datagen_process.run()
        # pylint: disable=broad-except
        except Exception as e:
            _logger.info(f'Error has occured, {e}. For help use "genomic_datagen --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
