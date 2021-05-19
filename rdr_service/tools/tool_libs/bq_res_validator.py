#! /bin/env python
#
# BigQuery/Resource participant summary validator.
# # TODO: Delete this tool after BigQuery DAO files are removed from project.
#

import argparse
import logging
import sys

from rdr_service.dao.bq_participant_summary_dao import BQParticipantSummaryGenerator
from rdr_service.resource.generators.participant import ParticipantSummaryGenerator
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "bq-res"
tool_desc = "put tool help description here"


STRIP_PREFIXES = {
    'modules': 'mod_',
    'pm': 'pm_',
    'bbo_samples': 'bbs_',
    'biobank_orders': 'bbo_'
}


class ProgramTemplateClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env

    def compare_dicts(self, d1: dict, d2: dict, sub=None, strip_prefix=None):

        not_in1 = list()
        diff = list()

        for k, v in d1.items():
            k2 = k
            if strip_prefix and k.startswith(strip_prefix):
                k2 = k[len(strip_prefix):]

            # Recursively compare list keys.
            if isinstance(d1[k], list):
                lst1 = d1[k]
                lst2 = d2[k2]

                # Sort lists so both are in the exact same order.
                if k == 'modules':
                    lst1 = sorted(lst1, key=lambda d:
                        d['mod_module'] + (d['mod_language'] if d['mod_language'] else 'None') +
                        (d['mod_authored'].isoformat() if d['mod_authored'] else 'None') +
                        (d['mod_created'].isoformat() if d['mod_created'] else 'None'))
                    lst2 = sorted(lst2, key=lambda d:
                        d['module'] + (d['language'] if d['language'] else 'None') +
                        (d['module_authored'].isoformat() if d['module_authored'] else 'None') +
                        (d['module_created'].isoformat() if d['module_created'] else 'None'))

                if k == 'patient_statuses':
                    lst1 = sorted(lst1, key=lambda d:
                        d['patient_status_authored'].isoformat() if d['patient_status_authored'] else 'None')
                    lst2 = sorted(lst2, key=lambda d:
                        d['patient_status_authored'].isoformat() if d['patient_status_authored'] else 'None')

                if k == 'bbo_samples':
                    lst1 = sorted(lst1, key=lambda d:
                        d['bbs_created'].isoformat() if d['bbs_created'] else 'None' +
                        d['bbs_confirmed'].isoformat() if d['bbs_confirmed'] else 'None')
                    lst2 = sorted(lst2, key=lambda d:
                        d['created'].isoformat() if d['created'] else 'None' +
                        d['confirmed'].isoformat() if d['confirmed'] else 'None')

                for n in range(len(lst1)):
                    self.compare_dicts(lst1[n], lst2[n], k, STRIP_PREFIXES.get(k, None))
                continue

            if k == 'participant_id':
                v = f'P{v}'
            if sub == 'modules':
                if k == 'mod_created':
                    k2 = 'module_created'
                elif k == 'mod_authored':
                    k2 = 'module_authored'

            if k2 not in d2:
                not_in1.append(k2)
                continue
            if v != d2[k2]:
                diff.append([k, v, d2[k2]])

        if not_in1 or diff:
            print(sub or 'Participant Summary:')
            print('  Not In:')
            print(f'    {not_in1}')
            print('  Diff:')
            print(f'    {diff}')


    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        """
        Example: Create a SQL Proxy DB connection for SQL Alchemy to use.

            Calling `activate_sql_proxy()` will make a new connection to a project DB instance and
            set the DB_CONNECTION_STRING environment var with the correct connection string for
            SQL Alchemy.  Once the function has returned, any DAO object can be then used.
            The connection will be closed and cleaned up when the Context Manager is released.
        """
        self.gcp_env.activate_sql_proxy()
        res_gen = ParticipantSummaryGenerator()
        bq_gen = BQParticipantSummaryGenerator()

        # Stable Environment PIDs.
        pids = [100343245, 101785380, 104831002, 104917510, 106133925, 111143764, 111693691, 111866379,
                112094567, 113264371, 115208964, 115667157, 116772345, 118069780, 118106279, 118275699,
                119524086, 120937445, 122162300, 123309305, 124352051, 130310274, 130332109, 133487339,
                134691836, 135119720, 135549758, 139676702, 142593735, 143459156, 145479186, 145975075,
                147661669, 147684969, 147942286, 147988733, 150116431, 150272201, 150540533, 150689114,
                155241356, 157694324, 158173894, 158855869, 160498800, 163572548, 163976185, 165315114,
                165488235, 167254162, 172505219, 175752932, 176130311, 176896382, 177254367, 181792216,
                182766464, 183436919, 186476418, 187164690, 198860684, 201005904, 204973609, 208794758,
                209241151, 209657895, 209658591, 211140790, 211217471, 211448524, 213605677, 214659165,
                215839869, 216862339, 217327537, 218719201, 220866527, 225078909, 228929489, 232688732,
                233009992, 233856454, 236294032, 236751506, 237543544, 243386095, 243643970, 243805484,
                244855041, 245743578, 245888395, 246639275, 247220397, 248064451, 248096002, 251457975,
                252651766, 254212435, 255107353 ]

        for pid in pids:
            print(f'PID: {pid}:')

            res = res_gen.make_resource(pid)
            res_data = res.get_data()

            bq = bq_gen.make_bqrecord(pid)
            bq_data = bq.to_dict()

            self.compare_dicts(bq_data, res_data)

        return 0


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
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = ProgramTemplateClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
