#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import json
import logging
import os
import sys

from sqlalchemy.orm.attributes import flag_modified

from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.model.measurements import PhysicalMeasurements
from rdr_service.services.system_utils import setup_logging, setup_i18n, print_progress_bar
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "fast-pm-fix"
tool_desc = "Fix PM fhir documents"


class ProgramTemplateClass(object):



    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env


    def test_update(self):

        path = os.path.expanduser('~/pm.json')
        with open(path) as h:
            resource = json.loads(h.read())

        resource = self.update_author_extensions(resource)
        resource = self.update_cancellation(resource)
        resource = self.update_restored(resource)
        resource = self.update_location_extension(resource)
        resource = self.remove_status(resource)

        with open(os.path.expanduser('~/pm-out.json'), 'w+') as h:
            h.write(json.dumps(resource, indent=2))

    def get_composition(self, resource):

        for item in resource['entry']:
            if item['resource']['resourceType'] == 'Composition':
                return item['resource']
        return None

    def update_author_extensions(self, resource):

        comp = self.get_composition(resource)

        if 'author' in comp:
            for item in comp['author']:
                if 'extension' not in item:
                    continue
                if not isinstance(item['extension'], list):
                    item['extension'] = [item['extension']]

        return resource

    def update_cancellation(self, resource):

        if 'status' not in resource or resource['status'].lower() != 'cancelled':
            return resource

        comp = self.get_composition(resource)

        if 'extension' not in comp:
            comp['extension'] = list()

        if 'cancelledSiteId' in resource:
            comp['extension'].append({
                "url": "http://terminology.pmi-ops.org/StructureDefinition/cancelled-site",
                "valueInteger": resource['cancelledSiteId']
            })
            resource.pop('cancelledSiteId')
        if 'cancelledTime' in resource:
            comp['extension'].append({
                "url": "http://terminology.pmi-ops.org/StructureDefinition/cancelled-time",
                "valueString": resource['cancelledTime']
            })
            resource.pop('cancelledTime')
        if 'cancelledUsername' in resource:
            comp['extension'].append({
                "url": "http://terminology.pmi-ops.org/StructureDefinition/cancelled-username",
                "valueString": resource['cancelledUsername']
            })
            resource.pop('cancelledUsername')

        if 'reason' in resource:
            comp['extension'].append({
                "url": "http://terminology.pmi-ops.org/StructureDefinition/cancelled-reason",
                "valueString": resource['reason']
            })
            resource.pop('reason')

        resource.pop('status')

        comp['status'] = 'entered-in-error'

        return resource

    def update_restored(self, resource):

        if 'status' not in resource or resource['status'].lower() != 'restored':
            return resource

        comp = self.get_composition(resource)

        if 'extension' not in comp:
            comp['extension'] = list()

        if 'reason' in resource:
            comp['extension'].append({
                "url": "http://terminology.pmi-ops.org/StructureDefinition/restore-reason",
                "valueString": resource['reason']
            })
            resource.pop('reason')

        comp['status'] = 'final'

        resource.pop('status')

        return resource

    def update_location_extension(self, resource):

        comp = self.get_composition(resource)

        if 'extension' not in comp:
            return resource

        for item in comp['extension']:
            if '-location' in item['url'] and 'valueReference' in item:
                item['valueString'] = item['valueReference']
                item.pop('valueReference')

        return resource

    def remove_status(self, resource):

        if 'status' in resource:
            resource.pop('status')

        return resource


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
        dao = PhysicalMeasurementsDao()

        with dao.session() as session:
            ids = session.query(PhysicalMeasurements.physicalMeasurementsId).\
                order_by(PhysicalMeasurements.physicalMeasurementsId).all()

            total = len(ids)
            count = 0
            errors = 0
            changed = 0

            for pm_id in ids:
                count += 1
                if count < 193680:
                    continue

                record = session.query(PhysicalMeasurements).filter(
                        PhysicalMeasurements.physicalMeasurementsId == pm_id).first()

                if not record:
                    errors += 1
                    _logger.error(f'PM Id {pm_id} not found.')
                    continue

                resource = record.resource

                resource = self.update_author_extensions(resource)
                resource = self.update_cancellation(resource)
                resource = self.update_restored(resource)
                resource = self.update_location_extension(resource)
                resource = self.remove_status(resource)

                if record.resource != resource:
                    changed += 1

                record.resource = resource
                # sqlalchemy does not mark the 'resource' field as dirty, we need to force it.
                flag_modified(record, 'resource')
                session.commit()

                if not self.args.debug:
                    print_progress_bar(
                        count, total, prefix="{0}/{1}:".format(count, total), suffix="complete"
                    )

            _logger.warning(f'Warning: updated {changed} records.')

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
