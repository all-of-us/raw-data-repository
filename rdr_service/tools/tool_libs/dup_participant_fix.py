#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import csv
import logging
import os
import sys
from collections import namedtuple

import dateutil

from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.measurements import PhysicalMeasurements
from rdr_service.model.participant import Participant
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier
from rdr_service.model.participant_summary import ParticipantSummary

from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "fix-dup-pids"
tool_desc = "Fix duplicated participants"


class ProgramTemplateClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.pid_mappings = list()

    def get_participant_records(self, dao, pid):
        """
        Return the participant and summary records for the
        participant ID in mapping.
        :param dao: DAO object to run queries with.
        :param pid: Participant ID
        :return: Participant and ParticipantSummary objects
        """
        with dao.session() as session:
            p = session.query(Participant).filter(Participant.participantId == pid).first()
            ps = session.query(ParticipantSummary).filter(ParticipantSummary.participantId == pid).first()

        return p, ps

    def get_biobank_records_for_participant(self, dao, mappings):
        """
        Return the participant, biobank order, and stored sample records mappings.
        :param dao: DAO object to run queries with.
        :param mappings: Tuple with old_pid, new_pid, biobank_order_id.
        :return: old participant, new participant,
        biobank order, stored sample records
        """
        with dao.session() as session:
            op = session.query(Participant).filter(Participant.participantId == mappings[0]).first()
            np = session.query(Participant).filter(Participant.participantId == mappings[1]).first()
            bbo = session.query(BiobankOrder).filter(BiobankOrder.biobankOrderId == mappings[2]).first()
            ss = session.query(BiobankStoredSample).join(
                BiobankOrderIdentifier,
                BiobankOrderIdentifier.value == BiobankStoredSample.biobankOrderIdentifier
            ).filter(
                BiobankOrderIdentifier.biobankOrderId == mappings[2]
            ).all()

        return op, np, bbo, ss

    def get_biobank_order_for_participant(self, dao, pid):
        """
        Return the biobank order, only to be ran for pids with 1 order
        :param dao:
        :param pid:
        :return: biobank order
        """
        with dao.session() as session:
            return session.query(BiobankOrder).filter(BiobankOrder.participantId == pid).first()

    def get_pm_records_for_participant(self, dao, mappings):
        """
       Return the physical measurement and participant records in mappings.
       :param dao: DAO object to run queries with.
       :param mappings: Tuple with old_pid, new_pid, and pm_id.
       :return: old participant, new participant, and pm records
       """
        with dao.session() as session:
            op = session.query(Participant).filter(Participant.participantId == mappings[0]).first()
            np = session.query(Participant).filter(Participant.participantId == mappings[1]).first()
            pm = session.query(PhysicalMeasurements).filter(
                PhysicalMeasurements.physicalMeasurementsId == mappings[2]
            ).first()
        return op, np, pm

    def get_pm_for_participant(self, dao, pid):
        """
        Return the biobank order, only to be ran for pids with 1 pm
        :param dao:
        :param pid:
        :return: biobank order
        """
        with dao.session() as session:
            return session.query(PhysicalMeasurements).filter(PhysicalMeasurements.participantId == pid).first()

    def fix_biobank_order(self, dao, np, bbo):
        """
        Updates the Biobank Order object to the new PID
        :param dao: the dao
        :param np: new participant
        :param bbo: biobank order object
        :return: updated biobank order object
        """
        bbo.participantId = np.participantId
        with dao.session() as session:
            return session.merge(bbo)

    def fix_biobank_stored_sample(self, dao, np, ss):
        """
        Updates the Biobank stored sample to the new biobank_id
        :param dao:
        :param np: new participant
        :param ss: stored sample object
        :return: updated sample object
        """
        ss.biobankId = np.biobankId
        with dao.session() as session:
            return session.merge(ss)

    def fix_pm(self, dao, op, np, pm):
        """
        Updates the Physical Measurement object to the new PID
        :param dao: the dao
        :param op: the old participant
        :param np: new participant
        :param pm: physical measurement object
        :return: updated physical measurment object
        """
        pid_strings = (str(op.participantId), str(np.participantId))
        pm.participantId = np.participantId
        # Update the resource request with the correct PID
        for entry in pm.resource['entry']:
            new_ref = entry['resource']['subject']['reference'].replace(*pid_strings)
            entry['resource']['subject']['reference'] = new_ref
        with dao.session() as session:
            return session.merge(pm)

    def fix_signup_time(self, dao, np, nps, new_time):
        """
        Updates the participant and participant_summary signup_time
        :param dao:
        :param np:
        :param nps:
        :param new_time:
        :return:
        """
        new_time_dt = dateutil.parser.parse(new_time)
        np.signUpTime = new_time_dt
        nps.signUpTime = new_time_dt
        with dao.session() as session:
            updated_participant = session.merge(np)
            updated_summary = session.merge(nps)
        return updated_participant, updated_summary

    def set_old_to_new_mappings(self):
        """
        Reads from old->new PID mapping file and
        sets the pid_mapping attribute
        """
        # get old -> new mappings
        with open(self.args.mapping_source_csv, encoding='utf-8-sig') as s:
            reader = csv.reader(s)
            Mapping = namedtuple("Mapping", next(reader))
            for mapping in map(Mapping._make, reader):
                self.pid_mappings.append(mapping)

    def map_biobank_orders(self, old_pids, dao):
        """
        Iterates old_pids list and looks up BBO ID
        writes to temp CSV directory
        """
        # Write mapping output
        with open('.tmp/new_mappings_bbo.csv', 'w', newline='') as out:
            writer = csv.writer(out)
            writer.writerow(["old_pid", "new_pid", "biobank_order_id"])
            for old_pid in old_pids:
                pid_mapping = filter(lambda x: x.old_pid == old_pid, self.pid_mappings)
                for i in pid_mapping:
                    bbo = self.get_biobank_order_for_participant(dao, old_pid)
                    writer.writerow([i.old_pid, i.new_pid, bbo.biobankOrderId])
        _logger.info('Biobank Order mapping file generated:')
        _logger.info('    .tmp/new_mappings_bbo.csv')

    def map_physical_measurements(self, old_pids, dao):
        """
        Iterates old_pids list and looks up PM ID
        writes to temp CSV directory
        """
        # Write mapping output
        with open('.tmp/new_mappings_pm.csv', 'w', newline='') as out:
            writer = csv.writer(out)
            writer.writerow(["old_pid", "new_pid", "pm_id"])
            for old_pid in old_pids:
                pid_mapping = filter(lambda x: x.old_pid == old_pid, self.pid_mappings)
                for i in pid_mapping:
                    pm = self.get_pm_for_participant(dao, old_pid)
                    writer.writerow([i.old_pid, i.new_pid, pm.physicalMeasurementsId])
        _logger.info('Physical Measurement mapping file generated:')
        _logger.info('    .tmp/new_mappings_pm.csv')

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        self.gcp_env.activate_sql_proxy()
        dao = ParticipantDao()

        mappings_list = list()

        if self.args.participant:
            mappings_list.append(tuple(i.strip() for i in self.args.participant.split(',')))
        else:
            with open(self.args.csv, encoding='utf-8-sig') as h:
                lines = h.readlines()
                for line in lines:
                    if self.args.generate_mapping:
                        mappings_list.append(line.strip())
                    else:
                        mappings_list.append(tuple(i.strip() for i in line.split(',')))

        if self.args.fix_biobank_orders:
            headers = mappings_list.pop(0)
            if headers != ("old_pid", "new_pid", "biobank_order_id"):
                _logger.error("Invalid columns in CSV")
                _logger.error(f"   {headers}")
                return 1
            for mapping in mappings_list:
                old_p, new_p, biobank_order, stored_samples = self.get_biobank_records_for_participant(dao, mapping)

                if not old_p or not new_p or not biobank_order or not stored_samples:
                    _logger.error(
                        f'  ERROR: missing data for {old_p.participantId}, '
                        f'{new_p.participantId}, '
                        f'{biobank_order.biobankOrderId}')
                    continue

                # Process Biobank Orders
                _logger.warning(
                    f'  reassigning Biobank Order {biobank_order.biobankOrderId} | '
                    f'{old_p.participantId} -> {new_p.participantId}')
                updated_bbo = self.fix_biobank_order(dao, new_p, biobank_order)
                _logger.info(f'  update successful for {updated_bbo.biobankOrderId}: '
                             f'{updated_bbo.participantId}')

                # Process Biobank Stored Samples
                for sample in stored_samples:
                    _logger.warning(
                        f'  reassigning Stored Sample {sample.biobankStoredSampleId} | '
                        f'{old_p.biobankId} -> {new_p.biobankId}')
                    updated_ss = self.fix_biobank_stored_sample(dao, new_p, sample)
                    _logger.info(f'  update successful for {updated_ss.biobankStoredSampleId}: '
                                 f'{updated_ss.biobankId}')

                # Update participant summary
                bbo_dao = BiobankOrderDao()
                with bbo_dao.session() as session:
                    bb_obj = session.query(BiobankOrder).filter(
                        BiobankOrder.biobankOrderId == updated_bbo.biobankOrderId
                    ).first()
                    _logger.warning(
                        f'    updating participant summary for {bb_obj.participantId}')
                    bbo_dao._update_participant_summary(session, bb_obj)

        if self.args.fix_physical_measurements:
            headers = mappings_list.pop(0)
            if headers != ("old_pid", "new_pid", "pm_id"):
                _logger.error("Invalid columns in CSV")
                _logger.error(f"   {headers}")
                return 1
            for mapping in mappings_list:
                old_p, new_p, physical_measurement = self.get_pm_records_for_participant(dao, mapping)

                if not old_p or not new_p or not physical_measurement:
                    _logger.error(
                        f'  ERROR: missing data for {old_p.participantId}, '
                        f'{new_p.participantId}, '
                        f'{physical_measurement.physicalMeasurementsId}')
                    continue

                # Process Physical Measurement
                _logger.warning(
                    f'  reassigning Physical Measurement {physical_measurement.physicalMeasurementsId} | '
                    f'{old_p.participantId} -> {new_p.participantId}')
                updated_pm = self.fix_pm(dao, old_p, new_p, physical_measurement)
                _logger.info(f'  update successful for PM ID {updated_pm.physicalMeasurementsId}: '
                             f'{updated_pm.participantId}')

                # TODO: _update_participant_summary(updated_pm)
                # Update participant summary
                pm_dao = PhysicalMeasurementsDao()
                with pm_dao.session() as session:
                    pm_obj = session.query(PhysicalMeasurements).filter(
                        PhysicalMeasurements.participantId == updated_pm.participantId
                    ).first()
                    _logger.warning(
                        f'    updating participant summary for {pm_obj.participantId}')
                    pm_dao._update_participant_summary(session, pm_obj)

        if self.args.fix_signup_time:
            headers = mappings_list.pop(0)
            if headers != ("new_pid", "signup_time"):
                _logger.error("Invalid columns in CSV")
                _logger.error(f"   {headers}")
                return 1
            _logger.info("Fixing signup time.")
            for mapping in mappings_list:
                np, nps = self.get_participant_records(dao, mapping[0])

                if not np or not nps:
                    _logger.error(f'  ERROR: no pid {mapping[0]}')
                    continue

                # Make the updates for signup time
                _logger.warning(
                    f'  updating signup_time {np.participantId} | '
                    f'{np.signUpTime} -> {mapping[1]}')
                updated_p, updated_s = self.fix_signup_time(dao, np, nps, mapping[1])
                _logger.info(f'      update successful for participant {updated_p.participantId}: '
                             f'{updated_p.signUpTime}')
                _logger.info(f'      update successful for participant summary {updated_s.participantId}: '
                             f'{updated_s.signUpTime}')

        if self.args.generate_mapping is not None:
            # generates the mapping file from the ptsc report
            self.set_old_to_new_mappings()

            headers = mappings_list.pop(0)
            if headers != "old_pid":
                _logger.error("Invalid columns in CSV")
                _logger.error(f"   {headers}")
                return 1

            if self.args.generate_mapping.lower().strip() == 'bbo':
                # generate biobank order mappings
                self.map_biobank_orders(mappings_list, dao)

            if self.args.generate_mapping.lower().strip() == 'pm':
                # generate pm mappings
                self.map_physical_measurements(mappings_list, dao)

        # TODO: Fix Patient Status/Update Participant Summary
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
    parser.add_argument("--csv", help="csv file with participant ids", default=None)  # noqa
    parser.add_argument("--participant", help="old pid,new pid 1,new pid 2", default=None)  # noqa
    parser.add_argument("--fix-biobank-orders", help="Fix Biobank Orders", default=False, action="store_true")  # noqa
    parser.add_argument("--fix-physical-measurements", help="Fix Physical Measurements",
                        default=False, action="store_true")  # noqa
    parser.add_argument("--fix-signup-time", help="Fix signup time", default=False, action="store_true")  # noqa
    parser.add_argument("--generate-mapping", help="Create the mapping csv for 'pm' or 'bbo'.", default=None)  # noqa
    parser.add_argument("--mapping-source-csv", help="Old -> New PID mapping source.", default=None)  # noqa
    args = parser.parse_args()
    process_args = (args.fix_biobank_orders,
                    args.fix_physical_measurements,
                    args.fix_signup_time,
                    (args.generate_mapping is not None))
    if not any(process_args):
        _logger.error('Either --fix-biobank-orders, --fix-physical-measurements,'
                      ' --fix-signup-time,'
                      'or --generate-mapping must be provided')
        return 1

    if sum(process_args) > 1:
        _logger.error('Arguments --fix_biobank_orders, --fix_physical_measurements,'
                      ' --fix-signup-time, and --generate-mapping '
                      'may not be used together.')
        return 1
    if args.generate_mapping is None:
        if not args.participant and not args.csv:
            _logger.error('Either --csv or --participant argument must be provided.')
            return 1

        if args.participant and args.csv:
            _logger.error('Arguments --csv and --participant may not be used together.')
            return 1

        if args.participant:
            # Verify that we have a string with 3 comma delimited values.
            if len(args.participant.split(',')) != 3:
                _logger.error('Invalid participant argument, must be 3 PIDs in comma delimited format.')
                return 1
    else:
        valid_mapping = ("pm", "bbo")
        if args.generate_mapping.lower().strip() not in valid_mapping:
            _logger.error(f'--generate-mapping must be one of {valid_mapping}')
            return 1
        if args.mapping_source_csv is None:
            _logger.error(f'--mapping-source-csv required if --generate-mapping')
            return 1
        if not os.path.exists(args.mapping_source_csv):
            _logger.error(f'File {args.mapping_source_csv} was not found.')
            return 1

    if args.csv:
        if not os.path.exists(args.csv):
            _logger.error(f'File {args.csv} was not found.')
            return 1

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = ProgramTemplateClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
