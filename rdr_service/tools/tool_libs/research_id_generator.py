#! /bin/env python
#
# Generate research ID
# when --type is 'import', import from csv file
# when --type is 'new', generate new research id for participant who doesn't have a research id yet
#

import argparse
import csv
import logging
import sys
import random

from sqlalchemy.exc import IntegrityError
from rdr_service.dao import database_factory
from rdr_service.dao.base_dao import MAX_INSERT_ATTEMPTS, _MIN_RESEARCH_ID, _MAX_RESEARCH_ID
from rdr_service.model.participant import Participant
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "generate-research-id"
tool_desc = "Generate research ID with a input csv file or randomly"


class ResearchIdGeneratorClass(object):
    def __init__(self, args, gcp_env):
        self.args = args
        self.gcp_env = gcp_env

    @staticmethod
    def load_participants(session, participant_id_list):
        participants = session.query(Participant).filter(
            Participant.participantId.in_(participant_id_list)
        ).all()

        return participants

    @staticmethod
    def load_participant_without_research_id(session):
        participant = session.query(Participant).filter(
            Participant.researchId == None
        ).one_or_none()

        return participant

    @staticmethod
    def update_participants(participants, pid_rid_mapping):
        count = 0
        for participant in participants:
            if participant.researchId is None:
                participant.researchId = pid_rid_mapping.get(participant.participantId)
                count = count + 1
        return count

    @staticmethod
    def update_participant_with_random_research_id(session, participant):
        for _ in range(0, MAX_INSERT_ATTEMPTS):
            try:
                random_id = random.randint(_MIN_RESEARCH_ID, _MAX_RESEARCH_ID)
                participant.researchId = random_id
                session.commit()
                return
            except IntegrityError as e:
                if "UNIQUE constraint failed" in str(e) or "Duplicate entry" in str(e):
                    logging.warning("Failed updated with {}: {}".format(random_id, str(e)))
        _logger.error("Giving up after {} insert attempts for participant {}".format(MAX_INSERT_ATTEMPTS,
                                                                                     participant.participantId))

    def run(self):
        proxy_pid = self.gcp_env.activate_sql_proxy()
        if not proxy_pid:
            _logger.error("activating google sql proxy failed.")
            return 1
        if self.args.type == 'new':
            with database_factory.make_server_cursor_database().session() as session:
                while True:
                    participant = self.load_participant_without_research_id(session)
                    if not participant:
                        print('no more participant found, generate research ID done')
                        break
                    else:
                        self.update_participant_with_random_research_id(session, participant)
                        print('updated random research ID for participant: {}'.format(participant.participantId))

        elif self.args.type == 'import':
            with open(self.args.input_file) as csv_file:
                csv_reader = csv.DictReader(csv_file)
                with database_factory.make_server_cursor_database().session() as session:
                    count = 0
                    participant_id_list = []
                    pid_rid_mapping = {}
                    for csv_record in csv_reader:
                        participant_id = csv_record['participant_id']
                        research_id = csv_record['research_id']
                        pid_rid_mapping[participant_id] = research_id
                        count = count + 1
                        participant_id_list.append(participant_id)
                        if count % 1000 == 0:
                            participants = self.load_participants(session, participant_id_list)
                            updated_count = self.update_participants(participants, pid_rid_mapping)
                            participant_id_list.clear()
                            pid_rid_mapping.clear()
                            session.commit()
                            print('processed {} records, updated {} records'.format(len(participant_id_list),
                                                                                    updated_count))

                    if len(participant_id_list) > 0:
                        participants = self.load_participants(session, participant_id_list)
                        updated_count = self.update_participants(participants, pid_rid_mapping)
                        session.commit()
                        print('processed {} records, updated {} records'.format(len(participant_id_list),
                                                                                updated_count))
                    print('generate research ID done')
        else:
            print('invalid parameter type: ' + self.args.type)

        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--type", required=True, help="generate type: import or new", default='import')  # noqa
    parser.add_argument('--input-file', help='csv file providing participant_id and research_id')  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = ResearchIdGeneratorClass(args, gcp_env)
        return process.run()


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
