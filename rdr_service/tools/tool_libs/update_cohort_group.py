import csv

from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.tools.tool_libs._tool_base import cli_run, ToolBase

tool_cmd = 'update-cohort-group'
tool_desc = 'Update cohort group with descripancies in file from PTSC'


class UpdateCohortGroup(ToolBase):

    def __init__(self, *args, **kwargs):
        super(UpdateCohortGroup, self).__init__(*args, **kwargs)
        self.import_file_descripancies = []

    def run(self):
        super(UpdateCohortGroup, self).run()
        if self.args.input_file:
            with open(self.args.input_file) as csv_file:
                csv_reader = csv.DictReader(csv_file)
                # remove backup argument
                with self.get_session(backup=True) as session:
                    file_denoted_mismatches = []
                    count = 0
                    for row in csv_reader:
                        assignment = self.convert_str_num(row.get('ptsc_cohort_assignment'))
                        if row.get('cohort_mismatch') == 'NULL' and assignment > 0:
                            count += 1
                            file_denoted_mismatches.append({
                                'participant_id': int(row.get('participant_id')),
                                'assignment': assignment
                            })
                            # set to 1000
                            # if count == 1000:
                            if count % 1000 == 0:
                                # self.update_cohort_items(session, mismatches)
                                # self.update_cohort_items_pilot(session, mismatches)
                                self.validate_data_sets(session, file_denoted_mismatches)
                                file_denoted_mismatches.clear()
                                print(count)
                                print('Current num descripancies {}'.format(len(self.import_file_descripancies)))
                    self.output_csv()

    def validate_data_sets(self, session, items):
        for item in items:
            obj = session.query(
                ParticipantSummary
                ).filter(ParticipantSummary.participantId == item.get('participant_id')).first()
            if obj:
                if item.get('assignment') != 2.1 and obj.consentCohort.number != item.get('assignment') \
                or item.get('assigment') == 2.1 and obj.consentCohort.number != 2 \
                or obj.cohort2PilotFlag and obj.cohort2PilotFlag.number != 1:
                    print('pid {} : ptsc_file_num {} => rdr_db_num {}'.format(
                        item.get('participant_id'), item.get('assignment'), obj.consentCohort.number))
                    self.import_file_descripancies.append({
                        'participant_id': obj.participantId,
                        'ptsc_consent_number': item.get('assignment'),
                        'rdr_consent_number': obj.consentCohort.number,
                        'consent_first_yes_authored':
                            obj.consentForStudyEnrollmentFirstYesAuthored.strftime("%b %d %Y %H:%M:%S"),
                        'ptsc_designated_pilot': 'True' if item.get('assignment') == 2.1 else 'False'
                    })

    def output_csv(self):
        print('Found {} number of descripancies'.format(len(self.import_file_descripancies)))
        fields = ['participant_id',
                  'ptsc_consent_number',
                  'rdr_consent_number',
                  'consent_first_yes_authored',
                  'ptsc_designated_pilot']
        filename = "descripancies_in_ptsc_cohort_file_rdr_data.csv"
        with open(filename, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            writer.writerows(self.import_file_descripancies)
        print('csv write finished')


    @staticmethod
    def update_cohort_items(session, items):
        updated_objs = []
        for item in items:
            obj = session.query(
                ParticipantSummary
                ).filter(ParticipantSummary.participantId == item.get('participant_id')).first()
            if obj:
                if item.get('participant_id') == 2.1:
                    obj.consentCohort = 2
                    obj.cohort2PilotFlag = 1
                else:
                    obj.consentCohort = item.get('participant_id')
                updated_objs.append(obj)

    @staticmethod
    def convert_str_num(string):
        if string.isnumeric():
            return int(string)
        return float(string)


def add_additional_arguments(arg_parser):
    arg_parser.add_argument('--input-file', help='csv file from PTSC providing descripancies', default=None)


def run():
    return cli_run(tool_cmd, tool_desc, UpdateCohortGroup, add_additional_arguments)
