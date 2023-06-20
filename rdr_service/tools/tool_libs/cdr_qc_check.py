import csv
import os.path
from datetime import datetime, timezone
from sqlalchemy import and_, or_, desc, func

from rdr_service.code_constants import BASICS_PROFILE_UPDATE_QUESTION_CODES
from rdr_service.dao.participant_dao import Participant
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.measurements import PhysicalMeasurements
from rdr_service.model.code import Code
from rdr_service.model.participant import ParticipantHistory
from rdr_service.model.questionnaire_response import (
    QuestionnaireResponse, QuestionnaireResponseClassificationType, QuestionnaireResponseAnswer
)
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireQuestion
from rdr_service.services.system_utils import list_chunks, print_progress_bar
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase

tool_cmd = 'cdr-qc-check'
tool_desc = 'Report activity details on participants before a CDR extract.  See DA-3570'

class CDRQcCheck(ToolBase):

    # Default cutoff date filter
    cutoff_date = datetime.today().date()
    cutoff_sql_filter = datetime(cutoff_date.year, cutoff_date.month, cutoff_date.day, tzinfo=timezone.utc)
    basics_profile_update_codes = []
    csv_out_file = 'cdr_qc_check.csv'
    # Column headers based on criteria in the DA-3570 ticket
    csv_headers = [
        'Participant ID', 'Origin', 'Research ID', 'Consented before cutoff', 'Primary Consent',
        'COMPLETE TheBasics before cutoff', 'TheBasics answers',
        'COMPLETE OverallHealth before cutoff', 'OverallHealth answers',
        'COMPLETE Lifestyle before cutoff', 'Lifestyle answers',
        'Clinical PM before cutoff', 'Self-reported PM before cutoff',
        'Withdrawal Status', 'Withdrawal Authored or Time', 'Is Test', 'Date made Test',
        'Is Ghost', 'Date made Ghost'
    ]
    participants_not_found = []

    def init_csv(self):
        """
        Create a new CSV output file and initialize with the header row
        """
        with open(self.csv_out_file, "w") as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(i for i in self.csv_headers)

    def write_to_csv(self, pid_dict):
        """
        Write a single participant row to the CSV output file
        """
        pid_row = pid_dict['pid_details']
        primary_consent = pid_row.consentForStudyEnrollmentFirstYesAuthored
        consented_before_cutoff = primary_consent.date() < self.cutoff_date
        # Believe that withdrawalAuthored may not always be populated for administrative (vs. participant-initiated)
        # withdrawals, so account for cases where withdrawalTime exists without a withdrawalAuthored
        withdrawal_datetime = pid_row.withdrawalAuthored if pid_row.withdrawalAuthored else pid_row.withdrawalTime

        csv_row = [pid_row.participantId, pid_row.participantOrigin, pid_row.researchId,
                   "Y" if consented_before_cutoff else "N", primary_consent,
                   ]

        for mod in ['TheBasics', 'OverallHealth', 'Lifestyle']:
            csv_row.extend([pid_dict[f'completed_{mod}'], pid_dict[f'{mod}_answers']])

        csv_row.extend([
            pid_dict['last_clinical_pm_date'], pid_dict['last_self_reported_pm_date'],
            str(pid_row.withdrawalStatus), withdrawal_datetime,
            "Y" if pid_dict['is_test'] else "N", pid_dict['made_test'],
            "Y" if pid_dict['is_ghost'] else "N", pid_dict['made_ghost']
        ])

        if self.args.verbose:
            for i in range(len(self.csv_headers)):
                print(f'{self.csv_headers[i]:40}:\t{csv_row[i]}')
            print('\n')

        # Append the pid's data row to the CSV
        with open(self.csv_out_file, "a") as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(c for c in csv_row)

    def set_basics_profile_update_codes(self, session):
        """
        Generate a list of code_id values for TheBasics profile update question code strings
        Used when verifying if a TheBasics response contained "valid" answer data to qualify as a full survey
        """
        codes = session.query(Code.codeId).filter(Code.value.in_(BASICS_PROFILE_UPDATE_QUESTION_CODES)).all()
        self.basics_profile_update_codes = [c.codeId for c in codes]

    def set_defaults_for_pid(self, pid, details):
        """
        Add default key/values to the participant details dict for calculated or dadditional fields added after the
        initial ParticipantSummary/Participant fields select
        """
        for key in ('is_test', 'made_test', 'is_ghost', 'made_ghost', 'last_clinical_pm_date',
                    'last_self_reported_pm_date'):
            details[pid][key] = None
        for mod in ['TheBasics', 'OverallHealth', 'Lifestyle']:
            details[pid][f'completed_{mod}'] = None
            details[pid][f'{mod}_answers'] = None

    def date_made_test(self, pid, session):
        """
        Find the earliest participant_history version where is_test_participant = 1 or hpo_id = 21 (TEST hpo)
        Return lastModified datetime associated with the version
        """
        result = session.query(
            ParticipantHistory
        ).filter(
            and_(
                ParticipantHistory.participantId == pid,
                or_(ParticipantHistory.isTestParticipant == 1, ParticipantHistory.hpoId == 21)
            )
        ).order_by(ParticipantHistory.version).first()

        if result:
            return result.lastModified

        return None

    def date_made_ghost(self, pid, session):
        """
        Find the earliest participant_history verion date where isGhostId is not null
        Return dateAddedGhost if a value exists, else lastModified from the row.
        """
        result = session.query(
            ParticipantHistory
        ).filter(
            and_(
                ParticipantHistory.participantId == pid,
                ParticipantHistory.isGhostId == 1
            )
        ).order_by(ParticipantHistory.version).first()

        # Does not appear that lastModified was always updated when the ghost flag was set for a participant, so use the
        # dateAddedGhost value if it's available
        if result:
            return result.dateAddedGhost if result.dateAddedGhost is not None else result.lastModified

        return None

    def completed_module(self, pid, module_name, session):
        """
        Determine if Participant has a COMPLETE QuestionnaireResponseClassificationType survey response payload authored
        before the cutoff date. Return most recent authored and questionnairResponseId for a qualifying response
        """
        row = session.query(
            QuestionnaireResponse.questionnaireResponseId, QuestionnaireResponse.authored
        ).select_from(
            QuestionnaireResponse
        ).join(
            QuestionnaireConcept, and_(
                 QuestionnaireResponse.questionnaireId == QuestionnaireConcept.questionnaireId,
                 QuestionnaireResponse.questionnaireVersion == QuestionnaireConcept.questionnaireVersion)
        ).join(
            Code, Code.codeId == QuestionnaireConcept.codeId
        ).filter(
            QuestionnaireResponse.participantId == pid, Code.value == module_name,
            QuestionnaireResponse.authored < self.cutoff_sql_filter,
            QuestionnaireResponse.classificationType == QuestionnaireResponseClassificationType.COMPLETE

        ).order_by(desc(QuestionnaireResponse.authored)).first()

        if row:
            return row.authored, row.questionnaireResponseId

        return None, None

    def module_has_answer_values(self, module_name, response_id: int, session) -> bool:
        """
        Confirm the module payload has associated answer data.  For TheBasics, it must contain values other than
        answers to PROFILE_UPDATE question codes
        """
        query = session.query(
             QuestionnaireResponseAnswer, Code.codeId.label('question_code')
        ).select_from(
            QuestionnaireResponseAnswer
        ).join(
            QuestionnaireQuestion,
            QuestionnaireResponseAnswer.questionId == QuestionnaireQuestion.questionnaireQuestionId
        ).outerjoin(
            Code, Code.codeId == QuestionnaireQuestion.codeId
        ).filter(
            QuestionnaireResponseAnswer.questionnaireResponseId == response_id,
            QuestionnaireResponseAnswer.ignore.isnot(False),
            func.coalesce(
                QuestionnaireResponseAnswer.valueCodeId, QuestionnaireResponseAnswer.valueDecimal,
                QuestionnaireResponseAnswer.valueInteger, QuestionnaireResponseAnswer.valueBoolean,
                QuestionnaireResponseAnswer.valueDate,  QuestionnaireResponseAnswer.valueDateTime,
                QuestionnaireResponseAnswer.valueString, QuestionnaireResponseAnswer.valueUri,
                QuestionnaireResponseAnswer.valueSystem
            ).isnot(None)
        ).distinct()

        results = query.all()

        if results and module_name == 'TheBasics':
            for r in results:
                if r.question_code and r.question_code not in self.basics_profile_update_codes:
                    return True
            return False

        return bool(results)

    def pm_measurements(self, pid, session, qr_id=None):
        """
        Return the most recent finalized / not cancelled or amended PM record that has related measurement table records
        If a questionnaire_response_id value is passed in, apply to filters to find self-reported PM record
        """
        query = session.query(
            PhysicalMeasurements.finalized
        ).filter(
            and_(PhysicalMeasurements.participantId == pid,
                 PhysicalMeasurements.final == 1, PhysicalMeasurements.finalized < self.cutoff_sql_filter,
                 PhysicalMeasurements.measurements.any())
        )
        if qr_id:
            query = query.filter(PhysicalMeasurements.questionnaireResponseId == qr_id)

        last_pm_rec = query.order_by(desc(PhysicalMeasurements.finalized)).first()

        if last_pm_rec:
            return last_pm_rec.finalized

        return None

    def self_reported_measurements(self, pid, session):
        """
        Return finalized(/authored) date of the last completed self-reported remote PM record before the cutoff date
        """
        # questionnaire_response authored value returned is unused; pm_measurements() returns a finalized date from
        # the related PM record
        _, self_reported_pm_qr_id = self.completed_module(pid, 'pm_height_weight', session)
        return self.pm_measurements(pid, session, qr_id=self_reported_pm_qr_id)

    def get_pid_baseline_ppi_module_details(self, pid, details_dict, session):
        """
        Add details about the participant's baseline PPI module completions to their details dict
        Uses the earliest response for the module that has a COMPLETE QuestionnaireResponseClassificationType
        """
        for mod in ['TheBasics', 'OverallHealth', 'Lifestyle']:
            authored_date, qr_id = self.completed_module(pid, mod, session)
            if authored_date:
                details_dict[pid][f'completed_{mod}'] = authored_date
                details_dict[pid][f'{mod}_answers'] = "Y" \
                    if self.module_has_answer_values(mod, qr_id, session) else "N"
            else:
                details_dict[pid][f'completed_{mod}'] = 'N'
                details_dict[pid][f'{mod}_answers'] = None

    def get_participant_summary_details(self, pid_list, session):
        """
         Select participant/participant_summary fields related to DA-3570 data points
         Return a dictionary with a key for each pid in the pid_list:
             {
                <int_pid key> : { pid_details: <result row from SELECT > },
                ...
             }
        """
        details = {}
        query = session.query(
            ParticipantSummary
        ).join(
            Participant
        ).with_entities(
            Participant.participantOrigin, Participant.participantId, Participant.researchId,
            Participant.isTestParticipant, Participant.hpoId, Participant.isGhostId,
            Participant.withdrawalStatus, Participant.withdrawalAuthored, Participant.withdrawalTime,
            ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored,
            ParticipantSummary.consentForElectronicHealthRecordsFirstYesAuthored,
            ParticipantSummary.questionnaireOnTheBasicsAuthored,
            ParticipantSummary.questionnaireOnOverallHealthAuthored,
            ParticipantSummary.questionnaireOnLifestyleAuthored,
            ParticipantSummary.clinicPhysicalMeasurementsFinalizedTime,
            ParticipantSummary.selfReportedPhysicalMeasurementsAuthored
        ).filter(ParticipantSummary.participantId.in_(pid_list))

        if self.args.origin:
            query = query.filter(ParticipantSummary.participantOrigin == self.args.origin)

        p_rows = query.all()

        for p_row in p_rows:
            details[p_row.participantId] = {'pid_details': p_row}

        return details

    def collect_pid_activity_details(self, pid, details, session):
        """
        After initial ParticipantSummary/Participant data is retrieved, collect and calculate remaining data points
        from DA-3570 for baseline PPI module responses, PM data, and Test/Ghost settings
        """
        self.get_pid_baseline_ppi_module_details(pid, details, session)

        pid_details = details[pid]['pid_details']
        # Determine Test/Ghost status and dates, for pids that have been explicitly flagged as Test or Ghost
        if pid_details.hpoId == 21 or pid_details.isTestParticipant:
            details[pid]['is_test'] = True
            details[pid]['made_test'] = self.date_made_test(pid, session)

        if pid_details.isGhostId:
            details[pid]['is_ghost'] = True
            details[pid]['made_ghost'] = self.date_made_ghost(pid, session)

        # Determine Physical Measurements activity (in clinic or self-reported)
        completed_clinic_pm, completed_remote_pm = None, None
        if pid_details.clinicPhysicalMeasurementsFinalizedTime:
            completed_clinic_pm = self.pm_measurements(pid, session)
        if pid_details.selfReportedPhysicalMeasurementsAuthored:
            completed_remote_pm = self.self_reported_measurements(pid, session)

        details[pid]['last_clinical_pm_date'] = completed_clinic_pm if completed_clinic_pm else "N"
        details[pid]['last_self_reported_pm_date'] = completed_remote_pm if completed_remote_pm else "N"

    def run(self):
        super(CDRQcCheck, self).run()

        if self.args.cutoff:
            self.cutoff_date = self.args.cutoff.date()
            self.cutoff_sql_filter = datetime(self.cutoff_date.year, self.cutoff_date.month, self.cutoff_date.day,
                                              tzinfo=timezone.utc)
        if self.args.output:
            self.csv_out_file = self.args.output

        if not os.path.isfile(self.args.from_file):
            raise ValueError(f'{self.args.from_file} file does not exist')

        if not self.args.append or not os.path.isfile(self.csv_out_file):
            self.init_csv()
            if self.args.append:
                logger.warning(f'Append specified for {self.csv_out_file} but existing file not found.  Append ignored')

        dao = ParticipantSummaryDao()
        # For testing:  supply specific pid lists and comment out assignment from self.get_int_ids_from_file()
        # pids_for_qc = [453188462, 106613862, 985341216, 995585549, 100002184]
        pids_for_qc = self.get_int_ids_from_file(self.args.from_file)
        chunks_processed = 0
        batch_size = 250
        with dao.session() as session:
            # Generate the list of code_ids for the profile update question codes in TheBasics.  Used when assessing
            # validity of TheBasics questionnaire responses
            self.set_basics_profile_update_codes(session)
            for pid_list in list_chunks(pids_for_qc, batch_size):
                if not self.args.verbose:
                    print_progress_bar(chunks_processed * batch_size, len(pids_for_qc))
                details = self.get_participant_summary_details(pid_list, session)
                for pid in pid_list:
                    if pid not in details or not details[pid]:
                        logger.error(f'No participant_summary record with matching filters for P{pid}')
                        self.participants_not_found.append(pid)
                        continue
                    # Add key/default value pairs for additional data points to the pid-specific dictionary
                    self.set_defaults_for_pid(pid, details)
                    # Only generate additional data points for participants who consented before the cutoff date
                    if details[pid]['pid_details'].consentForStudyEnrollmentFirstYesAuthored.date() < self.cutoff_date:
                        self.collect_pid_activity_details(pid, details, session)
                    self.write_to_csv(details[pid])

                chunks_processed += 1

        print('\n')
        if len(self.participants_not_found):
            logger.error(
                f'No output for pids: {self.participants_not_found}, no matching participant_summary records found'
            )

def add_additional_arguments(parser):
    parser.add_argument('--cutoff', required=False, type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help="Cutoff date in YYYY-MM-DD format.  Default is today's date")

    parser.add_argument('--origin', required=False,
                        help="vibrent or careevolution")

    parser.add_argument('--from-file', dest='from_file', required=True, help="File with participant ids to qc")

    parser.add_argument('--output', required=False,
                        help="Output file for CSV data.  Default is cdr_qc_check.csv")

    parser.add_argument('--append', default=False, action="store_true",
                        help="Keep existing contents of --output file if it exists and append new results")

    parser.add_argument('--verbose', default=False, action='store_true',
                        help="Print verbose/formatted output to stdout (in addition to csv file creation")

def run():
    return cli_run(tool_cmd, tool_desc, CDRQcCheck, parser_hook=add_additional_arguments, replica=True)
