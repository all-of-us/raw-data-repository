from typing import Optional

from rdr_service import code_constants
from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao
from rdr_service.domain_model.response import Answer, Response, ParticipantResponses
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire_response import QuestionnaireResponseAnswer
from rdr_service.services.system_utils import list_chunks
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'cope_filter'
tool_desc = 'Script for marking repeated answers to COPE questions that should have only been asked once as invalid'


class _CopeUtils:
    @classmethod
    def is_yes_answer(cls, answer: Optional[Answer]):
        if answer is None:
            return False

        return answer.value == code_constants.CONSENT_COPE_YES_CODE.lower()


class InvalidAnswers(Exception):
    def __init__(self, *args, answer_ids: set):
        super(InvalidAnswers, self).__init__(*args)
        self.invalid_answer_ids = answer_ids


class CodeRepeatedTracker:
    """
    Monitors answers across visited responses. Will dectect any answers for the given question codes and then flag
    any subsequent answers as invalid for the set of question codes.
    """

    def __init__(self, question_codes):
        self._codes = question_codes
        self.previous_answer_found = False

    def visit_response(self, response: Response):
        invalid_ids_found = set()
        answer_found = False
        for code in self._codes:
            if response.has_answer_for(code):
                if self.previous_answer_found:
                    invalid_ids_found.update({answer.id for answer in response.get_answers_for(code)})
                else:
                    answer_found = True

        if answer_found:
            self.previous_answer_found = True

        if invalid_ids_found:
            raise InvalidAnswers(
                f'Invalid answers found for one or more of "{self._codes}"',
                answer_ids=invalid_ids_found
            )


class DosesReceivedTracker:
    """
    The Feb COPE survey asked a question of 'did you get the vaccine' and another of 'how many doses' and the Minute
    surveys ask about the first and second doses in separate questions.

    This class tracks the mapping between the Feb and Minutes surveys, detecting when we already know a participant
    has responded with information on each of the doses and flagging any subsequent responses for that dose.
    """
    def __init__(self):
        self.first_dose_tracker = _MinuteSurveyDoseTracking(
            dose_received_question_code=code_constants.COPE_FIRST_DOSE_QUESTION,
            dose_type_question_code=code_constants.COPE_FIRST_DOSE_TYPE_QUESTION,
            dose_type_other_question_code=code_constants.COPE_FIRST_DOSE_TYPE_OTHER_QUESTION,
            dose_symptom_question_code=code_constants.COPE_FIRST_DOSE_SYMPTOM_QUESTION,
            dose_symptom_other_question_code=code_constants.COPE_FIRST_DOSE_SYMPTOM_OTHER_QUESTION,
            dose_date_question_code=code_constants.COPE_FIRST_DOSE_DATE_QUESTION
        )
        self.second_dose_tracker = _MinuteSurveyDoseTracking(
            dose_received_question_code=code_constants.COPE_SECOND_DOSE_QUESTION,
            dose_type_question_code=code_constants.COPE_SECOND_DOSE_TYPE_QUESTION,
            dose_type_other_question_code=code_constants.COPE_SECOND_DOSE_TYPE_OTHER_QUESTION,
            dose_symptom_question_code=code_constants.COPE_SECOND_DOSE_SYMPTOM_QUESTION,
            dose_symptom_other_question_code=code_constants.COPE_SECOND_DOSE_SYMPTOM_OTHER_QUESTION,
            dose_date_question_code=code_constants.COPE_SECOND_DOSE_DATE_QUESTION
        )
        # Create dose question trackers that track answers for doses 3 through 17
        self.additional_dose_trackers = [
            self._build_numbered_dose_tracker(number)
            for number in range(3, 18)
        ]

    @classmethod
    def _build_numbered_dose_tracker(cls, number):
        return _MinuteSurveyDoseTracking(
            dose_received_question_code=f'cdc_covid_xx_dose{number}',
            dose_type_question_code=f'cdc_covid_xx_b_dose{number}',
            dose_type_other_question_code=f'cdc_covid_xx_b_dose{number}_other',
            dose_symptom_question_code=f'cdc_covid_xx_symptom_dose{number}',
            dose_symptom_other_question_code=f'cdc_covid_xx_symptom_cope_350_dose{number}',
            dose_vol_question_code=f'cdc_covid_xx_type_dose{number}',
            dose_vol_other_question_code=f'cdc_covid_xx_type_dose{number}_other',
            dose_date_question_code=f'cdc_covid_xx_a_date{number}'
        )

    def visit_response(self, response: Response):
        if response.survey_code == code_constants.COPE_FEB_MODULE:
            self._visit_cope_feb(response)
        else:
            self._visit_minute_response(response)

    def _visit_cope_feb(self, response: Response):
        received_vaccine_answer = response.get_single_answer_for(code_constants.COPE_DOSE_RECEIVED_QUESTION)
        if _CopeUtils.is_yes_answer(received_vaccine_answer):
            number_of_doses_answer = response.get_single_answer_for(code_constants.COPE_NUMBER_DOSES_QUESTION)
            number_doses_value = number_of_doses_answer.value if number_of_doses_answer else None

            responding_for_first_dose = False
            responding_for_second_dose = False
            if number_doses_value == code_constants.COPE_TWO_DOSE_ANSWER:
                responding_for_first_dose = True
                responding_for_second_dose = True
            elif number_doses_value == code_constants.COPE_ONE_DOSE_ANSWER:
                responding_for_first_dose = True
                responding_for_second_dose = False

            has_answer_for_type = response.has_answer_for(code_constants.COPE_DOSE_TYPE_QUESTION)
            if responding_for_first_dose:
                self.first_dose_tracker.previously_confirmed_dose_received = True
                self.first_dose_tracker.previously_answered_dose_type = has_answer_for_type
            if responding_for_second_dose:
                self.second_dose_tracker.previously_confirmed_dose_received = True
                self.second_dose_tracker.previously_answered_dose_type = has_answer_for_type

    def _visit_minute_response(self, response: Response):
        self.first_dose_tracker.check_minute_response(response)
        self.second_dose_tracker.check_minute_response(response)

        invalid_answer_ids = set.union(
            self.first_dose_tracker.get_and_clear_ids(),
            self.second_dose_tracker.get_and_clear_ids(),
        )
        for tracker in self.additional_dose_trackers:
            tracker.check_minute_response(response)
            invalid_answer_ids.update(tracker.get_and_clear_ids())

        if invalid_answer_ids:
            raise InvalidAnswers(f'Invalid answers found for dose questions', answer_ids=invalid_answer_ids)


class _MinuteSurveyDoseTracking:
    def __init__(self, dose_received_question_code, dose_type_question_code, dose_type_other_question_code,
                 dose_symptom_question_code, dose_symptom_other_question_code, dose_date_question_code,
                 dose_vol_question_code=None, dose_vol_other_question_code=None):
        self.dose_received_question_code = dose_received_question_code
        self.dose_type_question_code = dose_type_question_code
        self.dose_type_other_question_code = dose_type_other_question_code
        self.dose_symptom_question_code = dose_symptom_question_code
        self.dose_symptom_other_question_code = dose_symptom_other_question_code
        self.dose_vol_question_code = dose_vol_question_code
        self.dose_vol_other_question_code = dose_vol_other_question_code
        self.dose_date_question_code = dose_date_question_code

        self.previously_confirmed_dose_received = False
        self.invalid_ids = set()

    def get_and_clear_ids(self):
        ids = self.invalid_ids
        self.invalid_ids = set()
        return ids

    def check_minute_response(self, response: Response):
        dose_received_answer = response.get_single_answer_for(self.dose_received_question_code)
        if dose_received_answer and self.previously_confirmed_dose_received:
            self.invalid_ids.add(dose_received_answer.id)

        dose_type_answer = response.get_single_answer_for(self.dose_type_question_code)
        if dose_type_answer and self.previously_confirmed_dose_received:
            self.invalid_ids.add(dose_type_answer.id)

        dose_type_other_answer = response.get_single_answer_for(self.dose_type_other_question_code)
        if dose_type_other_answer and self.previously_confirmed_dose_received:
            self.invalid_ids.add(dose_type_other_answer.id)

        dose_symptom_answer = response.get_answers_for(self.dose_symptom_question_code)
        if dose_symptom_answer and self.previously_confirmed_dose_received:
            self.invalid_ids.update({answer.id for answer in dose_symptom_answer})

        dose_symptom_other_answer = response.get_single_answer_for(self.dose_symptom_other_question_code)
        if dose_symptom_other_answer and self.previously_confirmed_dose_received:
            self.invalid_ids.update({answer.id for answer in dose_symptom_answer})

        dose_vol_answer = response.get_single_answer_for(self.dose_vol_question_code)
        if self.dose_vol_question_code and dose_vol_answer and self.previously_confirmed_dose_received:
            self.invalid_ids.add(dose_vol_answer.id)

        dose_vol_other_answer = response.get_single_answer_for(self.dose_vol_other_question_code)
        if self.dose_vol_other_question_code and dose_vol_other_answer and self.previously_confirmed_dose_received:
            self.invalid_ids.add(dose_vol_other_answer.id)

        dose_date_answer = response.get_single_answer_for(self.dose_date_question_code)
        if dose_date_answer and self.previously_confirmed_dose_received:
            self.invalid_ids.add(dose_date_answer.id)

        if _CopeUtils.is_yes_answer(dose_received_answer):
            self.previously_confirmed_dose_received = True
        elif dose_received_answer is None or dose_received_answer.value != 'pmi_skip':
            if dose_type_answer:
                print('ERROR: Got a response on the type of dose, but not that they took it')
            if dose_type_other_answer:
                print('ERROR: Got a response on the other type of dose, but not that they took it')
            if dose_symptom_answer:
                print('ERROR: Got a response on the symptom of dose, but not that they took it')
            if dose_symptom_other_answer:
                print('ERROR: Got a response on the other symptom of dose, but not that they took it')
            if dose_vol_answer:
                print('ERROR: Got a response on the vol of dose, but not that they took it')
            if dose_vol_other_answer:
                print('ERROR: Got a response on the other vol of dose, but not that they took it')
            if dose_date_answer:
                print('ERROR: Got a response on the date of dose, but not that they took it')


class CopeFilterTool(ToolBase):
    def run(self):
        super(CopeFilterTool, self).run()

        with self.get_session() as session:
            participant_ids = self._get_all_consented_participant_ids(session)
            cope_survey_codes = ['cope_feb', 'cope_vaccine1', 'cope_vaccine2', 'cope_vaccine3', 'cope_vaccine4']
            invalid_answer_ids = set()

            for id_chunk in list_chunks(participant_ids, chunk_size=1000):
                responses = QuestionnaireResponseDao.get_responses_to_surveys(
                    survey_codes=cope_survey_codes,
                    participant_ids=id_chunk,
                    session=session
                )
                for responses in responses.values():
                    invalid_answers_for_participant = self._get_invalid_answers_in_cope_responses(responses)
                    invalid_answer_ids.update(invalid_answers_for_participant)

            for invalid_id_chunk in list_chunks(list(invalid_answer_ids), chunk_size=1000):
                session.query(
                    QuestionnaireResponseAnswer
                ).filter(
                    QuestionnaireResponseAnswer.questionnaireResponseAnswerId.in_(invalid_id_chunk)
                ).update(
                    {
                        QuestionnaireResponseAnswer.ignore: True,
                        QuestionnaireResponseAnswer.ignore_reason:
                            'previously received COPE answer providing covid vaccine information (DA-2438)'
                    },
                    syncronize_session=False
                )
                session.commit()

    @classmethod
    def _get_all_consented_participant_ids(cls, session):
        db_results = session.query(ParticipantSummary.participantId).all()
        return [obj.participantId for obj in db_results]

    @classmethod
    def _get_invalid_answers_in_cope_responses(cls, responses: ParticipantResponses):
        invalid_answer_ids = set()
        validation_rule_trackers = [
            DosesReceivedTracker()  # covers all questions related to doses received
        ]
        for response in responses.in_authored_order:
            for tracker in validation_rule_trackers:
                try:
                    tracker.visit_response(response)
                except InvalidAnswers as err:
                    invalid_answer_ids.update(err.invalid_answer_ids)

        return invalid_answer_ids


def run():
    return cli_run(tool_cmd, tool_desc, CopeFilterTool)
