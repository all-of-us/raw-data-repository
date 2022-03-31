import csv
from datetime import datetime
from typing import Dict

from rdr_service import code_constants
from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao
from rdr_service.domain_model.response import ParticipantResponses
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.redcap_client import RedcapClient
from rdr_service.services.response_validation.validation import And, CanOnlyBeAnsweredIf, Condition, Not, \
    InAnySurvey, InAnyPreviousSurvey, Or, Question, ResponseRequirements
from rdr_service.services.system_utils import list_chunks
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'cope_filter'
tool_desc = 'Script for marking repeated answers to COPE questions that should have only been asked once as invalid'


class InvalidAnswers(Exception):
    def __init__(self, *args, answer_ids: set):
        super(InvalidAnswers, self).__init__(*args)
        self.invalid_answer_ids = answer_ids


class CopeFilterTool(ToolBase):

    def _build_dose_question_constraints(self, dose_question_codes, dose_received_code,
                                         previous_dose_received_constraint):
        result = {
            question_code: self._only_allow_answer_if_no_previous_yes(dose_received_code)
            for question_code in dose_question_codes
        }

        dose_received_constraint = result[dose_received_code]
        result[dose_received_code] = And([
            dose_received_constraint,
            previous_dose_received_constraint
        ])

        return result

    @classmethod
    def _only_allow_answer_if_no_previous_yes(cls, question_code):
        return Not(InAnyPreviousSurvey(
            Question(question_code).is_answered_with(
                code_constants.CONSENT_COPE_YES_CODE.lower()
            )
        ))

    def _build_numbered_dose_constraints(self, number, previous_dose_constraint=None):
        if previous_dose_constraint is None:
            previous_dose_constraint = InAnySurvey(
                Question(f'cdc_covid_xx_dose{number-1}').is_answered_with(
                    code_constants.CONSENT_COPE_YES_CODE.lower()
                )
            )

        return self._build_dose_question_constraints(
            dose_question_codes=[
                f'cdc_covid_xx_dose{number}',
                f'cdc_covid_xx_b_dose{number}',
                f'cdc_covid_xx_b_dose{number}_other',
                f'cdc_covid_xx_symptom_dose{number}',
                f'cdc_covid_xx_symptom_cope_350_dose{number}',
                f'cdc_covid_xx_type_dose{number}',
                f'cdc_covid_xx_type_dose{number}_other',
                f'cdc_covid_xx_a_date{number}'
            ],
            dose_received_code=f'cdc_covid_xx_dose{number}',
            previous_dose_received_constraint=previous_dose_constraint
        )

    def __init__(self, *args, **kwargs):
        super(CopeFilterTool, self).__init__(*args, **kwargs)
        self.reqs_by_survey = {}
        self.dose_received_question_codes = [
            code_constants.COPE_SECOND_DOSE_QUESTION,
            *[f'cdc_covid_xx_dose{number}' for number in range(3, 18)]
        ]

    def run(self):
        super(CopeFilterTool, self).run()

        constraints = {
            **self._build_dose_question_constraints(
                dose_question_codes=[
                    code_constants.COPE_FIRST_DOSE_QUESTION,
                    code_constants.COPE_FIRST_DOSE_TYPE_QUESTION,
                    code_constants.COPE_FIRST_DOSE_TYPE_OTHER_QUESTION,
                    code_constants.COPE_FIRST_DOSE_SYMPTOM_QUESTION,
                    code_constants.COPE_FIRST_DOSE_SYMPTOM_OTHER_QUESTION,
                    code_constants.COPE_FIRST_DOSE_DATE_QUESTION,
                ],
                dose_received_code=code_constants.COPE_FIRST_DOSE_QUESTION,
                previous_dose_received_constraint=Not(InAnyPreviousSurvey(
                    Or([
                        Question(code_constants.COPE_NUMBER_DOSES_QUESTION).is_answered_with(
                            code_constants.COPE_ONE_DOSE_ANSWER.lower()
                        ),
                        Question(code_constants.COPE_NUMBER_DOSES_QUESTION).is_answered_with(
                            code_constants.COPE_TWO_DOSE_ANSWER.lower()
                        )
                    ])
                ))
            ),
            **self._build_dose_question_constraints(
                dose_question_codes=[
                    code_constants.COPE_SECOND_DOSE_QUESTION,
                    code_constants.COPE_SECOND_DOSE_TYPE_QUESTION,
                    code_constants.COPE_SECOND_DOSE_TYPE_OTHER_QUESTION,
                    code_constants.COPE_SECOND_DOSE_SYMPTOM_QUESTION,
                    code_constants.COPE_SECOND_DOSE_SYMPTOM_OTHER_QUESTION,
                    code_constants.COPE_SECOND_DOSE_DATE_QUESTION
                ],
                dose_received_code=code_constants.COPE_SECOND_DOSE_QUESTION,
                previous_dose_received_constraint=And([
                    InAnySurvey(
                        Or([
                            And([
                                Question(code_constants.COPE_DOSE_RECEIVED_QUESTION).is_answered_with(
                                    code_constants.CONSENT_COPE_YES_CODE.lower()
                                ),
                                Question(code_constants.COPE_NUMBER_DOSES_QUESTION).is_answered_with(
                                    code_constants.COPE_ONE_DOSE_ANSWER.lower()
                                )
                            ]),
                            Question(code_constants.COPE_FIRST_DOSE_QUESTION).is_answered_with(
                                code_constants.CONSENT_COPE_YES_CODE.lower()
                            )
                        ])
                    ),
                    Not(
                        InAnyPreviousSurvey(
                            Question(code_constants.COPE_NUMBER_DOSES_QUESTION).is_answered_with(
                                code_constants.COPE_TWO_DOSE_ANSWER.lower()
                            )
                        )
                    )
                ])
            ),
            **self._build_numbered_dose_constraints(3, previous_dose_constraint=InAnySurvey(
                Or([
                    And([
                        Question(code_constants.COPE_DOSE_RECEIVED_QUESTION).is_answered_with(
                            code_constants.CONSENT_COPE_YES_CODE.lower()
                        ),
                        Question(code_constants.COPE_NUMBER_DOSES_QUESTION).is_answered_with(
                            code_constants.COPE_TWO_DOSE_ANSWER.lower()
                        )
                    ]),
                    Question(code_constants.COPE_SECOND_DOSE_QUESTION).is_answered_with(
                        code_constants.CONSENT_COPE_YES_CODE.lower()
                    )
                ])
            )),
            **self._build_numbered_dose_constraints(4),
            **self._build_numbered_dose_constraints(5),
            **self._build_numbered_dose_constraints(6),
            **self._build_numbered_dose_constraints(7),
            **self._build_numbered_dose_constraints(8),
            **self._build_numbered_dose_constraints(9),
            **self._build_numbered_dose_constraints(10),
            **self._build_numbered_dose_constraints(11),
            **self._build_numbered_dose_constraints(12),
            **self._build_numbered_dose_constraints(13),
            **self._build_numbered_dose_constraints(14),
            **self._build_numbered_dose_constraints(15),
            **self._build_numbered_dose_constraints(16),
            **self._build_numbered_dose_constraints(17)
        }
        self._add_redcap_survey_requirements(constraints)

        with open('output_file.csv', 'w') as file, self.get_session() as session:
            csv_writer = csv.DictWriter(file, [
                'participant_id',
                'question_code',
                'is_valid'
            ])
            csv_writer.writeheader()

            participant_ids = self._get_all_consented_participant_ids(session)
            cope_survey_codes = ['cope_feb', 'cope_vaccine1', 'cope_vaccine2', 'cope_vaccine3', 'cope_vaccine4']

            count = 0
            for id_chunk in list_chunks(participant_ids, chunk_size=1000):
                print(f'{datetime.now()} processed {count} of {len(participant_ids)}')
                count += 1000

                responses = QuestionnaireResponseDao.get_responses_to_surveys(
                    survey_codes=cope_survey_codes,
                    participant_ids=id_chunk,
                    session=session
                )
                for participant_id, responses in responses.items():
                    self._get_validation_errors_in_cope_responses(responses)
                    for response in responses.in_authored_order:
                        for question_code in self.dose_received_question_codes:
                            answer = response.get_single_answer_for(question_code, allow_invalid=True, allow_skips=True)
                            if answer and answer.value == code_constants.CONSENT_COPE_YES_CODE.lower():
                                csv_writer.writerow({
                                    'participant_id': participant_id,
                                    'question_code': question_code,
                                    'is_valid': answer.is_valid
                                })

                    for survey_requirements in self.reqs_by_survey.values():
                        survey_requirements.reset_state()

            # for invalid_id_chunk in list_chunks(list(invalid_answer_ids), chunk_size=1000):
            #     session.query(
            #         QuestionnaireResponseAnswer
            #     ).filter(
            #         QuestionnaireResponseAnswer.questionnaireResponseAnswerId.in_(invalid_id_chunk)
            #     ).update(
            #         {
            #             QuestionnaireResponseAnswer.ignore: True,
            #             QuestionnaireResponseAnswer.ignore_reason:
            #                 'previously received COPE answer providing covid vaccine information (DA-2438)'
            #         },
            #         syncronize_session=False
            #     )
            #     session.commit()

    def _add_redcap_survey_requirements(self, hand_written_requirements: Dict[str, Condition]):
        server_config = self.get_server_config()
        api_keys = server_config['project_api_keys']
        for module_name, api_key in api_keys.items():
            if 'cope' in module_name and module_name != 'cope_nov':
                module_conditions = {
                    question_code: condition
                    for question_code, condition in hand_written_requirements.items()
                }
                survey_conditions = self._build_conditions_for_survey(api_key)
                for question_code, condition in survey_conditions.items():
                    if question_code in module_conditions:
                        if question_code in self.dose_received_question_codes:
                            module_conditions[question_code] = Or([
                                module_conditions[question_code],
                                condition
                            ])
                        else:
                            module_conditions[question_code] = And([
                                module_conditions[question_code],
                                condition
                            ])
                    else:
                        module_conditions[question_code] = condition

                self.reqs_by_survey[module_name] = ResponseRequirements(
                    {
                        question_code: CanOnlyBeAnsweredIf(condition)
                        for question_code, condition in module_conditions.items()
                    }
                )


    @classmethod
    def _build_conditions_for_survey(cls, api_key):
        client = RedcapClient()
        data_dictionary = client.get_data_dictionary(api_key)
        result = {}
        for entry in data_dictionary:
            if 'branching_logic' in entry and len(entry['branching_logic']) > 0:
                branching_logic_str = entry['branching_logic']
                condition = Condition.from_branching_logic(branching_logic_str)

                result[entry['field_name']] = condition

        return result

    @classmethod
    def _get_all_consented_participant_ids(cls, session):
        db_results = session.query(ParticipantSummary.participantId).all()
        return [obj.participantId for obj in db_results]

    def _get_validation_errors_in_cope_responses(self, responses: ParticipantResponses):
        invalid_answers = {}
        for response in responses.in_authored_order:
            for survey_code, requirements in self.reqs_by_survey.items():
                if survey_code == response.survey_code:
                    invalid_answers[response.survey_code] = requirements.check_for_errors(response)
                else:
                    requirements.observe_response(response)

        return invalid_answers


def run():
    return cli_run(tool_cmd, tool_desc, CopeFilterTool)
