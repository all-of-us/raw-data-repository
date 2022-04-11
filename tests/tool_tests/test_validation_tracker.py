from datetime import datetime
from typing import Dict, List

from rdr_service import code_constants
from rdr_service.domain_model import response as response_domain_model
from rdr_service.model.questionnaire_response import QuestionnaireResponseStatus
from rdr_service.tools.tool_libs.cope_answer_filter import CodeRepeatedTracker, DosesReceivedTracker, InvalidAnswers
from tests.helpers.unittest_base import BaseTestCase


class TestValidationTracker(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(TestValidationTracker, self).__init__(*args, **kwargs)
        self.uses_database = False

        self.cope_yes_answer_code_str = code_constants.CONSENT_COPE_YES_CODE.lower()
        self.cope_no_answer_code_str = code_constants.CONSENT_COPE_NO_CODE.lower()

    def test_flags_same_code(self):
        """Make sure the tracker detects that it's code has ben answered again"""
        tracker = CodeRepeatedTracker(question_codes=['test_b'])

        tracker.visit_response(response=self._build_response(answers={
            'test': [self._build_answer(id_=23, value='any')],
            'test_b': [self._build_answer(id_=72, value='any')]
        }))

        with self.assertRaises(InvalidAnswers) as invalid_error:
            tracker.visit_response(response=self._build_response(answers={
                'test': [self._build_answer(id_=128, value='any')],
                'test_b': [
                    self._build_answer(id_=321, value='any'),
                    self._build_answer(id_=743, value='test other')
                ]
            }))
        self.assertEqual({321, 743}, invalid_error.exception.invalid_answer_ids)

    def test_flags_other_codes_in_group(self):
        tracker = CodeRepeatedTracker(question_codes=['test_b', 'test_b_x', 'test_b_c'])

        tracker.visit_response(response=self._build_response(answers={
            'test_b': [self._build_answer(id_=72, value='any')]
        }))

        with self.assertRaises(InvalidAnswers) as invalid_error:
            tracker.visit_response(response=self._build_response(answers={
                'test_b_c': [self._build_answer(id_=321, value='any')],
                'test_b_x': [self._build_answer(id_=743, value='any')]
            }))
        self.assertEqual({321, 743}, invalid_error.exception.invalid_answer_ids)

    def test_does_not_flag_in_same_response(self):
        tracker = CodeRepeatedTracker(question_codes=['test_b', 'test_b_x'])

        tracker.visit_response(response=self._build_response(answers={
            'test_b': [self._build_answer(id_=321, value='any')],
            'test_b_x': [self._build_answer(id_=743, value='any')]
        }))

    def test_two_dose_answer_shows_later_dose_answers_as_invalid(self):
        tracker = DosesReceivedTracker()

        tracker.visit_response(response=self._build_response(
            answers={
                code_constants.COPE_DOSE_RECEIVED_QUESTION: [
                    self._build_answer(id_=23, value=self.cope_yes_answer_code_str)
                ],
                code_constants.COPE_NUMBER_DOSES_QUESTION: [
                    self._build_answer(id_=72, value=code_constants.COPE_TWO_DOSE_ANSWER)
                ],
                code_constants.COPE_DOSE_TYPE_QUESTION: [
                    self._build_answer(id_=73, value='any')
                ]
            },
            survey_code=code_constants.COPE_FEB_MODULE
        ))

        with self.assertRaises(InvalidAnswers) as invalid_error:
            tracker.visit_response(response=self._build_response(answers={
                code_constants.COPE_FIRST_DOSE_QUESTION: [
                    self._build_answer(id_=128, value=self.cope_yes_answer_code_str)
                ],
                code_constants.COPE_FIRST_DOSE_TYPE_QUESTION: [self._build_answer(id_=129, value='any')],
                code_constants.COPE_FIRST_DOSE_TYPE_OTHER_QUESTION: [self._build_answer(id_=130, value='any')],
                code_constants.COPE_SECOND_DOSE_QUESTION: [
                    self._build_answer(id_=354, value=self.cope_yes_answer_code_str)
                ],
                code_constants.COPE_SECOND_DOSE_TYPE_QUESTION: [self._build_answer(id_=355, value='any')],
                code_constants.COPE_SECOND_DOSE_TYPE_OTHER_QUESTION: [self._build_answer(id_=356, value='any')]
            }))
        self.assertEqual({128, 129, 130, 354, 355, 356}, invalid_error.exception.invalid_answer_ids)

    def test_one_dose_answer_shows_later_dose_answers_as_invalid(self):
        tracker = DosesReceivedTracker()

        tracker.visit_response(response=self._build_response(
            answers={
                code_constants.COPE_DOSE_RECEIVED_QUESTION: [
                    self._build_answer(id_=23, value=self.cope_yes_answer_code_str)
                ],
                code_constants.COPE_NUMBER_DOSES_QUESTION: [
                    self._build_answer(id_=72, value=code_constants.COPE_ONE_DOSE_ANSWER)
                ],
                code_constants.COPE_DOSE_TYPE_QUESTION: [
                    self._build_answer(id_=73, value='any')
                ]
            },
            survey_code=code_constants.COPE_FEB_MODULE
        ))

        with self.assertRaises(InvalidAnswers) as invalid_error:
            tracker.visit_response(response=self._build_response(answers={
                code_constants.COPE_FIRST_DOSE_QUESTION: [
                    self._build_answer(id_=128, value=self.cope_yes_answer_code_str)
                ],
                code_constants.COPE_FIRST_DOSE_TYPE_QUESTION: [self._build_answer(id_=129, value='any')],
                code_constants.COPE_FIRST_DOSE_TYPE_OTHER_QUESTION: [self._build_answer(id_=130, value='any')],
                code_constants.COPE_SECOND_DOSE_QUESTION: [
                    self._build_answer(id_=354, value=self.cope_yes_answer_code_str)
                ],
                code_constants.COPE_SECOND_DOSE_TYPE_QUESTION: [self._build_answer(id_=355, value='any')],
                code_constants.COPE_SECOND_DOSE_TYPE_OTHER_QUESTION: [self._build_answer(id_=356, value='any')]
            }))
        self.assertEqual({128, 129, 130}, invalid_error.exception.invalid_answer_ids)

    def test_minute_dose_answers_flagged(self):
        tracker = DosesReceivedTracker()

        tracker.visit_response(response=self._build_response(
            answers={
                code_constants.COPE_FIRST_DOSE_QUESTION: [
                    self._build_answer(id_=128, value=self.cope_yes_answer_code_str)
                ],
                code_constants.COPE_FIRST_DOSE_TYPE_QUESTION: [self._build_answer(id_=129, value='any')],
                code_constants.COPE_FIRST_DOSE_TYPE_OTHER_QUESTION: [self._build_answer(id_=130, value='any')]
            }
        ))

        with self.assertRaises(InvalidAnswers) as invalid_error:
            tracker.visit_response(response=self._build_response(answers={
                code_constants.COPE_FIRST_DOSE_QUESTION: [
                    self._build_answer(id_=233, value=self.cope_yes_answer_code_str)
                ],
                code_constants.COPE_FIRST_DOSE_TYPE_QUESTION: [self._build_answer(id_=268, value='any')],
                code_constants.COPE_FIRST_DOSE_TYPE_OTHER_QUESTION: [self._build_answer(id_=297, value='any')],
                code_constants.COPE_SECOND_DOSE_QUESTION: [
                    self._build_answer(id_=354, value=self.cope_yes_answer_code_str)
                ],
                code_constants.COPE_SECOND_DOSE_TYPE_QUESTION: [self._build_answer(id_=355, value='any')],
                code_constants.COPE_SECOND_DOSE_TYPE_OTHER_QUESTION: [self._build_answer(id_=356, value='any')]
            }))
        self.assertEqual({233, 268, 297}, invalid_error.exception.invalid_answer_ids)

    def test_can_answer_no_then_yes(self):
        tracker = DosesReceivedTracker()

        tracker.visit_response(response=self._build_response(
            answers={
                code_constants.COPE_FIRST_DOSE_QUESTION: [
                    self._build_answer(id_=128, value=self.cope_no_answer_code_str)
                ]
            }
        ))

        tracker.visit_response(response=self._build_response(answers={
            code_constants.COPE_FIRST_DOSE_QUESTION: [
                self._build_answer(id_=233, value=self.cope_yes_answer_code_str)
            ],
            code_constants.COPE_FIRST_DOSE_TYPE_QUESTION: [self._build_answer(id_=268, value='any')],
            code_constants.COPE_FIRST_DOSE_TYPE_OTHER_QUESTION: [self._build_answer(id_=297, value='any')]
        }))

    def test_yes_flags_later_no(self):
        tracker = DosesReceivedTracker()

        tracker.visit_response(response=self._build_response(
            answers={
                code_constants.COPE_FIRST_DOSE_QUESTION: [
                    self._build_answer(id_=233, value=self.cope_yes_answer_code_str)
                ],
                code_constants.COPE_FIRST_DOSE_TYPE_QUESTION: [self._build_answer(id_=268, value='any')],
                code_constants.COPE_FIRST_DOSE_TYPE_OTHER_QUESTION: [self._build_answer(id_=297, value='any')]
            }
        ))

        with self.assertRaises(InvalidAnswers) as invalid_error:
            tracker.visit_response(response=self._build_response(answers={
                code_constants.COPE_FIRST_DOSE_QUESTION: [
                    self._build_answer(id_=128, value=self.cope_no_answer_code_str)
                ]
            }))
        self.assertEqual({128}, invalid_error.exception.invalid_answer_ids)

    @classmethod
    def _build_response(cls, answers: Dict[str, List[response_domain_model.Answer]], survey_code='test'):
        return response_domain_model.Response(
            id=1,
            survey_code=survey_code,
            authored_datetime=datetime.utcnow(),
            status=QuestionnaireResponseStatus.COMPLETED,
            answered_codes=answers
        )

    @classmethod
    def _build_answer(cls, id_, value):
        return response_domain_model.Answer(id=id_, value=value, data_type=response_domain_model.DataType.STRING)
