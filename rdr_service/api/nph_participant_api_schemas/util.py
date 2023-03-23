from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict
from graphene import List

from sqlalchemy.orm import Query, aliased

from rdr_service.ancillary_study_resources.nph.enums import ParticipantOpsElementTypes, ConsentOptInTypes
from rdr_service.api_util import parse_date
from rdr_service.model.participant_summary import ParticipantSummary as ParticipantSummaryModel
from rdr_service.participant_enums import QuestionnaireStatus


@dataclass
class QueryBuilder:
    query: Query
    order_expression = None
    filter_expressions: List = field(default_factory=list)
    references: Dict = field(default_factory=dict)
    join_expressions: List = field(default_factory=list)
    sort_table = None
    table = None

    def set_table(self, value):
        self.table = value

    def set_sort_table(self, reference):
        self.sort_table = self.references[reference]

    def add_filter(self, expr):
        self.filter_expressions.append(expr)

    def add_ref(self, table, ref_name):
        self.references[ref_name] = aliased(table)
        return self

    def add_join(self, joined_table, join_expr):
        self.join_expressions.append((joined_table, join_expr))
        return self

    def set_order_expression(self, expr):
        self.order_expression = expr

    def get_resulting_query(self):
        resulting_query = self.query

        for table, expr in self.join_expressions:
            resulting_query = resulting_query.join(table, expr)
        for expr in self.filter_expressions:
            resulting_query = resulting_query.filter(expr)

        return resulting_query.order_by(self.order_expression)


def check_field_value(value):
    if value is not None:
        return value
    return QuestionnaireStatus.UNSET


def load_participant_summary_data(query, prefix, biobank_prefix):

    def get_enrollment_statuses(enrollment_data):
        return list(map(
            lambda x: {'value': x['value'], 'time': parse_date(x['time']) if x['time'] else None},
            enrollment_data
        ))

    def get_consent_statuses(consent_data):
        return list(map(
            lambda x: {
                'value': x['value'],
                'time': parse_date(x['time']) if x['time'] else None,
                'opt_in': str(ConsentOptInTypes(int(x['opt_in'])))
            },
            consent_data
        ))

    def get_value_from_ops_data(participant_ops_data, enum):
        if not participant_ops_data:
            return QuestionnaireStatus.UNSET
        current_ops_value = list(filter(lambda x: x.source_data_element == enum, [participant_ops_data]))
        if not current_ops_value:
            return QuestionnaireStatus.UNSET
        return current_ops_value[0].source_value

    results = []
    records = query.all()

    for summary, site, nph_site, mapping, nph_participant, enrollment, consents, \
            deactivated, withdrawn, ops_data in records:
        results.append({
            'participantNphId': f"{prefix}{mapping.ancillary_participant_id}",
            'lastModified': summary.lastModified,
            'biobankId': f"{biobank_prefix}{nph_participant.biobank_id}",
            'firstName': summary.firstName,
            'middleName': summary.middleName,
            'lastName': summary.lastName,
            'dateOfBirth': summary.dateOfBirth,
            'nphDateOfBirth': get_value_from_ops_data(ops_data, ParticipantOpsElementTypes.BIRTHDATE),
            'zipCode': summary.zipCode,
            'phoneNumber': summary.phoneNumber,
            'email': summary.email,
            'deceasedStatus': {
                "value": check_field_value(summary.deceasedStatus),
                "time": summary.deceasedAuthored
            },
            'withdrawalStatus': {
                "value": check_field_value(summary.withdrawalStatus),
                "time": summary.withdrawalAuthored
            },
            'nphDeactivationStatus': {
                "value": "Deactivate" if deactivated else "NULL",
                "time": deactivated.event_authored_time if deactivated else None
            },
            'nphWithdrawalStatus': {
                "value": "Withdrawn" if withdrawn else "NULL",
                "time": withdrawn.event_authored_time if withdrawn else None
            },
            'nphEnrollmentStatus': get_enrollment_statuses(enrollment['enrollment_json']),
            'nphModule1ConsentStatus': get_consent_statuses(consents['consent_json']),
            'aianStatus': summary.aian,
            'suspensionStatus': {"value": check_field_value(summary.suspensionStatus),
                                 "time": summary.suspensionTime},
            'aouEnrollmentStatus': {"value": check_field_value(summary.enrollmentStatus),
                                    "time": summary.dateOfBirth},
            'questionnaireOnTheBasics': {
                "value": check_field_value(summary.questionnaireOnTheBasics),
                "time": summary.questionnaireOnTheBasicsAuthored
            },
            'questionnaireOnHealthcareAccess': {
                "value": check_field_value(summary.questionnaireOnHealthcareAccess),
                "time": summary.questionnaireOnHealthcareAccessAuthored
            },
            'questionnaireOnLifestyle': {
                "value": check_field_value(summary.questionnaireOnLifestyle),
                "time": summary.questionnaireOnLifestyleAuthored
            },
            'siteId': site.googleGroup,
            'externalId': nph_site.external_id,
            'organizationExternalId': nph_site.organization_external_id,
            'awardeeExternalId': nph_site.awardee_external_id,
            'questionnaireOnSocialDeterminantsOfHealth': {
                "value": check_field_value(summary.questionnaireOnSocialDeterminantsOfHealth),
                 "time": summary.questionnaireOnSocialDeterminantsOfHealthAuthored
            }
        })

    return results


def schema_field_lookup(value):
    try:
        field_lookup = {
            "DOB": {"field": "dateOfBirth", "table": ParticipantSummaryModel,
                    "value": ParticipantSummaryModel.dateOfBirth},
            "aouAianStatus": {"field": "aian", "table": ParticipantSummaryModel,
                              "value": ParticipantSummaryModel.aian},
            "aouBasicsStatus": {"field": "questionnaireOnTheBasics", "table": ParticipantSummaryModel,
                               "value": ParticipantSummaryModel.questionnaireOnTheBasics},
            "aouDeceasedStatus": {"field": "deceasedStatus", "table": ParticipantSummaryModel,
                                  "value": ParticipantSummaryModel.deceasedStatus,
                                  "time": ParticipantSummaryModel.deceasedAuthored},
            "aouWithdrawalStatus": {"field": "withdrawalStatus", "table": ParticipantSummaryModel,
                                    "value": ParticipantSummaryModel.withdrawalStatus,
                                    "time": ParticipantSummaryModel.withdrawalAuthored},
            "aouDeactivationStatus": {"field": "suspensionStatus", "table": ParticipantSummaryModel,
                                      "value": ParticipantSummaryModel.suspensionStatus,
                                      "time": ParticipantSummaryModel.suspensionTime},
            "aouEnrollmentStatus": {"field": "enrollmentStatus", "table": ParticipantSummaryModel,
                                    "value": ParticipantSummaryModel.enrollmentStatus,
                                    "time": ParticipantSummaryModel.enrollmentStatusParticipantV3_1Time},
            "aouOverallHealthStatus": {"field": "questionnaireOnHealthcareAccess", "table": ParticipantSummaryModel,
                                       "value": ParticipantSummaryModel.questionnaireOnHealthcareAccess,
                                       "time": ParticipantSummaryModel.questionnaireOnHealthcareAccessAuthored},
            "aouLifestyleStatus": {"field": "questionnaireOnLifestyle", "table": ParticipantSummaryModel,
                                   "value": ParticipantSummaryModel.questionnaireOnLifestyle,
                                   "time": ParticipantSummaryModel.questionnaireOnLifestyleAuthored},
            "aouSDOHStatus": {"field": "questionnaireOnSocialDeterminantsOfHealth", "table": ParticipantSummaryModel,
                              "value": ParticipantSummaryModel.questionnaireOnSocialDeterminantsOfHealth,
                              "time": ParticipantSummaryModel.questionnaireOnSocialDeterminantsOfHealthAuthored}
        }
        result = field_lookup.get(value, None)
        if not result:
            raise Exception(f"Invalid value : {value}")
        return result
    except KeyError as err:
        raise err


def load_participant_data(query):
    # query.session = sessions

    results = []
    for participants in query.all():
        samples_data = defaultdict(lambda: {
            'stored': {
                'parent': {
                    'current': None
                },
                'child': {
                    'current': None
                }
            }
        })
        for parent_sample in participants.samples:
            data_struct = samples_data[f'sample{parent_sample.test}']['stored']
            data_struct['parent']['current'] = {
                'value': parent_sample.status,
                'time': parent_sample.time
            }

            if len(parent_sample.children) == 1:
                child = parent_sample.children[0]
                data_struct['child']['current'] = {
                    'value': child.status,
                    'time': child.time
                }

        results.append(
            {
                'participantNphId': participants.participantId,
                'lastModified': participants.lastModified,
                'biobankId': participants.biobankId,
                **samples_data
            }
        )

    return []


def validation_error_message(errors):
    return {"errors": [error.formatted for error in errors]}


def error_message(message):
    return {"errors": message}
