from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union
from graphene import List as GrapheneList

from sqlalchemy.orm import Query, aliased

from rdr_service.ancillary_study_resources.nph.enums import ParticipantOpsElementTypes, ConsentOptInTypes
from rdr_service.api_util import parse_date
from rdr_service.model.study_nph import Participant as NphParticipant
from rdr_service.dao.study_nph_dao import NphOrderDao
from rdr_service.model.participant_summary import ParticipantSummary as ParticipantSummaryModel
from rdr_service.participant_enums import QuestionnaireStatus


@dataclass
class QueryBuilder:
    query: Query
    order_expression = None
    filter_expressions: GrapheneList = field(default_factory=list)
    references: Dict = field(default_factory=dict)
    join_expressions: GrapheneList = field(default_factory=list)
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


class NphParticipantData:

    nph_order_dao = NphOrderDao

    @classmethod
    def check_field_value(cls, value):
        if value is not None:
            return value
        return QuestionnaireStatus.UNSET

    @classmethod
    def get_values_from_obj(cls, obj, values: Union[set, dict]) -> dict:

        def check_conversion(attr) -> str:
            if not hasattr(obj, cls.nph_order_dao().camel_to_snake(attr)):
                return attr
            return cls.nph_order_dao().camel_to_snake(attr)

        if not obj:
            return {k: QuestionnaireStatus.UNSET for k in values}

        if type(values) is set:
            return {k: getattr(obj, check_conversion(k)) for k in values}
        return {k: getattr(obj, check_conversion(v)) for k, v in values.items()}

    @classmethod
    def get_enrollment_statuses(cls, enrollment_data: dict) -> Optional[List[dict]]:
        if not enrollment_data:
            return QuestionnaireStatus.UNSET
        return list(map(
            lambda x: {
                'value': x['value'],
                'time': parse_date(x['time']) if x['time'] else None
            },
            enrollment_data.get('enrollment_json')
        ))

    @classmethod
    def get_consent_statuses(cls, consent_data: dict) -> Optional[List[dict]]:
        if not consent_data:
            return QuestionnaireStatus.UNSET
        return list(map(
            lambda x: {
                'value': x['value'],
                'time': parse_date(x['time']) if x['time'] else None,
                'opt_in': str(ConsentOptInTypes(int(x['opt_in'])))
            },
            consent_data.get('consent_json')
        ))

    @classmethod
    def get_nph_biospecimens_for_participant(cls, nph_participant: NphParticipant):
        return cls.nph_order_dao().get_nph_biospecimens_for_participant(nph_participant)

    @classmethod
    def get_value_from_ops_data(cls, participant_ops_data: ParticipantOpsElementTypes, enum) -> Optional[str]:
        if not participant_ops_data:
            return QuestionnaireStatus.UNSET
        current_ops_value = list(filter(lambda x: x.source_data_element == enum, [participant_ops_data]))
        if not current_ops_value:
            return QuestionnaireStatus.UNSET
        return current_ops_value[0].source_value

    @classmethod
    def load_participant_summary_data(cls, query, biobank_prefix: str) -> List[dict]:
        results, records = [], query.all()
        for summary, site, nph_site, mapping, nph_participant, enrollments, consents, \
                deactivated, withdrawn, ops_data in records:
            participant_obj = {
                'participantNphId': mapping.ancillary_participant_id,
                'lastModified': summary.lastModified,
                'biobankId': f"{biobank_prefix}{nph_participant.biobank_id}",
                'firstName': summary.firstName,
                'middleName': summary.middleName,
                'lastName': summary.lastName,
                'nphDateOfBirth': cls.get_value_from_ops_data(ops_data, ParticipantOpsElementTypes.BIRTHDATE),
                'zipCode': summary.zipCode,
                'phoneNumber': summary.phoneNumber,
                'email': summary.email,
                'deceasedStatus': {
                    "value": cls.check_field_value(summary.deceasedStatus),
                    "time": summary.deceasedAuthored
                },
                'withdrawalStatus': {
                    "value": cls.check_field_value(summary.withdrawalStatus),
                    "time": summary.withdrawalAuthored
                },
                'nphDeactivationStatus': {
                    "value": "DEACTIVATED" if deactivated else "NULL",
                    "time": deactivated.event_authored_time if deactivated else None
                },
                'nphWithdrawalStatus': {
                    "value": "WITHDRAWN" if withdrawn else "NULL",
                    "time": withdrawn.event_authored_time if withdrawn else None
                },
                'nphEnrollmentStatus': cls.get_enrollment_statuses(enrollments),
                'nphModule1ConsentStatus': cls.get_consent_statuses(consents),
                "nphBiospecimens": cls.get_nph_biospecimens_for_participant(nph_participant),
                'aianStatus': summary.aian,
                'suspensionStatus': {"value": cls.check_field_value(summary.suspensionStatus),
                                     "time": summary.suspensionTime},
                'aouEnrollmentStatus': {"value": cls.check_field_value(summary.enrollmentStatus),
                                        "time": summary.dateOfBirth},
                'questionnaireOnTheBasics': {
                    "value": cls.check_field_value(summary.questionnaireOnTheBasics),
                    "time": summary.questionnaireOnTheBasicsAuthored
                },
                'questionnaireOnHealthcareAccess': {
                    "value": cls.check_field_value(summary.questionnaireOnHealthcareAccess),
                    "time": summary.questionnaireOnHealthcareAccessAuthored
                },
                'questionnaireOnLifestyle': {
                    "value": cls.check_field_value(summary.questionnaireOnLifestyle),
                    "time": summary.questionnaireOnLifestyleAuthored
                },
                'questionnaireOnSocialDeterminantsOfHealth': {
                    "value": cls.check_field_value(summary.questionnaireOnSocialDeterminantsOfHealth),
                    "time": summary.questionnaireOnSocialDeterminantsOfHealthAuthored
                }
            }
            participant_obj.update(
                cls.get_values_from_obj(
                    obj=site,
                    values={'siteId': 'googleGroup'}
                ))
            participant_obj.update(
                cls.get_values_from_obj(
                    obj=nph_site,
                    values={'externalId',
                            'organizationExternalId',
                            'awardeeExternalId'}
                ))
            results.append(participant_obj)
        return results

    @classmethod
    def schema_field_lookup(cls, value):
        try:
            field_lookup = {
                "aouAianStatus": {
                    "field": "aian",
                    "table": ParticipantSummaryModel,
                    "value": ParticipantSummaryModel.aian
                },
                "aouBasicsStatus": {
                    "field": "questionnaireOnTheBasics",
                    "table": ParticipantSummaryModel,
                    "value": ParticipantSummaryModel.questionnaireOnTheBasics
                },
                "aouDeceasedStatus": {
                    "field": "deceasedStatus",
                    "table": ParticipantSummaryModel,
                    "value": ParticipantSummaryModel.deceasedStatus,
                    "time": ParticipantSummaryModel.deceasedAuthored
                },
                "aouWithdrawalStatus": {
                    "field": "withdrawalStatus",
                    "table": ParticipantSummaryModel,
                    "value": ParticipantSummaryModel.withdrawalStatus,
                    "time": ParticipantSummaryModel.withdrawalAuthored
                },
                "aouDeactivationStatus": {
                    "field": "suspensionStatus",
                    "table": ParticipantSummaryModel,
                    "value": ParticipantSummaryModel.suspensionStatus,
                    "time": ParticipantSummaryModel.suspensionTime
                },
                "aouEnrollmentStatus": {
                    "field": "enrollmentStatus",
                    "table": ParticipantSummaryModel,
                    "value": ParticipantSummaryModel.enrollmentStatus,
                    "time": ParticipantSummaryModel.enrollmentStatusParticipantV3_2Time
                },
                "aouOverallHealthStatus": {
                    "field": "questionnaireOnHealthcareAccess",
                    "table": ParticipantSummaryModel,
                    "value": ParticipantSummaryModel.questionnaireOnHealthcareAccess,
                    "time": ParticipantSummaryModel.questionnaireOnHealthcareAccessAuthored
                },
                "aouLifestyleStatus": {
                    "field": "questionnaireOnLifestyle",
                    "table": ParticipantSummaryModel,
                    "value": ParticipantSummaryModel.questionnaireOnLifestyle,
                    "time": ParticipantSummaryModel.questionnaireOnLifestyleAuthored
                },
                "aouSDOHStatus": {
                    "field": "questionnaireOnSocialDeterminantsOfHealth",
                    "table": ParticipantSummaryModel,
                    "value": ParticipantSummaryModel.questionnaireOnSocialDeterminantsOfHealth,
                    "time": ParticipantSummaryModel.questionnaireOnSocialDeterminantsOfHealthAuthored
                },
            }
            result = field_lookup.get(value, None)
            if not result:
                raise Exception(f"Invalid value : {value}")
            return result
        except KeyError as err:
            raise err


def validation_error_message(errors):
    return {"errors": [error.formatted for error in errors]}


def error_message(message):
    return {"errors": message}
