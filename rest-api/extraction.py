from fhirclient.models.fhirelementfactory import FHIRElementFactory
from questionnaire_response import DAO as QuestionnaireResponseDAO, QuestionnaireResponse
from participant import DAO as ParticipantDAO, Participant, GenderIdentity, MembershipTier
import json
import os

# TODO: Grab this from a database
gender_mappings = {
    "Questionnaire/{questionnaire_id}": {
        "linkId": "sex",
        "values": [{
            "from": {
                "valueCoding": {"code": "f"}
                },
            "to": "FEMALE"
            },{
            "from": {
                "valueCoding": {"code": "m"}
                },
            "to": "MALE"
        }]
    }
}

def as_list(v):
    if type(v) != list:
        return [v]
    return v

def get_questions_by_link_id(qr, target_link_id):
    ret = []
    if hasattr(qr, 'linkId') and qr.linkId == target_link_id:
        ret += [qr]
    for prop in ('question', 'group', 'answer'):
        if hasattr(qr, prop):
            ret += [v
                    for q in as_list(getattr(qr, prop))
                    for v in get_questions_by_link_id(q, target_link_id)]
    return ret


def answer_matches(answer, template):
    for k,v in template.iteritems():
        if not hasattr(answer, k):
            return False
        if type(v) == dict:
            if not answer_matches(getattr(answer, k), v):
                return False
        elif v != getattr(answer, k):
            return False
    return True

class Extractor(object): pass

class PropertyExtractor(Extractor):
    model = Participant
    def __init__(self, property):
        self.property = property
    def extract(self, participant):
        # return getattr(participant, self.property)
        return str(getattr(participant, self.property))


class QuestionnaireResponseExtractor(Extractor):
    model = QuestionnaireResponse
    def __init__(self, target_type, mappings):
        self.target_type = target_type
        self.mappings = mappings

    def extract(self, obj):
        r = obj.resource
        r_fhir = FHIRElementFactory.instantiate(r['resourceType'], r)
        source_questionnaire = r_fhir.questionnaire.reference
        if source_questionnaire not in self.mappings:
            raise BaseException('No mappings for %s'%source_questionnaire)

        mapping = self.mappings[source_questionnaire]
        link_id = mapping['linkId']
        qs = get_questions_by_link_id(r_fhir, link_id)
        if len(qs) == 1 and len(qs[0].answer) == 1:
            answer = qs[0].answer[0]
            for v in mapping['values']:
                if answer_matches(answer, v['from']):
                    #return self.target_type(v['to'])
                    return str(v['to'])

extractors = {
        'gender': QuestionnaireResponseExtractor(GenderIdentity, gender_mappings),
        'membership_tier': PropertyExtractor("membership_tier"),
        }


# Hacky way to test this, by adding an API endpoint

from flask import Flask, request
from flask.ext.restful import Resource, reqparse, abort


def extract_from_history(q):
    return {
        'date': q.date.isoformat(),
        'properties': {
            ename: extractor.extract(q.obj)
            for ename, extractor in extractors.iteritems()
            if type(q.obj) == extractor.model
        }
    }

class Extraction(Resource):
    def get(self):
        sources = (ParticipantDAO, QuestionnaireResponseDAO)
        ret = [extract_from_history(q)
                for dao in sources
                for q in dao.history_model.query()
                ]
        return ret
