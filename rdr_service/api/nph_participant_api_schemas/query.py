from graphene import ObjectType, Schema, String
from graphene import relay


from typing import List, Dict
from rdr_service.api.nph_participant_api_schemas import db, schema
from rdr_service.api.nph_participant_api_schemas.util import expected_fields, camel_case_fields
import rdr_service.api.nph_participant_api_schemas.nested_event as ne


def fetch_json(field_name: str, field_value: str, field_type: str) -> List[Dict]:
    if field_type == 'event':
        nested_json_field = ne.Event(field_name)
    elif field_type == 'event_collection':
        nested_json_field = ne.EventCollection(field_name)
    elif field_type == 'sample_collection':
        nested_json_field = ne.SampleCollection(field_name)
    else:
        nested_json_field = ne.Sample(field_name)
    return nested_json_field.fetch_data(field_value)


def fetch(field_type: str, field: str, field_value: str) -> List[Dict]:
    if field_type == "date":
        return [x for x in db.datas if field_value == x.get(field)]
    elif field_type == "int":
        return [x for x in db.datas if int(field_value) == x.get(field)]
    else:
        return [x for x in db.datas if field_value == x.get(field)]


def check_field(field_name: str) -> [bool, str, List]:

    for field_type, field_names in expected_fields.items():
        if "." in field_name:
            names = field_name.split(".")
            if names[0] in field_names:
                return True, field_type
        else:
            if field_name in field_names:
                return True, field_type
    return False, "", []


def fetch_data(field_type: str, field_name: str, field_value: str) -> List[Dict]:
    if field_type in ["date", "int", "string"]:
        field = camel_case_fields.get(field_name)
        return fetch(field_type, field, field_value)
    elif field_type in ["event_collection", "event", "sample_collection"]:
        return fetch_json(field_name, field_value, field_type)


class AllParticipantQuery(ObjectType):
    class Meta:
        interfaces = (relay.Node,)
        connection_class = schema.AllParticipantConnection

    all_participant = relay.ConnectionField(schema.AllParticipantConnection)
    participant = relay.ConnectionField(schema.AllParticipantConnection, field_name=String(required=True),
                                        field_value=String(required=True))

    def resolve_all_participant(root):
        return db.datas

    def resolve_participant(root, field_name, field_value):
        existed, field_type = check_field(field_name)
        try:
            if existed:
                return fetch_data(field_type, field_name, field_value)
            raise KeyError(f"Cannot query '{field_name}': field is not available.")
        except Exception as ex:
            raise ex


participant_schema = Schema(query=AllParticipantQuery)

# event_query = '{ participant (fieldName: "AouBasicsQuestionnaire.value", fieldValue: "HAHA"){ totalCount resultCount
# pageInfo { startCursor endCursor hasNextPage } edges { node { sampleSa1{ordered{parent{current{value time}
# historical{value time} } child{current {value time} historical{value time}}} stored{parent{current {value time}
# historical{value time}} child{current{value time} historical{value time}}}}  } } } }'
#
# field_query = '{ participant (fieldName: "ParticipantNphId", fieldValue: "3"){ totalCount resultCount pageInfo
# { startCursor endCursor hasNextPage } ' \
#         'edges { node {aouBasicsQuestionnaire{value time} } } } }'
#
# Sample_collection_query = '{ participant (fieldName: "SampleSA1.ordered.parent.historical.value", fieldValue: "F")
# { totalCount resultCount pageInfo { startCursor endCursor hasNextPage } ' \
#         'edges { node {aouBasicsQuestionnaire{value time} } } } }'
#
# result = participant_schema.execute(Sample_collection_query)
#
# if result.errors:
#     print(f"Error: {result.errors}")
# else:
#     print(json.dumps(result.data, indent=4))
