from graphene import (
    ObjectType,
    String,
    Int,
    Date,
    Schema,
    Field
)

from rdr_service.api.nph_participant_api_schemas.schema import Event, EventCollection, SampleCollection, Sample
from datetime import datetime
from unittest import TestCase


mock_time_value = datetime(2022, 12, 2)

MOCK_DATA = {
    "event": {"value": "e_value", "time": mock_time_value},
    "string": "sample_string_value",
    "int": 0,
    "date": mock_time_value,
    "event_collection": {"current": {"value": "ec_current_value", "time": mock_time_value},
                         "historical": [{"value": "ec_historical_value1", "time": mock_time_value},
                                        {"value": "ec_historical_value2", "time": mock_time_value}]},
    "sample": {"parent": [{"current": {"value": "s_parent_current_value", "time": mock_time_value},
                          "historical": [{"value": "s_parent_historical_value1", "time": mock_time_value},
                                         {"value": "sample_parent_historical_value2", "time": mock_time_value}]}
                          ],
               "child": [{"current": {"value": "s_child_current_value", "time": mock_time_value},
                          "historical": [{"value": "s_child_historical_value1", "time": mock_time_value},
                                         {"value": "s_child_historical_value2", "time": mock_time_value}]}
                         ]
               },
    "sample_collection": {"ordered": [{"parent": [{"current": {"value": "sc_ordered_parent_current_value",
                                                               "time": mock_time_value},
                                                   "historical": [{"value": "sc_ordered_parent_historical_value1",
                                                                   "time": mock_time_value},
                                                                  {"value": "sc_ordered_parent_historical_value2",
                                                                   "time": mock_time_value}]}
                                                  ],
                                       "child": [{"current": {"value": "sc_ordered_child_current_value",
                                                              "time": mock_time_value},
                                                  "historical": [{"value": "sc_ordered_child_historical_value1",
                                                                  "time": mock_time_value},
                                                                 {"value": "sc_ordered_child_historical_value2",
                                                                  "time": mock_time_value}]
                                                  }
                                                 ]
                                       }
                                      ],
                          "stored": [{"parent": [{"current": {"value": "sc_stored_parent_current_value",
                                                              "time": mock_time_value},
                                                  "historical": [{"value": "sc_stored_parent_historical_value1",
                                                                  "time": mock_time_value},
                                                                 {"value": "sc_stored_parent_historical_value2",
                                                                  "time": mock_time_value}]}
                                                 ],
                                      "child": [{"current": {"value": "sc_stored_child_current_value",
                                                             "time": mock_time_value},
                                                 "historical": [{"value": "sc_stored_child_historical_value1",
                                                                 "time": mock_time_value},
                                                                {"value": "sc_stored_child_historical_value2",
                                                                 "time": mock_time_value}]
                                                 }
                                                ]
                                      }
                                     ]
                          }
}


class MockObjectType(ObjectType):

    event = Field(Event)
    event_collection = Field(EventCollection)
    sample = Field(Sample)
    sample_collection = Field(SampleCollection)
    string = Field(String)
    int = Field(Int)
    date = Field(Date)


class MockQuery(ObjectType):

    data = Field(MockObjectType)

    def resolve_data(root, info, **kwargs):
        print(info, kwargs)
        return MOCK_DATA


MOCK_SCHEMA = Schema(query=MockQuery)


class TestSchema(TestCase):

    def test_string_schema(self):
        mock_event_query = '{data {string}}'
        result = MOCK_SCHEMA.execute(mock_event_query)
        self.assertEqual('sample_string_value', result.data.get('data').get('string'))

    def test_int_schema(self):
        mock_event_query = '{data {int}}'
        result = MOCK_SCHEMA.execute(mock_event_query)
        self.assertEqual(0, result.data.get('data').get('int'))

    def test_date_schema(self):
        mock_event_query = '{data {date}}'
        result = MOCK_SCHEMA.execute(mock_event_query)
        self.assertEqual('2022-12-02', result.data.get('data').get('date'))

    def test_event_schema(self):
        mock_event_query = '{data {event{value time}}}'
        result = MOCK_SCHEMA.execute(mock_event_query)
        self.assertEqual('e_value', result.data.get('data').get('event').get('value'))
        self.assertEqual('2022-12-02T00:00:00', result.data.get('data').get('event').get('time'))

    def test_event_collection_schema(self):
        mock_event_query = '{data {eventCollection{current{value time} historical{value time}}}}'
        result = MOCK_SCHEMA.execute(mock_event_query)
        self.assertEqual('ec_current_value', result.data.get('data').get('eventCollection').get('current').get('value'))
        self.assertEqual('2022-12-02T00:00:00', result.data.get('data').get('eventCollection').get('current')
                         .get('time'))
        self.assertEqual('ec_historical_value1', result.data.get('data').get('eventCollection').get('historical')[0]
                         .get('value'))
        self.assertEqual('2022-12-02T00:00:00', result.data.get('data').get('eventCollection').get('historical')[0]
                         .get('time'))
        self.assertEqual('ec_historical_value2', result.data.get('data').get('eventCollection').get('historical')[1]
                         .get('value'))
        self.assertEqual('2022-12-02T00:00:00', result.data.get('data').get('eventCollection').get('historical')[1]
                         .get('time'))

    def test_sample_schema(self):
        mock_event_query = '{data {sample{parent{current{value time} historical{value time}} ' \
                           'child{current{value time} historical{value time}}}}}'
        result = MOCK_SCHEMA.execute(mock_event_query)
        self.assertEqual('s_parent_current_value',
                         result.data.get('data').get('sample').get('parent')[0].get('current').get('value'))
        self.assertEqual('2022-12-02T00:00:00',
                         result.data.get('data').get('sample').get('parent')[0].get('current').get('time'))
        self.assertEqual('s_parent_historical_value1',
                         result.data.get('data').get('sample').get('parent')[0].get('historical')[0].get('value'))
        self.assertEqual('2022-12-02T00:00:00',
                         result.data.get('data').get('sample').get('parent')[0].get('historical')[0].get('time'))
        self.assertEqual('sample_parent_historical_value2',
                         result.data.get('data').get('sample').get('parent')[0].get('historical')[1].get('value'))
        self.assertEqual('2022-12-02T00:00:00',
                         result.data.get('data').get('sample').get('parent')[0].get('historical')[1].get('time'))
        self.assertEqual('s_child_current_value',
                         result.data.get('data').get('sample').get('child')[0].get('current').get('value'))
        self.assertEqual('2022-12-02T00:00:00',
                         result.data.get('data').get('sample').get('child')[0].get('current').get('time'))
        self.assertEqual('s_child_historical_value1',
                         result.data.get('data').get('sample').get('child')[0].get('historical')[0].get('value'))
        self.assertEqual('2022-12-02T00:00:00',
                         result.data.get('data').get('sample').get('child')[0].get('historical')[0].get('time'))
        self.assertEqual('s_child_historical_value2',
                         result.data.get('data').get('sample').get('child')[0].get('historical')[1].get('value'))
        self.assertEqual('2022-12-02T00:00:00',
                         result.data.get('data').get('sample').get('child')[0].get('historical')[1].get('time'))

    def test_sample_collection_schema(self):
        mock_event_query = '''{data {sampleCollection{	ordered{parent{current{value time}historical{value time}}child
        {current {value time} historical {value time}}}	stored{parent {current{value time} historical{value time}}
        child	{current{value time} historical{value time}} }}}}'''
        result = MOCK_SCHEMA.execute(mock_event_query)
        self.assertEqual('sc_ordered_parent_current_value', result.data.get('data').get('sampleCollection')
                         .get('ordered')[0].get('parent')[0].get('current').get('value'))
        self.assertEqual('2022-12-02T00:00:00', result.data.get('data').get('sampleCollection')
                         .get('ordered')[0].get('parent')[0].get('current').get('time'))
        self.assertEqual('sc_ordered_child_historical_value1', result.data.get('data').get('sampleCollection')
                         .get('ordered')[0].get('child')[0].get('historical')[0].get('value'))
        self.assertEqual('2022-12-02T00:00:00', result.data.get('data').get('sampleCollection')
                         .get('ordered')[0].get('child')[0].get('historical')[0].get('time'))
        self.assertEqual('sc_ordered_child_historical_value2', result.data.get('data').get('sampleCollection')
                         .get('ordered')[0].get('child')[0].get('historical')[1].get('value'))
        self.assertEqual('2022-12-02T00:00:00', result.data.get('data').get('sampleCollection')
                         .get('ordered')[0].get('child')[0].get('historical')[1].get('time'))
        self.assertEqual('sc_stored_parent_current_value', result.data.get('data').get('sampleCollection')
                         .get('stored')[0].get('parent')[0].get('current').get('value'))
        self.assertEqual('2022-12-02T00:00:00', result.data.get('data').get('sampleCollection')
                         .get('stored')[0].get('parent')[0].get('current').get('time'))
        self.assertEqual('sc_stored_child_historical_value1', result.data.get('data').get('sampleCollection')
                         .get('stored')[0].get('child')[0].get('historical')[0].get('value'))
        self.assertEqual('2022-12-02T00:00:00', result.data.get('data').get('sampleCollection')
                         .get('stored')[0].get('child')[0].get('historical')[0].get('time'))
        self.assertEqual('sc_stored_child_historical_value2', result.data.get('data').get('sampleCollection')
                         .get('stored')[0].get('child')[0].get('historical')[1].get('value'))
        self.assertEqual('2022-12-02T00:00:00', result.data.get('data').get('sampleCollection')
                         .get('stored')[0].get('child')[0].get('historical')[1].get('time'))
