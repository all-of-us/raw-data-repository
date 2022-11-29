from typing import List
from rdr_service.api.nph_participant_api_schemas import db
from rdr_service.api.nph_participant_api_schemas.util import camel_case_fields


class Event:

    # For Event Data: e.g. AouBasicsQuestionnaire.value  names[0] = Field; names[1] = parameter

    def __init__(self, field_name: str):
        self.field, self.parameter = field_name.split(".")
        self.field = camel_case_fields.get(self.field)

    def fetch_data(self, value: str) -> List:
        return [x for x in db.datas if value == x.get(self.field).get(self.parameter)]


class EventCollection:

    # For EventCollection Data: e.g. InformedConsentModule1.current.value  names[0]= Field;
    # names[1]= current/historical names[2]= parameter

    def __init__(self, field_name: str):
        self.field, self.sub_field, self.parameter = field_name.split(".")
        self.field = camel_case_fields.get(self.field)

    def fetch_data(self, value: str) -> List:
        if self.sub_field == 'current':
            return self._fetch_current(value)
        elif self.sub_field == 'historical':
            return self._fetch_historical(value)

    def _fetch_current(self, value: str):
        return [x for x in db.datas if value == x.get(self.field).get(self.sub_field).get(self.parameter)]

    def _fetch_historical(self, value: str):
        result = []
        for each in db.datas:
            [result.append(each) for x in each.get(self.field).get(self.sub_field) if x.get(self.parameter) == value]
        return result


class Sample:

    # For Sample Data: e.g. InformedConsentModule1.parent.current.value  names[0]= Field;
    # names[1]= parent/child names[2]= current/historical names[3]= value/time

    def __init__(self, field_name: str):
        self.field, self.sub_field, self.sub_parameter, self.parameter = field_name.split(".")
        self.field = camel_case_fields.get(self.field)

    def fetch_data(self, value: str) -> List:
        return self._fetch(value)

    def _fetch(self, value: str):
        result = []
        for x in db.datas.get(self.field):
            if x.get(self.sub_field).get(self.sub_parameter).get(self.parameter) == value:
                result.append(x)
        return result


class SampleCollection:

    # For Sample Data: e.g. InformedConsentModule1.stored.parent.current.value  names[0]= Field;
    # names[1]= parent/child names[2]= current/historical names[3]= value/time

    def __init__(self, field_name: str):
        self.field, self.title, self.sub_field, self.sub_parameter, self.parameter = field_name.split(".")
        self.field = camel_case_fields.get(self.field)

    def fetch_data(self, value: str) -> List:
        return self._fetch(value)

    def _fetch(self, value: str):
        append = False
        result = []
        for each in db.datas:
            data = [x.get(self.sub_field) for x in each.get(self.field).get(self.title)][0]
            if self.sub_parameter == "current":
                [result.append(each) for a in [x.get(self.sub_parameter) for x in data]
                 if a.get(self.parameter) == value]
            if self.sub_parameter == "historical":
                for a in [x.get(self.sub_parameter) for x in data]:
                    for b in a:
                        if b.get(self.parameter) == value:
                            append = True
                            break
                    if append:
                        result.append(each)
                append = False
        return result
