import datetime
import json


class RdrJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)
