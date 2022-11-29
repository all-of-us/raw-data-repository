import names
import faker
from datetime import datetime

fake = faker.Faker()

datas = []


def get_gender():
    profile = fake.simple_profile()
    return profile.get('sex')


def get_state():
    states = fake.military_state()
    return states


for num in range(1, 1001):
    data = {'participant_nph_id': num}
    gender = get_gender()
    first_name = names.get_first_name(gender)
    last_name = names.get_last_name()
    data['first_name'] = first_name
    data['last_name'] = last_name
    data['state'] = get_state()
    data['city'] = first_name
    data['street_address'] = fake.address().replace("\n", " ")
    data['gender'] = gender
    data['aou_basics_questionnaire'] = {'value': "HAHA", 'time': datetime.utcnow()}
    data['sample_sa_1'] = {"ordered": [{"parent": [{"current": {"value": gender, "time": datetime.utcnow()},
                                                   "historical": [{"value": gender, "time": datetime.utcnow()}]}],
                                       "child": [{"current": {"value": gender, "time": datetime.utcnow()},
                                                  "historical": [{"value": gender, "time": datetime.utcnow()}]}]}],
                           "stored": [{"parent": [{"current": {"value": gender, "time": datetime.utcnow()},
                                                   "historical": [{"value": gender, "time": datetime.utcnow()}]}],
                                       "child": [{"current": {"value": gender, "time": datetime.utcnow()},
                                                  "historical": [{"value": gender, "time": datetime.utcnow()}]}]}]
                           }

    datas.append(data)

