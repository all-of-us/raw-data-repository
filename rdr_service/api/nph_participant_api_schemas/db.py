from collections import defaultdict
import faker

from rdr_service.dao import database_factory
from rdr_service.model.participant import Participant
from rdr_service.model.nph_sample import NphSample

fake = faker.Faker()


with database_factory.get_database().session() as session:
    # session.query(Participant).delete()

    num = session.query(Participant).count()
    print(f'NPH TESTING: found {num} participants')

    if num < 10:
        print('NPH TESTING: generating test data')

        for index in range(1000):
            participant = Participant(
                biobankId=fake.random.randint(100000000, 999999999),
                participantId=fake.random.randint(100000000, 999999999),
                version=1,
                lastModified=fake.date_time_this_decade(),
                signUpTime=fake.date_time_this_decade(),
                withdrawalStatus=1,
                suspensionStatus=1,
                participantOrigin='test',
                hpoId=0
            )
            session.add(participant)

            session.add(
                NphSample(
                    test='SA2',
                    status='received',
                    time=fake.date_time_this_decade(),
                    participant=participant,
                    children=[NphSample(
                        test='SA2',
                        status='disposed',
                        time=fake.date_time_this_decade()
                    )]
                )
            )
            session.add(
                NphSample(
                    test='RU3',
                    status='disposed',
                    time=fake.date_time_this_decade(),
                    participant=participant
                )
            )


def loadParticipantData(query):
    with database_factory.get_database().session() as sessions:
        query.session = sessions

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

        return results

#
# for num in range(1, 1001):
#     data = {'participant_nph_id': num}
#     gender = get_gender()
#     first_name = names.get_first_name(gender)
#     last_name = names.get_last_name()
#     data['first_name'] = first_name
#     data['last_name'] = last_name
#     data['state'] = get_state()
#     data['city'] = first_name
#     data['street_address'] = fake.address().replace("\n", " ")
#     data['gender'] = gender
#     data['food_insecurity'] = {"current": {"value": gender, "time": datetime.utcnow()}, "historical": [
#         {"value": gender, "time": datetime.utcnow()}, {"value": names.get_first_name(gender),
#                                                        "time": datetime.utcnow()},
#         {"value": names.get_first_name(gender), "time": datetime.utcnow()}]}
#     data['aou_basics_questionnaire'] = {'value': "HAHA", 'time': datetime.utcnow()}
#     data['sample_sa_1'] = {"ordered": [{"parent": [{"current": {"value": gender, "time": datetime.utcnow()},
#                                                    "historical": [{"value": gender, "time": datetime.utcnow()}]}],
#                                        "child": [{"current": {"value": gender, "time": datetime.utcnow()},
#                                                   "historical": [{"value": gender, "time": datetime.utcnow()}]}]}],
#                            "stored": [{"parent": [{"current": {"value": gender, "time": datetime.utcnow()},
#                                                    "historical": [{"value": gender, "time": datetime.utcnow()}]}],
#                                        "child": [{"current": {"value": gender, "time": datetime.utcnow()},
#                                                   "historical": [{"value": gender, "time": datetime.utcnow()}]}]}]
#                            }
#
#     datas.append(data)
#
