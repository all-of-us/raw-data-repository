from model.base import Base
from model.participant import Participant, ParticipantHistory
from model.hpo_id import HPOId

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///:memory:', echo=True)
bar = Participant
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

hpoId = HPOId(id=1, name='UNSET')
session.add(hpoId)
session.commit()

p = Participant(id=1, version=1, biobankId=2, hpoId=1)
session.add(p)
session.commit()
