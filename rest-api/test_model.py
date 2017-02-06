from model.base import Base
from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from model.hpo import HPO

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///:memory:', echo=True)
bar = Participant
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

hpo = HPO(id=1, name='UNSET')
session.add(hpo)
session.commit()

p = Participant(id=1, version=1, biobankId=2, hpoId=1)
session.add(p)
session.commit()
