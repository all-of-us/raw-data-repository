
import sqlalchemy as sa

from rdr_service.model.base import Base


class NphSample(Base):
    __tablename__ = 'nph_sample'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    test = sa.Column(sa.String(40))
    status = sa.Column(sa.String(40))
    time = sa.Column(sa.DateTime)

    participant_id = sa.Column(sa.Integer, sa.ForeignKey('participant.participant_id'))
    participant = sa.orm.relationship('Participant', foreign_keys='NphSample.participant_id', backref='samples')

    parent_id = sa.Column(sa.Integer, sa.ForeignKey('nph_sample.id'))
    children = sa.orm.relationship('NphSample')
