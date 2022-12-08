from sqlalchemy import Column, Integer, String

from rdr_service.model.base import RexBase


class TestModel(RexBase):

    __tablename__ = "test"

    id = Column("id", Integer, primary_key=True)
    name = Column("name", String(256), nullable=False)
    place = Column("place", String(256), nullable=True)
