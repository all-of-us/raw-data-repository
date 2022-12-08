from sqlalchemy import Column, Integer, String

from rdr_service.model.base import NphBase


class TestModel(NphBase):

    __tablename__ = "test"

    id = Column("id", Integer, primary_key=True)
    name = Column("name", String(256), nullable=False)
    place = Column("place", String(256), nullable=True)
