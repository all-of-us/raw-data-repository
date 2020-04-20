
from sqlalchemy import event, Index, Column, Integer, ForeignKey, BigInteger

from rdr_service.model.base import Base, model_insert_listener
from rdr_service.model.utils import UTCDateTime6


class ResourceSearchResults(Base):
    """
    Resource Search Results Model
    """
    __tablename__ = "resource_search_results"

    # Primary Key
    id = Column("id", BigInteger, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    searchKey = Column("search_key", Integer, nullable=False)
    pageNo = Column("pageNo", Integer, nullable=False)
    resourceDataID = Column("resource_data_id", ForeignKey("resource_data.id"), nullable=False)


Index("ix_res_data_type_modified_hpo_id", ResourceSearchResults.searchKey, ResourceSearchResults.pageNo)

event.listen(ResourceSearchResults, "before_insert", model_insert_listener)

