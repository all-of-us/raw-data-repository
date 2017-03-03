from dao.base_dao import BaseDao
from model.hpo import HPO

class HPODao(BaseDao):
  def __init__(self):
    super(HPODao, self).__init__(HPO, cache_ttl_seconds=600)
    
  def get_by_name_with_session(self, session, name):  
    return session.query(HPO).filter(HPO.name == name).first()

  def get_by_name(self, name):
    with self.session() as session:
      return self.get_by_name_with_session(session, name)