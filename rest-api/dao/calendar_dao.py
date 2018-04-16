from dao.base_dao import BaseDao
from model.calendar import Calendar

class CalendarDao(BaseDao):

  def __init__(self):
    super(CalendarDao, self).__init__(Calendar)

  def get_id(self, calendar):
    return calendar.day
