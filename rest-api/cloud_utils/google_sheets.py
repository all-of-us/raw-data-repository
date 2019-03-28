import csv
from StringIO import StringIO

import requests


class GoogleSheetCSVReader(csv.DictReader):

  def __init__(self, sheet_id, gid=0, *args, **kwds):
    self._sheet_id = sheet_id
    self._gid = gid
    response = requests.get(self._get_sheet_url(self._sheet_id, self._gid))
    csv.DictReader.__init__(self, StringIO.StringIO(response.text), *args, **kwds)

  @staticmethod
  def _get_sheet_url(sheet_id, gid):
    return (
      "https://docs.google.com/spreadsheets/d/{id}/export"
      "?format=csv"
      "&id={id}"
      "&gid={gid}"
    ).format(
      id=sheet_id,
      gid=gid
    )
