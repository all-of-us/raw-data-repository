import csv
import httplib2
import StringIO


class HttpException(Exception):
  pass


class GoogleSheetCSVReader(csv.DictReader):

  def __init__(self, sheet_id, gid=0, *args, **kwds):
    self._sheet_id = sheet_id
    self._gid = gid
    url = self._get_sheet_url(self._sheet_id, self._gid)
    response_body = self._get_response_body(url)
    csv.DictReader.__init__(self, StringIO.StringIO(response_body), *args, **kwds)

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

  @staticmethod
  def _get_response_body(url):
    http = httplib2.Http()
    response, content = http.request(url, "GET")
    if response.status != 200:
      raise HttpException(' '.join([
        'Could not retreive response:',
        response.status,
        response.reason,
        content
      ]))
    return content
