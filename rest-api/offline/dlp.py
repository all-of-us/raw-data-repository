import google.auth
import google_auth_httplib2
import json


class DataLossPrevention(object):
  def __init__(self):
    self.credentials, self.project_id = google.auth.default(
      scopes=['https://www.googleapis.com/auth/cloud-platform'])

    self.http = google_auth_httplib2.AuthorizedHttp(self.credentials)

    self.body = {
      "item":{
        "table":{
          "headers": [{"name":"column1"}],
          "rows": [{
            "values":[
              {"string_value": "My phone number is (206) 555-0123"},
            ]},
          ],
        }
      },
      "inspectConfig":{
        "infoTypes":[
          {
            "name":"PHONE_NUMBER"
          },
          {
            "name":"US_TOLLFREE_PHONE_NUMBER"
          }
        ],
        "minLikelihood":"POSSIBLE",
        "limits":{
          "maxFindingsPerItem":0
        },
        "includeQuote":True
      }
    }

  def dlp_content_inspection(self, body):
    url = 'https://dlp.googleapis.com/v2/projects/%s/content:inspect' % self.project_id
    response, content = self.dlp_request(body, url)
    return response, content

  def dlp_request(self, body, url):
    response, content = self.http.request(method='POST', uri=url, body=json.dumps(body))
    return response, content



    print response
    print '^^^^^^^^^^^^^'
    print content

  # may subclass sqlExporter, call dlp whether or not transformf, call around the writer in
  # sqlexporter
