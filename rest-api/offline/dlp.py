import google.auth
import google_auth_httplib2
import json
import logging
import google.auth
import httplib2
""" IAM permissions required for Data Loss Prevention API:
    serviceusage.services.use
"""


class DataLossPrevention(object):
  def __init__(self):
    httplib2.debuglevel = 4
    self.credentials, self.project_id = google.auth.default(
      scopes=['https://www.googleapis.com/auth/cloud-platform'])
    self.http = google_auth_httplib2.AuthorizedHttp(self.credentials)

    self.body = {
      "item":{
        "table":{
          "headers": [{"dlp header inspection":""}],
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
        "includeQuote": True
      }
    }

  def dlp_content_inspection(self, body):
    url = 'https://dlp.googleapis.com/v2/projects/%s/content:inspect?alt=json' % self.project_id
    return self.dlp_request(body, url)

  def dlp_request(self, body, url):
    headers = {"Content-Type": "application/json"}
    response, content = self.http.request(method='POST', uri=url, headers=headers, body=json.dumps(
      body))
    logging.info('response from dlp_request: %r.', response)
    logging.info('content from dlp_request: %r.', content)
    return response, content

  def setup_dlp_request(self, results):
    self.body['items']['table']['values'] = [results]
    return self.body


if __name__ == '__main__':
  d = DataLossPrevention()
  d.dlp_content_inspection(d.body)
