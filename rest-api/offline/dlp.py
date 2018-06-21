import google.auth
import google_auth_httplib2
import json
import logging
import google.auth
""" IAM permissions required:
    serviceusage.services.use

request body for content inspection:

{
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
    "includeQuote":true
  }
}

"""


class DataLossPrevention(object):
  def __init__(self):
    self.credentials, self.project_id = google.auth.default(
      scopes=['https://www.googleapis.com/auth/cloud-platform'])
    logging.info('credential: %r.', self.credentials)
    logging.info('project_id: %r.', self.project_id)
    self.http = google_auth_httplib2.AuthorizedHttp(self.credentials)
    logging.info('http: %r.', self.http)

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
        "includeQuote": True
      }
    }

  def dlp_content_inspection(self, body):
    url = 'https://dlp.googleapis.com/v2/projects/%s/content:inspect' % self.project_id
    response, content = self.dlp_request(body, url)
    logging.info('response from dlp_content_inspection: %r.', response)
    logging.info('content from dlp_content_inspection: %r.', content)
    return response, content

  def dlp_request(self, body, url):
    response, content = self.http.request(method='POST', uri=url, body=body)
    logging.info('response from dlp_request: %r.', response)
    logging.info('content from dlp_request: %r.', content)
    return response, content

  # may subclass sqlExporter, call dlp whether or not transformf, call around the writer in
  # sqlexporter


def localdef():

  credentials, project_id = google.auth.default(
    scopes=['https://www.googleapis.com/auth/cloud-platform'])
  http = google_auth_httplib2.AuthorizedHttp(credentials)
  url = 'https://dlp.googleapis.com/v2/projects/%s/content:inspect' % project_id
  body = {
    "item": {
      "table": {
        "headers": [{"name": "column1"}],
        "rows": [{
          "values": [
            {"string_value": "My phone number is (206) 555-0123"},
          ]},
        ],
      }
    },
    "inspectConfig": {
      "infoTypes": [
        {
          "name": "PHONE_NUMBER"
        },
        {
          "name": "US_TOLLFREE_PHONE_NUMBER"
        }
      ],
      "minLikelihood": "POSSIBLE",
      "limits": {
        "maxFindingsPerItem": 0
      },
      "includeQuote": True
    }
  }

  response, content = http.request(method='POST', uri=url, body=json.dumps(body))
  print response
  print '^^^^^^^^^^^^^'
  print content
  return response, content

if __name__ == '__main__':
  # LocalDataLossPrevention()
  localdef()
