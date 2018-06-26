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
          "headers": [{"name":"column 1"}],
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
            "name":"EMAIL_ADDRESS"
          },
          {
            "name":"IP_ADDRESS"
          },
          {
            "name":"LAST_NAME"
          }
        ],
        "minLikelihood":"POSSIBLE",
        "limits":{
          "maxFindingsPerItem":0
        },
        "includeQuote": True
      }
    }
    self.sample_input = {
  "inspectJob":{
    "storageConfig":{
      "cloudStorageOptions":{
        "fileSet":{
          "url":"gs://test_dlp_bucket/*."
        },
        "bytesLimitPerFile":"1073741824"
      }
    },
    "timespanConfig":{
      "startTime":"2017-11-13T12:34:29.965633345Z ",
      "endTime":"2018-01-05T04:45:04.240912125Z "
    }
  },
  "inspectConfig":{
    "infoTypes":[
      {
        "name":"PHONE_NUMBER"
      }
    ],
    "excludeInfoTypes":False,
    "includeQuote":True,
    "minLikelihood":"LIKELY"
  },
  "actions":[
    {
      "saveFindings":{
        "outputConfig":{
          "table":{
            "projectId":self.project_id,
            "datasetId":"testingDLP"
          }
        }
      }
    }
  ],
}

  def dlp_content_inspection(self):
    # url = 'https://dlp.googleapis.com/v2/projects/%s/content:inspect?alt=json' % self.project_id
    url = 'https://dlp.googleapis.com/v2/projects/%s/dlpJobs' % self.project_id
    return self.dlp_request(url)

  def dlp_request(self, url):
    headers = {"Content-Type": "application/json"}
    response, content = self.http.request(method='POST', uri=url, headers=headers, body=json.dumps(
      self.sample_input))
    logging.info('response from dlp_request: %r.', response)
    logging.info('content from dlp_request: %r.', content)
    return response, content

  def setup_dlp_request(self, results):
    # results is a list of tuples. values is a list of dicts.
    return self.body

    fields = []
    r = []
    for row in results :
      if not fields :
        fields = row.keys()
      _row = {}
      for name in fields :
        _row[name] = str(row[name]) if row[name] is not None else ''
        r.append(_row)
      break
    r = json.dumps(r)
    self.body['item']['table']['rows'][0]['values'] = r
      #self.body['item']['table']['rows'][0]['values'] = [json.dumps(dict(r)) for r in results]
    return self.body


if __name__ == '__main__':
  d = DataLossPrevention()
  d.dlp_content_inspection(d.body)
