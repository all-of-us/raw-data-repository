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
  def __init__(self, gcs_path=None):
    httplib2.debuglevel = 4
    self.credentials, self.project_id = google.auth.default(
      scopes=['https://www.googleapis.com/auth/cloud-platform'])
    self.http = google_auth_httplib2.AuthorizedHttp(self.credentials)
    self.content_inspection_template = {
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
    if gcs_path:
      self.gcs_path = gcs_path
      self.bucket_inspection_template = {
  "inspectJob":{
    "storageConfig":{
      "cloudStorageOptions":{
        "fileSet":{
          "url":"gs://%s/" % self.gcs_path
        },
        "bytesLimitPerFile":"1073741824"
      }
    },
    "timespanConfig":{
      "startTime":"2018-06-25",
      "endTime":"2018-06-26"
    }
  },
  "inspectConfig":{
    "infoTypes":[
      {
        "name":"LAST_NAME"
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

  def dlp_content_inspection(self, results):
    url = 'https://dlp.googleapis.com/v2/projects/%s/content:inspect?alt=json' % self.project_id
    return self.dlp_request(url, results)

  def dlp_bucket_inspection(self):
    url = 'https://dlp.googleapis.com/v2/projects/%s/dlpJobs' % self.project_id
    return self.dlp_request(url)

  def dlp_request(self, url, results=None):
    headers = {"Content-Type": "application/json"}
    if results:
      response, content = self.http.request(method='POST', uri=url, headers=headers,
                                            body=json.dumps(results))
    else:
      response, content = self.http.request(method='POST', uri=url, headers=headers)
    logging.info('response from dlp_request: %r.', response)
    logging.info('content from dlp_request: %r.', content)
    return response, content

  def setup_dlp_content_inspection_request(self, results):
    # results is a list of tuples. values is a list of dicts.
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
    self.content_inspection_template['item']['table']['rows'][0]['values'] = r
    return self.content_inspection_template
