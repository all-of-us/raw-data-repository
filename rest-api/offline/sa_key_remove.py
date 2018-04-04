# import google.auth.iam as ga
# from oauth2client.service_account import ServiceAccountCredentials
# from google.auth.credentials import Credentials
# from google.oauth2 import credentials, id_token, service_account
# import httplib2
# from oauth2client.contrib import gce
# import sys
# creds = 'pmi-drc-api-test@appspot.gserviceaccount.com'
# SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
# signer = ga.Signer(SCOPE, creds, creds)
#
# creds_file = sys.argv[1]
# sa = ServiceAccountCredentials('michael.mead@pmi-ops.org', signer)
# print sa.id_token
# print sa.access_token
# print sa.serialization_data
# print sa.scopes
# print sa.client_secret
# print sa.client_id
#
# print signer.key_id

token = '0d41bd307b8d0c04239062484739c0e001e2c4fd'
# curl 'https://iam.googleapis.com/v1/projects/pmi-drc-api-test/serviceAccounts' \
#   -H 'Authorization: Bearer [YOUR_BEARER_TOKEN]' \
#   -H 'Accept: application/json' \
#   --compressed

headers = {"Authorization":"Bearer 0d41bd307b8d0c04239062484739c0e001e2c4fd" , "Accept":"application/json"}

import requests
r = requests.get('https://iam.googleapis.com/v1/projects/pmi-drc-api-test/serviceAccounts',
                 headers=headers)
print r
