import dev_appserver
import appengine_config
import oauth2client
import google.auth.iam
from oauth2client.contrib import appengine
from oauth2client.service_account import ServiceAccountCredentials
from client import Client
import requests
client = Client(default_instance='localhost')

list_keys = 'https://iam.googleapis.com/v1/projects/pmi-drc-api-test/serviceAccounts/awardee' \
            '-pitt@pmi-drc-api-test.iam.gserviceaccount.com/keys'
r = requests.post(list_keys)
print r

