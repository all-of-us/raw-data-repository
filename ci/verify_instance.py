import logging
import sys

import requests


logger = logging.getLogger()
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)

response = requests.get('https://ks-check-instance-running-1-dot-all-of-us-rdr-sandbox.appspot.com/')

if response.status_code != 200:
    logger.error(f'ERROR: server responded with {response.status_code} status code')
    exit(1)
else:
    logger.info('Instance successfully responded!')
