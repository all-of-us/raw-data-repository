import logging
import sys

import requests


logger = logging.getLogger()
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)

project_id = sys.argv[1]
version_id = sys.argv[2]
app_engine_url = f'https://{version_id}-dot-{project_id}.appspot.com/'

logger.info(f'Checking "{app_engine_url}" for a running instance')
response = requests.get(app_engine_url)

if response.status_code != 200:
    logger.error(f'ERROR: server responded with {response.status_code} status code')
    exit(1)
else:
    logger.info('Instance successfully responded!')
