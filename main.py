#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import argparse
import subprocess
import sys
import os

from rdr_service.services.system_utils import setup_i18n
from rdr_service.main import app


if __name__ == '__main__':
    setup_i18n()

    parser = argparse.ArgumentParser(prog='rdr-service', description="RDR web service")
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--flask", help="launch flask app", default=False, action="store_true")  # noqa
    parser.add_argument("--celery", help="launch celery app worker", default=False, action="store_true")  # noqa
    parser.add_argument("--service", help="launch supervisor and start web service", default=False,
                            action="store_true")  # noqa
    parser.add_argument("--unittests", help="enable unittest mode", default=False, action="store_true")  # noqa

    args = parser.parse_args()
    env = dict(os.environ)
    if args.unittests:
        env["UNITTEST_FLAG"] = "True"

    if args.flask and args.celery:
        print("Flask and Celery options may not be combined, exiting.")
        exit(1)

    if args.flask:
        # This is used when running locally only. When deploying to Google App
        # Engine, a webserver process such as Gunicorn will serve the app. This
        # can be configured by adding an `entrypoint` to app.yaml.
        app.run(host='127.0.0.1', port=8080, debug=True)
        exit(0)

    if args.celery:
        p_args = ['celery', '-A', 'rdr_service.services.flask:celery', 'worker',
                  '--loglevel={0}'.format('debug' if '--debug' in sys.argv else 'info')]
        p = subprocess.Popen(p_args, env=env)
        p.wait()
        exit(0)

    p_args = ['supervisord', '-c', 'rdr_service/services/supervisor.conf']
    p = subprocess.Popen(p_args, env=env)
    p.wait()
    exit(0)