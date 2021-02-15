#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import argparse
import shlex
import subprocess
import os


def print_service_list():
    print("Possible services are --flask, --gunicorn and --service.")

if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog='rdr-service', description="RDR web service")
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--flask", help="launch flask app", default=False, action="store_true")  # noqa
    parser.add_argument("--gunicorn", help="launch gunicorn web service", default=False, action="store_true")  # noqa
    parser.add_argument("--service", help="launch supervisor service", default=False, action="store_true")  # noqa
    parser.add_argument("--unittests", help="enable unittest mode", default=False, action="store_true")  # noqa
    parser.add_argument("--offline", help="start offline web app", default=False, action="store_true")  # noqa
    parser.add_argument("--resource", help="start resource web app", default=False, action="store_true")  # noqa

    args = parser.parse_args()

    if args.unittests:
        # pylint: disable=unused-import
        import tests.helpers.mysql_helper  # need to execute file to set unittest db connection string
        os.environ["UNITTEST_FLAG"] = "1"
    env = dict(os.environ)

    service_count = sum([args.flask, args.gunicorn, args.service])

    if service_count == 0:
        print("You must specify a service to start up.")
        print_service_list()
        exit(1)

    if service_count != 1:
        print("Only one service option may be selected.")
        print_service_list()
        exit(1)

    if args.offline and args.resource:
        print("You may not start Offline and Resource apps at the same time.")
        exit(1)

    if args.flask:
        # This is used when running locally only. When deploying to Google App
        # Engine, a webserver process such as Gunicorn will serve the app. This
        # can be configured by adding an `entrypoint` to app.yaml.
        if args.offline:
            from rdr_service.offline.main import app
            app.run(host='127.0.0.1', port=8080, debug=args.debug)
        elif args.resource:
            from rdr_service.resource.main import app
            app.run(host='127.0.0.1', port=8080, debug=args.debug)
        else:
            from rdr_service.main import app
            app.run(host='127.0.0.1', port=8080, debug=args.debug)

        exit(0)

    if args.gunicorn:
        # Use the gunicorn command line defined for supervisor.
        import configparser
        config = configparser.ConfigParser()
        config.read('rdr_service/services/supervisor.conf')
        command = config['program:flask_wsgi']['command']

        if args.offline:
            command = command.replace('rdr_service.main:app', 'rdr_service.offline.main:app')
        if args.resource:
            command = command.replace('rdr_service.main:app', 'rdr_service.resource.main:app')
        p_args = shlex.split(command)
        print(p_args)
        p = subprocess.Popen(p_args, env=env)
        p.wait()
        exit(0)

    if args.offline:
        config_file = 'rdr_service/services/supervisor_offline.conf'
    elif args.resource:
        config_file = 'rdr_service/services/supervisor_resource.conf'
    else:
        config_file = 'rdr_service/services/supervisor.conf'

    p_args = ['supervisord', '-c', config_file]
    p = subprocess.Popen(p_args, env=env)
    p.wait()
    exit(0)
