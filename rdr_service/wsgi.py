#
# WSGI entry point for RD
#
from rdr_service.main import app as application

if __name__ == "__main__":
    application.run()
