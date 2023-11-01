import re
import sys
from gunicorn.app.wsgiapp import run

from app.config import get_settings
if __name__ == '__main__':
    try:
        server_settings = get_settings().server
    except Exception as ex:
        print("Error while loading settings file.")
        raise ex

    sys.argv[0] = re.sub(r'(-script\.pyw|\.exe)?$', '', sys.argv[0])
    sys.exit(run())
