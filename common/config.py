import os
import pathlib
import datetime

BACKEND_PATH = pathlib.Path(__file__).parent.parent

SUPER_USER = 1


class Config:
    LOGGING_FILE_D = os.path.dirname(os.path.dirname(__file__))
    LOGGING_FILE_DIR = os.path.join(LOGGING_FILE_D) + '/logs/'
    LOGGING_FILE_NAME = datetime.datetime.now().strftime('%Y-%m-%d') + '_log.log'
    LOGGING_FILE_PATH = os.path.join(LOGGING_FILE_DIR, LOGGING_FILE_NAME)
    LOGGING_LEVEL = 'INFO'
    # LOGGING_FORMAT = '{time} [{level}] {message}'
    LOGGING_FORMAT = "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"


settings = Config()