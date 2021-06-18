import logging
from logging.handlers import TimedRotatingFileHandler

from common.config import LOG_FILE_FOR_IMPORT_PIPELINE


class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """
    run_id = ""

    def __init__(self, run_id):
        self.run_id = run_id

    def filter(self, record):
        record.runId = self.run_id
        return True


LOG_FORMAT = '%(asctime)s - %(runId)s - %(name)s - %(levelname)s  - %(message)s'


def __get_rotating_file_handler():
    rotating_file_handler = TimedRotatingFileHandler(LOG_FILE_FOR_IMPORT_PIPELINE, when='midnight',
                                                     backupCount=7, interval=1)
    rotating_file_handler.setLevel(logging.DEBUG)
    rotating_file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return rotating_file_handler


def __get_stream_handler():
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return stream_handler


def get_logger_for_import_pipeline(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(__get_rotating_file_handler())
    logger.addHandler(__get_stream_handler())
    return logger
