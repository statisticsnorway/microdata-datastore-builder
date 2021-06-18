import logging
import os
from logging.handlers import TimedRotatingFileHandler

LOG_FILE_FOR_IMPORT_PIPELINE = "/Users/vak/temp/dataset_import.log"


def log_setup_for_import_pipeline():
    log_format = '%(asctime)s - %(runId)s - %(name)s - %(levelname)s  - %(message)s'
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(log_format))
    stream_handler.setLevel(logging.INFO)
    logging.basicConfig(filename=LOG_FILE_FOR_IMPORT_PIPELINE, format=log_format, level=logging.DEBUG)
    logging.getLogger('').addHandler(stream_handler)


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


def get_file_handler():
    file_handler = logging.FileHandler("x.log")
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return file_handler


def get_rotating_file_handler():
    rotating_file_handler = TimedRotatingFileHandler(
        "/Users/vak/temp/dataset_import_rotating.log",
        when='midnight', backupCount=7, interval=1)
    rotating_file_handler.setLevel(logging.DEBUG)
    rotating_file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return rotating_file_handler


def get_stream_handler():
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return stream_handler


def get_logger_for_import_pipeline(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(get_rotating_file_handler())
    logger.addHandler(get_stream_handler())
    return logger
