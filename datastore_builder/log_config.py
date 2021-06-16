import logging
import logging.handlers
import os


def set_up_logging():
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", "/Users/vak/temp/dataset_import.log"))
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
