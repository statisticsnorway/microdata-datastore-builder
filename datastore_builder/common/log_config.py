import logging

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
