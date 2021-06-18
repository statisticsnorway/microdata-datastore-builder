from common import log_config


class Reader:

    def __init__(self, log_filter):
        self.logger = log_config.get_logger_for_import_pipeline("Reader")
        self.logger.addFilter(log_filter)
        self.logger.info('creating an instance of Reader')

    def hello(self):
        self.logger.info("Hello world")
