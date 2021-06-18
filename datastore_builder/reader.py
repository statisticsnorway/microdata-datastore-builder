import logging


class Reader:

    def __init__(self, log_filter):
        self.logger = logging.getLogger('Reader')
        self.logger.addFilter(log_filter)
        self.logger.info('creating an instance of Reader')

    def hello(self):
        self.logger.info("Hello world")
