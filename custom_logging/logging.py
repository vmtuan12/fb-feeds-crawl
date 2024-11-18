import logging
from datetime import datetime
import pytz

class CustomFormatter(logging.Formatter):
    def __init__(self, *args, timezone='UTC', **kwargs):
        super().__init__(*args, **kwargs)
        self.timezone = pytz.timezone(timezone)

    def formatTime(self, record, datefmt=None):
        return super().formatTime(record, datefmt="%Y-%m-%d %H:%M:%S")

class TerminalLogging():

    @staticmethod
    def log_info(message: str):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(CustomFormatter('%(asctime)s %(levelname)s: %(message)s', timezone='Asia/Ho_Chi_Minh'))
        logging.basicConfig(level=logging.DEBUG, handlers=[console_handler])
        logging.info(message)

    @staticmethod
    def log_error(message: str):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(CustomFormatter('%(asctime)s %(levelname)s: %(message)s', timezone='Asia/Ho_Chi_Minh'))
        logging.basicConfig(level=logging.DEBUG, handlers=[console_handler])
        logging.error(message)

class FileLogging():
    @staticmethod
    def log_info(file_path: str, message: str):
        handler = logging.FileHandler(file_path)
        handler.setFormatter(CustomFormatter('%(asctime)s %(levelname)s: %(message)s', timezone='Asia/Ho_Chi_Minh'))
        logging.basicConfig(
            level=logging.INFO,
            handlers=[handler]
        )
        logging.info(message)

    @staticmethod
    def log_error(file_path: str, message: str):
        handler = logging.FileHandler(file_path)
        handler.setFormatter(CustomFormatter('%(asctime)s %(levelname)s: %(message)s', timezone='Asia/Ho_Chi_Minh'))
        logging.basicConfig(
            level=logging.ERROR,
            handlers=[handler]
        )
        logging.error(message)