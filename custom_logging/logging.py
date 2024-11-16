import logging
from datetime import datetime
import pytz

class FileLogging():
    @staticmethod
    def log_info(file_path: str, message: str):
        logging.basicConfig(filename=file_path,level=logging.DEBUG)
        now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"{now} --- {message}")

    @staticmethod
    def log_error(file_path: str, message: str):
        logging.basicConfig(filename=file_path,level=logging.DEBUG)
        now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%Y-%m-%d %H:%M:%S")
        logging.error(f"{now} --- {message}")