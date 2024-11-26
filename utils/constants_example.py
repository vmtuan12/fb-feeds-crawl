import os

class PostgresConnectionConstant():
    HOST = "localhost"
    PORT = 5432
    USER = "tuanvm"
    PWD = "tuanvm"
    DB = "tuanvm_db"

class KafkaConnectionConstant():
    BROKERS = [
        "localhost:9091"
    ]
    GROUP_ID_MIDDLE_CONSUMER = "MidCon1"
    TOPIC_RAW_POST = "fb.raw_post"
    TOPIC_COMMAND = "fb.command"
    TOPIC_DEAD_PAGES = "fb.dead_pages"
    TOPIC_RECHECK_PAGES = "fb.recheck_pages"

class SysConstant():
    BASE_DIR = os.getcwd()
    USER_DATA_DIR = f"{BASE_DIR}/chrome_profiles"
    CLEAR_DATA_CHROME_SCRIPT = f"{BASE_DIR}/clear_data.sh"
