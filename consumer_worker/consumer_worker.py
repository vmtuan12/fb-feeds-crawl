from concurrent.futures import ThreadPoolExecutor
import threading
from worker.fb_page.fb_page_driver_worker import FbPageDriverWorker
from worker.fb_page.fb_page_desktop_worker import FbPageDesktopDriverWorker
from custom_exception.exceptions import *
from utils.proxies_utils import ProxiesUtils
import traceback
from connectors.db_connector import KafkaProducerBuilder, KafkaConsumerBuilder, DbConnectorBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SysConstant, RedisConnectionConstant as RedisCons
from utils.command_utils import CommandType, CommandUtils
from utils.proxies_utils import ProxiesUtils
from custom_logging.logging import TerminalLogging
from selenium.common.exceptions import WebDriverException
from urllib3.exceptions import ReadTimeoutError
from requests.exceptions import ConnectionError
import subprocess

class ConsumerWorker():
    def __init__(self) -> None:
        self.thread_local_data = threading.local()
        self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build()
        self.kafka_consumer = KafkaConsumerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .set_group_id(Kafka.GROUP_ID_MIDDLE_CONSUMER)\
                                                    .set_topics(Kafka.TOPIC_COMMAND)\
                                                    .build()
        self.redis_conn = DbConnectorBuilder().set_host(RedisCons.HOST)\
                                                .set_port(RedisCons.PORT)\
                                                .set_username(RedisCons.USERNAME)\
                                                .set_password(RedisCons.PASSWORD)\
                                                .build_redis()

    def get_worker(self) -> FbPageDesktopDriverWorker:
        worker = getattr(self.thread_local_data, 'worker', None)
        if worker is None:
            profile_name = threading.current_thread().name.split("-")[-1]
            worker = FbPageDesktopDriverWorker(profile_name=profile_name, kafka_producer=self.kafka_producer, redis_conn=self.redis_conn)
            setattr(self.thread_local_data, 'worker', worker)
        
        return worker
    
    def run_worker(self, page_name_or_id: str | None, scrape_threshold: int | None, cmd_type: str):
        TerminalLogging.log_info(threading.current_thread().name + f" is running {cmd_type} {page_name_or_id}")

        worker = None
        try:
            page_not_ready_count = 0
            while (True):
                if worker == None:
                    worker = self.get_worker()

                try:
                    if cmd_type == CommandType.SCRAPE_PAGE:
                        worker.start(page_name_or_id=page_name_or_id, scrape_threshold=scrape_threshold)
                    elif cmd_type == CommandType.CLEAR_CACHE:
                        worker._clear_cache()
                        subprocess.run(f"{SysConstant.CLEAR_DATA_CHROME_SCRIPT}", shell=True)

                    break

                except PageNotReadyException as pe:
                    ProxiesUtils.finish_proxy(proxy_dir=worker.proxy_dir)
                    delattr(self.thread_local_data, 'worker')
                    worker = None
                    continue
                    
                except PageCannotAccessException as pcae:
                    worker.driver.save_screenshot('page_cannot_access.png')
                    break

                except (WebDriverException, ReadTimeoutError, ConnectionError) as wde:
                    TerminalLogging.log_error(f"Connection error\n{traceback.format_exc()}")
                    worker.driver.save_screenshot('connection_error.png')
                    ProxiesUtils.finish_proxy(proxy_dir=worker.proxy_dir)
                    delattr(self.thread_local_data, 'worker')
                    worker = None
                    continue

                except Exception as e:
                    TerminalLogging.log_error(traceback.format_exc())
                    break
                
        except Exception as eee:
            TerminalLogging.log_error(f"{page_name_or_id}\n{traceback.format_exc()}")

        TerminalLogging.log_info(threading.current_thread().name + f" done {cmd_type} {page_name_or_id}")

    def start(self, max_workers: int = 1):
        TerminalLogging.log_info("Start executing ...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            for message in self.kafka_consumer:
                command = message.value
                cmd_type = CommandUtils.get_command_type(command=command)
                page = CommandUtils.get_page(command=command)
                scrape_threshold = CommandUtils.get_scrape_threshold(command=command)
                TerminalLogging.log_info(f"{cmd_type} {page} {scrape_threshold}")

                executor.submit(self.run_worker, page, scrape_threshold, cmd_type)

    def clean_up(self):
        self.kafka_producer.flush()
        self.kafka_producer.close(timeout=5)
        self.kafka_consumer.close(autocommit=False)
        self.redis_conn.close()

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()
