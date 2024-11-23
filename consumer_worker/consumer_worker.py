from concurrent.futures import ThreadPoolExecutor
import threading
from worker.fb_page.fb_page_driver_worker import FbPageDriverWorker
from custom_exception.exceptions import *
from utils.proxies_utils import ProxiesUtils
import traceback
from connectors.db_connector import KafkaProducerBuilder, KafkaConsumerBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SysConstant
from utils.command_utils import CommandType, CommandUtils
from custom_logging.logging import TerminalLogging
import subprocess

class ConsumerWorker():
    def __init__(self) -> None:
        self.thread_local_data = threading.local()
        self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build()
        self.kafka_consumer = KafkaConsumerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .set_group_id("test")\
                                                    .set_topics(Kafka.TOPIC_COMMAND)\
                                                    .build()

    def get_worker(self) -> FbPageDriverWorker:
        worker = getattr(self.thread_local_data, 'worker', None)
        if worker is None:
            profile_name = threading.current_thread().name.split("-")[-1]
            worker = FbPageDriverWorker(profile_name=profile_name, kafka_producer=self.kafka_producer, min_post_count=40)
            setattr(self.thread_local_data, 'worker', worker)
        
        return worker
    
    def run_worker(self, page_name_or_id: str, cmd_type: str):
        TerminalLogging.log_info(threading.current_thread().name + f" is running {cmd_type} {page_name_or_id}")

        worker = None
        try:
            page_not_ready_count = 0
            while (True):
                if worker == None:
                    worker = self.get_worker()

                try:
                    if cmd_type == CommandType.SCRAPE_PAGE:
                        worker.start(page_name_or_id=page_name_or_id)
                    elif cmd_type == CommandType.CLEAR_CACHE:
                        worker._clear_cache()

                    break

                except PageNotReadyException as pe:
                    page_not_ready_count += 1
                    proxy_is_working = ProxiesUtils.proxy_is_working(proxy_dir=pe.proxy_dir)
                    if (not proxy_is_working) or (page_not_ready_count % 3 == 0):
                        delattr(self.thread_local_data, 'worker')
                        worker = None
                        continue
                    
                except PageCannotAccessException as pcae:
                    break

                except Exception as e:
                    TerminalLogging.log_error(traceback.format_exc())
                    break
                
        except Exception as eee:
            print(eee)

        TerminalLogging.log_info(threading.current_thread().name + f" done {cmd_type} {page_name_or_id}")

    def start(self, max_workers: int = 1):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            for message in self.kafka_consumer:
                command = message.value
                cmd_type = CommandUtils.get_command_type(command=command)
                page = CommandUtils.get_page(command=command)
                TerminalLogging.log_info(f"{cmd_type} {page}")

                if cmd_type == CommandType.CLEAR_CACHE:
                    subprocess.run(f"{SysConstant.CLEAR_DATA_CHROME_SCRIPT}", shell=True)
                    for future in futures:
                        future.result()
                    
                    futures.clear()
                    for _ in range(max_workers):
                        executor.submit(self.run_worker, page, cmd_type)

                else:
                    job = executor.submit(self.run_worker, page, cmd_type)
                    futures.append(job)

    def clean_up(self):
        self.kafka_producer.flush()
        self.kafka_producer.close(timeout=5)
        self.kafka_consumer.close(autocommit=False)

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()
