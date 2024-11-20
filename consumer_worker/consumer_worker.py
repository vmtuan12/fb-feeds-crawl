from concurrent.futures import ThreadPoolExecutor
import threading
from worker.fb_page.fb_page_driver_worker import FbPageDriverWorker
from custom_exception.exceptions import *
from utils.proxies_utils import ProxiesUtils
import traceback
from connectors.db_connector import KafkaProducerBuilder, KafkaConsumerBuilder
from utils.constants import KafkaConnectionConstant as Kafka

class ConsumerWorker():
    def __init__(self) -> None:
        self.thread_local_data = threading.local()
        self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build()
        self.kafka_consumer = KafkaConsumerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .set_group_id("test")\
                                                    .set_topics("fb.command")\
                                                    .build()

    def get_worker(self) -> FbPageDriverWorker:
        worker = getattr(self.thread_local_data, 'worker', None)
        if worker is None:
            worker = FbPageDriverWorker()
            setattr(self.thread_local_data, 'worker', worker)
        
        return worker
    
    def run_worker(self, page_name_or_id):
        print(threading.current_thread().name)
        worker = None
        while (True):
            if worker == None:
                worker = self.get_worker()

            try:
                worker.start(page_name_or_id=page_name_or_id)
                break
            except PageNotReadyException as pe:
                proxy_is_working = ProxiesUtils.proxy_is_working(proxy_dir=pe.proxy_dir)
                if not proxy_is_working:
                    delattr(self.thread_local_data, 'worker')
                    worker = None
                    continue

            except Exception as e:
                print(traceback.format_exc())        

    def start(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(self.run_worker, "K14vn")
