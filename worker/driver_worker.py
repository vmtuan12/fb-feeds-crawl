from worker.base_worker import BaseWorker
from driver.custom_driver import CustomDriver, CustomUCDriver
from kafka import KafkaProducer

class DriverWorker(BaseWorker):
    def __init__(self, driver: CustomDriver | CustomUCDriver) -> None:
        self.driver = driver
        # self.kafka_producer = KafkaProducer()

    def _check_ready(self) -> bool:
        return True
    
    def on_close(self):
        self.driver.close()