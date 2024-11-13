from worker.base_worker import BaseWorker
from driver.custom_driver import CustomDriver

class DriverWorker(BaseWorker):
    def __init__(self) -> None:
        self.driver = CustomDriver()