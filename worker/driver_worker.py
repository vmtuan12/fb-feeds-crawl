from worker.base_worker import BaseWorker
from driver.custom_driver import CustomDriver, CustomUCDriver

class DriverWorker(BaseWorker):
    def __init__(self, driver: CustomDriver | CustomUCDriver) -> None:
        self.driver = driver