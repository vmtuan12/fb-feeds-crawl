from worker.base_worker import BaseWorker
from driver.custom_driver import CustomDriver, CustomUCDriver
from kafka import KafkaProducer

class DriverWorker(BaseWorker):
    def __init__(self, driver: CustomDriver | CustomUCDriver) -> None:
        self.driver = driver
        self.blocked_urls = [
            "*.woff2",
            "*.css",
            "https://lookaside.fbsbx.com/lookaside/crawler/media*"
        ]
        self.driver.execute_cdp_cmd('Network.setBlockedURLs', {"urls": self.blocked_urls})
        self.driver.execute_cdp_cmd('Network.enable', {})
        # self.kafka_producer = KafkaProducer()

    def _clear_cache(self):
        self.driver.get('chrome://settings/clearBrowserData')
        clear_data_path = "document.querySelector('settings-ui').shadowRoot.querySelector('#main').shadowRoot.querySelector('settings-basic-page').shadowRoot.querySelector('settings-section[section=\"privacy\"]').querySelector('settings-privacy-page').shadowRoot.querySelector('settings-clear-browsing-data-dialog').shadowRoot.querySelector('cr-dialog').querySelector('#clearButton')"
        self.driver.execute_script(f"return {clear_data_path}").click()
        self.driver.execute_script('localStorage.clear();')
        self.driver.delete_all_cookies()

    def _check_ready(self) -> bool:
        return True
    
    def __del__(self):
        self.driver.close()
    
    def __delete__(self):
        self.driver.close()