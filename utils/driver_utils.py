from driver.custom_driver import DriverType
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chromium.options import ChromiumOptions
import undetected_chromedriver as uc

class DriverUtils():
    @classmethod
    def create_option(cls, arguments_dict: dict, driver_type: int = DriverType.SELENIUM) -> ChromiumOptions:
        options = Options() if driver_type == DriverType.SELENIUM else uc.ChromeOptions()
        for arg in arguments_dict.keys():
            options.add_argument(f"{arg}={arguments_dict.get(arg)}")

        return options
    
    