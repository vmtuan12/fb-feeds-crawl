from selenium.webdriver.common.by import By
from time import sleep, time
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
import threading
import undetected_chromedriver as uc
from selenium.webdriver.common.proxy import Proxy, ProxyType
from urllib.parse import urlparse, parse_qs
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import os
import zipfile
import json
import random
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.common.exceptions import TimeoutException
from tkinter import Tk
from selenium.webdriver import Chrome
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import pickle
import re
from datetime import datetime, timedelta
import pytz

class DriverType():
    SELENIUM = 0
    UC = 1

class DriverSelector():
    @classmethod
    def get_driver(cls, driver_type: int = DriverType.SELENIUM, **kwargs) -> Chrome:
        if driver_type == DriverType.SELENIUM:
            return CustomDriver(**kwargs)
        else:
            return CustomUCDriver(**kwargs)

class CustomDriver(Chrome):
    def __init__(self, options: Options = None, service: Service = None, keep_alive: bool = True) -> None:
        if options == None:
            chrome_options = Options()
            chrome_options.add_argument('--window-size=820,1180')
        else:
            chrome_options = options

        if service == None:
            chrome_service = Service(ChromeDriverManager().install())
        else:
            chrome_service = service

        super().__init__(chrome_options, chrome_service, keep_alive)

    def find_element_by_xpath(self, value: str, implicitly_wait_time: float = None) -> EC.WebElement:
        if implicitly_wait_time != None:
            self.implicitly_wait(implicitly_wait_time)
            
        data = super().find_element(By.XPATH, value)
        return data
    
    def find_elements_by_xpath(self, value: str, implicitly_wait_time: float = None) -> EC.List[EC.WebElement]:
        if implicitly_wait_time != None:
            self.implicitly_wait(implicitly_wait_time)
            
        data = super().find_elements(By.XPATH, value)
        return data
    
    def find_element_by_id(self, value: str, implicitly_wait_time: float = None) -> EC.WebElement:
        if implicitly_wait_time != None:
            self.implicitly_wait(implicitly_wait_time)
            
        data = super().find_element(By.ID, value)
        return data

class CustomUCDriver(uc.Chrome):
    def __init__(self, options=None, user_data_dir=None, driver_executable_path=None, browser_executable_path=None, port=0, enable_cdp_events=False, desired_capabilities=None, advanced_elements=False, keep_alive=True, log_level=0, headless=False, version_main=None, patcher_force_close=False, suppress_welcome=True, use_subprocess=True, debug=False, no_sandbox=True, user_multi_procs: bool = False, **kw):
        if options == None:
            chrome_options = uc.ChromeOptions()
            chrome_options.add_argument('--window-size=820,1180')
        else:
            chrome_options = options

        super().__init__(options, user_data_dir, driver_executable_path, browser_executable_path, port, enable_cdp_events, desired_capabilities, advanced_elements, keep_alive, log_level, headless, version_main, patcher_force_close, suppress_welcome, use_subprocess, debug, no_sandbox, user_multi_procs, **kw)

    def find_element_by_xpath(self, value: str, implicitly_wait_time: float = None) -> EC.WebElement:
        if implicitly_wait_time != None:
            self.implicitly_wait(implicitly_wait_time)
            
        data = super().find_element(By.XPATH, value)
        return data
    
    def find_elements_by_xpath(self, value: str, implicitly_wait_time: float = None) -> EC.List[EC.WebElement]:
        if implicitly_wait_time != None:
            self.implicitly_wait(implicitly_wait_time)
            
        data = super().find_elements(By.XPATH, value)
        return data
    
    def find_element_by_id(self, value: str, implicitly_wait_time: float = None) -> EC.WebElement:
        if implicitly_wait_time != None:
            self.implicitly_wait(implicitly_wait_time)
            
        data = super().find_element(By.ID, value)
        return data