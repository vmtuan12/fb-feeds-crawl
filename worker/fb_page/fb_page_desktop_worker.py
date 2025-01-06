from worker.driver_worker import DriverWorker
from driver.custom_driver import DriverSelector, DriverType
from utils.driver_utils import DriverUtils
from utils.proxies_utils import ProxiesUtils
from utils.user_agent_utils import UserAgentUtils
from utils.xpath_utils import FbPageDesktopXpathUtils
from utils.parser_utils import ParserUtils
from entities.entities import RawPostEntity
from custom_exception.exceptions import *
from custom_logging.logging import TerminalLogging
from connectors.db_connector import KafkaProducerBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SysConstant, RedisConnectionConstant as RedisCons
from selenium.webdriver.common.action_chains import ActionChains
from kafka import KafkaProducer
from time import sleep, time
from redis import Redis
import pytz
from datetime import datetime
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
import os
import traceback

class FbPageDesktopDriverWorker(DriverWorker):
    def __init__(self, profile_name: str, kafka_producer: KafkaProducer | None = None, redis_conn: Redis | None = None) -> None:
        self.base_url = "https://www.facebook.com/{}?locale=en_US"
        self.timeout_sec = int(os.getenv("TIMEOUT_SEC", "180"))
        self.count_load_more_threshold = int(os.getenv("COUNT_LOAD_MORE_THRESHOLD", "400"))
        self.sleep_to_load_time = float(os.getenv("SLEEP_TO_LOAD_TIME", "0.25"))

        self.window_w, self.window_h = 1200, 900

        self.proxy_dir = ProxiesUtils.get_proxy_dir()
        options = DriverUtils.create_option(arguments_dict={
            "--window-size": f"{self.window_w},{self.window_h}",
            "--load-extension": self.proxy_dir,
            "--disable-blink-features": "AutomationControlled",
            "user-data-dir": f"{SysConstant.USER_DATA_DIR}/{profile_name}",
            "profile-directory": "Default"
        })
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        if kafka_producer == None:
            self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                        .build()
        else:
            self.kafka_producer = kafka_producer

        self.redis_conn = redis_conn
        self.checked_posts = dict()

        driver = DriverSelector.get_driver(driver_type=DriverType.SELENIUM, options=options)

        super().__init__(driver)
    
    def _check_ready(self, page: str) -> bool:
        attempt = 0
        while (attempt <= 50):
            posts = self.driver.find_elements_by_xpath(value=FbPageDesktopXpathUtils.XPATH_TEXT)
            if len(posts) > 0:
                return True
            attempt += 1

            if self.driver.find_element_by_id(value="main-frame-error") != None:
                TerminalLogging.log_info(f"Cannot reach page")
                raise PageNotReadyException(proxy_dir=self.proxy_dir)

            if ("login" in self.driver.current_url):
                raise PageNeedLoginDesktop()

            if ("not found" in self.driver.title) or \
                (self.driver.find_element_by_xpath(value="//span[contains(text(), 'No posts available')]") != None) or \
                (self.driver.find_element_by_xpath(value="""//h2//*[text() and contains(text(), "This content isn't available")]""") != None):
                TerminalLogging.log_info(f"Page is dead")
                self.kafka_producer.send(Kafka.TOPIC_DEAD_PAGES, {"page": page})
                self.kafka_producer.flush()
                raise PageCannotAccessException()
            
            sleep(0.5)
        
        f = open("not_ready.html", "w+")
        f.write(self.driver.find_element_by_xpath(value="//html").get_attribute("outerHTML"))
        f.close()
        return False
    
    def _close_login_dialog(self, t_index: int):
        try:
            close = self.driver.find_element_by_xpath(value=FbPageDesktopXpathUtils.XPATH_DIALOG_CLOSE)
            if close != None:
                TerminalLogging.log_info(f"Found close at post {t_index}")
                self.driver.execute_script("arguments[0].click();", close)
        except Exception as e:
            pass

    def _retrieve_checked_posts(self, page_name_or_id: str):
        if page_name_or_id in self.checked_posts:
            return
        
        page_keys = f'{RedisCons.PREFIX_POST_ID}.{page_name_or_id}.*'
        urls = set()
        for key in page_keys:
            split_index = key.find('https')
            urls.add(key[split_index:])

        self.checked_posts[page_name_or_id] = urls
            
    def start(self, page_name_or_id: str, scrape_threshold: int):
        target_url = self.base_url.format(page_name_or_id)
        self._retrieve_checked_posts(page_name_or_id=page_name_or_id)

        user_agent = UserAgentUtils.get_user_agent_fb_desktop()
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})

        self.driver.get(target_url)
        TerminalLogging.log_info(f"Load {target_url} successfully")

        # section: check page is ready or not
        page_is_ready = self._check_ready(page=page_name_or_id)
        if not page_is_ready:
            TerminalLogging.log_info(f"Page {page_name_or_id} needs to be rechecked")
            self.driver.save_screenshot('page_need_rechecked.png')
            self.kafka_producer.send(Kafka.TOPIC_RECHECK_PAGES, {"page": page_name_or_id})
            self.kafka_producer.flush()
            raise PageNotReadyException(proxy_dir=self.proxy_dir, recheck=True)
        
        TerminalLogging.log_info(f"{target_url} is ready")
        
        while (True):
            try:
                self.driver.find_element_by_xpath(value=FbPageDesktopXpathUtils.XPATH_DIALOG_CLOSE).click()
                break
            except Exception as e:
                continue 

        # section: remove obstacles
        for xp in FbPageDesktopXpathUtils.XPATH_OBSTABLES:
            obstacle = self.driver.find_element_by_xpath(value=xp)
            if obstacle != None:
                self.driver.execute_script("""
                var element = arguments[0];
                element.parentNode.removeChild(element);
                """, obstacle)
            
        texts = self.driver.find_elements_by_xpath(value=FbPageDesktopXpathUtils.XPATH_TEXT)
        TerminalLogging.log_info(f"Found {len(texts)} posts in page {page_name_or_id}!")

        count_scraped = 0
        for t_index, t in enumerate(texts):
            try:
                see_more = t.find_element('xpath', FbPageDesktopXpathUtils.XPATH_SEE_MORE_BTN)
                self.driver.execute_script("arguments[0].click();", see_more)
            except Exception as e:
                pass

            try:
                post_time = t.find_element(by='xpath', value=FbPageDesktopXpathUtils.XPATH_ADDITIONAL_POST_TIME)
            except Exception as e:
                continue
            
            while (True):
                try:
                    self._close_login_dialog(t_index=t_index)

                    try:
                        hover = ActionChains(self.driver).move_to_element(post_time)
                        hover.perform()
                    except Exception as e:
                        TerminalLogging.log_error(f"Error hover at post {t_index}")
                        break

                    self._close_login_dialog(t_index=t_index)

                    while (True):
                        try:
                            real_post_time = self.driver.find_element(by='xpath', value="//*[@role='tooltip']").get_attribute("innerHTML")
                            post_url = post_time.get_attribute("href")
                            break
                        except Exception as e:
                            TerminalLogging.log_error(f"Failed get time {t_index}")
                            self._close_login_dialog(t_index=t_index)
                            hover.perform()
                            self._close_login_dialog(t_index=t_index)

                except Exception as eeee:
                    TerminalLogging.log_error(f"Unknown Error when hovering\n{eeee}")
                    continue
                break
            
            if post_url in self.checked_posts[page_name_or_id]:
                break

            try:
                images = t.find_elements(by='xpath', value=FbPageDesktopXpathUtils.XPATH_ADDITIONAL_IMAGES)
                images_src = [i.get_attribute("src") for i in images if "emoji" not in i.get_attribute("src")]
                reaction = t.find_element(by='xpath', value=FbPageDesktopXpathUtils.XPATH_ADDITIONAL_REACTION).get_attribute("innerHTML")

                raw_post_entity = RawPostEntity(text=t.text,
                                                images=images_src,
                                                reaction_count=reaction,
                                                post_time=real_post_time,
                                                page=page_name_or_id,
                                                url=post_url).to_dict()
                
                self.kafka_producer.send("test-ui-raw", raw_post_entity)
                self.checked_posts[page_name_or_id].add(post_url)

                count_scraped += 1

                if t_index >= scrape_threshold:
                    break

            except Exception as e:
                TerminalLogging.log_error(f"Post at {t_index} failed\n{e}")
                continue
        
        TerminalLogging.log_info(f"Scraped {count_scraped}/{scrape_threshold} of {page_name_or_id}")
        self.kafka_producer.flush()