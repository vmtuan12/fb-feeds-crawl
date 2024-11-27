from worker.driver_worker import DriverWorker
from driver.custom_driver import DriverSelector, DriverType
from utils.driver_utils import DriverUtils
from utils.proxies_utils import ProxiesUtils
from utils.user_agent_utils import UserAgentUtils
from utils.xpath_utils import FbPageXpathUtils
from utils.parser_utils import ParserUtils
from entities.entities import RawPostEntity
from custom_exception.exceptions import *
from custom_logging.logging import TerminalLogging
from connectors.db_connector import KafkaProducerBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SysConstant
from kafka import KafkaProducer
from time import sleep
import random
import pytz
from datetime import datetime
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
import json
import traceback

class FbPageDriverWorker(DriverWorker):
    def __init__(self, profile_name: str, kafka_producer: KafkaProducer | None = None, min_post_count: int = 10) -> None:
        self.base_url = "https://m.facebook.com/{}?locale=en_US"
        self.min_post_count = min_post_count

        self.window_w, self.window_h = 1200, 900

        self.proxy_dir = ProxiesUtils.get_proxy_dir()
        print(self.proxy_dir)
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

        driver = DriverSelector.get_driver(driver_type=DriverType.SELENIUM, options=options)

        super().__init__(driver)

    def __find_post_time(self, p: WebElement) -> WebElement | None:
        try:
            post_time = p.find_element(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_POST_TIME)
            return post_time
        except Exception as get_time_exc:
            post_time = None

        try:
            post_time = p.find_element(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_POST_TIME_ALTER)
            return post_time
        except Exception as get_time_exc:
            post_time = None

        return post_time

    def __find_post_reactions(self, p: WebElement) -> WebElement | None:
        try:
            reactions = p.find_element(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_REACTION)
            return reactions
        except Exception as get_react_exc:
            reactions = None

        try:
            reactions = p.find_element(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_REACTION_ALTER)
            return reactions
        except Exception as get_react_exc:
            reactions = None

        return reactions

    def _get_raw_post_dict(self, p: WebElement, now: datetime, page_name_or_id: str, has_no_img: bool = False) -> dict:
        try:
            post_time = self.__find_post_time(p=p)
            # real_post_time_str = ParserUtils.approx_post_time_str(now=now, raw_post_time=post_time.get_attribute("innerHTML"))
            reactions = self.__find_post_reactions(p=p)

            if post_time == None or reactions == None:
                raise Exception("Post not valid")

            # reaction_count = ParserUtils.approx_reactions(reactions.get_attribute("innerHTML"))

            images = p.find_elements(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_IMAGES)
            list_img_src = None if has_no_img else [i.get_attribute("src") for i in images]

            # try:
            #     post_text = p.find_element(by='xpath', value=".//div[text()]")
            # except NoSuchElementException as e:
            #     post_text = p.find_element(by='xpath', value=".//span[text()]")

            raw_post_entity = RawPostEntity(text=p.get_attribute("outerHTML"),
                                            images=list_img_src,
                                            reaction_count=reactions.get_attribute("innerHTML"),
                                            post_time=post_time.get_attribute("innerHTML"),
                                            page=page_name_or_id).to_dict()
            
            return raw_post_entity
        except StaleElementReferenceException as sere:
            raise sere
        except Exception as e:
            TerminalLogging.log_error(message=f"{traceback.format_exc()}\n{p.get_attribute('outerHTML')}\n")
            raise e
    
    def _get_scroll_value(self, is_up = False) -> float:
        # base_value = max(float(self.driver.execute_script("return document.documentElement.scrollHeight")), 
        #                 float(self.driver.execute_script("return document.body.scrollHeight")), 
        #                 float(self.driver.execute_script("""return document.querySelector('[data-type="vscroller"]').scrollHeight""")))
        
        if is_up:
            return random.uniform(-200.0, -20.0)
        return random.uniform(-20.0, 500.0)
    
    def _check_ready(self, page: str) -> bool:
        attempt = 0
        while (attempt <= 50):
            posts = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT) + self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT_WITH_BG_IMG)
            if len(posts) > 0:
                return True
            attempt += 1

            if self.driver.find_element_by_id(value="main-frame-error") != None:
                TerminalLogging.log_info(f"Cannot reach page")
                sleep(1)
                raise PageNotReadyException(proxy_dir=self.proxy_dir)

            if ("login.php" in self.driver.current_url) or ("not found" in self.driver.title) or (self.driver.find_element_by_xpath(value="//span[text() = 'No Posts or Tags']") != None):
                TerminalLogging.log_info(f"Page is dead")
                self.kafka_producer.send(Kafka.TOPIC_DEAD_PAGES, {"page": page})
                self.kafka_producer.flush()
                raise PageCannotAccessException()

            sleep(0.5)
        
        f = open("not_ready.html", "w+")
        f.write(self.driver.find_element_by_xpath(value="//html").get_attribute("outerHTML"))
        f.close()
        return False

    def _load_more_post_text(self, perform=False, check_invalid=False):
        texts_load_more = self.driver.find_elements_by_xpath(value=FbPageXpathUtils.XPATH_TEXT_WITH_LOAD_MORE)
        print(f"load more {len(texts_load_more)} posts")
        if check_invalid:
            for t in texts_load_more:
                try:
                    if (self.__find_post_reactions(t) != None) and (self.__find_post_time(t) != None):
                        return True
                except StaleElementReferenceException as sere:
                    continue
            return False
            
        if perform:
            for index, t in enumerate(texts_load_more):
                try:
                    self.driver.execute_script("arguments[0].click();", t)
                except StaleElementReferenceException as sere:
                    continue

        return True
            
    def start(self, page_name_or_id: str):
        target_url = self.base_url.format(page_name_or_id)

        now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
        user_agent = UserAgentUtils.get_user_agent_fb_page()
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})

        self.driver.get(target_url)
        TerminalLogging.log_info(f"Load {target_url} successfully")

        page_is_ready = self._check_ready(page=page_name_or_id)
        if not page_is_ready:
            TerminalLogging.log_info(f"Page {page_name_or_id} needs to be rechecked")
            self.kafka_producer.send(Kafka.TOPIC_RECHECK_PAGES, {"page": page_name_or_id})
            self.kafka_producer.flush()
            raise PageNotReadyException(proxy_dir=self.proxy_dir, recheck=True)
        
        TerminalLogging.log_info(f"{target_url} is ready")

        vscroller_el = """document.querySelector('[data-type="vscroller"]')"""

        len_post_less_than_5 = 1
        post_not_change_count = 0
        prev_post_len = 0
        while (True):
            posts = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT) + self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT_WITH_BG_IMG)

            TerminalLogging.log_info(f"{target_url} - {len(posts)} posts")
            # if len(posts) <= 5:
            #     len_post_less_than_5 += 1
            if (len(posts) >= self.min_post_count) or (post_not_change_count >= 500):
                break
            
            # if len_post_less_than_5 % 300 == 0:
            #     raise PageNotReadyException(proxy_dir=self.proxy_dir)
            
            if len(posts) - prev_post_len == 0:
                post_not_change_count += 1
            else:
                post_not_change_count = 0
            # if post_not_change_count >= 500:
            #     raise PageNotReadyException(proxy_dir=self.proxy_dir)
            prev_post_len = len(posts)

            scroll_value = self._get_scroll_value()
            self.driver.execute_script(f"window.scrollTo(0, window.scrollY + {scroll_value});")
            self.driver.execute_script(f"""{vscroller_el}.scrollTo(0, {vscroller_el}.scrollTop + {scroll_value})""")

        self._load_more_post_text(perform=True)
        
        count_load_more = 0
        while (True):
            TerminalLogging.log_info(f"{target_url} - wait load more")
            count_load_more += 1

            if count_load_more % 24 == 0:
                TerminalLogging.log_info(f"{page_name_or_id} need load more")
                self._load_more_post_text()

            if count_load_more % 200 == 0:
                keep_waiting = self._load_more_post_text(check_invalid=True)
                if not keep_waiting:
                    break

            if count_load_more >= 400:
                f = open("page_source.html", "w+")
                f.write(self.driver.page_source)
                f.close()
                raise PageNotReadyException(proxy_dir=self.proxy_dir)
            
            if len(self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT_WITH_LOAD_MORE)) == 0:
                break

        while (True):
            try:
                TerminalLogging.log_info(f"{target_url} - getting posts")
                posts_without_image_bg = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT)
                posts_with_image_bg = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT_WITH_BG_IMG)

                for p in posts_without_image_bg:
                    try:
                        post_entity = self._get_raw_post_dict(p=p, now=now, page_name_or_id=page_name_or_id)
                    except Exception as e:
                        continue
                    self.kafka_producer.send(Kafka.TOPIC_RAW_POST, post_entity)

                for p in posts_with_image_bg:
                    try:
                        post_entity = self._get_raw_post_dict(p=p, now=now, page_name_or_id=page_name_or_id, has_no_img=True)
                    except Exception as e:
                        continue
                    self.kafka_producer.send(Kafka.TOPIC_RAW_POST, post_entity)
                    
                break
            except StaleElementReferenceException as sere:
                pass
        self.kafka_producer.flush()
