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
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        if kafka_producer == None:
            self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                        .build()
        else:
            self.kafka_producer = kafka_producer

        driver = DriverSelector.get_driver(driver_type=DriverType.SELENIUM, options=options)

        super().__init__(driver)

    def _get_raw_post_dict(self, p: WebElement, now: datetime, page_name_or_id: str, has_no_img: bool = False) -> dict:
        try:
            try:
                post_time = p.find_element(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_POST_TIME)
            except Exception as get_time_exc:
                post_time = p.find_element(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_POST_TIME_ALTER)
            real_post_time_str = ParserUtils.approx_post_time_str(now=now, raw_post_time=post_time.get_attribute("innerHTML"))

            try:
                reactions = p.find_element(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_REACTION)
            except Exception as get_react_exc:
                reactions = p.find_element(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_REACTION_ALTER)

            reaction_count = ParserUtils.approx_reactions(reactions.get_attribute("innerHTML"))

            images = p.find_elements(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_IMAGES)
            list_img_src = None if has_no_img else [i.get_attribute("src") for i in images]

            try:
                post_text = p.find_element(by='xpath', value=".//div[text()]")
            except NoSuchElementException as e:
                post_text = p.find_element(by='xpath', value=".//span[text()]")

            raw_post_entity = RawPostEntity(text=(post_text.text if post_text.text.strip() != "" else post_text.get_attribute("innerHTML")),
                                            images=list_img_src,
                                            reaction_count=reaction_count,
                                            post_time=real_post_time_str,
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
        
        return random.uniform(-20.0, 500.0)
    
    def _check_ready(self) -> bool:
        attempt = 0
        while (attempt <= 3):
            posts = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT) + self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT_WITH_BG_IMG)
            if len(posts) > 0:
                return True
            attempt += 1

            if ("login.php" in self.driver.current_url) or ("not found" in self.driver.title):
                raise PageCannotAccessException()

            sleep(1)

        return False

    def _load_more_post_text(self):
        texts_load_more = self.driver.find_elements_by_xpath(value=FbPageXpathUtils.XPATH_TEXT_WITH_LOAD_MORE)
        for t in texts_load_more:
            self.driver.execute_script("arguments[0].click();", t)

    def start(self, page_name_or_id: str):
        target_url = self.base_url.format(page_name_or_id)

        now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
        user_agent = UserAgentUtils.get_user_agent_fb_page()
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})

        self.driver.get(target_url)
        TerminalLogging.log_info(f"Load {target_url} successfully")

        page_is_ready = self._check_ready()
        if not page_is_ready:
            raise PageNotReadyException(proxy_dir=self.proxy_dir)
        
        TerminalLogging.log_info(f"{target_url} is ready")

        vscroller_el = """document.querySelector('[data-type="vscroller"]')"""

        len_post_less_than_5 = 1
        while (True):
            posts = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT) + self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT_WITH_BG_IMG)
            TerminalLogging.log_info(f"{target_url} - {len(posts)} posts")
            if len(posts) <= 5:
                len_post_less_than_5 += 1
            if len(posts) >= self.min_post_count:
                break
            
            if len_post_less_than_5 % 100 == 0:
                raise PageNotReadyException(proxy_dir=self.proxy_dir)
            
            scroll_value = self._get_scroll_value()
            self.driver.execute_script(f"window.scrollTo(0, window.scrollY + {scroll_value});")
            self.driver.execute_script(f"""{vscroller_el}.scrollTo(0, {vscroller_el}.scrollTop + {scroll_value})""")

        self._load_more_post_text()
        
        count_load_more = 0
        while (True):
            TerminalLogging.log_info(f"{target_url} - wait load more")
            count_load_more += 1

            if count_load_more == 80:
                self._load_more_post_text()
            if count_load_more >= 150:
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
            # print(post_entity, "\n\n----####----####----####----####----####----####----####----####----####\n\n")
        self.kafka_producer.flush()

        # with open(f'test/{page_name_or_id.replace(".", "_")}.json', "w") as f:
        #     json.dump(data_list, f, ensure_ascii=False, indent=4)
