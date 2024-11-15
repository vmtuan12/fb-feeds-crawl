from worker.driver_worker import DriverWorker
from driver.custom_driver import DriverSelector, DriverType
from utils.driver_utils import DriverUtils
from utils.proxies_utils import ProxiesUtils
from utils.user_agent_utils import UserAgentUtils
from utils.xpath_utils import FbPageXpathUtils
from utils.parser_utils import ParserUtils
from entities.entities import RawPostEntity
from time import sleep
import random
import pytz
from datetime import datetime
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException

class FbPageDriverWorker(DriverWorker):
    def __init__(self, page_name_or_id: str, min_post_count: int = 10) -> None:
        self.url = f"https://m.facebook.com/{page_name_or_id}?locale=en_US"
        self.min_post_count = min_post_count

        self.window_w, self.window_h = 1200, 900

        proxy_dir = ProxiesUtils.get_proxy_dir()
        options = DriverUtils.create_option(arguments_dict={
            "--window-size": f"{self.window_w},{self.window_h}",
            "--load-extension": proxy_dir,
            "--disable-blink-features": "AutomationControlled"
        })
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        driver = DriverSelector.get_driver(driver_type=DriverType.SELENIUM, options=options)

        super().__init__(driver)

    def _get_raw_post_dict(self, p: WebElement, now: datetime, has_no_img: bool = False) -> dict:
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

            if post_text == None or post_text.text.strip() == "":
                print("==========\n", post_text.get_attribute("outerHTML"), "\n==========\n")

            raw_post_entity = RawPostEntity(text=(post_text.text if post_text.text.strip() != "" else post_text.get_attribute("innerHTML")),
                                            images=list_img_src,
                                            reaction_count=reaction_count,
                                            post_time=real_post_time_str).to_dict()
            
            return raw_post_entity
        except Exception as e:
            print("Error", e, "\n$$$$$$$\n")
            print(p.get_attribute("outerHTML"))
            return None
    
    def _get_scroll_value(self, is_up = False) -> float:
        base_value = max(float(self.driver.execute_script("return document.documentElement.scrollHeight")), 
                        float(self.driver.execute_script("return document.body.scrollHeight")), 
                        float(self.driver.execute_script("""return document.querySelector('[data-type="vscroller"]').scrollHeight""")))
        
        divisor = 8 if not is_up else 15
        return (base_value / divisor) * random.uniform(1.0, 2.0)

    def start(self):
        user_agent = UserAgentUtils.get_user_agent_fb_page()
        print(user_agent)
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})

        self.driver.get(self.url)

        temp_posts = []
        while (True):
            posts = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT) + self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT_WITH_BG_IMG)
            temp_posts = posts.copy()
            check_page_ready = len(posts) > 0
            if check_page_ready:
                break
            sleep(1)

        count_scroll = 0
        vscroller_el = """document.querySelector('[data-type="vscroller"]')"""

        while (True):
            posts = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT) + self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT_WITH_BG_IMG)
            if len(posts) >= self.min_post_count:
                break

            self.driver.execute_script(f"window.scrollTo(0, window.scrollY + {self._get_scroll_value()});")
            self.driver.execute_script(f"""{vscroller_el}.scrollTo(0, {vscroller_el}.scrollTop + {self._get_scroll_value()})""")
            count_scroll += 1

            sleep(random.uniform(1.0, 3.0))

            if count_scroll % 3 == 0 or count_scroll % 7 == 0:
                self.driver.execute_script(f"window.scrollTo(0, window.scrollY - {self._get_scroll_value(is_up=True)});")
                self.driver.execute_script(f"""{vscroller_el}.scrollTo(0, {vscroller_el}.scrollTop - {self._get_scroll_value(is_up=True)})""")

        texts_load_more = self.driver.find_elements_by_xpath(value=FbPageXpathUtils.XPATH_TEXT_WITH_LOAD_MORE)
        for t in texts_load_more:
            self.driver.execute_script("arguments[0].click();", t)
        
        sleep(random.uniform(1.0, 3.0))

        posts_without_image_bg = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT)
        posts_with_image_bg = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT_WITH_BG_IMG)

        now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))

        for p in posts_without_image_bg:
            post_entity = self._get_raw_post_dict(p=p, now=now)
            print(post_entity, "\n\n----####----####----####----####----####----####----####----####----####\n\n")

        for p in posts_with_image_bg:
            post_entity = self._get_raw_post_dict(p=p, now=now, has_no_img=True)
            print(post_entity, "\n\n----####----####----####----####----####----####----####----####----####\n\n")