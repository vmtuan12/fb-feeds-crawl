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

class FbPageDriverWorker(DriverWorker):
    def __init__(self, page_name: str, min_post_count: int = 10) -> None:
        self.url = f"https://m.facebook.com/{page_name}?locale=en_US"
        self.min_post_count = min_post_count

        self.window_w, self.window_h = 820, 1180

        proxy_dir = ProxiesUtils.get_proxy_dir()
        options = DriverUtils.create_option(arguments_dict={
            "--window-size": f"{self.window_w},{self.window_h}"
        })
        driver = DriverSelector.get_driver(driver_type=DriverType.SELENIUM, options=options)

        super().__init__(driver)

    def start(self):
        user_agent = UserAgentUtils.get_user_agent_fb_page()
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})

        self.driver.get(self.url)

        count_scroll = 0
        while (True):
            posts = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT) + self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT_WITH_BG_IMG)
            if len(posts) >= self.min_post_count:
                break

            self.driver.execute_script(f"window.scrollTo(0, window.scrollY + {self.window_h / 4});")
            count_scroll += 1

            if count_scroll % 3 == 0:
                self.driver.execute_script(f"window.scrollTo(0, window.scrollY - {self.window_h / 3});")

            sleep(random.uniform(1.0, 3.0))

        texts_load_more = self.driver.find_elements_by_xpath(value=FbPageXpathUtils.XPATH_TEXT_WITH_LOAD_MORE)
        for t in texts_load_more:
            self.driver.execute_script("arguments[0].click();", t)
        
        sleep(random.uniform(1.0, 3.0))

        posts_without_image_bg = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT)
        posts_with_image_bg = self.driver.find_elements_by_xpath(FbPageXpathUtils.XPATH_TEXT_WITH_BG_IMG)

        now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))

        for p in posts_without_image_bg:
            images = p.find_elements(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_IMAGES)
            reactions = p.find_element(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_REACTION)
            post_time = p.find_element(by='xpath', value=FbPageXpathUtils.XPATH_ADDITIONAL_POST_TIME)

            post_time_str = ParserUtils.match_text_and_number(post_time.text)
            real_post_time_str = ParserUtils.approx_post_time_str(now=now, post_time=post_time_str)
            reaction_count = ParserUtils.approx_reactions(reactions.text)

            raw_post_entity = RawPostEntity(text=p.text, 
                                            images=[i.get_attribute("src") for i in images],
                                            reaction_count=reaction_count,
                                            post_time=real_post_time_str).to_dict()
            