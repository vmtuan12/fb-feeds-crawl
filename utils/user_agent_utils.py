import os
import secrets

class UserAgentUtils():
    ASSETS_PATH = f"{os.getcwd()}/assets"
    USER_AGENT_FB_PAGE_FILE_PATH = f"{ASSETS_PATH}/user_agents_fbm.txt"

    @classmethod
    def get_user_agent_fb_page(cls) -> str:
        f = open(cls.USER_AGENT_FB_PAGE_FILE_PATH, "r")
        ua_list = [l.strip() for l in f.readlines()]
        selection = secrets.choice(ua_list)
        return selection