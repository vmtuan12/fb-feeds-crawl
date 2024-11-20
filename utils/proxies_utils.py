import os
import secrets
import requests
import json
from utils.general_utils import GeneralUtils
from custom_logging.logging import TerminalLogging

class ProxiesUtils():
    BASE_PROXY_FOLDER = f"{os.getcwd()}/assets/proxies_dirs"

    @classmethod
    def create_proxy_dir(cls, proxy: str):
        proxy_host, proxy_port, proxy_user, proxy_pass = proxy.split(":")
        manifest_json = """
        {
            "version": "1.0.0",
            "manifest_version": 2,
            "name": "Chrome Proxy",
            "permissions": [
                "proxy",
                "tabs",
                "unlimitedStorage",
                "storage",
                "<all_urls>",
                "webRequest",
                "webRequestBlocking"
            ],
            "background": {
                "scripts": ["background.js"]
            },
            "minimum_chrome_version":"22.0.0"
        }
        """

        background_js = """
        var config = {
                mode: "fixed_servers",
                rules: {
                singleProxy: {
                    scheme: "http",
                    host: "%s",
                    port: parseInt(%s)
                },
                bypassList: ["localhost"]
                }
            };

        chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

        function callbackFn(details) {
            return {
                authCredentials: {
                    username: "%s",
                    password: "%s"
                }
            };
        }

        chrome.webRequest.onAuthRequired.addListener(
                    callbackFn,
                    {urls: ["<all_urls>"]},
                    ['blocking']
        );
        """ % (proxy_host, proxy_port, proxy_user, proxy_pass)

        spec_proxy_dir = f"{cls.BASE_PROXY_FOLDER}/{proxy_host}_{proxy_port}_{proxy_user}_{proxy_pass}"
        if not os.path.exists(spec_proxy_dir):
            os.makedirs(spec_proxy_dir)

        with open(f"{spec_proxy_dir}/manifest.json","w") as f:
            f.write(manifest_json)
        with open(f"{spec_proxy_dir}/background.js","w") as f:
            f.write(background_js)

    @classmethod
    def get_proxy_dir(cls) -> str:
        list_proxies_dirs = os.listdir(cls.BASE_PROXY_FOLDER)
        selection = secrets.choice(list_proxies_dirs)
        return f"{cls.BASE_PROXY_FOLDER}/{selection}"

    @classmethod
    def proxy_is_working(cls, proxy_dir: str) -> str:
        proxy_host, proxy_port, proxy_user, proxy_pass = (proxy_dir.split("/"))[-1].split("_")
        url = f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}"
        proxy_json = {
            "http": url,
            "https": url
        }

        try:
            requests.get("https://ipinfo.io/json", timeout=3, proxies=proxy_json)
        except requests.exceptions.ProxyError as err:
            TerminalLogging.log_error(f"ERROR PROXY {proxy_dir}")
            GeneralUtils.remove_dir(proxy_dir)
            return False

        return True