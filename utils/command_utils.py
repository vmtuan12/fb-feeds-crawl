class CommandType():
    SCRAPE_PAGE = "scrape_page"
    CHANGE_PROXY = "change_proxy"
    CLEAR_CACHE = "clear_cache"

class CommandUtils():
    @classmethod
    def get_command_type(cls, command: dict) -> str:
        return command.get("type")
    
    @classmethod
    def get_page(cls, command: dict) -> str | None:
        return command.get("page")
    
    @classmethod
    def get_scrape_threshold(cls, command: dict) -> int | None:
        return command.get("scrape_threshold")