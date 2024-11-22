class PageNotReadyException(Exception):
    def __init__(self, proxy_dir: str, *args: object) -> None:
        self.proxy_dir = proxy_dir
        super().__init__(*args)

class PageCannotAccessException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)