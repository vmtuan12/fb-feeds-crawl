class PageNotReadyException(Exception):
    def __init__(self, proxy_dir: str, recheck=False, *args: object) -> None:
        self.proxy_dir = proxy_dir
        self.recheck = recheck
        super().__init__(*args)

class PageCannotAccessException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
