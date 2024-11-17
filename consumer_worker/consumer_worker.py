from concurrent.futures import ThreadPoolExecutor
import threading
from worker.fb_page.fb_page_driver_worker import FbPageDriverWorker
from custom_exception.exceptions import *
from utils.proxies_utils import ProxiesUtils
import traceback

class ConsumerWorker():
    def __init__(self) -> None:
        self.thread_local_data = threading.local()

    def get_worker(self) -> FbPageDriverWorker:
        thread_id = threading.current_thread().name
        worker = getattr(self.thread_local_data, 'worker', None)
        if worker is None:
            worker = FbPageDriverWorker()
            setattr(self.thread_local_data, 'worker', worker)
        
        return worker
    
    def run_worker(self, page_name_or_id):
        print(threading.current_thread().name)
        worker = None
        while (True):
            if worker == None:
                worker = self.get_worker()

            try:
                worker.start(page_name_or_id=page_name_or_id)
            except PageNotReadyException as pe:
                proxy_is_working = ProxiesUtils.proxy_is_working(proxy_dir=pe.proxy_dir)
                if not proxy_is_working:
                    delattr(self.thread_local_data, 'worker', worker)
                    worker = None
                    continue

            except Exception as e:
                print(traceback.format_exc())        

    def start(self):
        futures = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures.append(executor.submit(self.run_worker, "thoibaonganhang.vn"))
            futures.append(executor.submit(self.run_worker, "beatvn.network"))
            futures.append(executor.submit(self.run_worker, "Theanh28"))
            futures.append(executor.submit(self.run_worker, "K14vn"))

        for future in futures:
            future.result()
