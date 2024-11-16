from abc import ABC, abstractmethod

class BaseWorker(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def on_close(self):
        pass