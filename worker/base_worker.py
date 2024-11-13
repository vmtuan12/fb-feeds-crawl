from abc import ABC, abstractmethod

class BaseWorker(ABC):
    @abstractmethod
    def start(self):
        pass