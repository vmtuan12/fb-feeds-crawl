from abc import ABC, abstractmethod
from datetime import datetime
import pytz

class BaseEntity():
    def to_dict(self) -> dict:
        return self.__dict__
    
class RawPostEntity(BaseEntity):

    def __init__(self, text: str, images: list[str], reaction_count: int, post_time: str):
        now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%Y-%m-%d %H:%M:%S")
        self.text = text
        self.images = images
        self.reaction_count = reaction_count
        self.post_time = post_time
        self.first_scraped_at = now
        self.last_updated_at = now
        
class Builder(ABC):
    @abstractmethod
    def build(self) -> None:
        pass

class RawPostEntityBuilder(Builder):
    def set_text(self, text: str):
        self.text = text
        return self
    
    def set_images(self, images: list[str]):
        self.images = images
        return self
    
    def set_reaction_count(self, reaction_count: int):
        self.reaction_count = reaction_count
        return self
    
    def set_post_time(self, post_time: str):
        self.post_time = post_time
        return self

    def build(self) -> RawPostEntity:
        return RawPostEntity(text=self.text, images=self.images, reaction_count=self.reaction_count, post_time=self.post_time)