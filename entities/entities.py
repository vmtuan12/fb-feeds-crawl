from abc import ABC, abstractmethod
from datetime import datetime
import pytz

class BaseEntity():
    def to_dict(self) -> dict:
        return self.__dict__
    
class CommandEntity(BaseEntity):
    def __init__(self, cmd_type: str, page: str | None = None) -> None:
        now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%Y-%m-%d %H:%M:%S")
        self.command_time = now
        self.type = cmd_type
        self.page = page

class RawPostEntity(BaseEntity):

    def __init__(self, text: str, images: list[str], reaction_count: int, post_time: str, page: str):
        now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%Y-%m-%d %H:%M:%S")
        self.text = text
        self.images = images
        self.reaction_count = reaction_count
        self.post_time = post_time
        self.page = page
        self.first_scraped_at = now
        self.last_updated_at = now
        
class Builder(ABC):
    @abstractmethod
    def build(self) -> None:
        pass

class RawPostEntityBuilder(Builder):
    def __init__(self):
        self.text = None
        self.images = None
        self.reaction_count = None
        self.post_time = None

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
    
class KeywordNode():
    def __init__(self, keywords: set, post_ids: set, post_texts: set = None) -> None:
        self.keywords = keywords
        self.post_ids = post_ids
        self.post_texts = post_texts
        self.relevant_nodes = set()

    def similar_in_post_ids(self, other_node: 'KeywordNode'):
        if len(other_node.keywords) == 1 and len(other_node.keywords.intersection(self.keywords)) > 0:
            return False
        
        posts_intersection = self.post_ids.intersection(other_node.post_ids)
        if len(posts_intersection) >= round(len(self.post_ids) / 2, 2):
            return True
        return False
    
    def add_relevant_node(self, other_node: 'KeywordNode'):
        self.relevant_nodes.add(other_node)

class FastTrendEntity(BaseEntity):
    def __init__(self, title: str, content: str, images: list[str], keywords: list[str], update_time: str):
        self.title = title
        self.content = content
        self.images = images
        self.keywords = keywords
        self.update_time = update_time