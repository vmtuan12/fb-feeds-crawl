from abc import ABC, abstractmethod

class BaseEntity():
    def to_dict(self) -> dict:
        return self.__dict__
    
class RawPostEntity(BaseEntity):

    def __init__(self, text: str, images: list[str], reaction_count: int, post_time: str):
        self.text = text
        self.images = images
        self.reaction_count = reaction_count
        self.post_time = post_time
        
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