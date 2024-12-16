from abc import ABC, abstractmethod
from datetime import datetime
import pytz
from utils.graph_utils import GraphUtils
from collections.abc import Iterable

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
    
class PostCluster(BaseEntity):
    def __init__(self, cluster_id: int) -> None:
        self.id = cluster_id
        self.keywords = set()
        self.sub_clusters = set()
        self.posts = dict()

    def add_post(self, post: dict):
        self.posts[post.get("id")] = post
        self.add_keywords(new_keywords=post.get("keywords"))

    def add_keywords(self, new_keywords: Iterable | None):
        if new_keywords == None:
            return
        self.keywords.update(new_keywords)

    def add_sub_cluster(self, new_sub_cluster_id: int):
        self.sub_clusters.add(new_sub_cluster_id)
    
class KeywordNode():
    def __init__(self, keywords: set, post_ids: set = None, post_texts_dict: dict = None, node_id: int = None) -> None:
        self.keywords = keywords
        self.post_ids = post_ids
        self.post_texts_dict = post_texts_dict
        self.id = node_id
        self.relevant_nodes = set()
        self.relevant_node_texts_mapping = dict()

    def add_relevant_node_text(self, other_node: 'KeywordNode', other_text: str):
        if self.relevant_node_texts_mapping.get(other_node.id) == None:
            self.relevant_node_texts_mapping[other_node.id] = [other_text]
        else:
            self.relevant_node_texts_mapping[other_node.id].append(other_text)

    def similar_in_post_texts(self, other_node: 'KeywordNode'):
        other_node_is_similar = False
        set_other_post_ids = set(other_node.post_texts_dict.keys())
        set_cur_post_ids = set(self.post_texts_dict.keys())
        post_ids_intersection = set_cur_post_ids.intersection(set_other_post_ids)

        if len(post_ids_intersection) > 0:
            for pid in post_ids_intersection:
                self.add_relevant_node_text(other_node=other_node, other_text=self.post_texts_dict.get(pid))

            return True
        return False

        # if len(post_ids_intersection) > 0:
        #     other_node_is_similar = True
        #     for pid in post_ids_intersection:
        #         self.add_relevant_node_text(other_node=other_node, other_text=self.post_texts_dict.get(pid))
        #     if len(post_ids_intersection) == len(set_cur_post_ids) or len(post_ids_intersection) == len(set_other_post_ids):
        #         return True

        # not_checked_other_post_ids = set_other_post_ids - post_ids_intersection
        # not_checked_cur_post_ids = set_cur_post_ids - post_ids_intersection

        # for other_pid in not_checked_other_post_ids:
        #     is_relevant_partial_texts = False
        #     other_text = other_node.post_texts_dict.get(other_pid)
        #     for cur_pid in not_checked_cur_post_ids:
        #         cur_text = self.post_texts_dict.get(cur_pid)
        #         cosine_similarity = GraphUtils.get_cosine(text1=other_text, text2=cur_text)
        #         if cosine_similarity >= 0.4:
        #             is_relevant_partial_texts = True
        #             break

        #     if is_relevant_partial_texts:
        #         other_node_is_similar = True
        #         self.add_relevant_node_text(other_node=other_node, other_text=other_text)

        # for other_text in other_node.post_texts:
        #     is_relevant_partial_texts = False
        #     for cur_text in self.post_texts:
        #         cosine_similarity = GraphUtils.get_cosine(text1=other_text, text2=cur_text)
        #         if cosine_similarity >= 0.4:
        #             is_relevant_partial_texts = True
        #             break

        #     if is_relevant_partial_texts:
        #         other_node_is_similar = True
        #         self.add_relevant_node_text(other_node=other_node, other_text=other_text)
        
        return other_node_is_similar
        
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