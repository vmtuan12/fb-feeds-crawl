from worker.base_worker import BaseWorker
from connectors.db_connector import KafkaConsumerBuilder, DbConnectorBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SchemaPathConstant as Schema, RedisConnectionConstant as RedisCons
from datetime import datetime, timedelta
from custom_logging.logging import TerminalLogging
from entities.entities import KeywordNode
from utils.constants import APIConstant as API
import requests
import json

class TrendSummarizerWorker(BaseWorker):
    def _match_acronym(self, longer_text: str, shorter_text: str) -> bool:
        if longer_text.replace(" ", "") == shorter_text.replace(" ", ""):
            return True
        
        acronym = ""
        for word in longer_text.split(" "):
            acronym += word[0]
            
        if acronym == shorter_text:
            return True
        
        short_text_breakdown= shorter_text.split(" ")
        for index, word in enumerate(short_text_breakdown):
            if (len(word) >= 2) and (word == acronym[index:(index + len(word))]) and ((len(word) + len(short_text_breakdown) - 1) == len(acronym)):
                return True
            
        return False
    
    def _add_node(self, node_list: list[KeywordNode], node: KeywordNode):
        for other_node in node_list:
            if node.similar_in_posts(other_node=other_node):
                node.add_relevant_node(other_node=other_node)

        node_list.append(node)

    def compute_keyword_nodes(self, list_keywords: list[str], dict_keyword_posts: dict) -> list[KeywordNode]:
        node_list = []

        for keyword1 in list_keywords:
            k1_posts = dict_keyword_posts.get(keyword1)
            if " " not in keyword1:
                kn = KeywordNode(keywords={keyword1}, post_ids=k1_posts)
                self._add_node(node_list=node_list, node=kn)
                continue
            
            dict_found = dict()
            for keyword2 in list_keywords:
                if len(keyword2) <= len(keyword1):
                    continue
                
                k2_posts = dict_keyword_posts.get(keyword2)
                # if ((keyword1 in keyword2) and (len(keyword1.split(" ")) * 2 >= len(keyword2.split(" "))) and (abs(len(k1_posts) - len(k2_posts)) < 10)) or \
                #     self._match_acronym(longer_text=keyword2, shorter_text=keyword1):
                if self._match_acronym(longer_text=keyword2, shorter_text=keyword1):
                    dict_found[keyword2] = k2_posts

            if len(dict_found.keys()) == 0:
                kn = KeywordNode(keywords={keyword1}, post_ids=k1_posts)
                self._add_node(node_list=node_list, node=kn)

            if len(dict_found.keys()) == 1:
                found_keyword = list(dict_found.keys())[0]
                found_posts = list(dict_found.values())[0]
                kn = KeywordNode(keywords={keyword1, found_keyword}, 
                                post_ids=k1_posts.union(found_posts))
                self._add_node(node_list=node_list, node=kn)
                
            elif len(dict_found.keys()) >= 2:
                kn = KeywordNode(keywords={keyword1}, post_ids=k1_posts)
                self._add_node(node_list=node_list, node=kn)

        return node_list
    
    def _summarize_texts(self, list_text: list) -> dict:
        prompt = """Given a list of texts. You are an experienced writer. Summarize given texts into 1 or 2 short paragraphs, then create a interesting title. Everything is written in Vietnamese.
        If there is a irrelevant text, do not summarize it.
        The answer must be json-formatted, with the format {"title": <the title you have written>, "content": <the content you have summarized>}"""
        body = {"prompt": [
            {'role': 'system', 'content': prompt},
            {'role': 'user', 'content': f'{str(list_text)}'}
        ], "json_output": True}

        response = requests.post(url=API.MODEL_API, data=json.dumps(body))
        result = json.loads(response.text).get("data")
        return result
    
    def start(self):
        pass