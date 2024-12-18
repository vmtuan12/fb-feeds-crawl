from worker.base_worker import BaseWorker
from connectors.db_connector import KafkaConsumerBuilder, DbConnectorBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SchemaPathConstant as Schema, RedisConnectionConstant as RedisCons
from datetime import datetime, timedelta
from custom_logging.logging import TerminalLogging
from entities.entities import KeywordNode
from utils.constants import APIConstant as API
from utils.graph_utils import GraphUtils
import requests
import re
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
            if node.similar_in_post_ids(other_node=other_node):
                node.add_relevant_node(other_node=other_node)

        node_list.append(node)

    def compute_keyword_nodes_sequentially(self, list_posts: list[dict]) -> list[KeywordNode]:
        node_dict = dict()
        index = 0

        for doc in list_posts['hits']['hits']:
            _source = doc.get("_source")
            text = _source.get("text")
            if "See more" in text or text.count(" ") < 9:
                continue

            keywords = _source.get("keywords")
            set_keywords = set(keywords) if keywords != None else set()

            doc_id = doc.get("_id")
            TerminalLogging.log_info(f"Processing id {doc_id}")

            node_post_dict = {doc_id: text}
            if len(node_dict.keys()) == 0:
                kn = KeywordNode(keywords=set_keywords, post_texts_dict=node_post_dict, node_id=index)
                index += 1
                node_dict[kn.id] = kn
            else:
                doc_belong_to_a_node = False
                max_avg_cosine = 0
                node_with_max_cosine = 0

                for node_id in node_dict.keys():
                    node = node_dict.get(node_id)
                    similar_to_all_text = True
                    cosine_list = []
                    for pid in node.post_texts_dict.keys():
                        cosine_sim = GraphUtils.get_cosine(text1=text, text2=node.post_texts_dict.get(pid))
                        if cosine_sim < 0.45:
                            similar_to_all_text = False
                            break
                        else:
                            cosine_list.append(cosine_sim)

                    if similar_to_all_text:
                        doc_belong_to_a_node = True
                        avg_cosine = sum(cosine_list)/len(cosine_list)
                        if avg_cosine > max_avg_cosine:
                            max_avg_cosine = avg_cosine
                            node_with_max_cosine = node_id

                if doc_belong_to_a_node:
                    node_dict[node_with_max_cosine].keywords.update(set_keywords)
                    node_dict[node_with_max_cosine].post_texts_dict.update(node_post_dict)
                else:
                    kn = KeywordNode(keywords=set_keywords, post_texts_dict=node_post_dict, node_id=index)
                    index += 1
                    node_dict[kn.id] = kn

        return list(node_dict.values())

    def compute_keyword_nodes_with_cosine(self, list_keywords: list[str], list_posts: list[dict]) -> list[KeywordNode]:
        index = 0
        node_list = []

        for keyword, r in zip(list_keywords, list_posts):
            node_post_ids = set()
            node_post_dict = dict()
            for doc in r['hits']['hits']:
                _source = doc.get("_source")
                text = _source.get("text")
                if "See more" in text or len(re.compile(r"\w+").findall(text)) < 10:
                    continue
                node_post_ids.add(doc.get("_id"))
                node_post_dict[doc.get("_id")] = text

            kn = KeywordNode(keywords={keyword}, post_ids=node_post_ids, post_texts_dict=node_post_dict, node_id=index)
            TerminalLogging.log_info(f"Processing node {keyword}")
            index += 1

            for other_node in node_list:
                if other_node.similar_in_post_texts(other_node=kn):
                    other_node.add_relevant_node(other_node=kn)

            node_list.append(kn)

        return node_list

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
        prompt = """Bạn là một nhà văn với nhiều năm kinh nghiệm. Với dữ liệu đầu vào là 1 danh sách các đoạn văn, tóm tắt các đoạn văn đó thành một đoạn văn đầy đủ chi tiết.
        Ngoài ra, viết thêm 1 tiêu đề thú vị cho đoạn văn vừa tóm tắt.
        Nếu có đoạn văn nào không liên quan tới phần lớn các đoạn văn, không tóm tắt nội dung đoạn văn đó.
        Kết quả đầu ra có dạng Json như sau {"title": <tiêu đề>, "content": <nội dung tóm tắt>}"""
        body = {"prompt": [
            {'role': 'system', 'content': prompt},
            {'role': 'user', 'content': f'{str(list_text)}'}
        ], "json_output": True}

        response = requests.post(url=API.MODEL_API, data=json.dumps(body))
        result = json.loads(response.text).get("data")
        return result
    
    def start(self):
        pass