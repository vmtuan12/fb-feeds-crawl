from worker.trend.trend_summarizer import TrendSummarizerWorker
from connectors.db_connector import KafkaConsumerBuilder, DbConnectorBuilder
from utils.constants import ElasticsearchConnectionConstant as ES, RedisConnectionConstant as RedisCons
from datetime import datetime, timedelta
from custom_logging.logging import TerminalLogging
from entities.entities import KeywordNode
import pytz
import math

class FastTrendSummarizerWorker(TrendSummarizerWorker):
    def __init__(self) -> None:
        self.redis_conn = DbConnectorBuilder().set_host(RedisCons.HOST)\
                                                .set_port(RedisCons.PORT)\
                                                .set_username(RedisCons.USERNAME)\
                                                .set_password(RedisCons.PASSWORD)\
                                                .build_redis()
        self.es_client = DbConnectorBuilder().set_host(ES.HOST)\
                                                .set_port(ES.PORT)\
                                                .set_username(ES.USERNAME)\
                                                .set_password(ES.PASSWORD)\
                                                .build_es_client()
        
    def _append_search_body(self, msearch_body: list, id_list: list[str], current_date: str):
        _index = f'fb_post-{current_date}'
        msearch_body.append({ "index": _index })
        msearch_body.append({ "query": { "terms": { "_id" : id_list }}, "size": 10000, "sort": [{ "post_time": { "order": "desc" } }]})

    def _keywords_sets_can_be_merged(self, set_keywords: set, other_set_keywords: set) -> bool:
        len_set_keywords = len(set_keywords)
        len_other_set_keywords = len(other_set_keywords)
        intersection = set_keywords.intersection(other_set_keywords)

        if len_set_keywords > len_other_set_keywords:
            if len_other_set_keywords <= 2 and len(intersection) == len_other_set_keywords:
                return True
            if len_other_set_keywords > 2 and len(intersection) >= math.ceil(len_other_set_keywords/2):
                return True
        elif len_set_keywords < len_other_set_keywords:
            if len_set_keywords <= 2 and len(intersection) == len_set_keywords:
                return True
            if len_set_keywords > 2 and len(intersection) >= math.ceil(len_set_keywords/2):
                return True
        else:
            if len(intersection) == len_set_keywords:
                return True
            if len_set_keywords > 2:
                if len(intersection) >= math.ceil(len_set_keywords/2):
                    return True
        
        return False

    def start(self):
        now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
        current_date = now.strftime("%Y-%m-%d")
        prev_1_day_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")

        dict_keyword_posts = dict()
        keyword_keys = self.redis_conn.keys(f"{RedisCons.PREFIX_KEYWORD}.{current_date}.*")

        pipeline = self.redis_conn.pipeline()
        for k in keyword_keys:
            pipeline.smembers(k)
        results = pipeline.execute()
        for k, s in zip(keyword_keys, results):
            dict_keyword_posts[k.split(".")[-1]] = s

        filtered_keys = []
        for k in keyword_keys:
            keyword = k.split(".")[-1]
            if len(dict_keyword_posts[keyword]) > 5:
                filtered_keys.append(keyword)
                
        keyword_nodes_list = self.compute_keyword_nodes(list_keywords=filtered_keys, dict_keyword_posts=dict_keyword_posts)
        list_grouping = []
        for node in keyword_nodes_list:
            if len(node.relevant_nodes) == 0:
                continue
            
            set_keywords = node.keywords
            set_posts = node.post_ids
            for rn in node.relevant_nodes:
                set_keywords.update(rn.keywords)
            
            found_related = False
            for g in list_grouping:
                other_set_keywords = g["set_keywords"]
                other_posts = g["posts"]
                if self._keywords_sets_can_be_merged(set_keywords=set_keywords, other_set_keywords=other_set_keywords):
                    other_set_keywords.update(set_keywords)
                    other_posts.update(set_posts)
                    found_related = True
                    break
            
            if not found_related:
                list_grouping.append({
                    "set_keywords": set_keywords,
                    "posts": set_posts
                })

        msearch_body = []
        list_set_keywords = []
        for g in list_grouping:
            list_set_keywords.append(g["set_keywords"])
            self._append_search_body(msearch_body=msearch_body, id_list=list(g["posts"]), current_date=current_date)
        
        es_search_res = self.es_client.msearch(body=msearch_body)
        for sk, r in zip(list_set_keywords, es_search_res['responses']):
            list_texts = []
            for doc in r['hits']['hits']:
                _source = doc.get("_source")
                keywords = set(_source.get("keywords"))
                text = _source.get("text")
                if len(sk) <= 2:
                    if len(keywords.intersection(sk)) == len(sk):
                        list_texts.append(text)
                elif len(keywords.intersection(sk)) >= math.ceil(len(sk)/3):
                    list_texts.append(text)

            print(sk)
            self._summarize_texts(list_text=list_texts)
            print("##################################################")

        self.clean_up()
    
    def clean_up(self):
        self.redis_conn.close()
        self.es_client.close()
 
    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()