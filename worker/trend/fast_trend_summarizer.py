from worker.trend.trend_summarizer import TrendSummarizerWorker
from connectors.db_connector import DbConnectorBuilder
from utils.constants import ElasticsearchConnectionConstant as ES, RedisConnectionConstant as RedisCons, PostgresConnectionConstant as PgCons
from utils.insert_pg_utils import InsertPgUtils
from datetime import datetime, timedelta
from custom_logging.logging import TerminalLogging
from entities.entities import FastTrendEntity
import pytz
import math

class FastTrendSummarizerWorker(TrendSummarizerWorker):
    def __init__(self) -> None:
        self.redis_conn = DbConnectorBuilder().set_host(RedisCons.HOST)\
                                                .set_port(RedisCons.PORT)\
                                                .set_username(RedisCons.USERNAME)\
                                                .set_password(RedisCons.PASSWORD)\
                                                .build_redis()
        self.pg_conn = DbConnectorBuilder().set_host(PgCons.HOST)\
                                            .set_port(PgCons.PORT)\
                                            .set_username(PgCons.USER)\
                                            .set_password(PgCons.PWD)\
                                            .set_db_name(PgCons.DB)\
                                            .build_pg()
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
        TerminalLogging.log_info(f"Computed {len(keyword_nodes_list)} nodes!")
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
            if len(g["posts"]) == 0:
                continue
            self._append_search_body(msearch_body=msearch_body, id_list=list(g["posts"]), current_date=current_date)

        es_search_res = self.es_client.msearch(body=msearch_body)
        list_trends = []
        for sk, r in zip(list_set_keywords, es_search_res['responses']):
            list_texts = []
            set_images = set()
            for doc in r['hits']['hits']:
                _source = doc.get("_source")
                if _source.get("keywords") == None:
                    continue
                keywords = set(_source.get("keywords"))
                images = _source.get("images")
                text = _source.get("text")

                if len(sk) <= 2:
                    if len(keywords.intersection(sk)) == len(sk):
                        list_texts.append(text)
                        if images != None:
                            set_images.update(set(images))
                elif len(keywords.intersection(sk)) >= math.ceil(len(sk)/3):
                    list_texts.append(text)
                    if images != None:
                        set_images.update(set(images))

            TerminalLogging.log_info(f"Keywords {sk}")
            content_dict = self._summarize_texts(list_text=list_texts)
            TerminalLogging.log_info(f"{content_dict}")
            sum_trend = FastTrendEntity(title=content_dict.get("title"),
                                        content=content_dict.get("content"),
                                        images=list(set_images),
                                        keywords=list(sk),
                                        update_time=now.strftime("%Y-%m-%d %H:%M:%S"))
            list_trends.append(sum_trend.to_dict())

        insert_statement = "INSERT INTO fb.fast_trends (%s) VALUES %s"
        query_insert, value_insert = InsertPgUtils.generate_query_insert_list_dict(insert_statement=insert_statement, data_list=list_trends)
        self.pg_conn.cursor().execute(query_insert, value_insert)
        self.pg_conn.commit()

        self.clean_up()
    
    def clean_up(self):
        self.redis_conn.close()
        self.es_client.close()
        self.pg_conn.close()
 
    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()