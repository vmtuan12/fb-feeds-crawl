from connectors.db_connector import KafkaConsumerBuilder, KafkaProducerBuilder, DbConnectorBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SchemaPathConstant as Schema, RedisConnectionConstant as RedisCons, SysConstant as Sys
from utils.keyword_extract_utils import KeywordExtractionUtils
from utils.parser_utils import ParserUtils
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep
from threading import Thread
from custom_logging.logging import TerminalLogging
from concurrent.futures import ThreadPoolExecutor, as_completed
from unidecode import unidecode
import traceback
import hashlib
import ahocorasick

class ParserConsumer():
    def __init__(self) -> None:
        self.kafka_consumer = KafkaConsumerBuilder()\
                                .set_brokers(Kafka.BROKERS)\
                                .set_group_id(Kafka.GROUP_ID_PARSER)\
                                .set_auto_offset_reset("latest")\
                                .set_topics(Kafka.TOPIC_RAW_POST)\
                                .build()
        self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build(avro_schema_path=Schema.PARSED_POST_SCHEMA)
        self.kafka_producer_fail_msg = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build()
        
        self.redis_conn = DbConnectorBuilder().set_host(RedisCons.HOST)\
                                                .set_port(RedisCons.PORT)\
                                                .set_username(RedisCons.USERNAME)\
                                                .set_password(RedisCons.PASSWORD)\
                                                .build_redis()
        
        self.ahocorasick_automaton = ahocorasick.Automaton()
        with open(Sys.KEYWORDS, "r") as f:
            for l in f.readlines():
                kw = l.strip()
                self.ahocorasick_automaton.add_word(kw, kw)
        self.ahocorasick_automaton.make_automaton()
        
    def __parse_text(self, text: str) -> str:
        soup = BeautifulSoup(text, 'html.parser')
        result = soup.text.strip().replace("\n", " ").replace("\t", " ").replace("See more", "")
        return result
        
    def __parse_reactions(self, reactions: str) -> list:
        soup = BeautifulSoup(reactions, 'html.parser')
        reaction_text = soup.text.strip()
        result = [ParserUtils.approx_reactions(reaction_text)]
        return result
    
    def __match_available_keywords(self, raw_text: str) -> set:
        found_keywords = set()
        text = ParserUtils.clean_text(text=raw_text).lower()
        if len(text) == 0:
            return found_keywords
        
        for end_index, found_keyword in self.ahocorasick_automaton.iter(text):
            start_index = end_index - len(found_keyword) + 1

            if (start_index == 0 or not text[start_index-1].isalnum()) and \
            (end_index + 1 == len(text) or not text[end_index + 1].isalnum()):
                found_keywords.add(found_keyword)

        return found_keywords
        
    def __generate_id(self, text: str, page: str) -> str:
        text_remove_space = ''.join([c for c in text if not c.isspace()]).lower()
        union_text = unidecode(text_remove_space + page)
        return hashlib.sha256(union_text.encode('utf-8')).hexdigest()
    
    def _flush(self):
        sleep(5)
        self.kafka_producer.flush()

    def _parse_message(self, msg: dict) -> dict:
        now = datetime.strptime(msg["first_scraped_at"], "%Y-%m-%d %H:%M:%S")
        msg["text"] = self.__parse_text(msg["text"])
        msg["id"] = self.__generate_id(text=msg["text"], page=msg["page"])
        msg["post_time"] = ParserUtils.approx_post_time_str(now=now, raw_post_time=msg["post_time"])
        msg["update_time"] = [msg["first_scraped_at"]]
        msg["reaction_count"] = self.__parse_reactions(reactions=msg["reaction_count"])
        msg["keywords"] = self.__match_available_keywords(raw_text=msg["text"])

        msg.pop('first_scraped_at')
        msg.pop('last_updated_at')
        return msg
    
    def _split_list(self, original_list: list, size: int) -> list:
        return [original_list[i:i + size] for i in range(0, len(original_list), size)]
    
    def _list_posts_have_and_not_have_keywords(self, list_posts: list[dict]) -> tuple:
        dict_post_by_id = dict()
        for p in list_posts:
            dict_post_by_id[p.get("id")] = p

        list_ids = list(dict_post_by_id.keys())
        pipeline = self.redis_conn.pipeline()
        for _id in list_ids:
            key = f'{RedisCons.PREFIX_POST_ID}.{_id}'
            pipeline.get(key)
        values = pipeline.execute()
        pipeline.close()

        list_have_keywords = []
        list_not_have_keywords = []

        result = {key: value for key, value in zip(list_ids, values)}
        for k in result.keys():
            if result.get(k) != None:
                list_have_keywords.append(dict_post_by_id.get(k))
            else:
                list_not_have_keywords.append(dict_post_by_id.get(k))

        return list_have_keywords, list_not_have_keywords
    
    def _extract_keywords(self, list_need_extract_keywords: list, chunk_size: int):
        with ThreadPoolExecutor(max_workers=5) as thread_pool:
            sublists = self._split_list(list_need_extract_keywords.copy(), chunk_size)
            futures = []
            for chunk in sublists:
                job = thread_pool.submit(KeywordExtractionUtils.enrich_keywords, chunk)
                job.add_done_callback(self.callback_enrich_keyword)
                futures.append(job)

            for future in as_completed(futures):
                pass
        
        list_need_extract_keywords.clear()
    
    def callback_enrich_keyword(self, res):
        # result = res
        result = res.result()
        for item in result:
            try:
                self.kafka_producer.send(Kafka.TOPIC_PARSED_POST, value=item)
            except Exception as e:
                TerminalLogging.log_error(traceback.format_exc())

    
    def start(self, max_records=100, chunk_size=25):
        Thread(target=self._flush).start()
        list_need_extract_keywords = []

        while (True):
            try:
                records = self.kafka_consumer.poll(max_records=max_records, timeout_ms=10000)
                if len(records.items()) == 0:
                    if len(list_need_extract_keywords) > 0:
                        self._extract_keywords(list_need_extract_keywords=list_need_extract_keywords,
                                                chunk_size=chunk_size)
                
                temp_parsed_posts = []
                for topic_data, consumer_records in records.items():
                    for consumer_record in consumer_records:
                        raw_post = consumer_record.value
                        try:
                            parsed_post = self._parse_message(msg=raw_post)
                            if parsed_post.get("reaction_count") == None or parsed_post.get("post_time") == None:
                                continue
                            temp_parsed_posts.append(parsed_post)
                        except Exception as e:
                            TerminalLogging.log_error(message=f"Failed message at offset {consumer_record.offset} in partition {consumer_record.partition}")
                            parsed_post["err"] = traceback.format_exc()
                            parsed_post["partition"] = consumer_record.partition
                            parsed_post["offset"] = consumer_record.offset
                            self.kafka_producer_fail_msg.send(Kafka.TOPIC_FAILED_PARSED_POST, value=parsed_post)
                            self.kafka_producer_fail_msg.flush()

                posts_have_keywords, posts_not_have_keywords = self._list_posts_have_and_not_have_keywords(list_posts=temp_parsed_posts.copy())
                TerminalLogging.log_info(f"{len(posts_have_keywords)} posts have keywords, {len(posts_not_have_keywords)} posts dont have keywords")
                for p in posts_have_keywords:
                    p["keywords"] = list(p["keywords"])
                    self.kafka_producer.send(Kafka.TOPIC_PARSED_POST, value=p)
                    
                list_need_extract_keywords += posts_not_have_keywords
                temp_parsed_posts.clear()

                if len(list_need_extract_keywords) >= chunk_size:
                    self._extract_keywords(list_need_extract_keywords=list_need_extract_keywords,
                                            chunk_size=chunk_size)

                    # sleep(1000)

            except Exception as e:
                TerminalLogging.log_error(traceback.format_exc())
            
    def clean_up(self):
        self.kafka_producer.flush()
        self.kafka_producer.close(timeout=5)
        self.kafka_producer_fail_msg.flush()
        self.kafka_producer_fail_msg.close(timeout=5)
        self.kafka_consumer.close(autocommit=False)
        self.redis_conn.close()

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()