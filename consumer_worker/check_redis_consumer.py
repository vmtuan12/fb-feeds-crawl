from connectors.db_connector import KafkaConsumerBuilder, DbConnectorBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SchemaPathConstant as Schema, RedisConnectionConstant as RedisCons
from utils.keyword_extract_utils import KeywordExtractionUtils
from utils.parser_utils import ParserUtils
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep
from threading import Thread
from custom_logging.logging import TerminalLogging
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import hashlib

class ConsumerWorker():
    def __init__(self) -> None:
        self.kafka_consumer = KafkaConsumerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .set_group_id(Kafka.GROUP_ID_CACHE_CHECKER)\
                                                    .set_topics(Kafka.TOPIC_PARSED_POST)\
                                                    .build(avro_schema_path=Schema.PARSED_POST_SCHEMA)
        self.redis_conn = DbConnectorBuilder().set_host(RedisCons.HOST)\
                                                .set_port(RedisCons.PORT)\
                                                .set_username(RedisCons.USERNAME)\
                                                .set_password(RedisCons.PASSWORD)\
                                                .build_redis()
    
    def _insert_recent_id(self, docs: list[dict], ttl=7200):
        list_keys = set()
        for d in docs:
            key = f'{RedisCons.PREFIX_POST_ID}.{d.get("id")}'
            list_keys.add(key)

        value = ""
        pipeline = self.redis_conn.pipeline()
        for key in list_keys:
            pipeline.set(key, value, ex=ttl)
        pipeline.execute()
        pipeline.close()

    def start(self):
        pass

    def clean_up(self):
        self.kafka_consumer.close(autocommit=False)
        self.redis_conn.close()

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()