from connectors.db_connector import KafkaConsumerBuilder, DbConnectorBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SchemaPathConstant as Schema, RedisConnectionConstant as RedisCons
from datetime import datetime
from custom_logging.logging import TerminalLogging
import traceback
from time import sleep
import os

class CheckRedisConsumer():
    def __init__(self) -> None:
        self.kafka_consumer = KafkaConsumerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .set_group_id(Kafka.GROUP_ID_CACHE_CHECKER)\
                                                    .set_auto_offset_reset("latest")\
                                                    .set_topics(Kafka.TOPIC_PARSED_POST)\
                                                    .build(avro_schema_path=Schema.PARSED_POST_SCHEMA)
        self.redis_conn = DbConnectorBuilder().set_host(RedisCons.HOST)\
                                                .set_port(RedisCons.PORT)\
                                                .set_username(RedisCons.USERNAME)\
                                                .set_password(RedisCons.PASSWORD)\
                                                .build_redis()
        self.ttl_keyword = int(os.getenv("TTL_KEYWORD", "21600"))
        self.ttl_id = int(os.getenv("TTL_ID", "172800")) # 2 days
        self.redis_processing_threshold = int(os.getenv("REDIS_PROCESSING_THRESHOLD", "20"))
    
    def _process_recent_docs(self, docs: list[dict], ttl_id=86400, ttl_keyword=21600):
        current_time = datetime.now()
        list_keys = set()
 
        TerminalLogging.log_info(f"Inserting into redis...")
        for d in docs:
            post_id = d.get("id")
            post_page = d.get("page")
            post_time = datetime.strptime(d["post_time"], "%Y-%m-%d %H:%M:%S")
            if d.get("keywords") != None and len(d.get("keywords")) != 0:
                list_keys.add(f'{RedisCons.PREFIX_POST_ID}.{post_page}.{post_id}')
 
            if (current_time - post_time).days > 2 or (d.get("keywords") == None):
                continue
 
        try:
            value = ""
            pipeline = self.redis_conn.pipeline()
 
            for key in list_keys:
                pipeline.set(key, value, ex=ttl_id)
 
            pipeline.execute()
            pipeline.close()

            TerminalLogging.log_info(f"Done inserting into redis!")
        except Exception as e:
            TerminalLogging.log_error(traceback.format_exc())
            sleep(10000)
 
    def start(self, max_records=50):
        post_list = []
 
        while (True):
            records = self.kafka_consumer.poll(max_records=max_records, timeout_ms=10000)
            TerminalLogging.log_info(f"Polled {len(records.items())} items!")
            if len(records.items()) == 0 and len(post_list) > 0:
                self._process_recent_docs(docs=post_list.copy(), ttl_id=self.ttl_id, ttl_keyword=self.ttl_keyword)
                post_list.clear()
 
            for topic_data, consumer_records in records.items():
                TerminalLogging.log_info(f"Processing {len(consumer_records)} records!")
                for consumer_record in consumer_records:
                    parsed_post = consumer_record.value
                    post_list.append(parsed_post)
 
            if len(post_list) >= self.redis_processing_threshold:
                self._process_recent_docs(docs=post_list.copy(), ttl_id=self.ttl_id, ttl_keyword=self.ttl_keyword)
                post_list.clear()
 
    def clean_up(self):
        self.kafka_consumer.close(autocommit=False)
        self.redis_conn.close()
 
    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()