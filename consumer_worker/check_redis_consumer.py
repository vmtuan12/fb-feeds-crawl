from connectors.db_connector import KafkaConsumerBuilder, DbConnectorBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SchemaPathConstant as Schema, RedisConnectionConstant as RedisCons
from datetime import datetime
from custom_logging.logging import TerminalLogging

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
    
    def _process_recent_docs(self, docs: list[dict], ttl_id=7200, ttl_keyword=86400):
        current_time = datetime.now()
        list_keys = set()
        dict_keyword_post = dict()

        for d in docs:
            post_id = d.get("id")
            post_time = datetime.strptime(d["post_time"], "%Y-%m-%d %H:%M:%S")
            list_keys.add(f'{RedisCons.PREFIX_POST_ID}.{post_id}')

            if (current_time - post_time).days > 7:
                continue

            for keyword in d.get("keywords"):
                if dict_keyword_post.get(keyword) == None:
                    dict_keyword_post[keyword] = [post_id]
                else:
                    dict_keyword_post[keyword].append(post_id)

        value = ""
        pipeline = self.redis_conn.pipeline()

        for key in list_keys:
            pipeline.set(key, value, ex=ttl_id)

        for keyword in dict_keyword_post.keys():
            corresponding_posts = dict_keyword_post.get(keyword)
            pipeline.sadd(keyword, *corresponding_posts)
            pipeline.expire(keyword, ttl_keyword)

        pipeline.execute()
        pipeline.close()

    def start(self, max_records=50):
        post_list = []

        while (True):
            records = self.kafka_consumer.poll(max_records=max_records, timeout_ms=20000)
            TerminalLogging.log_info(f"Polled {len(records.items())} items!")
            if len(records.items()) == 0:
                self._process_recent_docs(rows=post_list.copy())
                post_list.clear()

            for topic_data, consumer_records in records.items():
                TerminalLogging.log_info(f"Processing {len(consumer_records)} records!")
                for consumer_record in consumer_records:
                    parsed_post = consumer_record.value
                    post_list.append(parsed_post)

            if len(post_list) >= 50:
                self._process_recent_docs(rows=post_list.copy())
                post_list.clear()

    def clean_up(self):
        self.kafka_consumer.close(autocommit=False)
        self.redis_conn.close()

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()