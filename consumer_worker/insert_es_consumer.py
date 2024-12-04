from connectors.db_connector import KafkaConsumerBuilder, KafkaProducerBuilder, DbConnectorBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SchemaPathConstant as Schema, ElasticsearchConnectionConstant as ES
from elasticsearch import helpers
from datetime import datetime
from custom_logging.logging import TerminalLogging
import traceback
from datetime import datetime

class InsertESConsumer():
    def __init__(self) -> None:
        self.kafka_consumer = KafkaConsumerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .set_group_id(Kafka.GROUP_ID_INSERT_DB)\
                                                    .set_auto_offset_reset("earliest")\
                                                    .set_topics(Kafka.TOPIC_PARSED_POST)\
                                                    .build(avro_schema_path=Schema.PARSED_POST_SCHEMA)
        self.es_client = DbConnectorBuilder().set_host(ES.HOST)\
                                                .set_port(ES.PORT)\
                                                .set_username(ES.USERNAME)\
                                                .set_password(ES.PASSWORD)\
                                                .build_es_client()
        self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build()
        
    def _create_document_index(self, document: dict) -> dict:
        time_format = "%Y-%m-%d %H:%M:%S"
        current_year = datetime.now().year

        formatted_doc = {
            "text": document["text"],
            "page": document["page"],
            "images": document["images"],
            "reaction_count": document["reaction_count"],
            "post_time": datetime.strptime(document["post_time"], time_format),
            "update_time": [datetime.strptime(t, time_format) for t in document["update_time"]],
            "keywords": document["keywords"]
        }
        if formatted_doc.get("post_time").year < current_year:
            return None
        
        _index = f'fb_post-{document["post_time"].split(" ")[0]}'

        return {
            "_op_type": 'update',
            "_index": _index,
            "_id": f"{document['id']}",
            "script": {
                "source": """
                    if (ctx._source.text == null) {
                        ctx._source.text = params.text;
                    }
                    if (ctx._source.page == null) {
                        ctx._source.page = params.page;
                    }
                    if (ctx._source.images == null || ctx._source.images.length == 0) {
                        ctx._source.images = params.images;
                    }
                    if (ctx._source.post_time == null) {
                        ctx._source.post_time = params.post_time;
                    }
                    if (ctx._source.keywords == null || ctx._source.keywords.length == 0) {
                        ctx._source.keywords = params.keywords;
                    }
                    if (ctx._source.keywords == null || ctx._source.keywords.length == 0) {
                        ctx._source.keywords = params.keywords;
                    }
                    if (ctx._source.reaction_count == null || ctx._source.reaction_count.length == 0) {
                        ctx._source.reaction_count = params.reaction_count;
                    } else {
                        ctx._source.reaction_count.addAll(params.reaction_count);
                    }
                    if (ctx._source.update_time == null || ctx._source.update_time.length == 0) {
                        ctx._source.update_time = params.update_time;
                    } else {
                        ctx._source.update_time.addAll(params.update_time);
                    }
                """,
                "params": formatted_doc
            },
            "upsert": formatted_doc
        }
    
    def _insert(self, rows: list):
        if len(rows) == 0:
            return
        
        indexed_docs = []
        for r in rows:
            d = self._create_document_index(document=r)
            if d != None:
                indexed_docs.append(d)

        try:
            helpers.bulk(self.es_client, indexed_docs)
            TerminalLogging.log_info(f"Successfully inserted {len(indexed_docs)} docs into elasticsearch!")
        except Exception as e:
            _trace_back = traceback.format_exc()
            TerminalLogging.log_error(_trace_back)
            for r in rows:
                r["error"] = _trace_back
                self.kafka_producer.send(Kafka.TOPIC_FAILED_INSERT_ES, r)
            self.kafka_producer.flush()

    def start(self, max_records=100):
        post_list = []

        while (True):
            records = self.kafka_consumer.poll(max_records=max_records, timeout_ms=20000)
            TerminalLogging.log_info(f"Polled {len(records.items())} items!")
            if len(records.items()) == 0:
                self._insert(rows=post_list.copy())
                post_list.clear()

            for topic_data, consumer_records in records.items():
                TerminalLogging.log_info(f"Processing {len(consumer_records)} records!")
                for consumer_record in consumer_records:
                    parsed_post = consumer_record.value
                    post_list.append(parsed_post)

            if len(post_list) >= 200:
                self._insert(rows=post_list.copy())
                post_list.clear()

    def clean_up(self):
        self.es_client.close()
        self.kafka_producer.close(timeout=5)
        self.kafka_consumer.close(autocommit=False)

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()