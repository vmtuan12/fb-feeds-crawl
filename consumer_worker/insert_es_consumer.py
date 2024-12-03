from connectors.db_connector import KafkaConsumerBuilder, DbConnectorBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SchemaPathConstant as Schema, ElasticsearchConnectionConstant as ES
from utils.keyword_extract_utils import KeywordExtractionUtils
from elasticsearch import helpers
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep
from threading import Thread
from custom_logging.logging import TerminalLogging
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from datetime import datetime
import hashlib

class ConsumerWorker():
    def __init__(self) -> None:
        self.kafka_consumer = KafkaConsumerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .set_group_id(Kafka.GROUP_ID_INSERT_DB)\
                                                    .set_topics(Kafka.TOPIC_PARSED_POST)\
                                                    .build(avro_schema_path=Schema.PARSED_POST_SCHEMA)
        self.es_client = DbConnectorBuilder().set_host(ES.HOST)\
                                                .set_port(ES.PORT)\
                                                .set_username(ES.USERNAME)\
                                                .set_password(ES.PASSWORD)\
                                                .build_es_client()
        
    def _create_document_index(self, document: dict) -> dict:
        return {
            "_op_type": 'update',
            "_index": 'test-3',
            "_id": "aaaa",
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
                "params": {
                    "text": document["text"],
                    "page": document["page"],
                    "images": document["images"],
                    "reaction_count": document["reaction_count"],
                    "post_time": document["post_time"],
                    "update_time": document["update_time"],
                    "keywords": document["keywords"]
                }
            },
            "upsert": {
                "text": document["text"],
                "page": document["page"],
                "images": document["images"],
                "reaction_count": document["reaction_count"],
                "post_time": document["post_time"],
                "update_time": document["update_time"],
                "keywords": document["keywords"]
            }
        }
    
    def _insert(self, rows: list):
        try:
            helpers.bulk(self.es_client, rows)
        except Exception as e:
            pass

    def start():
        # note: need to implement
        pass