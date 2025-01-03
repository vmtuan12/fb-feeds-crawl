from worker.base_worker import BaseWorker
from connectors.db_connector import DbConnectorBuilder, KafkaProducerBuilder
from utils.constants import ElasticsearchConnectionConstant as ES, KafkaConnectionConstant as Kafka
from entities.entities import ReactionCommandEntity
from utils.command_utils import CommandType
from custom_logging.logging import TerminalLogging
from datetime import datetime, timedelta
import pytz

class CommandWorker(BaseWorker):
    def __init__(self) -> None:
        self.es_client = DbConnectorBuilder().set_host(ES.HOST)\
                                                .set_port(ES.PORT)\
                                                .set_username(ES.USERNAME)\
                                                .set_password(ES.PASSWORD)\
                                                .build_es_client()
        self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build()
        
    def _get_posts_list(self) -> list:
        now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
        prev_12h = now - timedelta(hours=12)

        now_str = now.strftime("%Y-%m-%dT%H:%M:%S")
        prev_12h_str = prev_12h.strftime("%Y-%m-%dT%H:%M:%S")

        query = {
            "query": {
                "range": {
                    "update_time": {
                        "gte": f"{prev_12h_str}",
                        "lt": f"{now_str}"
                    }
                }
            },
            "size": 10000
        }

        result = []

        index_name_prefix = "fb_post_desktop-{}"
        indices = [index_name_prefix.format(now.strftime("%Y%m")), index_name_prefix.format(prev_12h.strftime("%Y%m"))]
        if now.month == prev_12h.month:
            indices = indices[:-1]

        for index in indices:
            response = self.es_client.search(index=index, body=query)
            result += response['hits']['hits']

        return result

    def start(self):
        posts = self._get_posts_list()
        current_partition = 0

        for index, p in enumerate(posts):
            command_msg = ReactionCommandEntity(url=p["_id"]).to_dict()
            try:
                self.kafka_producer.send(Kafka.TOPIC_REACTION_COMMAND, value=command_msg, partition=current_partition)
            except AssertionError as ae:
                current_partition = 0
                self.kafka_producer.send(Kafka.TOPIC_REACTION_COMMAND, value=command_msg, partition=current_partition)
                
            current_partition += 1

        self.kafka_producer.flush()

    def clean_up(self):
        self.kafka_producer.flush()
        self.kafka_producer.close(timeout=5)
        self.es_client.close()

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()