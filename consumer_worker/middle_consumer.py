from connectors.db_connector import KafkaConsumerBuilder, KafkaProducerBuilder
from utils.constants import KafkaConnectionConstant as Kafka
from utils.parser_utils import ParserUtils
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep
from threading import Thread
from custom_logging.logging import TerminalLogging
import traceback

class MiddleConsumer():
    def __init__(self) -> None:
        self.kafka_consumer = KafkaConsumerBuilder()\
                                .set_brokers(Kafka.BROKERS)\
                                .set_group_id(Kafka.GROUP_ID_PARSER)\
                                .set_auto_offset_reset("earliest")\
                                .set_topics(Kafka.TOPIC_RAW_POST)\
                                .build()
        self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build()
        
    def __parse_text(self, text: str) -> str:
        soup = BeautifulSoup(text, 'html.parser')
        result = soup.text.strip().replace("\n", " ").replace("\t", " ")
        return result
    
    def _flush(self):
        sleep(5)
        self.kafka_producer.flush()

    def _parse_message(self, msg: dict) -> dict:
        now = datetime.strptime(msg["first_scraped_at"], "%Y-%m-%d %H:%M:%S")
        msg["text"] = self.__parse_text(msg["text"])
        msg["post_time"] = ParserUtils.approx_post_time_str(now=now, raw_post_time=msg["post_time"])
        msg["reaction_count"] = ParserUtils.approx_reactions(msg["reaction_count"])
        return msg
    
    
    def start(self):
        Thread(target=self._flush).start()
        for message in self.kafka_consumer:
            raw_post = message.value
            try:
                parsed_post = self._parse_message(msg=raw_post)
                if parsed_post.get("reaction_count") == None or parsed_post.get("post_time") == None:
                    continue

                self.kafka_producer.send(Kafka.TOPIC_PARSED_POST, value=parsed_post)
                TerminalLogging.log_info(message=f"Sent message at offset {message.offset} in partition {message.partition}")
            except Exception as e:
                TerminalLogging.log_error(message=f"Failed message at offset {message.offset} in partition {message.partition}")
                parsed_post["err"] = traceback.format_exc()
                self.kafka_producer.send(Kafka.TOPIC_FAILED_PARSED_POST, value=parsed_post)
            
    def clean_up(self):
        self.kafka_producer.flush()
        self.kafka_producer.close(timeout=5)
        self.kafka_consumer.close(autocommit=False)

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()