from connectors.db_connector import KafkaConsumerBuilder, KafkaProducerBuilder
from utils.constants import KafkaConnectionConstant as Kafka
from utils.keyword_extract_utils import KeywordExtractionUtils
from utils.parser_utils import ParserUtils
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep
from threading import Thread
from custom_logging.logging import TerminalLogging
from concurrent.futures import ThreadPoolExecutor
import traceback
import hashlib

class ParserConsumer():
    def __init__(self) -> None:
        self.kafka_consumer = KafkaConsumerBuilder()\
                                .set_brokers(Kafka.BROKERS)\
                                .set_group_id(Kafka.GROUP_ID_PARSER)\
                                .set_auto_offset_reset("earliest")\
                                .set_topics(Kafka.TOPIC_RAW_POST)\
                                .build()
        self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build()
        
        self.api_keys = [
            "AIzaSyBksEFncDgCSAHiqD2lnWIj_eVaMXkvwwg",
            "AIzaSyC12tc0ZSPeikNIuo5_hHnL1NOJRCT4QVo",
            "AIzaSyC8Ih3TaWJ25Wj7Fw0GiCBqmEnXqUdI1fE",
            "AIzaSyBAjXoxTSoaGf2PH4upsneVRxkst_Tq0WY",
            "AIzaSyAWTaGQMr4ufeakTQUfuv70n0mzHBcSMOs",
            "AIzaSyDktty6N2PngkPILMMO1WwAO_ponRU7MqI",
            "AIzaSyCUBRmd0pqeM358g6L0eM5M2QVPRnqKjLE",
            "AIzaSyBG0fPW2-WRT4NPiXZiLIgJn2lsdBCjaX0",
            "AIzaSyBXcdj4af0k5-piXYux1NjI46vgy4A9GTQ",
            "AIzaSyDpZq0WYZyc20havT8awUCyJJ8EPuVCzG0",
            "AIzaSyD9vQwUa0pgrfuJBrIUKvVeMr50UGz7L7I",
            "AIzaSyBV7lCA5AH0vPG1Z9D6oC1FXlzGDz2F4GU"
        ]
        
    def __parse_text(self, text: str) -> str:
        soup = BeautifulSoup(text, 'html.parser')
        result = soup.text.strip().replace("\n", " ").replace("\t", " ")
        return result
        
    def __generate_id(self, text: str, page: str) -> str:
        text_remove_space = ''.join([c for c in text if not c.isspace()]).lower()
        union_text = text_remove_space + page
        return hashlib.sha256(union_text.encode('utf-8')).hexdigest()
    
    def _flush(self):
        sleep(5)
        self.kafka_producer.flush()

    def _parse_message(self, msg: dict) -> dict:
        now = datetime.strptime(msg["first_scraped_at"], "%Y-%m-%d %H:%M:%S")
        msg["text"] = self.__parse_text(msg["text"])
        msg["id"] = self.__generate_id(text=msg["text"], page=msg["page"])
        msg["post_time"] = ParserUtils.approx_post_time_str(now=now, raw_post_time=msg["post_time"])
        msg["reaction_count"] = ParserUtils.approx_reactions(msg["reaction_count"])
        return msg
    
    def _post_already_has_keywords(self, id: str) -> bool:
        # note: need implement
        return False
    
    def callback_enrich_keyword(self, res):
        result = res.result()
        for item in result:
            self.kafka_producer.send(Kafka.TOPIC_PARSED_POST, value=item)
    
    def start(self):
        Thread(target=self._flush).start()
        list_need_extract_keywords = []
        api_key_number = 0

        with ThreadPoolExecutor(max_workers=5) as thread_pool:
            while (True):
                try:
                    records = self.kafka_consumer.poll(max_records=10, timeout_ms=5000)
                    if len(records.items()) == 0:
                        pass
                    for topic_data, consumer_records in records.items():
                        for consumer_record in consumer_records:
                            raw_post = consumer_record.value
                            try:
                                parsed_post = self._parse_message(msg=raw_post)
                                if parsed_post.get("reaction_count") == None or parsed_post.get("post_time") == None:
                                    continue
                            except Exception as e:
                                TerminalLogging.log_error(message=f"Failed message at offset {consumer_record.offset} in partition {consumer_record.partition}")
                                parsed_post["err"] = traceback.format_exc()
                                self.kafka_producer.send(Kafka.TOPIC_FAILED_PARSED_POST, value=parsed_post)

                            if self._post_already_has_keywords(id=parsed_post.get("id")):
                                self.kafka_producer.send(Kafka.TOPIC_PARSED_POST, value=parsed_post)
                                TerminalLogging.log_info(message=f"Message at offset {consumer_record.offset} in partition {consumer_record.partition} has already had keywords. Send directly")
                            else:
                                list_need_extract_keywords.append(parsed_post)
                                if len(list_need_extract_keywords) >= 20:
                                    job = thread_pool.submit(KeywordExtractionUtils.enrich_keywords, 
                                                            list_need_extract_keywords.copy(), 
                                                            self.api_keys[api_key_number])
                                    job.add_done_callback(self.callback_enrich_keyword)

                                    list_need_extract_keywords.clear()
                                    api_key_number = (api_key_number + 1) if api_key_number < len(self.api_keys) - 1 else 0

                except Exception as e:
                    TerminalLogging.log_error(traceback.format_exc())

            # for message in self.kafka_consumer:
            #     raw_post = message.value
            #     try:
            #         parsed_post = self._parse_message(msg=raw_post)
            #         if parsed_post.get("reaction_count") == None or parsed_post.get("post_time") == None:
            #             continue
            #     except Exception as e:
            #         TerminalLogging.log_error(message=f"Failed message at offset {message.offset} in partition {message.partition}")
            #         parsed_post["err"] = traceback.format_exc()
            #         self.kafka_producer.send(Kafka.TOPIC_FAILED_PARSED_POST, value=parsed_post)

            #     if self._post_already_has_keywords(id=parsed_post.get("id")):
            #         self.kafka_producer.send(Kafka.TOPIC_PARSED_POST, value=parsed_post)
            #         TerminalLogging.log_info(message=f"Message at offset {message.offset} in partition {message.partition} has already had keywords. Send directly")
            #     else:
            #         list_need_extract_keywords.append(parsed_post)
            #         if len(list_need_extract_keywords) >= 20:
            #             job = thread_pool.submit(KeywordExtractionUtils.enrich_keywords, 
            #                                      list_need_extract_keywords.copy(), 
            #                                      self.api_keys[api_key_number])
            #             job.add_done_callback(self.callback_enrich_keyword)

            #             list_need_extract_keywords.clear()
            #             api_key_number = (api_key_number + 1) if api_key_number < len(self.api_keys) - 1 else 0
            
    def clean_up(self):
        self.kafka_producer.flush()
        self.kafka_producer.close(timeout=5)
        self.kafka_consumer.close(autocommit=False)

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()