from custom_exception.exceptions import *
from custom_logging.logging import TerminalLogging
from connectors.db_connector import KafkaConsumerBuilder, DbConnectorBuilder
from utils.constants import KafkaConnectionConstant as Kafka, ElasticsearchConnectionConstant as ES
from requests_html import HTMLSession
from time import sleep, time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import pytz
import traceback
from elasticsearch import helpers

class FbUpdateReactionWorker():
    def __init__(self) -> None:
        self.kafka_consumer = KafkaConsumerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .set_group_id(Kafka.GROUP_ID_MIDDLE_CONSUMER)\
                                                    .set_topics(Kafka.TOPIC_REACTION_COMMAND)\
                                                    .build()
        self.es_client = DbConnectorBuilder().set_host(ES.HOST)\
                                                .set_port(ES.PORT)\
                                                .set_username(ES.USERNAME)\
                                                .set_password(ES.PASSWORD)\
                                                .build_es_client()
        
    def _request_post(self, command: dict):
        url = command["url"]
        index = command["index"]

        session = HTMLSession()
        res = session.get(url=url, headers={
            "user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
        })

        soup = BeautifulSoup(res.text, 'html.parser')
        scripts = soup.select('script')
        for script in scripts:
            if 'i18n_reaction_count' in script.text:
                data = json.loads(script.text)
                with open('test.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                break
        
        general_section = None
        for el in data["require"][0][3][0]["__bbox"]["require"]:
            if len(el) < 4:
                continue
            str_json = str(el)
            if "i18n_reaction_count" in str_json:
                general_section = el
                break

        if general_section == None:
            TerminalLogging.log_error(f"Error getting url {url}")

        general_section = general_section[3][1]["__bbox"]["result"]["data"]
        if "/videos" not in url:
            text = general_section["node"]["comet_sections"]["content"]["story"]["message"]["text"]
            reactions = general_section["node"]["comet_sections"]["feedback"]["story"]["story_ufi_container"]["story"]["feedback_context"]["feedback_target_with_context"]["comet_ufi_summary_and_actions_renderer"]["feedback"]["reaction_count"]["count"]
        else:
            text = general_section["creation_story"]["message"]["text"]
            reactions = general_section["feedback"]["reaction_count"]["count"]

        return {
            "text": text,
            "reactions": reactions,
            "index": index
        }

    def _create_document_index(self, res) -> dict:
        current_time = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).replace(tzinfo=None)

        result = res.result()
        url = result["url"]
        _index = result["index"]
        reactions = result["reactions"]
        text = result["text"]

        formatted_doc = {
            "text": text,
            "reaction_count": [reactions],
            "update_time": [current_time]
        }

        self.indexed_docs.append({
            "_op_type": 'update',
            "_index": _index,
            "_id": f"{url}",
            "script": {
                "source": """
                    if (ctx._source.text == null) {
                        ctx._source.text = params.text;
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
        })

    def _insert_es(self):
        try:
            helpers.bulk(self.es_client, self.indexed_docs)
            self.indexed_docs.clear()
        except Exception as e:
            TerminalLogging.log_error(f"Error insert into elasticsearch\n{traceback.format_exc()}")
        
    def start(self, max_records=200):
        self.indexed_docs = []

        while (True):
            records = self.kafka_consumer.poll(max_records=max_records, timeout_ms=10000)
            if len(records.items()) == 0 and len(self.indexed_docs) > 0:
                self._insert_es()
            
            with ThreadPoolExecutor(max_workers=10) as thread_pool:
                for topic_data, consumer_records in records.items():
                    TerminalLogging.log_info(f"Processing {len(consumer_records)} records!")
                    for consumer_record in consumer_records:
                        command = consumer_record.value
                        job = thread_pool.submit(self._request_post, command)
                        job.add_done_callback(self._create_document_index)

            if len(self.indexed_docs) >= max_records:
                self._insert_es()
        
    def clean_up(self):
        self.kafka_consumer.close(autocommit=False)
        self.es_client.close()

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()