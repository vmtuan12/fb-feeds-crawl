from worker.trend.trend_summarizer import TrendSummarizerWorker
from connectors.db_connector import DbConnectorBuilder, KafkaConsumerBuilder
from utils.constants import ElasticsearchConnectionConstant as ES, KafkaConnectionConstant as Kafka, PostgresConnectionConstant as PgCons
from datetime import datetime, timedelta
from entities.entities import TrendSummaryCluster
from custom_logging.logging import TerminalLogging
from utils.parser_utils import ParserUtils
import pytz
import math
import os
import hashlib

class FastTrendSummarizerWorker(TrendSummarizerWorker):
    def __init__(self) -> None:
        self.kafka_consumer = KafkaConsumerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .set_group_id(Kafka.GROUP_ID_TREND_SUMMARIZER)\
                                                    .set_auto_offset_reset("latest")\
                                                    .set_topics(Kafka.TOPIC_RISING_TRENDS)\
                                                    .build()
        self.pg_conn = DbConnectorBuilder().set_host(PgCons.HOST)\
                                            .set_port(PgCons.PORT)\
                                            .set_username(PgCons.USER)\
                                            .set_password(PgCons.PWD)\
                                            .set_db_name(PgCons.DB)\
                                            .build_pg()
        self.es_client = DbConnectorBuilder().set_host(ES.HOST)\
                                                .set_port(ES.PORT)\
                                                .set_username(ES.USERNAME)\
                                                .set_password(ES.PASSWORD)\
                                                .build_es_client()
        
        self.consumed_clusters = dict()
        self.cluster_expired_threshold = int(os.getenv("CLUSTER_EXPIRED_THRESHOLD", "86400"))
        
    def _append_search_body(self, msearch_body: list, id_list: list[str], current_date: str):
        _index = f'fb_post-{current_date}'
        msearch_body.append({ "index": _index })
        msearch_body.append({ "query": { "terms": { "_id" : id_list }}, "size": 10000, "sort": [{ "post_time": { "order": "desc" } }]})

    def _get_current_time(self) -> datetime:
        return datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).replace(tzinfo=None)

    def _generate_cluster_id(self, cluster: dict) -> str:
        timestamp = int(datetime.strptime(cluster.get("time"), "%Y-%m-%d %H:%M:%S").timestamp())
        union_text = "".join(cluster.get("data").keys())
        return timestamp + "_" + hashlib.sha256(union_text.encode('utf-8')).hexdigest()

    def _compute_texts_max_and_avg_length(self, data: dict) -> tuple[int, float]:
        sum_len = 0
        max_len = 0
        count_valid_text = 0
        for p in data.values():
            text = p.get("text")
            cleaned_text = ParserUtils.clean_text(text=text)
            count_valid_text += (1 if len(cleaned_text > 0) else 0)
            sum_len += len(cleaned_text)
            if len(cleaned_text) > max_len:
                max_len = len(cleaned_text)
        return max_len, round(sum_len / count_valid_text, 2)
    
    def _create_cluster(self, cluster: dict) -> TrendSummaryCluster:
        cluster_data = cluster.get("data")
        cluster_id = self._generate_cluster_id(cluster=cluster)

        cluster_entity = TrendSummaryCluster(cluster_id=cluster_id)
        cluster_entity.set_max_and_avg_text_len(self._compute_texts_max_and_avg_length(data=cluster_data))
        cluster_entity.update_data(cluster_data)
        self.consumed_clusters[cluster_id] = cluster_entity

        return cluster_entity
    
    def _merge_2_clusters(self, old_cluster: TrendSummaryCluster, new_cluster_dict: dict) -> TrendSummaryCluster:
        pass
    
    def add_cluster(self, cluster: dict):
        cluster_data = cluster.get("data")
        cluster_data_keys = set(cluster_data.keys())
        if len(self.consumed_clusters.values()) == 0:
            self._create_cluster(cluster=cluster)
        else:
            expired_clusters = []
            current_timestamp_sec = self._get_current_time().timestamp()
            for cid in self.consumed_clusters.keys():
                cluster_created_timestamp = int(cid.split("_")[0])
                if current_timestamp_sec - cluster_created_timestamp >= self.cluster_expired_threshold:
                    expired_clusters.append(cid)

            for cid in expired_clusters:
                self.consumed_clusters.pop(cid)
            
            cluster_can_be_merged = False
            for other_cluster in self.consumed_clusters.values():
                other_cluster_data_keys = set(other_cluster.keys())
                intersection = cluster_data_keys.intersection(other_cluster_data_keys)

                if len(intersection) >= math.floor(len(cluster_data_keys) * 2/3) or \
                    len(intersection) >= math.floor(len(other_cluster_data_keys) * 2/3):
                    self._merge_2_clusters(old_cluster=other_cluster, new_cluster_dict=cluster)
                    cluster_can_be_merged = True
                    break

            if not cluster_can_be_merged:
                self._create_cluster(cluster=cluster)

    def start(self):
        for message in self.kafka_consumer:
            msg_value = message.value
            self.add_cluster(cluster=msg_value)

    def clean_up(self):
        self.kafka_consumer.close()
        self.es_client.close()
        self.pg_conn.close()
 
    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()