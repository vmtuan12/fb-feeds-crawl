from worker.trend.trend_summarizer import TrendSummarizerWorker
from connectors.db_connector import DbConnectorBuilder, KafkaConsumerBuilder
from utils.constants import KafkaConnectionConstant as Kafka, PostgresConnectionConstant as PgCons
from datetime import datetime, timedelta
from entities.entities import TrendSummaryCluster
from custom_logging.logging import TerminalLogging
from utils.parser_utils import ParserUtils
from utils.insert_pg_utils import InsertPgUtils
import pytz
import math
import os
import hashlib
import traceback

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
        
        self.consumed_clusters = dict()
        self.cluster_expired_threshold = int(os.getenv("CLUSTER_EXPIRED_THRESHOLD", "86400"))
        self.max_text_len_threshold = float(os.getenv("MAX_TEXT_LEN_THRESHOLD", "1.5"))
        self.avg_text_len_threshold = float(os.getenv("AVG_TEXT_LEN_THRESHOLD", "1.5"))
        
    def _append_search_body(self, msearch_body: list, id_list: list[str], current_date: str):
        _index = f'fb_post-{current_date}'
        msearch_body.append({ "index": _index })
        msearch_body.append({ "query": { "terms": { "_id" : id_list }}, "size": 10000, "sort": [{ "post_time": { "order": "desc" } }]})

    def _get_current_time(self) -> datetime:
        return datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).replace(tzinfo=None)

    def _generate_cluster_id(self, cluster: dict) -> str:
        timestamp = int(datetime.strptime(cluster.get("time"), "%Y-%m-%d %H:%M:%S").timestamp())
        union_text = "".join(cluster.get("data").keys())
        return str(timestamp) + "_" + hashlib.sha256(union_text.encode('utf-8')).hexdigest()

    def _compute_texts_max_and_avg_length(self, data: dict) -> tuple[int, float]:
        sum_len = 0
        max_len = 0
        count_valid_text = 0
        for p in data.values():
            text = p.get("text")
            cleaned_text = ParserUtils.clean_text(text=text)
            count_valid_text += (1 if len(cleaned_text) > 0 else 0)
            sum_len += len(cleaned_text)
            if len(cleaned_text) > max_len:
                max_len = len(cleaned_text)
        return max_len, round(sum_len / count_valid_text, 2)
    
    def _create_cluster(self, cluster: dict) -> TrendSummaryCluster:
        TerminalLogging.log_info(f"Creating new cluster")
        cluster_data = cluster.get("data")
        cluster_id = self._generate_cluster_id(cluster=cluster)

        cluster_entity = TrendSummaryCluster(cluster_id=cluster_id)
        cluster_entity.set_max_and_avg_text_len(self._compute_texts_max_and_avg_length(data=cluster_data))
        cluster_entity.update_data(cluster_data)
        self.consumed_clusters[cluster_id] = cluster_entity

        self._summarize_and_save_content(cluster=cluster_entity, need_rewrite=True)

        return cluster_entity
    
    def _merge_2_clusters(self, old_cluster: TrendSummaryCluster, new_cluster_dict: dict) -> TrendSummaryCluster | None:
        TerminalLogging.log_info(f"Merging 2 clusters")
        new_cluster_data = new_cluster_dict.get("data")
        new_posts_dict = dict()

        old_cluster_post_ids = set(old_cluster.data.keys())
        for pid in new_cluster_data.keys():
            if pid not in old_cluster_post_ids:
                new_posts_dict[pid] = new_cluster_data[pid]

        if len(new_posts_dict.keys()) == 0:
            return None
        
        new_max_text_len, new_avg_text_len = self._compute_texts_max_and_avg_length(data=new_posts_dict)
        need_rewrite = False
        if round(new_max_text_len / old_cluster.max_text_len, 1) >= self.max_text_len_threshold or \
            round(new_avg_text_len / old_cluster.avg_text_len, 1) >= self.avg_text_len_threshold:
            old_cluster.set_max_and_avg_text_len(len_tuple=(new_max_text_len, new_avg_text_len))
            need_rewrite = True

        old_cluster.update_data(new_data=new_posts_dict)
        self._summarize_and_save_content(cluster=old_cluster, need_rewrite=need_rewrite)

        return old_cluster
    
    def _summarize_and_save_content(self, cluster: TrendSummaryCluster, need_rewrite=False):
        TerminalLogging.log_info(f"Saving cluster {cluster.id}")
        current_time_str = self._get_current_time().strftime("%Y-%m-%d %H:%M:%S")

        title, content = None, None
        if need_rewrite:
            title, content = None, None

        set_keywords = set()
        event_posts_dict = []
        for d in cluster.data.values():
            formatted_d = d.copy()
            d_keywords = set(formatted_d.pop("keywords"))

            formatted_d["reactions"] = formatted_d.pop("reaction_count")[0]
            formatted_d["post_id"] = formatted_d.pop("id")
            formatted_d["event_id"] = cluster.id

            event_posts_dict.append(formatted_d)

            if len(set_keywords) == 0:
                set_keywords.update(d_keywords)
            else:
                if len(d_keywords) == 0:
                    continue
                set_keywords = set_keywords.intersection(d_keywords)

        event_dict = [{
            "id": cluster.id,
            "update_time": current_time_str,
            "main_keywords": list(set_keywords) if len(set_keywords) != 0 else None,
            "title": title,
            "content": content
        }]

        cursor = self.pg_conn.cursor()

        insert_event_statement = f"""INSERT INTO {PgCons.TABLE_EVENTS} (%s) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
        update_time = EXCLUDED.update_time,
        main_keywords = COALESCE(EXCLUDED.main_keywords, {PgCons.TABLE_EVENTS}.main_keywords),
        title = COALESCE(EXCLUDED.title, {PgCons.TABLE_EVENTS}.title),
        content = COALESCE(EXCLUDED.content, {PgCons.TABLE_EVENTS}.content)
        """
        insert_event_query, formatted_event_value = InsertPgUtils.generate_query_insert_list_dict(insert_statement=insert_event_statement, data_list=event_dict)

        insert_posts_statement = f"""INSERT INTO {PgCons.TABLE_EVENTS_POSTS} (%s) VALUES %s
        ON CONFLICT (event_id, post_id) DO UPDATE SET
        images = COALESCE(EXCLUDED.images, {PgCons.TABLE_EVENTS_POSTS}.images),
        reactions = EXCLUDED.reactions
        """
        insert_posts_query, formatted_posts_value = InsertPgUtils.generate_query_insert_list_dict(insert_statement=insert_posts_statement, data_list=event_posts_dict)

        try:
            cursor.execute(insert_event_query, formatted_event_value)
            cursor.execute(insert_posts_query, formatted_posts_value)
            self.pg_conn.commit()
        except Exception as e:
            TerminalLogging.log_error(f"Failed saving cluster {cluster.id}. Error:\n{traceback.format_exc()}")
            raise e

        cursor.close()
        TerminalLogging.log_info(f"Successfully saved cluster {cluster.id}")
    
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
                other_cluster_data_keys = set(other_cluster.data.keys())
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
            TerminalLogging.log_info(f"Consumed message at partition {message.partition}, offset {message.offset}")
            msg_value = message.value
            self.add_cluster(cluster=msg_value)

    def clean_up(self):
        self.kafka_consumer.close()
        self.pg_conn.close()
 
    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()