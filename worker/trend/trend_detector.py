from connectors.db_connector import KafkaConsumerBuilder, KafkaProducerBuilder
from utils.constants import KafkaConnectionConstant as Kafka, SchemaPathConstant as Schema
from datetime import datetime
from custom_logging.logging import TerminalLogging
from entities.entities import PostCluster
from utils.graph_utils import GraphUtils
import traceback
from time import sleep
import os
import pytz
import math

class TrendDetector():
    def __init__(self) -> None:
        self.kafka_consumer = KafkaConsumerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .set_group_id(Kafka.GROUP_ID_TREND_DETECTOR)\
                                                    .set_auto_offset_reset("latest")\
                                                    .set_topics(Kafka.TOPIC_PARSED_POST)\
                                                    .build(avro_schema_path=Schema.PARSED_POST_SCHEMA)
        self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build()
        
        self.clustered_posts = set()
        self.posts_clusters = dict()
        self.detected_clusters = dict()
        self.current_index = 0
        self.cosine_threshold = float(os.getenv("COSINE_THRESHOLD", "0.45"))
        self.merge_clusters_threshold = int(os.getenv("MERGE_CLUSTERS_THRESHOLD", "200"))
        self.post_time_sec_threshold = int(os.getenv("POST_TIME_SEC_THRESHOLD", "30600"))

        self.start_time = self._get_current_time()

    def _get_current_time(self) -> datetime:
        return datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).replace(tzinfo=None)
    
    def _need_to_reset_clusters(self) -> bool:
        current_time = self._get_current_time()
        if (((0 <= self.start_time.hour and self.start_time.hour <= 7) or (23 <= self.start_time.hour and current_time.day > self.start_time.day)) and current_time.hour >= 8) or \
            (8 <= self.start_time.hour and self.start_time.hour <= 11 and current_time.hour >= 12) or \
            (12 <= self.start_time.hour and self.start_time.hour <= 17 and current_time.hour >= 18) or \
            (18 <= self.start_time.hour and self.start_time.hour <= 22 and current_time.hour >= 23):

            if self.start_time.hour <= 7 or self.start_time.hour >= 23:
                self.clustered_posts.clear()
            self.start_time = current_time
            return True
        
        return False

    def _create_new_cluster(self, post: dict):
        cl = PostCluster(cluster_id=self.current_index)
        cl.add_post(post=post)
        self.posts_clusters.update({self.current_index: cl})
        self.current_index += 1
        return cl
    
    def _detect_cluster(self, cluster_id: int, post_len: int):
        cluster = self.posts_clusters.get(cluster_id)
        TerminalLogging.log_info(f"Cluster with keywords {cluster.keywords} has been detected")
        self.kafka_producer.send(Kafka.TOPIC_RISING_TRENDS, {
            "time": self._get_current_time().strftime("%Y-%m-%d %H:%M:%S"), 
            "data": cluster.posts
        })
        self.kafka_producer.flush()
        self.detected_clusters[cluster_id] = post_len

    def _detect_high_reaction(self, post: dict):
        reaction_count = post.get("reaction_count")
        if reaction_count != None and len(reaction_count) > 0:
            reaction_int = reaction_count[0]
        if reaction_int >= 1000:
            self.kafka_producer.send(Kafka.TOPIC_HIGH_REACTION_POSTS, post)
            self.kafka_producer.flush()
        
    def cluster_post(self, post: dict) -> int:
        current_time = self._get_current_time()
        post_time = datetime.strptime(post["post_time"], "%Y-%m-%d %H:%M:%S")

        post_id = post.get("id")
        post_text = post.get("text")
        post_images = post.get("images")
        post_keywords = post.get("keywords")
        post_reaction = post.get("reaction_count")
        list_keywords = post_keywords if post_keywords != None else []
        post_dict = {
            "id": post_id,
            "text": post_text,
            "images": post_images,
            "reaction_count": post_reaction,
            "post_time": post["post_time"],
            "keywords": list_keywords
        }

        if (current_time - post_time).total_seconds() >= self.post_time_sec_threshold:
            TerminalLogging.log_info(f"Id {post_id} has invalid post time. Skipped.")
            return 0
        
        self._detect_high_reaction(post=post_dict)
        if post_id in self.clustered_posts:
            return 0
        
        self.clustered_posts.add(post_id)

        if "See more" in post_text or post_text.count(" ") < 9:
            TerminalLogging.log_info(f"Id {post_id} has invalid text. Skipped.")
            return 0
        
        if len(self.posts_clusters.keys()) == 0:
            self._create_new_cluster(post=post_dict)
            return 1
        
        TerminalLogging.log_info(f"Processing id {post_id}")
        doc_belong_to_a_node = False
        max_avg_cosine = 0
        cluster_with_max_cosine = 0

        for cl_id in self.posts_clusters.keys():
            cluster = self.posts_clusters.get(cl_id)
            similar_to_all_text = True
            cosine_list = []
            for pid in cluster.posts.keys():
                cosine_sim = GraphUtils.get_cosine(text1=post_text, text2=cluster.posts.get(pid).get("text"))
                if cosine_sim < self.cosine_threshold:
                    similar_to_all_text = False
                    break
                else:
                    cosine_list.append(cosine_sim)

            if similar_to_all_text:
                doc_belong_to_a_node = True
                avg_cosine = sum(cosine_list)/len(cosine_list)
                if avg_cosine > max_avg_cosine:
                    max_avg_cosine = avg_cosine
                    cluster_with_max_cosine = cl_id

        if doc_belong_to_a_node:
            self.posts_clusters[cluster_with_max_cosine].add_post(post_dict)
            cluster_with_max_cosine_post_len = len(self.posts_clusters[cluster_with_max_cosine].posts.keys())
            if cluster_with_max_cosine_post_len % 3 == 0:
                self._detect_cluster(cluster_id=cluster_with_max_cosine, post_len=cluster_with_max_cosine_post_len)
        
        else:
            if len(list_keywords) == 0:
                return 0
            self._create_new_cluster(post=post_dict)

        return 1

    def merge_clusters(self):
        TerminalLogging.log_info(f"Merging clusters ...")
        merged_graph = dict()

        for cluster_id in self.posts_clusters:
            cluster = self.posts_clusters[cluster_id]
            if len(merged_graph.keys()) == 0:
                merged_graph[cluster_id] = cluster
            else:
                cluster_is_merged = False
                best_merged_cluster_id = None
                best_consine = 0
                best_high_intersection_count = 0
                best_high_intersection_count_cluster = None

                for merged_cluster_id in merged_graph:
                    merged_cluster = merged_graph.get(merged_cluster_id)
                    kw_intersection = cluster.keywords.intersection(merged_cluster.keywords)

                    if len(kw_intersection) >= 5 and len(kw_intersection) > best_high_intersection_count and \
                        (len(kw_intersection) >= math.ceil(len(cluster.keywords)*2/3) or \
                        len(kw_intersection) >= math.ceil(len(merged_cluster.keywords)*2/3)):
                        best_high_intersection_count = len(kw_intersection)
                        best_high_intersection_count_cluster = merged_cluster_id
                        continue

                    if best_high_intersection_count_cluster != None:
                        continue
                        
                    if len(kw_intersection) >= math.ceil(len(cluster.keywords)/2) or \
                        len(kw_intersection) >= math.ceil(len(merged_cluster.keywords)/2):
                        cosine = GraphUtils.get_cosine(text1=" ".join([cv["text"] for cv in cluster.posts.values()]), text2=" ".join([mcv["text"] for mcv in merged_cluster.posts.values()]))
                        if cosine >= 0.55:
                            if cosine > best_consine:
                                best_consine = cosine
                                best_merged_cluster_id = merged_cluster_id

                if best_high_intersection_count_cluster != None:
                    for post in cluster.posts.values():
                        merged_graph[best_high_intersection_count_cluster].add_post(post)
                    merged_graph[best_high_intersection_count_cluster].add_sub_cluster(cluster)
                    continue
                    
                if best_merged_cluster_id != None:
                    for post in cluster.posts.values():
                        merged_graph[best_merged_cluster_id].add_post(post)
                    merged_graph[best_merged_cluster_id].add_sub_cluster(cluster)
                    cluster_is_merged = True

                if not cluster_is_merged:
                    merged_graph[cluster_id] = cluster

        self.posts_clusters = merged_graph
        self.save_cluster_checkpoint()
        TerminalLogging.log_info(f"Done merging clusters")

        detected_cluster_ids = set(self.detected_clusters.keys())
        for cluster in self.posts_clusters.values():
            cluster_id = cluster.id
            set_cluster_and_subs = cluster.sub_clusters.union({cluster_id})
            if len(cluster.posts.keys()) >= 3:
                current_cluster_posts_len = len(self.posts_clusters[cluster_id].posts.keys())
                cluster_intersection = set_cluster_and_subs.intersection(detected_cluster_ids)

                if len(cluster_intersection) == 0:
                    TerminalLogging.log_info(f"Detect cluster after merging")
                    self._detect_cluster(cluster_id=cluster_id, post_len=current_cluster_posts_len)
                else:
                    count_detected_posts = sum([self.detected_clusters[cid] for cid in cluster_intersection])
                    if current_cluster_posts_len - count_detected_posts >= 3:
                        TerminalLogging.log_info(f"Detect cluster after merging")
                        self._detect_cluster(cluster_id=cluster_id, post_len=current_cluster_posts_len)

    def save_cluster_checkpoint(self):
        pass
        
    def start_test(self, list_posts: list):
        count_consumed_msgs = 0
        cluster_is_just_merged = False

        for index, parsed_post in enumerate(list_posts):
            print(index + 1)
            self.cluster_post(post=parsed_post)
            cluster_is_just_merged = False
            count_consumed_msgs += 1

            if count_consumed_msgs >= self.merge_clusters_threshold:
                self.merge_clusters()
                count_consumed_msgs = 0

        for cluster in self.posts_clusters.values():
            print(cluster.keywords)
            for post in cluster.posts.values():
                print(post)


    def start(self, max_records=1):
        count_consumed_msgs = 0
        cluster_is_just_merged = False
        
        while (True):
            records = self.kafka_consumer.poll(max_records=max_records, timeout_ms=3000)
            TerminalLogging.log_info(f"Polled {len(records.items())} items!")

            if len(records.items()) == 0 and count_consumed_msgs >= int(self.merge_clusters_threshold / 2):
                self.merge_clusters()
                cluster_is_just_merged = True
                count_consumed_msgs = 0
                continue
 
            for topic_data, consumer_records in records.items():
                TerminalLogging.log_info(f"Processing {len(consumer_records)} records!")
                for consumer_record in consumer_records:
                    parsed_post = consumer_record.value
                    valid_msg = self.cluster_post(post=parsed_post)

                    cluster_is_just_merged = False
                    count_consumed_msgs += valid_msg

            if count_consumed_msgs >= self.merge_clusters_threshold:
                self.merge_clusters()
                count_consumed_msgs = 0

            if self._need_to_reset_clusters():
                if not cluster_is_just_merged:
                    self.merge_clusters()
                    count_consumed_msgs = 0

                self.posts_clusters = dict()
                self.current_index = 0

    def clean_up(self):
        self.kafka_consumer.close(autocommit=False)
        self.kafka_producer.close(timeout=5)
 
    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()