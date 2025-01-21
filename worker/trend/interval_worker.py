from worker.trend.trend_summarizer import TrendSummarizerWorker
from connectors.db_connector import DbConnectorBuilder
from utils.constants import PostgresConnectionConstant as PgCons, ElasticsearchConnectionConstant as ES
from datetime import datetime, timedelta
from custom_logging.logging import TerminalLogging
from utils.insert_pg_utils import InsertPgUtils
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from utils.constants import APIConstant as API
from utils.model_api_utils import ModelApiUtils
import utils.main_ne_grpc.event_ne_pb2 as event_ne_pb2
import utils.main_ne_grpc.event_ne_pb2_grpc as event_ne_pb2_grpc
from google.protobuf import json_format
import grpc
import pytz
import os
import json
import requests
import traceback

class IntervalTrendWorker(TrendSummarizerWorker):
    def __init__(self) -> None:
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
        
        self.time_update_post_reaction_threshold = int(os.getenv("TIME_UPDATE_POST_REACTION_THRESHOLD", "21600"))
        self.time_query_events_threshold = int(os.getenv("TIME_QUERY_EVENTS_THRESHOLD", "43200"))
        self.max_event_text_length_threshold = int(os.getenv("MAX_EVENT_TEXT_LENGTH_THRESHOLD", "100"))
        self.sum_reactions_threshold = int(os.getenv("SUM_REACTIONS_THRESHOLD", "100"))

        self.graph_constructor_host = os.getenv("GRAPH_CONSTRUCTOR_HOST", "localhost")
        self.graph_constructor_port = os.getenv("GRAPH_CONSTRUCTOR_PORT", "50052")

    def _get_current_time(self) -> datetime:
        return datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).replace(tzinfo=None)
    
    def _send_event_to_graph(self, event_id: str, event_text: str):
        if event_text == None or event_id == None:
            TerminalLogging.log_info(f"Nothing to send")
            return

        try:
            with grpc.insecure_channel(f'{self.graph_constructor_host}:{self.graph_constructor_port}') as channel:
                stub = event_ne_pb2_grpc.ConsumerStub(channel)
                response = stub.ProcessEvent(event_ne_pb2.EventRequest(id=event_id, 
                                                                    text=event_text))
                TerminalLogging.log_info(json_format.MessageToDict(response))
        except Exception as e:
            TerminalLogging.log_error(f"Error sending event to graph service\n{traceback.format_exc()}")
    
    def _summarize_texts(self, list_text: list, event_id: str) -> dict:
        TerminalLogging.log_info(f"Start summarizing event {event_id}")
        prompt = """
        Bạn là một nhà báo VnExpress với nhiều năm kinh nghiệm. Với dữ liệu đầu vào là 1 danh sách các đoạn văn, tóm tắt các đoạn văn đó thành một đoạn văn đầy đủ chi tiết.
        Ngoài ra, viết thêm 1 tiêu đề thú vị cho đoạn văn vừa tóm tắt.
        Nếu có đoạn văn nào không liên quan tới phần lớn các đoạn văn, không tóm tắt nội dung đoạn văn đó.
        Không tóm tắt theo kiểu "Các bài văn này nói về ...", hãy nhập tâm vào làm chính chủ thể của đoạn văn. Không cần câu kết luận để tổng kết, chỉ tóm tắt sự kiện trong các đoạn văn. Viết các câu văn với đầy đủ các thực thể và các Event Trigger sao cho dễ dàng thực hiện Event Extraction.
        Viết tóm tắt và tiêu đề một cách gây tò mò nhất có thể, nửa úp nửa mở gây tò mò và nghi ngờ nếu có thể, sử dụng những từ thu hút sự chú ý cho các thông tin, trừ thông tin liên quan đến chính trị và các lãnh đạo.
        Kết quả đầu ra có dạng Json như sau {"title": <tiêu đề>, "content": <nội dung tóm tắt>}
        Viết tiêu đề dựa trên các mẫu sau đây: "Phương Mỹ Chi áp lực trong lần đầu đóng chính điện ảnh", "Soobin gây chú ý khi xuất hiện bên hoa hậu Thanh Thủy", "Khoảnh khắc lễ ăn hỏi của Phương Nhi và con trai tỷ phú Phạm Nhật Vượng", "Dàn hoa hậu dự thảm đỏ Làn Sóng Xanh 2024", "Tác phẩm trắng trơn đạt hơn một triệu USD", "Thượng Hải mời Taylor Swift làm concert", "Con trai cao gần 1,8 m của diễn viên Lê Phương", "Phim về mỹ nữ tài hoa thời Đường gây sốt". Viết một cách mập mờ để gây sự chú ý, với bố cục là Thực thể làm gì, không chứa dấu ':'
        """

        try:
            response = ModelApiUtils.send_request_directly(prompt=prompt, input_data=str(list_text))
            result = response
            result.update({"id": event_id})
            self.events.append(result)
        except Exception as e:
            TerminalLogging.log_error(f"Output response\n{response.text}")
            TerminalLogging.log_error(f"Failed summarizing event {event_id}\n{traceback.format_exc()}")
        finally:
            self._send_event_to_graph(event_id=event_id, event_text=result.get("content"))
    
    def _query_es_by_id(self, time_month: str, ids: Iterable) -> dict:
        TerminalLogging.log_info(f"Starting querying documents by ids from Elasticsearch...")
        index_name = f'fb_post-{time_month}'
        query = {
            "query" : {
                "terms" : {
                    "_id" : list(ids)
                }
            },
            "size": 10000
        }

        try:
            response = self.es_client.search(index=index_name, body=query)
        except Exception as e:
            TerminalLogging.log_error(f"Failed querying documents by ids from Elasticsearch")
            raise e

        TerminalLogging.log_info(f"Successfully querying documents by ids from Elasticsearch")
        return response
    
    def _enrich_reactions(self, ids: Iterable, posts: dict):
        current_time = self._get_current_time()
        current_time_month = current_time.strftime("%Y%m")
        prev_1_day_time = current_time - timedelta(days=1)

        post_reaction_dict = dict()
        
        data_response = self._query_es_by_id(time_month=current_time_month, ids=ids)
        for hit in data_response['hits']['hits']:
            _source = hit['_source']
            post_reaction_dict[hit["_id"]] = _source.get("reaction_count")[-1]

        if current_time.month != prev_1_day_time.month:
            prev_1_day_time_month = prev_1_day_time.strftime("%Y%m")
            data_response = self._query_es_by_id(time_month=prev_1_day_time_month, ids=ids)
            for hit in data_response['hits']['hits']:
                _source = hit['_source']
                if post_reaction_dict.get(hit["_id"]) != None:
                    continue
                post_reaction_dict[hit["_id"]] = _source.get("reaction_count")[-1]

        for p in posts:
            p["reactions"] = post_reaction_dict.get(p["post_id"])

        return posts
        
    def update_posts_reactions(self, cursor):
        TerminalLogging.log_info(f"Start querying events posts from Postgresql...")
        try:
            cursor.execute(f"""
            SELECT event_id, post_id FROM {PgCons.TABLE_EVENTS_POSTS}
            WHERE EXTRACT (EPOCH FROM CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Ho_Chi_Minh' - (to_timestamp(split_part(event_id, '_', 1)::bigint) AT TIME ZONE 'Asia/Ho_Chi_Minh')) < {self.time_update_post_reaction_threshold}
            """)
            rows = cursor.fetchall()
        except Exception as e:
            TerminalLogging.log_error(f"Failed querying events posts from Postgresql\n{traceback.format_exc()}")
            return

        TerminalLogging.log_info(f"Successfully querying events posts from Postgresql")
        posts = [{"event_id": r[0], "post_id": r[1]} for r in rows]
        post_ids = set([r[1] for r in rows])

        updated_reactions_posts = self._enrich_reactions(ids=post_ids, posts=posts)

        insert_statement = f"""
        INSERT INTO {PgCons.TABLE_EVENTS_POSTS} (%s) VALUES %s
        ON CONFLICT (event_id, post_id) DO UPDATE 
        SET reactions = COALESCE(EXCLUDED.reactions, {PgCons.TABLE_EVENTS_POSTS}.reactions)"""
        query, value = InsertPgUtils.generate_query_insert_list_dict(insert_statement=insert_statement, data_list=updated_reactions_posts)

        try:
            cursor.execute(query, value)
        except Exception as e:
            TerminalLogging.log_error(f"Failed updating events posts reactions into Postgresql\n{traceback.format_exc()}")
            return
        
        self.pg_conn.commit()
        TerminalLogging.log_info(f"Successfully updating events posts reactions into Postgresql")

    def write_content_for_trends(self, cursor):
        TerminalLogging.log_info(f"Start querying events from Postgresql...")
        query_select_unwritten_events = f"""
        SELECT e.id, json_agg(JSON_BUILD_OBJECT('text', ep.text, 'images', ep.images))
        FROM {PgCons.TABLE_EVENTS} e JOIN {PgCons.TABLE_EVENTS_POSTS} ep ON e.id = ep.event_id
        WHERE (e.title IS NULL OR e.content IS NULL) AND 
            EXTRACT (EPOCH FROM (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Ho_Chi_Minh') - (to_timestamp(split_part(event_id, '_', 1)::bigint) AT TIME ZONE 'Asia/Ho_Chi_Minh')) < {self.time_query_events_threshold}
        GROUP BY e.id
        HAVING (MAX(length(ep.text)) >= {self.max_event_text_length_threshold} OR AVG(COALESCE(array_length(ep.images, 1), 0)) >= 3 OR sum(ep.reactions) >= 1000) AND (
            CASE
                WHEN COUNT(ep.post_id) > 3 THEN TRUE
                WHEN (COUNT(ep.post_id) = 3) AND (sum(ep.reactions) >= {self.sum_reactions_threshold}) THEN TRUE
            ELSE FALSE
            END) = True
        ORDER BY sum(ep.reactions) DESC;
        """

        try:
            cursor.execute(query_select_unwritten_events)
            rows = cursor.fetchall()
        except Exception as e:
            TerminalLogging.log_error(f"Failed querying events from Postgresql\n{traceback.format_exc()}")
            return

        TerminalLogging.log_info(f"Successfully querying events from Postgresql")
        self.events = []
        with ThreadPoolExecutor(max_workers=5) as pool:
            for row in rows:
                list_text = []
                event_id = row[0]
                for attr in row[1]:
                    list_text.append(attr['text'])

                pool.submit(self._summarize_texts, list_text, event_id)

        if len(self.events) == 0:
            TerminalLogging.log_info(f"No new event to summarize")
            return
        
        insert_statement = f"""
        INSERT INTO {PgCons.TABLE_EVENTS} (%s) VALUES %s
        ON CONFLICT (id) DO UPDATE SET 
        title = COALESCE(EXCLUDED.title, {PgCons.TABLE_EVENTS}.title),
        content = COALESCE(EXCLUDED.content, {PgCons.TABLE_EVENTS}.content)
        """
        query, value = InsertPgUtils.generate_query_insert_list_dict(insert_statement=insert_statement, data_list=self.events)

        try:
            cursor.execute(query, value)
        except Exception as e:
            TerminalLogging.log_error(f"Failed updating events into Postgresql\n{traceback.format_exc()}")
            return

        self.pg_conn.commit()
        self.events.clear()
        TerminalLogging.log_info(f"Successfully updating events into Postgresql")

    def start(self):
        cursor = self.pg_conn.cursor()

        self.update_posts_reactions(cursor=cursor)
        self.write_content_for_trends(cursor=cursor)

        print("\n#######################################\n")

        cursor.close()

    def clean_up(self):
        self.es_client.close()
        self.pg_conn.close()

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()