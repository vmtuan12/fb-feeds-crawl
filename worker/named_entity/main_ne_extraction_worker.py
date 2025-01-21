from utils.parser_utils import ParserUtils
from utils.keyword_extract_utils import KeywordExtractionUtils
from custom_logging.logging import TerminalLogging
from utils.model_api_utils import ModelApiUtils
from utils.constants import RedisConnectionConstant as RedisCons
from connectors.db_connector import DbConnectorBuilder
import utils.main_ne_grpc.event_ne_pb2 as event_ne_pb2
import utils.main_ne_grpc.event_ne_pb2_grpc as event_ne_pb2_grpc
import os
import networkx as nx
from datetime import datetime
import pytz
import json

class MainNamedEntitiesExtractor(event_ne_pb2_grpc.ConsumerServicer):
    def __init__(self) -> None:
        self.event_ne_dict = dict()
        self.redis_conn = DbConnectorBuilder().set_host(RedisCons.HOST)\
                                                .set_port(RedisCons.PORT)\
                                                .set_username(RedisCons.USERNAME)\
                                                .set_password(RedisCons.PASSWORD)\
                                                .build_redis()
        self.expired_sec_threshold = int(os.getenv("EXPIRED_SEC_THRESHOLD", "2629743"))
        self.save_checkpoint_sec_threshold = int(os.getenv("LAST_SAVE_CHECKPOINT_THRESHOLD", "3600"))
        self.remove_expired_sec_threshold = int(os.getenv("LAST_REMOVE_EXPIRED_THRESHOLD", "43200"))
        self.last_remove_expired = 0
        self.last_save_checkpoint = 0
        self._load_graph_checkpoint()

    def _remove_expired_ne(self):
        current_timestamp_sec = self._get_current_time().timestamp()
        if current_timestamp_sec - self.last_remove_expired < self.remove_expired_sec_threshold:
            return
        
        TerminalLogging.log_info("Removing expired Named entities...")
        nes = list(self.graph.nodes)
        for node in nes:
            if current_timestamp_sec - self.graph.nodes[node]['last_updated'] > self.expired_sec_threshold:
                self.graph.remove_node(node)
                TerminalLogging.log_info(f"Removed Named entity {node}")

        self.last_remove_expired = current_timestamp_sec

    def _load_graph_checkpoint(self):
        graph_structure = self.redis_conn.get(f"{RedisCons.PREFIX_NE_GRAPH}.graph-structure")
        nodes_data = self.redis_conn.get(f"{RedisCons.PREFIX_NE_GRAPH}.nodes-data")

        if graph_structure == None:
            self.graph = nx.Graph()
        else:
            graph_structure = json.loads(graph_structure)
            for node in graph_structure:
                neighbors = graph_structure.get(node)
                for neighbor in neighbors:
                    neighbors[neighbor]['mutual_events'] = set(neighbors[neighbor]['mutual_events'])

            self.graph = nx.from_dict_of_dicts(graph_structure)
            nodes_data = json.loads(nodes_data)
            for node in nodes_data.keys():
                self.graph.nodes[node]["events"] = set(nodes_data[node]["events"])
                self.graph.nodes[node]["last_updated"] = nodes_data[node]["last_updated"]

    def _save_graph_checkpoint(self):
        current_timestamp_sec = self._get_current_time().timestamp()
        if current_timestamp_sec - self.last_save_checkpoint < self.save_checkpoint_sec_threshold:
            return
        
        graph_structure = nx.to_dict_of_dicts(self.graph)
        for node in graph_structure:
            neighbors = graph_structure.get(node)
            for neighbor in neighbors:
                neighbors[neighbor]['mutual_events'] = list(neighbors[neighbor]['mutual_events'])

        nodes_data = dict()
        for ne in self.graph.nodes:
            nodes_data.update({ne: {"events": list(self.graph.nodes[ne]["events"]), "last_updated": self.graph.nodes[ne]["last_updated"]}})

        graph_structure = json.dumps(graph_structure)
        nodes_data = json.dumps(nodes_data)
        self.redis_conn.set(f"{RedisCons.PREFIX_NE_GRAPH}.graph-structure", graph_structure)
        self.redis_conn.set(f"{RedisCons.PREFIX_NE_GRAPH}.nodes-data", nodes_data)

        self.last_save_checkpoint = current_timestamp_sec

    def _get_current_time(self) -> datetime:
        return datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).replace(tzinfo=None)
    
    def ProcessEvent(self, request, context):
        text = request.text
        event_id = request.id
        TerminalLogging.log_info(f"Processing event {event_id}")

        if event_id in self.event_ne_dict:
            return

        main_nes = self._extract_main_ne(text=text)
        if len(main_nes) == 0:
            return event_ne_pb2.GeneralResponse(response_code=200, message=f"Event {event_id} has no Named Entity")
        
        self._put_event_in_graph(main_nes=main_nes, event_id=event_id)
        return event_ne_pb2.GeneralResponse(response_code=200, message=f"Successfully processed {event_id} with Named Entities {main_nes}")
    
    def GetEventSeries(self, request, context):
        text = request.text
        event_id = request.id

        if event_id in self.event_ne_dict:
            main_nes = self.event_ne_dict[event_id]
        else:
            main_nes = self._extract_main_ne(text=text)
            self._put_event_in_graph(main_nes=main_nes, event_id=event_id)

        result = dict()
        for ne in main_nes:
            if ne in self.graph.nodes:
                events = list(self.graph.nodes[ne]["events"])
            else:
                events = []
            result[ne] = event_ne_pb2.StringList(items=events)

        for i in range(len(main_nes)):
            ne_i = main_nes[i].lower()
            for j in range(i + 1, len(main_nes)):
                ne_j = main_nes[j].lower()

                if ne_j in ne_i or ne_i in ne_j:
                    continue

                try:
                    mutual_events = self.graph[ne_i][ne_j]["mutual_events"]
                except Exception as e:
                    continue

                k = f"{ne_i},{ne_j}"
                if k not in result:
                    result[k] = event_ne_pb2.StringList(items=mutual_events)

        if len(result.keys()) == 0:
            return event_ne_pb2.EventSeriesResponse(response_code=200, ne_events=None)

        return event_ne_pb2.EventSeriesResponse(response_code=200, ne_events=result)
    
    def Ping(self, request, context):
        return event_ne_pb2.GeneralResponse(response_code=200, message="Pong")
    
    def _put_event_in_graph(self, main_nes: list, event_id: str):
        for ne in main_nes:
            lowered_ne = ne.lower()
            current_timestamp_sec = self._get_current_time().timestamp()
            if lowered_ne in self.graph.nodes:
                self.graph.nodes[lowered_ne]["events"].add(event_id)
                self.graph.nodes[lowered_ne]["last_updated"] = current_timestamp_sec
            else:
                self.graph.add_node(lowered_ne, events={event_id}, last_updated=current_timestamp_sec)

        for i in range(len(main_nes)):
            ne_i = main_nes[i].lower()
            for j in range(i + 1, len(main_nes)):
                ne_j = main_nes[j].lower()

                if not self.graph.has_edge(ne_i, ne_j):
                    self.graph.add_edge(ne_i, ne_j, mutual_events=self.graph.nodes[ne_i]["events"].intersection(self.graph.nodes[ne_j]["events"]))
                else:
                    if type(self.graph[ne_i][ne_j]["mutual_events"]) == list:
                        self.graph[ne_i][ne_j]["mutual_events"] = set(self.graph[ne_i][ne_j]["mutual_events"])
                    self.graph[ne_i][ne_j]["mutual_events"].add(event_id)

        self.event_ne_dict[event_id] = main_nes

        self._remove_expired_ne()
        self._save_graph_checkpoint()

    def __extract_named_entities(self, text: str) -> list[str]:
        cleaned_text = ParserUtils.strip_emoji(text)
        cleaned_text = ParserUtils.strip_url(cleaned_text)
        cleaned_text = ParserUtils.strip_hashtag(cleaned_text)

        nes = KeywordExtractionUtils.extract_ne_by_capitalization(text=cleaned_text)
        nes = nes.union(KeywordExtractionUtils.extract_ne_underthesea(text=cleaned_text))
        nes = nes.union(KeywordExtractionUtils.extract_ne_underthesea_basic(text=cleaned_text))

        return list(nes)
    
    def _extract_main_ne(self, text: str) -> list[str]:
        nes_list = self.__extract_named_entities(text=text)
        if len(nes_list) == 0:
            return []
        
        order = "\nGiven a paragraph and a list of Named Entities. Identify the main Named Entity that the paragraph mainly confesses, maximum 3 Named Entities, minimum 1 Named Entity. Answer the Named Entity only, Named Entities are separated by commas (For example: A,B,C). No explaination."
        prompt = f"{text}\n{str(nes_list)}\n{order}"

        response = ModelApiUtils.request_gemini(prompt=prompt)
        main_nes = [ne.strip().lower() for ne in response.split(",")]

        return main_nes
    
    def clean_up(self):
        self.redis_conn.close()
 
    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()