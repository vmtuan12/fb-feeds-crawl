from worker.base_worker import BaseWorker
from connectors.db_connector import DbConnectorBuilder, KafkaProducerBuilder
from utils.constants import PostgresConnectionConstant as Postgres, KafkaConnectionConstant as Kafka
from entities.entities import CommandEntity
from utils.command_utils import CommandType
from custom_logging.logging import TerminalLogging

class CommandWorker(BaseWorker):
    def __init__(self) -> None:
        self.pg_conn = DbConnectorBuilder()\
                        .set_host(Postgres.HOST)\
                        .set_port(Postgres.PORT)\
                        .set_username(Postgres.USER)\
                        .set_password(Postgres.PWD)\
                        .set_db_name(Postgres.DB)\
                        .build_pg()
        self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build()
        
    def _get_page_list(self) -> list:
        return []

    def start(self):
        list_pages = self._get_page_list()
        for index, p in enumerate(list_pages):
            scrape_msg = CommandEntity(cmd_type=CommandType.SCRAPE_PAGE, page=p).to_dict()
            self.kafka_producer.send(Kafka.TOPIC_COMMAND, value=scrape_msg)
            TerminalLogging(f"Command {scrape_msg}")

            if index > 0 and (index % 10 == 0 or index == (len(list_pages) - 1)):
                clear_cache_msg = CommandEntity(cmd_type=CommandType.CLEAR_CACHE).to_dict()
                TerminalLogging(f"Command {clear_cache_msg}")
                
        self.kafka_producer.flush()