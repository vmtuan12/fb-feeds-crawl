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
        
    def _get_page_list(self) -> set:
        cursor = self.pg_conn.cursor()
        cursor.execute("""
        SELECT po.username, po.scrape_threshold from fb.page_options po WHERE po.option = 'included';
        """)
        rows = cursor.fetchall()
        cursor.close()

        return set([(r[0], r[1]) for r in rows])

    def start(self):
        list_pages = self._get_page_list()
        current_partition = 0

        for index, p in enumerate(list_pages):
            scrape_msg = CommandEntity(cmd_type=CommandType.SCRAPE_PAGE, page=p[0], scrape_threshold=p[1]).to_dict()

            try:
                self.kafka_producer.send(Kafka.TOPIC_COMMAND, value=scrape_msg, partition=current_partition)
            except AssertionError as ae:
                current_partition = 0
                self.kafka_producer.send(Kafka.TOPIC_COMMAND, value=scrape_msg, partition=current_partition)

            current_partition += 1

            TerminalLogging.log_info(f"Command {scrape_msg}")

            if index == (len(list_pages) - 1):
                temp_partition = 0
                clear_cache_msg = CommandEntity(cmd_type=CommandType.CLEAR_CACHE).to_dict()
                TerminalLogging.log_info(f"Command {clear_cache_msg}")

                while (True):
                    try:
                        self.kafka_producer.send(Kafka.TOPIC_COMMAND, value=clear_cache_msg, partition=temp_partition)
                        temp_partition += 1
                    except AssertionError as ae:
                        break
                
        self.kafka_producer.flush()

    def clean_up(self):
        self.kafka_producer.flush()
        self.kafka_producer.close(timeout=5)
        self.pg_conn.close()

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()