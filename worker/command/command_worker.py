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
        cursor = self.pg_conn.cursor()
        cursor.execute("""
        SELECT * FROM (
        SELECT pl.username FROM fb.page_lang pl LEFT JOIN fb.facebook_page fp ON pl.pageid = fp.pageid
        WHERE pl.name_lang = 'vi' AND pl.about_lang = 'vi' AND pl.des_lang = 'vi' AND pl.username != 'NaN'
        AND pl.username NOT IN (SELECT po.username from fb.page_options po WHERE po.option = 'excluded')
        AND fp.categories && ARRAY[
            'Trang web tin tức & truyền thông',
            'Công ty truyền thông/tin tức',
            'Giải trí',
            'Công ty truyền thông xã hội',
            'Nghệ thuật & Giải trí',
            'Công ty truyền thông & sản xuất truyền thông',
            'Entertainment Website',
            'Báo',
            'Tạp chí',
            'Trang web giải trí',
            'Chuyên gia tin tức'
            ]
        AND NOT (fp.categories && ARRAY['Nghệ sỹ', 'Diễn viên hài', 'Diễn viên', 'Cửa hàng nội thất', 'Cửa hàng quần áo sơ sinh & trẻ em', 'Người của công chúng'])
        AND pl.numlikes >= 10000
        ORDER BY pl.numlikes DESC
        LIMIT 900) p1
        UNION
        (SELECT po.username from fb.page_options po WHERE po.option = 'included');
        """)
        rows = cursor.fetchall()
        cursor.close()
        return set([r[0] for r in rows])

    def start(self):
        list_pages = self._get_page_list()
        current_partition = 0

        for index, p in enumerate(list_pages):
            scrape_msg = CommandEntity(cmd_type=CommandType.SCRAPE_PAGE, page=p).to_dict()

            try:
                self.kafka_producer.send(Kafka.TOPIC_COMMAND, value=scrape_msg, partition=current_partition)
            except AssertionError as ae:
                current_partition = 0
                self.kafka_producer.send(Kafka.TOPIC_COMMAND, value=scrape_msg, partition=current_partition)

            current_partition += 1

            TerminalLogging.log_info(f"Command {scrape_msg}")

            if index > 0 and (index % 50 == 0 or index == (len(list_pages) - 1)):
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