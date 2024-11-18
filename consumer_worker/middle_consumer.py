from connectors.db_connector import DbConnectorBuilder, KafkaConsumerBuilder
from utils.constants import PostgresConnectionConstant as Postgres, KafkaConnectionConstant as Kafka

class MiddleConsumer():
    def __init__(self) -> None:
        self.pg_conn = DbConnectorBuilder()\
                        .set_host(Postgres.HOST)\
                        .set_port(Postgres.PORT)\
                        .set_username(Postgres.USER)\
                        .set_password(Postgres.PWD)\
                        .set_db_name(Postgres.DB)\
                        .build_pg()
        self.kafka_consumer = KafkaConsumerBuilder()\
                                .set_brokers(Kafka.BROKERS)\
                                .set_group_id(Kafka.GROUP_ID_MIDDLE_CONSUMER)\
                                .set_topics(Kafka.TOPIC_MIDDLE_CONSUMER)
        
    