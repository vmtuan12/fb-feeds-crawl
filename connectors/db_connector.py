import psycopg2
from kafka import KafkaConsumer, KafkaProducer
import json
import io
import avro.schema
from avro.io import DatumWriter, BinaryEncoder, BinaryDecoder, DatumReader

class DbConnector():
    def __init__(self, host: str, port: int, 
                 db_name: str | None, username: str | None, password: str | None):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.username = username
        self.password = password
        self.conn = None
    
    def close(self):
        pass

class PostgresConnector(DbConnector):
    def __init__(self, host: str, port: int, db_name: str | None, username: str | None, password: str | None):
        super().__init__(host, port, db_name, username, password)
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.db_name,
            connect_timeout=10
        )

    def close(self):
        self.conn.close()

class DbConnectorBuilder():
    def __init__(self):
        self.host = None
        self.port = None
        self.db_name = None
        self.username = None
        self.password = None

    def set_host(self, host: str):
        self.host = host
        return self

    def set_port(self, port: int):
        self.port = port
        return self

    def set_db_name(self, db_name: str):
        self.db_name = db_name
        return self

    def set_username(self, username: str):
        self.username = username
        return self

    def set_password(self, password: str):
        self.password = password
        return self
    
    def build_pg(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.db_name,
            connect_timeout=10
        )

class KafkaConsumerBuilder():
    def __init__(self):
        self.topics = []
        self.brokers = []
        self.group_id = "default"
        self.enable_auto_commit = True
        self.auto_offset_reset = 'latest'

    def set_topics(self, topics: str | list[str]):
        self.topics = topics
        return self

    def set_brokers(self, brokers: list[str]):
        self.brokers = brokers
        return self

    def set_group_id(self, group_id: str):
        self.group_id = group_id
        return self

    def set_enable_auto_commit(self, enable_auto_commit: bool):
        self.enable_auto_commit = enable_auto_commit
        return self

    def set_auto_offset_reset(self, auto_offset_reset: str):
        self.auto_offset_reset = auto_offset_reset
        return self
    
    def build(self, avro_schema_path: str | None = None):
        if avro_schema_path != None:
            schema = avro.schema.parse(open(avro_schema_path).read())
        else:
            schema = None
        
        def avro_deserialize(msg):
            bytes_reader = io.BytesIO(msg)
            decoder = BinaryDecoder(bytes_reader)
            reader = DatumReader(schema)
            return reader.read(decoder)
        
        def json_deserialize(msg):
            return json.loads(msg.decode('utf-8'))
        
        return KafkaConsumer(self.topics,
                            group_id=self.group_id,
                            bootstrap_servers=self.brokers,
                            enable_auto_commit=self.enable_auto_commit,
                            auto_offset_reset=self.auto_offset_reset,
                            value_deserializer=(json_deserialize if avro_schema_path == None else avro_deserialize))
    
class KafkaProducerBuilder():
    def __init__(self):
        self.brokers = []

    def set_brokers(self, brokers: list[str]):
        self.brokers = brokers
        return self
    
    def build(self, avro_schema_path: str | None = None):
        if avro_schema_path != None:
            schema = avro.schema.parse(open(avro_schema_path).read())
        else:
            schema = None

        def avro_serialize(msg):
            writer = DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = BinaryEncoder(bytes_writer)
            writer.write(msg, encoder)
            raw_bytes = bytes_writer.getvalue()
            return raw_bytes
        
        def json_serialize(msg):
            return json.dumps(msg).encode('utf-8')
        
        return KafkaProducer(bootstrap_servers=self.brokers, 
                        value_serializer=(json_serialize if avro_schema_path == None else avro_serialize))