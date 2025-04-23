from confluent_kafka import Consumer, Producer, KafkaException
from pymongo import MongoClient
import json
import logging
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Kafka server

kafka_remote_config = {
    'bootstrap.servers': '113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294',
    'group.id': 'ngohoainam_group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024'
}

# Local kafkak config
local_kafka_config = {
    'bootstrap.servers': 'localhost:9094,localhost:9194,localhost:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024'
}

#Mongo config
mongo_uri = "mongodb://localhost:27017"
mongo_db_name = "kafka_data"
mongo_collection_name = "product_view"

#Topic

remote_topic = "product_view"
local_topic = "product_viewed"

def create_remote_kafkaConsumer() :
    try :
        consumer = Consumer(kafka_remote_config)
        consumer.subscribe([remote_topic])
        return consumer
    except KafkaException as e :
        logger.error(f"Failed to create remote consumer : {e}")
        raise

def creat_local_kafkaProducer() :
    try :
        producer = Producer(local_kafka_config)
        return producer
    except KafkaException as e :
        logger.error((f"Faild to create local Producer: {e}"))
        raise

def create_local_kafkaConsumer() :
    try :
        consumer = Consumer({
            'bootstrap.servers': local_kafka_config['bootstrap.servers'],
            'group.id': 'local_product_view_group',
            'auto.offset.reset': 'earliest',
            'security.protocol': local_kafka_config['security.protocol'],
            'sasl.mechanism': local_kafka_config['sasl.mechanism'],
            'sasl.username': local_kafka_config['sasl.username'],
            'sasl.password': local_kafka_config['sasl.password']
        })
        consumer.subscribe([local_topic])
        return consumer
    except KafkaException as e :
        logger.error(f"Faild to create local Consumer : {e}")
        raise

def connect_mongodb() :
    try:
        client = MongoClient(mongo_uri)
        db = client[mongo_db_name]
        collection = db[mongo_collection_name]
        logger.info("Successfully connected to MongoDB")
        return collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def report_delivery(err, msg) :
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def consume_remote_and_produce_local():
    consumer = create_remote_kafkaConsumer()
    producer = creat_local_kafkaProducer()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Remote consumer error: {msg.error()}")
                continue

            message_value = msg.value().decode('utf-8')
            # logger.info(f"Consumed message from remote {remote_topic}: {message_value}")

            # Produce to local Kafka topic
            producer.produce(
                local_topic,
                value=message_value.encode('utf-8')
                # callback=report_deliveryS
            )
            producer.poll(0)

    except KeyboardInterrupt:
        logger.info("Remote consumer interrupted")
    finally:
        consumer.close()
        producer.flush()

def consume_local_and_store_to_mongodb():

    consumer = create_local_kafkaConsumer()
    collection = connect_mongodb()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Local consumer error: {msg.error()}")
                continue

            message_value = msg.value().decode('utf-8')
            try:
                data = json.loads(message_value)
                collection.replace_one({'_id': data['_id']}, data, upsert=True)
                logger.info(f"Stored message in MongoDB: {data}")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON: {e}")
                continue

    except KeyboardInterrupt:
        logger.info("Local consumer interrupted")
    finally:
        consumer.close()

if __name__ == "__main__" :
    remote_to_local_thread = threading.Thread(target=consume_remote_and_produce_local)
    local_to_mongodb_thread = threading.Thread(target=consume_local_and_store_to_mongodb)

    remote_to_local_thread.start()
    local_to_mongodb_thread.start()

    remote_to_local_thread.join()
    local_to_mongodb_thread.join()