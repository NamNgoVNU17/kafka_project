from confluent_kafka import Consumer, Producer, KafkaException
from pymongo import MongoClient
import json
import logging
import threading
import os
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka config from environment
kafka_remote_config = {
    'bootstrap.servers': os.getenv('KAFKA_REMOTE_BOOTSTRAP_SERVERS'),
    'group.id': os.getenv('KAFKA_REMOTE_GROUP_ID'),
    'auto.offset.reset': 'earliest',
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD')
}

local_kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_LOCAL_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD')
}

# Mongo config from environment
mongo_uri = os.getenv("MONGO_URI")
mongo_db_name = os.getenv("MONGO_DB_NAME")
mongo_collection_name = os.getenv("MONGO_COLLECTION_NAME")

# Topics
remote_topic = os.getenv("KAFKA_REMOTE_TOPIC")
local_topic = os.getenv("KAFKA_LOCAL_TOPIC")

def create_remote_kafkaConsumer():
    try:
        consumer = Consumer(kafka_remote_config)
        consumer.subscribe([remote_topic])
        return consumer
    except KafkaException as e:
        logger.error(f"Failed to create remote consumer: {e}")
        raise

def create_local_kafkaProducer():
    try:
        producer = Producer(local_kafka_config)
        return producer
    except KafkaException as e:
        logger.error(f"Failed to create local Producer: {e}")
        raise

def create_local_kafkaConsumer():
    try:
        consumer = Consumer({
            **local_kafka_config,
            'group.id': os.getenv('KAFKA_LOCAL_GROUP_ID'),
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([local_topic])
        return consumer
    except KafkaException as e:
        logger.error(f"Failed to create local Consumer: {e}")
        raise

def connect_mongodb():
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client[mongo_db_name]
        collection = db[mongo_collection_name]
        logger.info("Successfully connected to MongoDB")
        return collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def report_delivery(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def consume_remote_and_produce_local():
    consumer = create_remote_kafkaConsumer()
    producer = create_local_kafkaProducer()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Remote consumer error: {msg.error()}")
                continue

            message_value = msg.value().decode('utf-8')
            logger.info(f"Forwarded message to local topic: {message_value}")
            producer.produce(
                local_topic,
                value=message_value.encode('utf-8'),
                callback=report_delivery
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
                if '_id' in data:
                    collection.replace_one({'_id': data['_id']}, data, upsert=True)
                    logger.info(f"Stored message in MongoDB: {data}")
                else:
                    logger.warning(f"No _id in message, skipping: {data}")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON: {e}")
    except KeyboardInterrupt:
        logger.info("Local consumer interrupted")
    finally:
        consumer.close()

if __name__ == "__main__":
    remote_to_local_thread = threading.Thread(target=consume_remote_and_produce_local)
    local_to_mongodb_thread = threading.Thread(target=consume_local_and_store_to_mongodb)

    remote_to_local_thread.start()
    local_to_mongodb_thread.start()

    remote_to_local_thread.join()
    local_to_mongodb_thread.join()
