import logging
import os
import json

from dotenv import load_dotenv
from kafka.consumer import KafkaConsumer

load_dotenv(verbose=True)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
  print("Starting consumer", os.environ["BOOTSTRAP_SERVER"])
  consumer = KafkaConsumer( 
    bootstrap_servers=[os.environ["BOOTSTRAP_SERVER"]],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=os.environ["CONSUMER_GROUP"],
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
  )

  consumer.subscribe([os.environ["TOPICS_PEOPLE_BASIC_NAME"]])

  for message in consumer:
    try:
      kafka_message = f"""
      Message received: {message.value}
      Message key: {message.key}
      Message partition: {message.partition}
      Message offset: {message.offset}

      """
      logger.info(kafka_message)
    except Exception as e:
      logger.error(e)
  

if __name__ == "__main__":
  main()