from typing import List, Dict
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
from utils.logging import setup_logging
import time

# Set up the logger
logger = setup_logging(__name__)

class KafkaMessageBroker:
    def __init__(self, bootstrap_servers: str, group_id: str = "task_consumer_group"):
        """
        Initialize KafkaMessageBroker with Kafka producer and consumer.

        :param bootstrap_servers: Kafka broker(s) address.
        :param group_id: Consumer group ID (optional for load balancing).
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id  # Consumer group ID to share load among workers
        
        try:
            # Initialize Kafka producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.debug("Kafka producer initialized.")
            
            # Initialize Kafka consumer with consumer group for load balancing
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,  # Set the same consumer group for all workers
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset="earliest",  # Start reading from the earliest message if no offset exists
                enable_auto_commit=True  # Auto commit for message consumption
            )
            logger.debug("Kafka consumer initialized.")
        
        except KafkaError as e:
            logger.error(f"Error initializing Kafka producer/consumer: {e}")
            raise

    def publish_task(self, topic: str, task: Dict):
        """
        Publish a task to the Kafka topic.

        :param topic: Kafka topic to publish the task to.
        :param task: The task data to send to Kafka.
        """
        try:
            logger.debug(f"Attempting to send task {task} to Kafka topic: {topic}")
            self.producer.send(topic, value=task)
            self.producer.flush()  # Ensure the task is sent
            logger.info(f"Successfully sent task {task} to Kafka topic: {topic}")
        except KafkaError as e:
            logger.error(f"Error publishing task to {topic}: {e}")
            raise

    def consume_tasks(self, topic: str, max_wait=2) -> List[Dict]:
        """
        Consume tasks from a Kafka topic.

        :param topic: Kafka topic to consume tasks from.
        :param max_wait: Maximum time (in seconds) to wait for tasks.
        :return: List of tasks consumed from Kafka.
        """
        batch_size = 10  # Number of tasks to consume in one batch
        logger.debug(f"Attempting to consume tasks from Kafka topic: {topic}")
        
        self.consumer.subscribe([topic])  # Subscribe to the topic
        tasks = []
        start_time = time.time()  # Start timer for max_wait

        try:
            while True:
                # Poll for messages with a timeout of 100ms
                messages = self.consumer.poll(timeout_ms=100)
                if isinstance(messages, dict) and messages:
                    for partition, msgs in messages.items():
                        if isinstance(msgs, list):
                            for message in msgs:
                                if hasattr(message, 'value'):
                                    tasks.append(message.value)
                                    logger.debug(f"Received message from Kafka: {message.value}")

                                    # Check if batch size is reached
                                    if len(tasks) >= batch_size:
                                        logger.debug(f"Consumed {len(tasks)} tasks from {topic}.")
                                        return tasks
                # If the maximum wait time is exceeded, return whatever tasks have been consumed
                if time.time() - start_time > max_wait:
                    logger.debug(f"Time elapsed is {time.time() - start_time:.2f} seconds, returning {len(tasks)} tasks.")
                    return tasks

        except KafkaError as e:
            logger.error(f"Error consuming tasks from {topic}: {e}")
            return []

    def close(self):
        """
        Close the Kafka producer and consumer gracefully.
        """
        if self.producer:
            self.producer.close()
            logger.debug("Kafka producer closed.")
        if self.consumer:
            self.consumer.close()
            logger.debug("Kafka consumer closed.")
