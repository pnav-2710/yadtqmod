import uuid
from broker.broker import KafkaMessageBroker
from result_backend.redis_backend import RedisResultBackend
from utils.logging import setup_logging

logger = setup_logging(__name__)

class YADTQClient:
    def __init__(self, kafka_broker: KafkaMessageBroker, redis_backend: RedisResultBackend):
        logger.info("Initializing YADTQ Client")
        self.kafka_broker = kafka_broker
        self.redis_backend = redis_backend
        logger.info("YADTQ Client initialization completed")

    def submit_task(self, task_type,*args):
        task_id = str(uuid.uuid4())  # Create a unique task ID
        logger.debug(f"Generated task ID: {task_id}")
        task_args = {}
        if args:
            task_args = {"a": args[0], "b": args[1]}

        task = {
            "task_id": task_id,
            "task_type":task_type,
            "args": task_args if task_args else {},
        }
        
        logger.info(f"Submitting task to Kafka: Task ID: {task_id}, Task Type: {task_type},Args: {args if args else 'No arguments'}")

        # Publish the task to Kafka message broker
        try:
            self.kafka_broker.publish_task("task_queue", task)
            self.redis_backend.store_task_result(task_id, {}, "queued")
            logger.info(f"Task {task_id} successfully submitted to Kafka")
        except Exception as e:
            logger.error(f"Failed to publish task {task_id} to Kafka. Error: {str(e)}")
            raise  # Reraise the exception after logging

        # Get task result from Redis
        try:
            result = self.redis_backend.get_task_result(task_id)
            logger.info(f"Fetched task result for Task ID {task_id}: {result}")
        except Exception as e:
            logger.error(f"Failed to fetch result for Task ID {task_id} from Redis. Error: {str(e)}")
            raise  # Reraise the exception after logging

        return task_id

    def get_task_result(self, task_id):
        logger.info(f"Fetching result for Task ID: {task_id}")
        
        try:
            result = self.redis_backend.get_task_result(task_id)
            logger.info(f"Task ID {task_id} result: {result}")
        except Exception as e:
            logger.error(f"Failed to fetch result for Task ID {task_id}. Error: {str(e)}")
            raise  # Reraise the exception after logging

        return result
