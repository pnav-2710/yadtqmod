from utils.logging import setup_logging
from typing import Dict
from broker.broker import KafkaMessageBroker
from result_backend.redis_backend import RedisResultBackend
from worker.tasks import CodeExecutionWorker
import time
import uuid

# Initialize the logger
logger = setup_logging(__name__)

class YADTQWorker:
    def __init__(self, broker: KafkaMessageBroker, result_backend: RedisResultBackend, worker_id: str = None):
        """
        Initializes a worker that processes tasks from the Kafka queue and stores results in Redis.

        :param broker: KafkaMessageBroker instance to interact with Kafka.
        :param result_backend: RedisResultBackend instance to store task results.
        :param worker_id: Unique worker ID, generated if not provided.
        """
        self.broker = broker
        self.result_backend = result_backend
        self.task_executor = CodeExecutionWorker()
        self.worker_id = worker_id or str(uuid.uuid4())  # Generate a unique ID if not provided
        self._running = False  # Flag to control the worker's running state
        logger.info(f"YADTQ Worker {self.worker_id} initialized.")

    def start(self):
        """
        Start the worker to begin consuming tasks from Kafka.
        """
        self._running = True  # Set the worker to running state
        logger.info(f"YADTQ Worker {self.worker_id} starting...")
        self.consume_tasks()

    def stop(self):
        """
        Stop the worker from consuming tasks.
        """
        self._running = False  # Set the worker to stop consuming tasks
        logger.info(f"YADTQ Worker {self.worker_id} has been stopped.")

    def consume_tasks(self):
        """
        Consume tasks from the Kafka topic and process them.

        The worker will continuously poll the Kafka topic for new tasks
        and process them as long as the worker is running.
        """
        logger.info(f"YADTQ Worker {self.worker_id} is now consuming tasks.")
        while self._running:  # Continue only if the worker is running
            logger.debug(f"Worker {self.worker_id} polling for tasks from Kafka...")
            tasks = self.broker.consume_tasks("task_queue")  # Poll for new tasks

            if not tasks:
                logger.debug(f"Worker {self.worker_id} found no tasks in the queue.")
                time.sleep(1)  # Add a small delay to avoid busy waiting
                continue

            for task in tasks:
                if not self._running:  # Check if stop was called while processing tasks
                    logger.info(f"Worker {self.worker_id} is stopping while processing tasks.")
                    break
                logger.info(f"Worker {self.worker_id} is processing task: {task.get('task_id')}")
                self.process_task(task)

    def process_task(self, task: Dict):
        """
        Process an individual task.

        :param task: The task data, containing task_id, task_type, and arguments.
        """
        task_id = task["task_id"]
        task_type = task["task_type"]
        task_args = task["args"]

        # Update task status to "executing" in Redis, including worker_id
        self.result_backend.update_task_status(task_id, {"status": "executing", "worker_id": self.worker_id})
        try:
            logger.info(f"Worker {self.worker_id} executing task {task_id} of type {task_type} with arguments {task_args}.")
            result = getattr(self.task_executor, task_type)(**task_args)  # Call the task handler method
            store = {"result": result, "worker_id": self.worker_id}
            logger.info(f"Worker {self.worker_id} completed task {task_id} with result: {result}")
            self.result_backend.store_task_result(task_id, store, "completed")
        except Exception as e:
            logger.error(f"Worker {self.worker_id} encountered an error while processing task {task_id}: {str(e)}")
            error_result = {"status": "error", "error": str(e), "worker_id": self.worker_id}
            self.result_backend.store_task_result(task_id, error_result, "error")
