from broker.broker import KafkaMessageBroker
from result_backend.redis_backend import RedisResultBackend
from worker.worker import YADTQWorker
from utils.logging import setup_logging
import uuid

# Set up logging
logger = setup_logging(__name__)

# Setup Kafka and Redis connections
kafka_broker = KafkaMessageBroker(bootstrap_servers="localhost")  # Adjust based on your configuration
redis_backend = RedisResultBackend(host="localhost", port=6379)  # Adjust based on your Redis config

# Create a unique worker ID for each worker instance
worker_id = str(uuid.uuid4())  # Generate a unique ID for this worker

# Initialize the worker
worker = YADTQWorker(broker=kafka_broker, result_backend=redis_backend, worker_id=worker_id)

# Start the worker
worker.start()

try:
    # Let the worker run indefinitely (or use some condition to stop it)
    while True:
        pass
except KeyboardInterrupt:
    # Gracefully stop the worker when interrupted
    worker.stop()
