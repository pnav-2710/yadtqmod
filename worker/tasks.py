from utils.logging import setup_logging
import time

logger = setup_logging(__name__)

class CodeExecutionWorker:
    def add(self, a, b):
        logger.debug(f"Performing addition: {a} + {b}")
        time.sleep(5)
        return a + b

    def subtract(self, a, b):
        logger.debug(f"Performing subtraction: {a} - {b}")
        time.sleep(5)
        return a - b

    def multiply(self, a, b):
        logger.debug(f"Performing multiplication: {a} * {b}")
        time.sleep(2)
        return a * b
