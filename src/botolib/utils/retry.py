import time
import random

from .logger import get_logger

logger = get_logger()

class ExponentialBackoffRetry:
    def __init__(self, max_retries=5, base_delay=0.1, max_delay=5):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def execute(self, condition_function, execution_function, *args, **kwargs):
        retries = 0

        while retries < self.max_retries:
            try:
                result = execution_function(*args, **kwargs)
                if not condition_function(result):
                    return result

                retries += 1
                if retries > self.max_retries:
                    logger.debug("Max retries reached. Operation failed.")
                    raise Exception("Max retries reached.")
                
                delay = min(self.base_delay * (2 ** retries), self.max_delay)
                delay_with_jitter = delay + random.uniform(0, delay / 2)
                logger.debug(f"Retrying in {delay_with_jitter:.2f} seconds...")
                time.sleep(delay_with_jitter)

            except Exception as e:
                if retries > self.max_retries:
                    logger.debug(f"Operation failed after {retries} retries: {e}")
                    return
                retries += 1
                logger.debug(f"Retry attempt {retries} due to error: {e}")
