import time
import random
from typing import Any, Optional, Union, Iterator
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import logging

logger = logging.getLogger(__name__)


class Client(bigquery.Client):
    """
    BigQuery Client wrapper that adds automatic retry logic for concurrent update errors.
    Inherits all methods from google.cloud.bigquery.Client and overrides query() method
    to handle "Could not serialize access to table" errors with exponential backoff.
    """

    def __init__(self, *args, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 60.0, **kwargs):
        """
        Initialize the BigQuery client wrapper.

        Args:
            max_retries: Maximum number of retry attempts for concurrent update errors
            base_delay: Base delay in seconds for exponential backoff
            max_delay: Maximum delay in seconds between retries
            *args, **kwargs: Passed through to bigquery.Client
        """
        super().__init__(*args, **kwargs)
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def _is_concurrent_update_error(self, exception: Exception) -> bool:
        """Check if the exception is a concurrent update serialization error."""
        if not isinstance(exception, BadRequest):
            return False

        error_message = str(exception).lower()
        return (
                "could not serialize access to table" in error_message and
                "due to concurrent update" in error_message
        )

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for exponential backoff with jitter."""
        # Exponential backoff: base_delay * (2 ^ attempt)
        delay = self.base_delay * (2 ** attempt)

        # Cap at max_delay
        delay = min(delay, self.max_delay)

        # Add jitter (random factor between 0.5 and 1.5)
        jitter = random.uniform(0.5, 1.5)

        return delay * jitter

    def query(
            self,
            query: str,
            job_config: Optional[bigquery.QueryJobConfig] = None,
            job_id: Optional[str] = None,
            job_id_prefix: Optional[str] = None,
            location: Optional[str] = None,
            project: Optional[str] = None,
            job_retry: Optional[Any] = None,
            job_timeout: Optional[float] = None,
            *args,
            **kwargs
    ) -> bigquery.QueryJob:
        """
        Execute a query with automatic retry logic for concurrent update errors.

        This method has the exact same signature as bigquery.Client.query() but adds
        automatic retries with exponential backoff when encountering table serialization
        errors due to concurrent updates.

        Returns:
            bigquery.QueryJob: The query job object

        Raises:
            Same exceptions as bigquery.Client.query(), but with automatic retries
            for concurrent update errors up to max_retries attempts.
        """
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                # Call the parent class's query method with all original parameters
                return super().query(
                    query=query,
                    job_config=job_config,
                    job_id=job_id,
                    job_id_prefix=job_id_prefix,
                    location=location,
                    project=project,
                    job_retry=job_retry,
                    *args,
                    **kwargs
                )

            except Exception as e:
                last_exception = e

                # Check if this is a concurrent update error that we should retry
                if self._is_concurrent_update_error(e):
                    if attempt < self.max_retries:
                        delay = self._calculate_delay(attempt)
                        logger.warning(
                            f"Concurrent update error on attempt {attempt + 1}/{self.max_retries + 1}. "
                            f"Retrying in {delay:.2f} seconds. Error: {str(e)}"
                        )
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(
                            f"Max retries ({self.max_retries}) exceeded for concurrent update error. "
                            f"Final error: {str(e)}"
                        )

                # For non-concurrent-update errors or max retries exceeded, re-raise immediately
                raise e

        # This should never be reached, but just in case
        raise last_exception


# Convenience function for backward compatibility
def create_client(*args, **kwargs) -> Client:
    """
    Create a BigQuery client with concurrent update protection.

    Args:
        *args, **kwargs: Passed through to Client constructor

    Returns:
        Client: BigQuery client wrapper with retry logic
    """
    return Client(*args, **kwargs)