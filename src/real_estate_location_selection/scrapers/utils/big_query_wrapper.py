import time
import random
from typing import Any, Optional, Union, Iterator
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import logging

logger = logging.getLogger(__name__)


class Client(bigquery.Client):
    """
    BigQuery Client wrapper that adds automatic retry logic for concurrent update errors
    and DML resource limit errors. Inherits all methods from google.cloud.bigquery.Client
    and overrides query() method to handle table serialization and resource limit errors
    with exponential backoff.
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

    def _is_retryable_error(self, exception: Exception) -> bool:
        """Check if the exception is a retryable BigQuery error."""
        if not isinstance(exception, BadRequest):
            return False

        error_message = str(exception).lower()

        # Check for concurrent update serialization errors
        concurrent_update_error = (
                "could not serialize access to table" in error_message and
                "due to concurrent update" in error_message
        )

        # Check for DML resource limit errors
        dml_limit_error = (
                "too many dml statements outstanding" in error_message and
                "limit is 20" in error_message
        )

        # Check for other resource exceeded errors
        resource_exceeded_error = (
                "resources exceeded during query execution" in error_message
        )

        return concurrent_update_error or dml_limit_error or resource_exceeded_error

    def _calculate_delay(self, attempt: int, error_type: str = "default") -> float:
        """Calculate delay for exponential backoff with jitter."""
        # For DML limit errors, use longer delays since we need to wait for other operations to complete
        if "dml" in error_type.lower() or "resource" in error_type.lower():
            base_delay = max(self.base_delay * 2, 5.0)  # Minimum 5 seconds for DML errors
        else:
            base_delay = self.base_delay

        # Exponential backoff: base_delay * (2 ^ attempt)
        delay = base_delay * (2 ** attempt)

        # Cap at max_delay
        delay = min(delay, self.max_delay)

        # Add jitter (random factor between 0.5 and 1.5)
        jitter = random.uniform(0.5, 1.5)

        return delay * jitter

    def _get_error_type(self, exception: Exception) -> str:
        """Determine the type of error for better logging and delay calculation."""
        if not isinstance(exception, BadRequest):
            return "unknown"

        error_message = str(exception).lower()

        if "could not serialize access to table" in error_message:
            return "concurrent_update"
        elif "too many dml statements outstanding" in error_message:
            return "dml_limit"
        elif "resources exceeded" in error_message:
            return "resource_exceeded"
        else:
            return "unknown"

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
        Execute a query with automatic retry logic for concurrent update and resource limit errors.

        This method has the exact same signature as bigquery.Client.query() but adds
        automatic retries with exponential backoff when encountering:
        - Table serialization errors due to concurrent updates
        - DML resource limit errors (too many concurrent DML statements)
        - Other resource exceeded errors

        Returns:
            bigquery.QueryJob: The query job object

        Raises:
            Same exceptions as bigquery.Client.query(), but with automatic retries
            for retryable errors up to max_retries attempts.
        """
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                # Call the parent class's query method with all original parameters
                job = super().query(
                    query=query,
                    job_config=job_config,
                    job_id=job_id,
                    job_id_prefix=job_id_prefix,
                    location=location,
                    project=project,
                    job_retry=job_retry,
                    job_timeout=job_timeout,
                    *args,
                    **kwargs
                )

                # Wait for the job to complete to catch runtime errors
                try:
                    job.result()  # This will raise an exception if the job failed
                    return job
                except Exception as job_exception:
                    # If the job failed, treat it as the exception to potentially retry
                    raise job_exception

            except Exception as e:
                last_exception = e
                error_type = self._get_error_type(e)

                # Check if this is a retryable error
                if self._is_retryable_error(e):
                    if attempt < self.max_retries:
                        delay = self._calculate_delay(attempt, error_type)
                        logger.warning(
                            f"{error_type.replace('_', ' ').title()} error on attempt {attempt + 1}/{self.max_retries + 1}. "
                            f"Retrying in {delay:.2f} seconds. Error: {str(e)}"
                        )
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(
                            f"Max retries ({self.max_retries}) exceeded for {error_type} error. "
                            f"Final error: {str(e)}"
                        )

                # For non-retryable errors or max retries exceeded, re-raise immediately
                raise e

        # This should never be reached, but just in case
        raise last_exception


# Convenience function for backward compatibility
def create_client(*args, **kwargs) -> Client:
    """
    Create a BigQuery client with concurrent update and resource limit protection.

    Args:
        *args, **kwargs: Passed through to Client constructor

    Returns:
        Client: BigQuery client wrapper with retry logic
    """
    return Client(*args, **kwargs)