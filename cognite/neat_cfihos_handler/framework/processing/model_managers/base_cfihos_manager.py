"""Base CFIHOS Manager module.

This module defines the base manager class and supporting data structures for loading and processing
CFIHOS data models. It provides the foundational interface and result structure for subclasses that
implement specific CFIHOS data manager logic.
"""

from dataclasses import dataclass

from cognite.neat.core._issues import IssueList

from cognite.neat_cfihos_handler.framework.common.log import log_init

LOG_LEVEL = "i"  # Define a constant for the log level
logging = log_init(f"{__name__}", LOG_LEVEL)


@dataclass
class ReadResult:
    """Result structure for CFIHOS data loading operations."""

    Properties: list[dict]
    Containers: list[dict]
    Views: list[dict]
    Metadata: dict


class BaseCfihosManager:
    """Base class for CFIHOS data managers."""

    def __init__(
        self,
        processor_config: dict,
    ):
        """Initialize the base CFIHOS manager."""
        self.processor_config = processor_config
        self.processor_issue_list = IssueList()

    def _validate_config(self):
        """Validate the configuration dictionary to ensure it contains the required keys."""
        raise NotImplementedError(
            "This method should be implemented in a subclass. "
            "It is expected to process and collect data models from multiple model processors."
        )

    def read_model(self) -> None | ReadResult:
        """Read and process the model data.

        This method should be overridden in subclasses to implement specific
        model reading logic with appropriate parameters.
        """
        raise NotImplementedError(
            "This method should be implemented in a subclass. "
            "It is expected to read and process model data with specific parameters."
        )
