"""Base model interpreter for CFIHOS data processing."""
from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple

from cognite.neat_cfihos_handler.framework.common.log import log_init

logging = log_init(f"{__name__}", "i")


class BaseModelInterpreter(ABC):
    """Abstract base class for model interpreters."""

    @property
    @abstractmethod
    def model_interpreter_name(self):
        """Get the name of the model interpreter."""
        pass

    @property
    @abstractmethod
    def interpreting_model_name(self):
        """Get the name of the model being interpreted."""
        pass

    _map_dms_id_to_entity_id: Dict[str, str]
    _map_entity_id_to_dms_id: Dict[str, str]
    _map_entity_name_to_entity_id: Dict[str, str]
    _entities: Dict[str, dict]

    @abstractmethod
    def process(self) -> Tuple[Any, Any, Any]:
        """Process the data model."""
        pass

    @property
    def entity_name_to_entity_id(self):
        """Get the entity name to entity ID mapping."""
        return self._map_entity_name_to_entity_id

    @entity_name_to_entity_id.setter
    def entity_name_to_entity_id(self, mapping: dict):
        """Set the entity name to entity ID mapping."""
        self._map_entity_name_to_entity_id = mapping

    @property
    def entity_id_to_dms_id(self):
        """Get the entity ID to DMS ID mapping."""
        return self._map_entity_id_to_dms_id

    @entity_id_to_dms_id.setter
    def entity_id_to_dms_id(self, mapping: dict):
        """Set the entity ID to DMS ID mapping."""
        self._map_entity_id_to_dms_id = mapping

    @property
    def dms_id_to_entity_id(self):
        """Get the DMS ID to entity ID mapping."""
        return self._map_dms_id_to_entity_id

    @dms_id_to_entity_id.setter
    def dms_id_to_entity_id(self, mapping: dict):
        """Set the DMS ID to entity ID mapping."""
        self._map_dms_id_to_entity_id = mapping

    @property
    def data_model(self):
        """Get the data model entities."""
        return self._entities

    def _loggingDebug(self, msg: str) -> None:
        logging.debug(
            f"{self.model_interpreter_name}:{self.interpreting_model_name}] {msg}"
        )

    def _loggingInfo(self, msg: str) -> None:
        logging.info(
            f"[{self.model_interpreter_name}:{self.interpreting_model_name}] {msg}"
        )

    def _loggingWarning(self, msg: str) -> None:
        logging.warning(
            f"[{self.model_interpreter_name}:{self.interpreting_model_name}] {msg}"
        )

    def _loggingError(self, msg: str) -> None:
        logging.error(
            f"[{self.model_interpreter_name}:{self.interpreting_model_name}] {msg}"
        )

    def _loggingCritial(self, msg: str) -> None:
        logging.critical(
            f"[{self.model_interpreter_name}:{self.interpreting_model_name}] {msg}"
        )
