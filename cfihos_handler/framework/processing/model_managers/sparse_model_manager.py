"""Sparse CFIHOS Manager module.

This module defines the SparseCfihosManager class, which is responsible for loading and processing
sparse CFIHOS data models. It leverages the SparsePropertiesProcessor to interpret and collect
model properties, entities, and mappings, and provides validation and configuration management
for sparse CFIHOS data model ingestion.
"""

from cognite.neat.core._issues.errors import NeatValueError

from cfihos_handler.framework.common.generic_classes import SparseModelType
from cfihos_handler.framework.common.log import log_init
from cfihos_handler.framework.common.utils import collect_model_subset
from cfihos_handler.framework.neat_data_model.model_creater import (
    build_neat_model_from_entities,
)
from cfihos_handler.framework.processing.model_managers.base_cfihos_manager import (
    BaseCfihosManager,
    ReadResult,
)
from cfihos_handler.framework.processing.processors.sparse_properties import (
    SparsePropertiesProcessor,
)

LOG_LEVEL = "i"  # Define a constant for the log level
logging = log_init(f"{__name__}", LOG_LEVEL)


class SparseCfihosManager(BaseCfihosManager):
    """Sparse CFIHOS manager for processing sparse data models."""

    def __init__(
        self,
        processor_config: dict,
        *,
        model_type: str = "",
        scope: str = "",
    ):
        """Initialize the sparse CFIHOS manager."""
        super().__init__(processor_config)
        self._validate_config()
        self._processor_config = {
            k: v
            for k, v in self.processor_config.items()
            if k == "model_processors_config"
        }
        self.model_type = model_type
        self.scope = scope
        self._validate_important_parameters()
        self._containers_indexes = self.processor_config.get("containers_indexes", {})
        self._container_space = self.processor_config["container_data_model_space"]
        self._views_space = self.processor_config["views_data_model_space"]
        self._model_version = self.processor_config["model_version"]
        self._model_creator = self.processor_config["model_creator"]
        self._model_name = self.processor_config["data_model_name"]
        self._model_description = self.processor_config["data_model_description"]
        self._model_external_id = self.processor_config["data_model_external_id"]
        self.dms_identifire = self.processor_config["dms_identifire"]
        self.processor_type = self.processor_config["processor_type"]
        self.model_processor = SparsePropertiesProcessor(
            **self._processor_config, model_type=model_type
        )
        self.model_processor.process_and_collect_models()
        self._model_properties = self.model_processor.model_properties
        self._map_dms_id_to_entity_id = self.model_processor.map_dms_id_to_entity_id
        self._map_entity_id_to_dms_id = self.model_processor.map_entity_id_to_dms_id
        self._model_entities = self.model_processor.model_entities
        self._scopes_by_name = {
            scope["scope_name"]: scope for scope in self.processor_config["scopes"]
        }
        self.processor_issue_list = self.model_processor.issue_list

    def _validate_important_parameters(self) -> None:
        """Validate the important parameters to ensure they are valid."""
        # Validate model_type is not None or empty string
        if not self.model_type or self.model_type.strip() == "":
            raise NeatValueError("model_type cannot be None or empty string")

        # Validate model_type has valid values
        valid_model_types = [SparseModelType.CONTAINERS, SparseModelType.VIEWS]
        if self.model_type not in valid_model_types:
            raise NeatValueError(
                f"Invalid model_type '{self.model_type}'. Valid values are: {', '.join(valid_model_types)}"
            )

        # If model_type is "views", scope should not be None or empty string
        if self.model_type == SparseModelType.VIEWS:
            if not self.scope or self.scope.strip() == "":
                raise NeatValueError(
                    "scope cannot be None or empty string when model_type is 'views'"
                )

    def _validate_config(self) -> None:
        """Validate the configuration dictionary to ensure it contains the required keys."""
        required_keys = [
            "model_processors_config",
            "containers_indexes",
            "container_data_model_space",
            "views_data_model_space",
            "model_version",
            "model_creator",
            "data_model_name",
            "data_model_description",
            "data_model_external_id",
            "dms_identifire",
            "scope_config",
            "processor_type",
        ]
        missing_keys = [
            key for key in required_keys if key not in self.processor_config
        ]
        if missing_keys:
            raise NeatValueError(
                f"Missing required keys in configuration: {', '.join(missing_keys)}"
            )

    def get_scope_by_name(self, scope_name: str) -> dict:
        """Get scope configuration by name."""
        try:
            return self._scopes_by_name[scope_name]
        except KeyError:
            raise NeatValueError(f"Scope '{scope_name}' not found.")

    def _build_containers_model(self) -> ReadResult:
        # Setup containers from models
        logging.info(f"Buidling {len(self._model_properties)} container properties ...")
        (
            lst_views,
            lst_properties,
            lst_containers,
        ) = build_neat_model_from_entities(
            entities=self._model_entities,
            dms_identifire=self.dms_identifire,
            include_containers=True,
            include_cdm=True,
            containers_indexes=self._containers_indexes,
            containers_space=self._container_space,
            force_code_as_view_id=True,
        )

        logging.info("Generating NEAT Data Model ...")

        return ReadResult(
            Properties=lst_properties,
            Containers=lst_containers,
            Views=lst_views,
            Metadata={
                "role": "DMS Architect",
                "dataModelType": "enterprise",
                "schema": "complete",
                "space": self._container_space,
                "name": self._model_name,
                "description": self._model_description,
                "external_id": self._model_external_id,
                "version": self._model_version,
                "creator": self._model_creator,
            },
        )

    def _build_scoped_views_models(self, scope) -> ReadResult:
        views_scope = self._scopes_by_name.get(scope)
        if not views_scope:
            raise NeatValueError(
                f"Scope '{scope}' not found in cfihos_model_config['scopes']"
            )

        logging.info(f"Building model for scope '{scope}' ...")

        scoped_model = collect_model_subset(
            full_model=self.model_processor.model_entities,
            scope_config=self.processor_config["scope_config"],
            scope=views_scope["scope_subset"],
            containers_space=self._container_space,
        )

        logging.info("Building scoped query model views ...")
        lst_entity_views, lst_entity_properties, [] = build_neat_model_from_entities(
            containers_space=self._container_space,
            entities=scoped_model,
            dms_identifire=self.dms_identifire,
            include_containers=False,
        )

        logging.info(f"Building {len(scoped_model)} scoped entity views")
        # Validate required keys in views_scope
        required_keys = ["scope_name", "scope_model_external_id", "scope_model_version"]
        missing_keys = [key for key in required_keys if key not in views_scope or views_scope[key] is None]
        if missing_keys:
            raise NeatValueError(
                f"Scope '{scope}' is missing required keys: {', '.join(missing_keys)}"
            )

        return ReadResult(
            Properties=lst_entity_properties,
            Containers=[],
            Views=lst_entity_views,
            Metadata={
                "role": "DMS Architect",
                "dataModelType": "enterprise",
                "schema": "complete",
                "space": self._views_space,
                "name": "CFIHOS_"
                + views_scope["scope_name"].replace(" ", "_").replace("-", "_").upper(),
                "description": views_scope.get("scope_description", "")
                if views_scope.get("scope_description")
                else "",
                "external_id": "CFIHOS_"
                + views_scope["scope_model_external_id"].replace(" ", "_").replace("-", "_").upper(),
                "version": views_scope["scope_model_version"],
                "creator": self._model_creator,
            },
        )        

    def read_model(self) -> None | ReadResult:
        """Read and process the model according to the configured model_type.

        Raises errors if required parameters are missing or invalid.

        Returns:
            ReadResult: ReadResult for 'containers' or 'views' model types, or None otherwise.
        """
        if self.model_type == "":
            raise NeatValueError("model_type is a required parameter")
        if self.model_type == SparseModelType.VIEWS and self.scope == "":
            raise NeatValueError(
                "scope is a required parameter when model_type is 'views'"
            )
        if self.model_type not in ["containers", "views"]:
            raise NeatValueError(f"Invalid model_type: {self.model_type}")
        return (
            self._build_containers_model()
            if self.model_type == SparseModelType.CONTAINERS
            else self._build_scoped_views_models(self.scope)
            if self.model_type == SparseModelType.VIEWS
            else None
        )
