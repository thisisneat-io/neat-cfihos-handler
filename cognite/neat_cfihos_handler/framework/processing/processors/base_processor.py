"""Base Processor module.

This module defines the BaseProcessor class, which serves as an abstract base for
processing and handling model data, including entities, properties, and their mappings.
It provides foundational attributes and structures for derived processor classes
to implement specific data model processing logic.
"""
from dataclasses import dataclass, field

import pandas as pd
from cognite.neat.core._issues import IssueList

from cognite.neat_cfihos_handler.framework.common.generic_classes import DataSource
from cognite.neat_cfihos_handler.framework.common.log import log_init

logging = log_init(f"{__name__}", "i")


@dataclass
class BaseProcessor:
    """Processor class for handling model processing.

    Attributes:
        model_processors_config (List[dict]): Configuration for model processors.
    """

    # List of available processor
    model_processors_config: list[dict]

    # Dataframes containing all entities and properties of a given project model
    _df_entities: pd.DataFrame = field(default_factory=pd.DataFrame, init=False)
    _df_entity_properties: pd.DataFrame = field(
        default_factory=pd.DataFrame, init=False
    )  # will hold all the FCC properties in the FCC containers and single property per attribute in wide containers
    _df_properties_metadata: pd.DataFrame = field(
        default_factory=pd.DataFrame, init=False
    )

    # Model entities
    _model_entities: dict = field(default_factory=dict, init=False)
    _model_properties: dict = field(default_factory=dict, init=False)
    _model_property_groups: dict = field(default_factory=dict, init=False)

    _property_groupings: list[str] = field(default_factory=list, init=False)

    # Source of input data (CSVs, Github, CDF)
    source: DataSource = field(default=DataSource.default(), init=False)

    _map_entity_name_to_dms_name: dict = field(default_factory=dict, init=False)
    _map_entity_id_to_dms_id: dict = field(default_factory=dict, init=False)
    _map_entity_name_to_entity_id: dict = field(default_factory=dict, init=False)
    _map_dms_id_to_entity_id: dict = field(default_factory=dict, init=False)
    _map_entity_id_to_dms_name: dict = field(default_factory=dict, init=False)
    _issue_list: IssueList = field(default_factory=IssueList, init=False)

    @property
    def model_entities(self) -> dict:
        """Model entities."""
        return self._model_entities

    @property
    def model_properties(self) -> dict:
        """Model properties."""
        return self._model_properties

    @property
    def model_property_groups(self) -> dict:
        """Model property groups."""
        return self._model_property_groups

    @property
    def map_entity_id_to_dms_id(self) -> dict:
        """Mapping of entity IDs to DMS IDs."""
        return self._map_entity_id_to_dms_id

    @property
    def map_dms_id_to_entity_id(self) -> dict:
        """Mapping of DMS IDs to entity IDs."""
        return self._map_dms_id_to_entity_id

    @property
    def issue_list(self) -> IssueList:
        """List of issues encountered during processing."""
        return self._issue_list

    def process_and_collect_models(self):
        """Process and collect data models from multiple model processors.

        This function aggregates entities, properties,
        and properties metadata from different processors, validates them for uniqueness, and prepares them for further
        processing.

        This method performs the following steps:
        - Processes data from each model processor and collects entities, properties, and properties metadata.
        - Ensures the uniqueness of entity IDs and property validation IDs.
        - Filters and combines metadata with entity properties.
        - Adds string properties for remaining `_rel` properties.
        - Prepares the final data model by creating properties, entities, and extending first-class citizen properties.

        Returns:
            None
        """
        logging.info("Starting model processing...")

        # Step 1: Setup model processors (to be implemented by subclasses)
        logging.info("Step 1: Setting up model processors...")
        self._setup_model_processors()

        # Step 2: Collect data from all processors
        logging.info("Step 2: Collecting data from processors...")
        self._collect_processor_data()

        # Step 3: Validate collected data
        logging.info("Step 3: Validating collected data...")
        self._validate_collected_data()

        # Step 4: Process and transform data
        logging.info("Step 4: Processing and transforming data...")
        self._process_collected_data()

        # Step 5: Build final model structures
        logging.info("Step 5: Building final model structures...")
        self._build_model_structures()

        logging.info("Model processing completed successfully")

    def _setup_model_processors(self):
        """Set up model processors based on configuration.

        This method should be implemented by subclasses to initialize
        specific processor types based on the model_processors_config.
        """
        raise NotImplementedError(
            "Subclasses must implement _setup_model_processors() to initialize "
            "specific processor types based on configuration."
        )

    def _collect_processor_data(self):
        """Collect data from all configured processors.

        This method should be implemented by subclasses to process data
        from each configured processor and populate the dataframes.
        """
        raise NotImplementedError(
            "Subclasses must implement _collect_processor_data() to process "
            "data from configured processors."
        )

    def _validate_collected_data(self):
        """Validate the collected data for consistency and uniqueness.

        This method should be implemented by subclasses to perform
        validation specific to the processor type.
        """
        raise NotImplementedError(
            "Subclasses must implement _validate_collected_data() to perform "
            "validation specific to the processor type."
        )

    def _process_collected_data(self):
        """Process and transform the collected data.

        This method should be implemented by subclasses to perform
        data transformation and enrichment.
        """
        raise NotImplementedError(
            "Subclasses must implement _process_collected_data() to perform "
            "data transformation and enrichment."
        )

    def _build_model_structures(self):
        """Build final model structures from processed data.

        This method should be implemented by subclasses to create
        the final model entities, properties, and property groups.
        """
        raise NotImplementedError(
            "Subclasses must implement _build_model_structures() to create "
            "final model structures."
        )

    def _sync_processor_mapping_tables(self):
        """Synchronize mapping tables across processors.

        This method should be implemented by subclasses to ensure
        consistent mapping between different processors.
        """
        raise NotImplementedError(
            "Subclasses must implement _sync_processor_mapping_tables() to "
            "synchronize mapping tables across processors."
        )
