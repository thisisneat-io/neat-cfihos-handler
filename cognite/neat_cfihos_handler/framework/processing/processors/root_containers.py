"""Root Containers Processor module.

This module defines the SparsePropertiesProcessor class, which is responsible for
processing and collecting sparse property and entity models for CFIHOS data models.

Overview:
---------
The processor denormalizes non-first-class-citizen entities and classes into groups,
where each group contains a range of CFIHOS identifiers (the default group size is 100).
First-class citizen entities are assigned to groups in a one-to-one correspondence with
their original structure, preserving their distinct identity.

Purpose and Efficiency:
-----------------------
The sparse structure produced by this processor enables efficient handling of CFIHOS
models by significantly reducing the number of CDF containers required. Instead of
creating thousands of containers to mirror the original CFIHOS structure, non-first-class
entities are grouped, resulting in a much more manageable and scalable container model
for CDF.

"""
import re
from dataclasses import dataclass, field

import pandas as pd
from cognite.neat.core._issues.errors import NeatValueError

from cognite.neat_cfihos_handler.framework.common.constants import (
    CONTAINER_PROPERTY_LIMIT,
)
from cognite.neat_cfihos_handler.framework.common.generic_classes import (
    EntityStructure,
    PropertyStructure,
    Relations,
    SparseModelType,
)
from cognite.neat_cfihos_handler.framework.common.log import log_init
from cognite.neat_cfihos_handler.framework.importer.cfihos_loader import (
    CfihosModelLoader,
)
from cognite.neat_cfihos_handler.framework.processing.processors.base_processor import (
    BaseProcessor,
)

logging = log_init(f"{__name__}", "i")


@dataclass
class CfihosClassNode:
    """Class node for the CFIHOS class tree."""

    NodeId: str
    ParentNodeId: str


@dataclass
class RootContainersProcessor(BaseProcessor):
    """Processor class for handling model processing.

    Attributes:
        model_processors_config (List[dict]): Configuration for model processors.
        model_type (str): The type of model to process.
    """

    model_type: str = field(default=SparseModelType.CONTAINERS)
    add_scalar_properties_for_direct_relations: bool = field(init=True, default=False)
    model_processors: list[CfihosModelLoader] = field(default_factory=list, init=False)
    tag_and_equipment_classes_to_root_nodes: dict[str, str] = field(
        default_factory=dict, init=False
    )

    def __post_init__(self):
        """Initialize the processor but don't run setup methods yet.

        The setup methods will be called by process_and_collect_models()
        when the template method is executed.
        """
        # Don't call setup methods here - they'll be called by the template method
        pass

    def _setup_model_processors(self):
        """Set up CFIHOS model processors based on configuration."""
        for model_processor in self.model_processors_config:
            for model_processor_type, processor_data in model_processor.items():
                processor_data["processor_config_name"] = model_processor_type
                self._loggingInfo(f"Setting up {model_processor_type}")
                self.model_processors.append(CfihosModelLoader(**processor_data))
                self._setup_property_groups(processor_data["id_prefix"])

        # Synchronize mapping tables after all processors are set up
        self._sync_processor_mapping_tables()

    def _setup_property_groups(self, processor_id_prefix: str):
        self._property_groupings.extend(
            [f"{processor_id_prefix}_{idx}" for idx in range(0, 10)]
        )

    def _sync_processor_mapping_tables(self):
        """Synchronize mapping tables across the processor to ensure global mapping between models.

        Updated mapping tables are:
         - map_entity_id_to_dms_id
         - map_dms_id_to_entity_id
         - map_entity_name_to_entity_id.
        """
        if len(self.model_processors) == 0:
            self._loggingWarning("Processor received no models")

        # First, collect all mapping tables from all processors
        for processor in self.model_processors:
            self._map_entity_id_to_dms_id.update(processor._map_entity_id_to_dms_id)
            self._map_dms_id_to_entity_id.update(processor._map_dms_id_to_entity_id)
            self._map_entity_name_to_entity_id.update(
                processor._map_entity_name_to_entity_id
            )

        # Then, update all processors with the combined mapping tables
        for processor in self.model_processors:
            processor._map_entity_id_to_dms_id = self._map_entity_id_to_dms_id.copy()
            processor._map_dms_id_to_entity_id = self._map_dms_id_to_entity_id.copy()
            processor._map_entity_name_to_entity_id = (
                self._map_entity_name_to_entity_id.copy()
            )

    def _collect_processor_data(self):
        """Collect data from all CFIHOS processors."""
        list_of_entities = []
        list_of_properties = []
        list_of_properties_metadata = []

        for processor in self.model_processors:
            (
                df_processor_entities,
                df_processor_properties,
                df_processor_properties_metadata,
            ) = processor.process()

            # TODO: Validate dfs according to req. columns
            # Add processor name to id to check for uniqueness
            df_processor_properties_metadata["unique_val_id"] = (
                df_processor_properties_metadata[PropertyStructure.ID]
                + f"_metadata_{processor.processor_config_name}"
            )
            list_of_entities.append(df_processor_entities)
            list_of_properties.append(df_processor_properties)
            list_of_properties_metadata.append(df_processor_properties_metadata)

        self._df_entities = pd.concat(list_of_entities)
        self._df_entity_properties = pd.concat(list_of_properties)
        self._df_properties_metadata = pd.concat(list_of_properties_metadata)

        self._df_entity_properties.loc[
            self._df_entity_properties[EntityStructure.ID].isin(
                self._df_entities.loc[
                    self._df_entities[EntityStructure.FIRSTCLASSCITIZEN],
                    EntityStructure.ID,
                ]
            ),
            EntityStructure.FIRSTCLASSCITIZEN,
        ] = True

    def _validate_collected_data(self):
        """Validate the collected CFIHOS data for consistency and uniqueness."""
        if not self._df_entities[EntityStructure.ID].is_unique:
            duplicated_entities = self._df_entities[
                self._df_entities.duplicated([EntityStructure.ID], keep=False)
            ][EntityStructure.ID].values
            NeatValueError(
                f"Processed Entities has overlapping ids. Duplicated entity ids {duplicated_entities}"
            )

        # check for duplicate entity by DMS ID
        if len(self._df_entities[EntityStructure.DMS_NAME].unique()) != len(
            self._df_entities
        ):
            duplicates = self._df_entities[
                self._df_entities.duplicated([EntityStructure.DMS_NAME], keep=False)
            ]
            duplicate_info = [
                f"EntityId: {row[EntityStructure.ID]}, DMS Name: {row.get(EntityStructure.DMS_NAME, 'Unknown')}, "
                for _, row in duplicates.iterrows()
            ]
            raise NeatValueError(
                "Processed Entities has overlapping DMS Names. Duplicated entities:\n"
                + "\n".join(duplicate_info)
            )
        if not self._df_entity_properties[
            PropertyStructure.UNIQUE_VALIDATION_ID
        ].is_unique:
            duplicated_entities_props = self._df_entity_properties[
                self._df_entity_properties.duplicated(
                    [PropertyStructure.UNIQUE_VALIDATION_ID], keep=False
                )
            ][PropertyStructure.UNIQUE_VALIDATION_ID].values
            raise NeatValueError(
                "Processed Properties has overlapping entity-property-ids. "
                f"Duplicated entity ids {duplicated_entities_props}"
            )

    def _process_collected_data(self):
        """Process and transform the collected CFIHOS data."""
        # Keep only properties metadata rows that do not exist in entity properties
        self._df_properties_metadata = self._df_properties_metadata[
            ~self._df_properties_metadata[PropertyStructure.ID].isin(
                self._df_entity_properties[PropertyStructure.ID]
            )
        ]

        # Add properties from metadata that are not already in the entity properties df
        self._df_entity_properties = pd.concat(
            [self._df_entity_properties, self._df_properties_metadata],
            ignore_index=True,
        )

        # Set FIRSTCLASSCITIZEN column to False where value is NaN
        self._df_entity_properties[PropertyStructure.FIRSTCLASSCITIZEN] = (
            self._df_entity_properties[PropertyStructure.FIRSTCLASSCITIZEN]
            .astype("boolean")
            .fillna(False)
        )

        # Step 0: create the denormalization mapping for the root containers
        self._create_denormalization_mapping()
        # Step 1: build the full inheritance of entities
        # Step 2: extend the direct relations properties with sclarified properties
        if self.add_scalar_properties_for_direct_relations:
            self._extend_additional_properties_for_direct_relations()
        # Step 3: extend the properties that have relevant UOM with string properties suffexed with _UOM
        self._extend_UOM_properties()

    def _build_model_structures(self):
        """Build final model structures from processed CFIHOS data."""
        # Step 4: create the model properties and store them in the global model_properties dict
        if self.model_type == SparseModelType.CONTAINERS:
            self._create_container_model_entities()
            # Step 5: Append the first-class citizen entities' properties to the model properties
            self._extend_container_model_first_class_citizens_entities()
        elif self.model_type == SparseModelType.VIEWS:
            # Step 4: create the model entities and store them in the global model_entities dict
            self._create_views_model_entities()
        else:
            raise NeatValueError(f"Invalid model type: {self.model_type}")

    def _dict_key_exists(list_of_dicts, key):
        return any(key in d for d in list_of_dicts)

    def _create_property_row(
        self,
        property_item: dict,
        property_group=None,
        is_uom_variant=False,
        is_relationship_variant=False,
        is_custom_property=False,
        is_first_class_citzen=False,
        is_edge_property=False,
        is_reverse_relation=False,
        target_type=None,
        is_required=False,
    ):
        """Unified method to handle property row creation with variations for UOM, relationship, and default properties.

        Args:
            property_item (dict): The property data (dictionary).
            property_group (str, optional): The property group for the property, if applicable.
            is_uom_variant (bool, optional): Flag to indicate if this is a UOM variant.
            is_relationship_variant (bool, optional): Flag to indicate if this is a relationship variant.
            is_custom_property (bool, optional): Flag to indicate if this is a custom property.
            is_first_class_citzen (bool, optional): Flag to indicate if this is a first class citizen.
            is_edge_property (bool, optional): Flag to indicate if this is an edge property.
            is_reverse_relation (bool, optional): Flag to indicate if this is a reverse relation.
            target_type (str, optional): The target type for the property.
            is_required (bool, optional): Flag to indicate if this property is required.

        Returns:
            dict: A dictionary representing the property row.
        """
        # Base property row structure
        property_row = {
            PropertyStructure.ID: property_item[PropertyStructure.ID].replace("-", "_"),
            PropertyStructure.NAME: property_item.get(PropertyStructure.NAME, None),
            PropertyStructure.DMS_NAME: property_item.get(
                PropertyStructure.DMS_NAME, None
            ),
            PropertyStructure.DESCRIPTION: property_item.get(
                PropertyStructure.DESCRIPTION, None
            ),
            PropertyStructure.PROPERTY_TYPE: property_item.get(
                PropertyStructure.PROPERTY_TYPE, None
            ),
            PropertyStructure.TARGET_TYPE: (
                property_item[PropertyStructure.TARGET_TYPE]
                if PropertyStructure.TARGET_TYPE in property_item
                and property_item[PropertyStructure.TARGET_TYPE] is not None
                else target_type
            ),
            PropertyStructure.MULTI_VALUED: property_item.get(
                PropertyStructure.MULTI_VALUED, None
            ),
            PropertyStructure.IS_REQUIRED: is_required,
            PropertyStructure.IS_UNIQUE: False,
            PropertyStructure.UOM: property_item.get(PropertyStructure.UOM, None),
            PropertyStructure.ENUMERATION_TABLE: property_item.get(
                PropertyStructure.ENUMERATION_TABLE, None
            ),
            PropertyStructure.INHERITED: False,
            PropertyStructure.PROPERTY_GROUP: property_group,
            PropertyStructure.CUSTOM_PROPERTY: is_custom_property,
            PropertyStructure.FIRSTCLASSCITIZEN: is_first_class_citzen,  # label the first class citizen
            EntityStructure.ID: property_item.get(EntityStructure.ID, None),
            PropertyStructure.UNIQUE_VALIDATION_ID: (
                property_item[PropertyStructure.UNIQUE_VALIDATION_ID].replace("-", "_")
                if PropertyStructure.UNIQUE_VALIDATION_ID in property_item
                else None
            ),
            "cfihosId": property_item[PropertyStructure.ID],
        }

        # Adjustments for UOM variant
        if is_uom_variant:
            property_row.update(
                {
                    PropertyStructure.ID: f"{property_item[PropertyStructure.ID].replace('-', '_')}_UOM",
                    PropertyStructure.NAME: f"{property_item[PropertyStructure.NAME]}_UOM",
                    PropertyStructure.DMS_NAME: f"{property_item[PropertyStructure.DMS_NAME]}_UOM",
                    PropertyStructure.DESCRIPTION: f"{property_item[PropertyStructure.DESCRIPTION]} unit of measure",
                    PropertyStructure.PROPERTY_TYPE: "BASIC_DATA_TYPE",
                    PropertyStructure.TARGET_TYPE: "String",
                    "cfihosId": f"{property_item[PropertyStructure.ID]}_UOM",
                }
            )

        # Adjustments for relationship variant
        if is_relationship_variant:
            property_row.update(
                {
                    PropertyStructure.ID: property_item[PropertyStructure.ID].replace(
                        "_rel", ""
                    ),
                    PropertyStructure.UNIQUE_VALIDATION_ID: property_item[
                        PropertyStructure.UNIQUE_VALIDATION_ID
                    ].replace("_rel", ""),
                    PropertyStructure.PROPERTY_TYPE: "BASIC_DATA_TYPE",
                    PropertyStructure.TARGET_TYPE: target_type,
                    "cfihosId": property_item[PropertyStructure.ID].replace("_rel", ""),
                }
            )

        # Adjustments for reverse relation variant
        if is_reverse_relation:
            property_row.update(
                {
                    PropertyStructure.REV_THROUGH_PROPERTY: property_item[
                        PropertyStructure.REV_THROUGH_PROPERTY
                    ].replace("-", "_"),
                    PropertyStructure.REV_PROPERTY_NAME: property_item[
                        PropertyStructure.REV_PROPERTY_NAME
                    ],
                    PropertyStructure.REV_PROPERTY_DMS_NAME: property_item[
                        PropertyStructure.REV_PROPERTY_DMS_NAME
                    ],
                    PropertyStructure.REV_PROPERTY_DESCRIPTION: property_item[
                        PropertyStructure.REV_PROPERTY_DESCRIPTION
                    ],
                }
            )

        # Added for edge support
        if is_edge_property:
            property_row.update(
                {
                    PropertyStructure.EDGE_EXTERNAL_ID: property_item[
                        PropertyStructure.EDGE_EXTERNAL_ID
                    ].replace("-", "_"),
                    PropertyStructure.EDGE_SOURCE: property_item[
                        PropertyStructure.EDGE_SOURCE
                    ].replace("-", "_"),
                    PropertyStructure.EDGE_TARGET: property_item[
                        PropertyStructure.EDGE_TARGET
                    ].replace("-", "_"),
                    PropertyStructure.EDGE_SOURCE_DMS_NAME: property_item[
                        PropertyStructure.EDGE_SOURCE_DMS_NAME
                    ],
                    PropertyStructure.EDGE_TARGET_DMS_NAME: property_item[
                        PropertyStructure.EDGE_TARGET_DMS_NAME
                    ],
                    PropertyStructure.EDGE_DIRECTION: property_item[
                        PropertyStructure.EDGE_DIRECTION
                    ],
                }
            )

        return property_row

    def _extend_additional_properties_for_direct_relations(self):
        # Add string property for remaining _rel properties
        self._df_entity_properties = pd.concat(
            [
                self._df_entity_properties,
                (
                    self._df_entity_properties.loc[
                        lambda d: d[PropertyStructure.PROPERTY_TYPE]
                        == "ENTITY_RELATION"
                    ]
                    .assign(
                        **{
                            PropertyStructure.ID: lambda x: x[PropertyStructure.ID]
                            .astype(str)
                            .fillna("")
                            .str.replace("_rel", "", regex=False),
                            PropertyStructure.DMS_NAME: lambda x: x[
                                PropertyStructure.DMS_NAME
                            ]
                            .astype(str)
                            .fillna("")
                            .str.replace("_rel", "", regex=False),
                            PropertyStructure.PROPERTY_TYPE: "BASIC_DATA_TYPE",
                            PropertyStructure.TARGET_TYPE: lambda x: x[
                                PropertyStructure.ORIGINAL_TARGET_TYPE
                            ],
                            PropertyStructure.UNIQUE_VALIDATION_ID: lambda x: x[
                                PropertyStructure.UNIQUE_VALIDATION_ID
                            ]
                            .astype(str)
                            .fillna("")
                            .str.replace("_rel", "", regex=False),
                        }
                    )
                    .loc[
                        lambda d: ~d[PropertyStructure.UNIQUE_VALIDATION_ID].isin(
                            self._df_entity_properties[
                                PropertyStructure.UNIQUE_VALIDATION_ID
                            ]
                        )
                    ]
                ),
            ],
            ignore_index=True,
        )

    def _extend_UOM_properties(self):
        # Add string UOM properties
        self._df_entity_properties = pd.concat(
            [
                self._df_entity_properties,
                (
                    self._df_entity_properties.loc[
                        lambda d: (d[PropertyStructure.UOM].notna())
                        & (d[PropertyStructure.UOM] != "")
                    ]
                    .assign(
                        **{
                            PropertyStructure.ID: lambda x: x[PropertyStructure.ID]
                            + "_UOM",
                            PropertyStructure.DMS_NAME: lambda x: x[
                                PropertyStructure.DMS_NAME
                            ]
                            + "_UOM",
                            PropertyStructure.PROPERTY_TYPE: "BASIC_DATA_TYPE",
                            PropertyStructure.TARGET_TYPE: "String",
                            PropertyStructure.UNIQUE_VALIDATION_ID: lambda x: x[
                                PropertyStructure.UNIQUE_VALIDATION_ID
                            ]
                            + "_UOM",
                        }
                    )
                    .loc[
                        lambda d: ~d[PropertyStructure.UNIQUE_VALIDATION_ID].isin(
                            self._df_entity_properties[
                                PropertyStructure.UNIQUE_VALIDATION_ID
                            ]
                        )
                    ]
                ),
            ],
            ignore_index=True,
        )

    def _create_container_model_entities2(self):
        """Create and validate model properties from the collected entity properties.

        This function processes the entity
        properties DataFrame, checks for consistency, and constructs a DataFrame of unique model properties.

        This method performs the following steps:
        - Extracts unique properties from the entity properties DataFrame.
        - Validates that each property has consistent attribute values across non-first-class citizen entries.
        - Constructs property rows and appends them to a list of properties.
        - Converts the list of properties into a DataFrame and updates the model properties dictionary.

        Returns:
            None
        """
        properties = []
        entities: dict[str, list[dict[str, str]]] = {}
        # unique_properties = self._df_entity_properties[PropertyStructure.ID].unique()
        tag_equipment_class_entity_properties = self._df_entity_properties.loc[
            (~self._df_entity_properties[PropertyStructure.FIRSTCLASSCITIZEN])
            & (
                ~self._df_entity_properties[PropertyStructure.PROPERTY_TYPE].isin(
                    [Relations.EDGE, Relations.REVERSE]
                )
            )
            & (
                self._df_entity_properties[EntityStructure.ID].str.startswith(
                    ("T", "E")
                )
            )
        ]
        lst_group_properties = []
        for _, entity_property in tag_equipment_class_entity_properties.iterrows():
            entity_id = self._assign_root_nodes_to_tag_and_equipment_classes(
                entity_property[EntityStructure.ID]
            )
            lst_group_properties.append(
                {entity_property[EntityStructure.ID]: entity_id}
            )

        print("xx")

    def _create_container_model_entities(self):
        """Create and validate model properties from the collected entity properties.

        This function processes the entity
        properties DataFrame, checks for consistency, and constructs a DataFrame of unique model properties.

        This method performs the following steps:
        - Extracts unique properties from the entity properties DataFrame.
        - Validates that each property has consistent attribute values across non-first-class citizen entries.
        - Constructs property rows and appends them to a list of properties.
        - Converts the list of properties into a DataFrame and updates the model properties dictionary.

        Returns:
            None
        """
        entities: dict[str, list[dict[str, str]]] = {}
        # Track property IDs per property group for efficient duplicate checking
        property_ids_per_group: dict[str, set[str]] = {}
        # unique_properties = self._df_entity_properties[PropertyStructure.ID].unique()
        df_properties = self._df_entity_properties.loc[
            (~self._df_entity_properties[PropertyStructure.FIRSTCLASSCITIZEN])
            & (
                ~self._df_entity_properties[PropertyStructure.PROPERTY_TYPE].isin(
                    [Relations.EDGE, Relations.REVERSE]
                )
            )
            & (self._df_entity_properties[EntityStructure.ID].notna())
            # & (
            #     # self._df_entity_properties[EntityStructure.ID].str.startswith(
            #     #     ("T", "E")
            #     # )
            # )
        ]
        # Check that all target types are present
        for _, prop in df_properties.iterrows():
            property_group_id = (
                self._assign_root_nodes_to_tag_and_equipment_classes(
                    prop[EntityStructure.ID], prop[PropertyStructure.ID]
                )
                if prop[EntityStructure.ID].startswith(("T", "E"))
                else self._assign_property_group(
                    prop[PropertyStructure.ID], CONTAINER_PROPERTY_LIMIT
                )
            )
            if property_group_id is None:
                print(
                    f"Property group ID is None for property {prop[PropertyStructure.ID]}"
                )
                continue
            entity_property_row = self._create_property_row(
                {
                    PropertyStructure.ID: prop[PropertyStructure.ID],
                    PropertyStructure.NAME: prop[PropertyStructure.NAME],
                    PropertyStructure.DMS_NAME: prop[PropertyStructure.DMS_NAME],
                    PropertyStructure.DESCRIPTION: prop[PropertyStructure.DESCRIPTION],
                    PropertyStructure.PROPERTY_TYPE: prop[
                        PropertyStructure.PROPERTY_TYPE
                    ],
                    PropertyStructure.TARGET_TYPE: prop[PropertyStructure.TARGET_TYPE],
                    PropertyStructure.IS_REQUIRED: prop[PropertyStructure.IS_REQUIRED],
                },
                property_group=property_group_id,
            )
            if property_group_id not in entities:
                entity = self._df_entities.loc[
                    self._df_entities[EntityStructure.ID] == prop[EntityStructure.ID]
                ].iloc[0]
                entities[property_group_id] = {
                    EntityStructure.ID: property_group_id,
                    EntityStructure.NAME: entity[EntityStructure.NAME],
                    EntityStructure.DMS_NAME: entity[EntityStructure.DMS_NAME],
                    EntityStructure.DESCRIPTION: entity[EntityStructure.DESCRIPTION],
                    EntityStructure.INHERITS_FROM_ID: None,
                    EntityStructure.INHERITS_FROM_NAME: None,
                    EntityStructure.FULL_INHERITANCE: None,
                    EntityStructure.PROPERTIES: [],
                    EntityStructure.FIRSTCLASSCITIZEN: False,
                    EntityStructure.IMPLEMENTS_CORE_MODEL: None,
                    EntityStructure.VIEW_FILTER: None,
                }
                # Initialize the set for tracking property IDs in this group
                property_ids_per_group[property_group_id] = set()
                entity_type_property = self._create_property_row(
                    {
                        PropertyStructure.ID: "entityType",
                        PropertyStructure.NAME: "entityType",
                        PropertyStructure.DMS_NAME: "entityType",
                        PropertyStructure.DESCRIPTION: "entityType",
                        PropertyStructure.PROPERTY_TYPE: "BASIC_DATA_TYPE",
                        PropertyStructure.TARGET_TYPE: "String",
                        PropertyStructure.IS_REQUIRED: True,
                    },
                    property_group="EntityTypeGroup",
                )
                entities[property_group_id]["properties"].append(entity_type_property)
                # Track the entityType property ID
                property_ids_per_group[property_group_id].add(
                    entity_type_property[PropertyStructure.ID]
                )
            # Check if property with the same ID already exists in this property group
            # Use the transformed ID from entity_property_row (dashes replaced with underscores)
            property_id = entity_property_row[PropertyStructure.ID]
            if property_id not in property_ids_per_group[property_group_id]:
                entities[property_group_id]["properties"].append(entity_property_row)
                # Track the property ID to prevent duplicates
                property_ids_per_group[property_group_id].add(property_id)

        entities["EntityTypeGroup"] = {
            EntityStructure.ID: "EntityTypeGroup",
            EntityStructure.NAME: "EntityTypeGroup",
            EntityStructure.DMS_NAME: "EntityTypeGroup",
            EntityStructure.DESCRIPTION: "Container that holds CFIHOS IDs to be used in filtering instances in wide containers",
            EntityStructure.INHERITS_FROM_ID: None,
            EntityStructure.INHERITS_FROM_NAME: None,
            EntityStructure.FULL_INHERITANCE: None,
            "cfihosType": "EntityTypeGroup",
            "cfihosId": "EntityTypeGroup",
            EntityStructure.PROPERTIES: [
                self._create_property_row(
                    {
                        PropertyStructure.ID: "entityType",
                        PropertyStructure.NAME: "entityType",
                        PropertyStructure.DMS_NAME: "entityType",
                        PropertyStructure.DESCRIPTION: "entityType",
                        PropertyStructure.PROPERTY_TYPE: "BASIC_DATA_TYPE",
                    },
                    property_group="EntityTypeGroup",
                    is_first_class_citzen=True,
                    is_edge_property=False,
                    is_reverse_relation=False,
                    target_type="String",
                    is_required=True,
                )
            ],
            EntityStructure.FIRSTCLASSCITIZEN: True,  # set to True to avoid denormalizing the EntityTypeGroup in wide containers
            EntityStructure.IMPLEMENTS_CORE_MODEL: None,
            EntityStructure.VIEW_FILTER: None,
        }
        self._model_entities.update(entities)

    def _extend_container_model_first_class_citizens_entities(self):
        """Extend the model properties with first-class citizen properties.

        This function processes the first-class citizen properties from the entity properties DataFrame,
        validates them for consistency, and appends them to the model properties.

        This method performs the following steps:
        - Identifies first-class citizen properties from the entity properties DataFrame.
        - Validates that each property has consistent attribute values across entries.
        - Constructs property rows and appends them to a list of properties.
        - Converts the list of properties into a DataFrame and updates the model properties dictionary.

        Returns:
            None
        """
        fcc_properties = self._df_entity_properties.loc[
            self._df_entity_properties[PropertyStructure.FIRSTCLASSCITIZEN]
        ]
        entities: dict[str, list[dict[str, str]]] = {}
        properties = []
        if fcc_properties.empty:
            logging.warning(
                "No first-class citizen properties found. Skipping extension."
            )
            return
        # Check that all target types are present
        for _, prop in fcc_properties.iterrows():
            df_property_subset = self._df_entity_properties.loc[
                (
                    self._df_entity_properties[PropertyStructure.UNIQUE_VALIDATION_ID]
                    == prop[PropertyStructure.UNIQUE_VALIDATION_ID]
                )
            ]
            df_property_subset_groups = df_property_subset.groupby(
                PropertyStructure.PROPERTY_TYPE
            )  # Note: If other than basic or entity appears, this breaks
            for idx, df_subset in df_property_subset_groups:
                if len(df_subset) > 0:
                    columns_to_check = {
                        PropertyStructure.NAME: df_subset[
                            PropertyStructure.NAME
                        ].unique(),
                        PropertyStructure.TARGET_TYPE: (
                            df_subset.loc[
                                df_subset[PropertyStructure.FIRSTCLASSCITIZEN]
                            ][PropertyStructure.TARGET_TYPE].unique()
                        ),
                        PropertyStructure.PROPERTY_TYPE: df_subset[
                            PropertyStructure.PROPERTY_TYPE
                        ].unique(),
                        PropertyStructure.MULTI_VALUED: df_subset[
                            PropertyStructure.MULTI_VALUED
                        ].unique(),
                        PropertyStructure.DMS_NAME: df_subset[
                            PropertyStructure.DMS_NAME
                        ].unique(),
                        PropertyStructure.UNIQUE_VALIDATION_ID: df_subset[
                            PropertyStructure.UNIQUE_VALIDATION_ID
                        ].unique(),
                        # Add validation for checking the reverse direct relations
                        # Break with error if the reverse relation is not found
                    }
                    for col_name, data in columns_to_check.items():
                        if len(data) != 1:
                            raise NeatValueError(
                                f"Found properties '{col_name}' with lacking or multiple values: {data}"
                            )

                if columns_to_check:
                    # for key, value in columns_to_check.items():
                    #     if key not in [
                    #         PropertyStructure.FIRSTCLASSCITIZEN,
                    #         PropertyStructure.UNIQUE_VALIDATION_ID,
                    #     ]:
                    #         prop[key] = value[0]

                    property_group_id = prop[EntityStructure.ID].replace("-", "_")
                    if property_group_id not in entities:
                        # get the first class citizen entity
                        fcc_entity = self._df_entities.loc[
                            self._df_entities[EntityStructure.ID]
                            == prop[EntityStructure.ID]
                        ].iloc[0]
                        entities[property_group_id] = {
                            EntityStructure.ID: property_group_id,
                            EntityStructure.NAME: fcc_entity[EntityStructure.NAME],
                            EntityStructure.DMS_NAME: fcc_entity[
                                EntityStructure.DMS_NAME
                            ],
                            EntityStructure.DESCRIPTION: fcc_entity[
                                EntityStructure.DESCRIPTION
                            ],
                            EntityStructure.INHERITS_FROM_ID: None,
                            EntityStructure.INHERITS_FROM_NAME: None,
                            EntityStructure.FULL_INHERITANCE: None,
                            EntityStructure.PROPERTIES: [],
                            EntityStructure.FIRSTCLASSCITIZEN: True,
                            EntityStructure.IMPLEMENTS_CORE_MODEL: (
                                fcc_entity[EntityStructure.IMPLEMENTS_CORE_MODEL]
                                if isinstance(
                                    fcc_entity[EntityStructure.IMPLEMENTS_CORE_MODEL],
                                    list,
                                )
                                else None
                            ),
                            EntityStructure.VIEW_FILTER: None,
                        }
                    if prop[PropertyStructure.PROPERTY_TYPE] in [
                        Relations.DIRECT,
                        Relations.EDGE,
                        Relations.REVERSE,
                    ]:
                        if not self._validate_relation_is_eligible(prop):
                            prop[PropertyStructure.TARGET_TYPE] = None

                    entity_property_row = self._create_property_row(
                        prop,
                        property_group=prop[EntityStructure.ID].replace("-", "_"),
                        is_first_class_citzen=True,
                        is_edge_property=prop[PropertyStructure.PROPERTY_TYPE]
                        == Relations.EDGE,
                        is_reverse_relation=prop[PropertyStructure.PROPERTY_TYPE]
                        == Relations.REVERSE,
                    )
                    entities[property_group_id]["properties"].append(
                        entity_property_row
                    )
                    properties.append(entity_property_row)

        self._model_entities.update(entities)

    def _create_views_model_entities(self):
        """Create model entities from the collected entity data.

        This function processes the entity DataFrame, validates them for
        uniqueness, maps entity IDs, and constructs a dictionary of entities with their properties.

        This method performs the following steps:
        - Iterates through the entities DataFrame and constructs a dictionary of entities.
        - Validates that each entity and its properties have unique IDs.
        - Maps entity IDs from _map_entity_id_to_dms_id and constructs property rows for each entity.
        - Adds custom extended search properties from the configuration file (if enabled).
        - Adds inherited properties to the entities.
        - Regroups and updates model properties after adding UOM properties.

        Returns:
            None
        """
        entities = {}

        # Build quick lookup of propertyIds per entity
        entity_props_lookup = (
            self._df_entity_properties.groupby(EntityStructure.ID)[PropertyStructure.ID]
            .apply(set)
            .to_dict()
        )

        # Process each entity row
        for _, row in self._df_entities.iterrows():
            unique_entity_id = self._map_entity_id_to_dms_id[row[EntityStructure.ID]]

            # Check for duplicates
            if unique_entity_id in entities:
                raise NeatValueError(
                    f"Found duplicate cfihos entity id: {unique_entity_id}"
                )

            entities[unique_entity_id] = {
                EntityStructure.ID: unique_entity_id,
                EntityStructure.NAME: row[EntityStructure.NAME],
                EntityStructure.DMS_NAME: row[EntityStructure.DMS_NAME],
                EntityStructure.DESCRIPTION: row[EntityStructure.DESCRIPTION],
                EntityStructure.INHERITS_FROM_ID: (
                    [
                        self._map_entity_id_to_dms_id[parent_id]
                        for parent_id in row[EntityStructure.INHERITS_FROM_ID]
                    ]
                    if row[EntityStructure.INHERITS_FROM_ID] is not None
                    else None
                ),
                EntityStructure.INHERITS_FROM_NAME: row[
                    EntityStructure.INHERITS_FROM_NAME
                ],
                EntityStructure.FULL_INHERITANCE: row[EntityStructure.FULL_INHERITANCE],
                "cfihosType": row["type"],
                "cfihosId": row[EntityStructure.ID],
                EntityStructure.PROPERTIES: [],
                EntityStructure.FIRSTCLASSCITIZEN: bool(
                    row[EntityStructure.FIRSTCLASSCITIZEN]
                ),
                EntityStructure.IMPLEMENTS_CORE_MODEL: (
                    row[EntityStructure.IMPLEMENTS_CORE_MODEL]
                    if isinstance(row[EntityStructure.IMPLEMENTS_CORE_MODEL], list)
                    else None
                ),
            }

            cur_entity_prop_ids = {}
            cur_fcc_entity_prop_ids = {}

            # Compute inherited properties (to be excluded)
            inherited_props = set().union(
                *[
                    entity_props_lookup.get(parent_id, set())
                    for parent_id in row[EntityStructure.FULL_INHERITANCE]
                ]
            )

            # Loop over own properties (excluding inherited ones)
            for _, prop_row in self._df_entity_properties[
                (
                    (
                        self._df_entity_properties[EntityStructure.ID]
                        == row[EntityStructure.ID]
                    )
                    & (self._df_entity_properties[PropertyStructure.IN_MODEL])
                )
            ].iterrows():
                if prop_row[PropertyStructure.ID] in inherited_props:
                    continue  # skip inherited property

                # Check for duplicates
                if (
                    not prop_row[PropertyStructure.FIRSTCLASSCITIZEN]
                    and prop_row[PropertyStructure.ID] in cur_entity_prop_ids
                ):
                    raise NeatValueError(
                        f"Found duplicate property id '{prop_row[PropertyStructure.ID]}' in {unique_entity_id}"
                    )
                if (
                    prop_row[PropertyStructure.FIRSTCLASSCITIZEN]
                    and prop_row[PropertyStructure.ID] in cur_fcc_entity_prop_ids
                ):
                    raise NeatValueError(
                        f"Found duplicate property id '{prop_row[PropertyStructure.ID]}' in FCC {unique_entity_id}"
                    )
                if prop_row[PropertyStructure.FIRSTCLASSCITIZEN]:
                    cur_fcc_entity_prop_ids[prop_row[PropertyStructure.ID]] = 1
                else:
                    cur_entity_prop_ids[prop_row[PropertyStructure.ID]] = 1

                # Skip relation if target type can't be mapped
                if prop_row[
                    PropertyStructure.PROPERTY_TYPE
                ] == "ENTITY_RELATION" and not self._map_dms_id_to_entity_id.get(
                    prop_row[PropertyStructure.TARGET_TYPE], False
                ):
                    logging.warning(
                        f"[WARNING] Could not map target property "
                        f"{prop_row[PropertyStructure.TARGET_TYPE]} for {row[EntityStructure.ID]}"
                    )
                    # TODO: add NEAT warning
                    continue

                property_group = (
                    prop_row[EntityStructure.ID].replace("-", "_")
                    if row[EntityStructure.FIRSTCLASSCITIZEN]
                    else self._assign_property_group(prop_row[PropertyStructure.ID])
                )

                target_type = self._map_entity_id_to_dms_name.get(
                    prop_row[PropertyStructure.TARGET_TYPE],
                    prop_row[PropertyStructure.TARGET_TYPE],
                )

                property_row = self._create_property_row(
                    prop_row,
                    property_group=property_group,
                    is_first_class_citzen=row[EntityStructure.FIRSTCLASSCITIZEN],
                    is_edge_property=prop_row[PropertyStructure.PROPERTY_TYPE]
                    == Relations.EDGE,
                    is_reverse_relation=prop_row[PropertyStructure.PROPERTY_TYPE]
                    == Relations.REVERSE,
                    target_type=target_type,
                )

                self._model_property_groups.setdefault(property_group, []).append(
                    property_row
                )
                entities[unique_entity_id][EntityStructure.PROPERTIES].append(
                    property_row
                )
            if not row[EntityStructure.FIRSTCLASSCITIZEN]:
                entities[unique_entity_id]["properties"].append(
                    self._create_property_row(
                        {
                            PropertyStructure.ID: "entityType",
                            PropertyStructure.NAME: "entityType",
                            PropertyStructure.DMS_NAME: "entityType",
                            PropertyStructure.DESCRIPTION: "entityType",
                            PropertyStructure.PROPERTY_TYPE: "BASIC_DATA_TYPE",
                            PropertyStructure.TARGET_TYPE: "String",
                            PropertyStructure.IS_REQUIRED: True,
                        },
                        property_group="EntityTypeGroup",
                    )
                )

        self._model_entities = entities

    # Custom function to extract numeric part of the string
    def _extract_property_numeric_part(self, property):
        # This regular expression matches the first group of one or more digits in the string
        matches = re.search(r"\d+", property)
        if matches:
            return int(matches.group())
        else:
            return 0  # Return 0 if no number is found

    def _get_property_group_prefix(self, propertyId: str) -> str:
        for group_prefix in self._property_groupings:
            if propertyId.startswith(group_prefix):
                return group_prefix
        return None

    def _assign_property_group(
        self, propertyId: str, container_property_limit: int = 100
    ) -> str:
        """Group non-FCC properties into groups of 100.

        Example: CFIHOS_1_10000001_10000101, CFIHOS_4_40000001_40000101, etc.
        """
        propertyId = propertyId.replace("-", "_")
        id_number = int(self._get_property_id_number(propertyId))
        property_group_prefix = self._get_property_group_prefix(propertyId)
        if property_group_prefix is None:
            return None
        if id_number % container_property_limit == 0:
            property_group_id = (
                f"{id_number - (id_number - 1) % container_property_limit}_"
                f"{id_number - (id_number - 1) % container_property_limit + container_property_limit}"
            )
        else:
            property_group_id = (
                f"{id_number - id_number % container_property_limit + 1}_"
                f"{id_number - id_number % container_property_limit + container_property_limit + 1}"
            )
        property_group_id = (
            f"{property_group_id}_ext"
            if (
                self.add_scalar_properties_for_direct_relations
                and propertyId.lower().endswith("_rel")
            )
            or (propertyId.lower().endswith("_uom"))
            else property_group_id
        )
        return f"{property_group_prefix}_{property_group_id}"

    def _build_entities_full_inheritance(self):
        """Update a 'full_inheritance' column to df_entities containing all ancestor entityIds."""
        entity_to_parents = self._df_entities.set_index(EntityStructure.ID)[
            EntityStructure.INHERITS_FROM_ID
        ].to_dict()
        memo = {}

        def get_ancestors(eid):
            if eid in memo:
                return memo[eid]

            parents = entity_to_parents.get(eid, [])
            if not parents or parents == [None]:
                memo[eid] = []
            else:
                ancestors = []
                for parent in parents:
                    if parent is not None:
                        ancestors.append(parent)
                        ancestors.extend(get_ancestors(parent))
                memo[eid] = ancestors
            return memo[eid]

        self._df_entities[EntityStructure.FULL_INHERITANCE] = self._df_entities[
            EntityStructure.ID
        ].apply(get_ancestors)

    def _validate_relation_is_eligible(self, enitity_property: dict) -> bool:
        if enitity_property[PropertyStructure.PROPERTY_TYPE] == Relations.DIRECT:
            if not self._df_entities.loc[
                (
                    self._df_entities[EntityStructure.ID]
                    == enitity_property[PropertyStructure.TARGET_TYPE].replace("_", "-")
                )
                & (self._df_entities[EntityStructure.FIRSTCLASSCITIZEN])
            ].empty:
                return True
        elif enitity_property[PropertyStructure.PROPERTY_TYPE] == Relations.REVERSE:
            if not self._df_entities.loc[
                (
                    self._df_entities[EntityStructure.ID]
                    == enitity_property[PropertyStructure.TARGET_TYPE].replace("_", "-")
                )
                & (self._df_entities[EntityStructure.FIRSTCLASSCITIZEN])
            ].empty:
                return True
            else:
                raise NeatValueError(
                    f"Reverse property {enitity_property[PropertyStructure.ID]} has a through property that is not a first class citizen"
                )
        elif enitity_property[PropertyStructure.PROPERTY_TYPE] == Relations.EDGE:
            # Check if source entity exists and is a first class citizen
            source_exists = not self._df_entities.loc[
                (
                    self._df_entities[EntityStructure.ID]
                    == enitity_property[PropertyStructure.EDGE_SOURCE]
                )
                & (self._df_entities[EntityStructure.FIRSTCLASSCITIZEN])
            ].empty

            # Check if target entity exists and is a first class citizen
            target_exists = not self._df_entities.loc[
                (
                    self._df_entities[EntityStructure.ID]
                    == enitity_property[PropertyStructure.EDGE_TARGET]
                )
                & (self._df_entities[EntityStructure.FIRSTCLASSCITIZEN])
            ].empty

            if source_exists and target_exists:
                return True
            else:
                raise NeatValueError(
                    f"Edge property {enitity_property[PropertyStructure.ID]} has a source or target type that is not a first class citizen"
                )

        return False

    def _get_property_id_number(self, property_id: str) -> str:
        property_id_number = re.findall(r"\d+", property_id)[0]
        return property_id_number

    def _loggingDebug(self, msg: str) -> None:
        logging.debug(f"[Model Processor] {msg}")

    def _loggingInfo(self, msg: str) -> None:
        logging.info(f"[Model Processor] {msg}")

    def _loggingWarning(self, msg: str) -> None:
        logging.warning(f"[Model Processor] {msg}")

    def _loggingError(self, msg: str) -> None:
        logging.error(f"[Model Processor] {msg}")

    def _loggingCritical(self, msg: str) -> None:
        logging.critical(f"[Model Processor] {msg}")

    def _create_denormalization_mapping(self) -> None:
        """Create a dictionary mapping entity IDs to their denormalized parent IDs.

        This function denormalizes all classes under the first children of TCFIHOS-30000311
        (for tags) or ECFIHOS-30000311 (for equipment), assigning their properties to their
        parent nodes. Classes in node_list are treated as root nodes, and their children
        are denormalized under them instead.

        The denormalization map uses CFIHOS codes without T/E prefix for both keys and values.
        """
        # Hard-coded root node list (CFIHOS codes without T/E prefix)
        node_list = [
            "CFIHOS-30000550",
            "CFIHOS-30000390",
            "CFIHOS-30000594",
            "CFIHOS-30000524",
            "CFIHOS-30000181",
        ]

        # Root nodes for each type
        TAG_ROOT = "TCFIHOS-30000311"
        EQUIPMENT_ROOT = "ECFIHOS-30000311"

        denormalization_map = {}

        # Build parent-child mapping
        entity_to_parents = {}
        entity_to_children = {}

        for _, row in self._df_entities.iterrows():
            entity_id = row[EntityStructure.ID]

            parents = row[EntityStructure.INHERITS_FROM_ID]
            if parents is not None and isinstance(parents, list):
                entity_to_parents[entity_id] = [p for p in parents if p is not None]
            else:
                entity_to_parents[entity_id] = []

            # Initialize children dict
            entity_to_children[entity_id] = []

        # Build children mapping
        for entity_id, parents in entity_to_parents.items():
            for parent in parents:
                if parent in entity_to_children:
                    entity_to_children[parent].append(entity_id)

        def normalize_cfihos_id(entity_id: str) -> str:
            """Remove T or E prefix from entity ID if present.

            Examples:
                TCFIHOS-30000397 -> CFIHOS-30000397
                ECFIHOS-30000397 -> CFIHOS-30000397
                TEPC-30000397 -> EPC-30000397
                EEPC-30000397 -> EPC-30000397
            """
            if entity_id and entity_id[0] in ("T", "E"):
                # Split by '-' to get code and number parts
                parts = entity_id.split("-", 1)
                if len(parts) == 2:
                    code = parts[0]
                    number = parts[1]
                    # Remove the first character (T or E) from the code
                    normalized_code = code[1:] if len(code) > 1 else code
                    return f"{normalized_code}-{number}"
            return entity_id

        def find_ancestor_in_root_list(entity_id: str) -> str:
            """Find the closest ancestor that is in the root node list.

            Returns the normalized CFIHOS ID (without T/E prefix).
            """
            visited = set()
            queue = [entity_id]

            while queue:
                current = queue.pop(0)
                if current in visited:
                    continue
                visited.add(current)

                # Normalize current and check if it's in root list
                current_normalized = normalize_cfihos_id(current)
                if current_normalized in node_list:
                    return current_normalized

                parents = entity_to_parents.get(current, [])
                for parent in parents:
                    queue.append(parent)

            return None

        def find_first_child_ancestor(entity_id: str, first_children: list) -> str:
            """Find which first child this entity descends from."""
            visited = set()
            queue = [entity_id]

            while queue:
                current = queue.pop(0)
                if current in visited:
                    continue
                visited.add(current)

                if current in first_children:
                    return current

                parents = entity_to_parents.get(current, [])
                for parent in parents:
                    queue.append(parent)

            return None

        # Create a mapping from normalized CFIHOS IDs to their T/E versions for lookup
        # This helps us check if an entity (in T/E format) is in the root list
        root_list_actual_ids = set()
        for entity_id in entity_to_parents:
            normalized = normalize_cfihos_id(entity_id)
            if normalized in node_list:
                root_list_actual_ids.add(entity_id)

        # Process both tag and equipment roots together
        root_nodes = [TAG_ROOT, EQUIPMENT_ROOT]
        all_first_children = []

        for root_node in root_nodes:
            if root_node in entity_to_children:
                all_first_children.extend(entity_to_children[root_node])

        # Get all entities that start with T or E (tag/equipment entities)
        # Get entities from the dataframe to ensure we process all entities, not just those in mappings
        entities_to_process = []
        for _, row in self._df_entities.iterrows():
            entity_id = row[EntityStructure.ID]
            # Skip None, NaN, or empty entity IDs
            if pd.isna(entity_id) or not entity_id or not isinstance(entity_id, str):
                continue
            # Only process entities that start with TCFIHOS- or ECFIHOS-
            if entity_id.startswith(("TCFIHOS-", "ECFIHOS-")):
                entities_to_process.append(entity_id)

        # Process each entity
        for entity_id in entities_to_process:
            # Normalize the entity ID for the map key
            entity_key = normalize_cfihos_id(entity_id)

            # Skip if already mapped
            if entity_key in denormalization_map:
                continue

            # Priority 1: Check if this entity itself is in the root node list
            # If so, it maps to itself (this handles first children in root_list,
            # and sub-children in root_list)
            if entity_id in root_list_actual_ids:
                # Map to itself (normalized)
                denormalization_map[entity_key] = normalize_cfihos_id(entity_id)
                continue

            # Priority 2: Check if any of its ancestors is in root_node_list
            # This ensures that if a first child or sub-child is in root_list,
            # all its descendants map to it (not to a more distant ancestor)
            ancestor_root_normalized = find_ancestor_in_root_list(entity_id)

            if ancestor_root_normalized:
                # Denormalize to the closest root node ancestor (normalized)
                denormalization_map[entity_key] = ancestor_root_normalized
                continue

            # Priority 3: Check if entity is a first child (not in root_list)
            # First children that are not in root_list map to themselves
            if entity_id in all_first_children:
                # Map first children to themselves (normalized)
                denormalization_map[entity_key] = normalize_cfihos_id(entity_id)
                continue

            # Priority 4: Check if direct parent is a first child (more direct check)
            # This handles the case where direct children of first children
            # might not be found by find_first_child_ancestor
            parents = entity_to_parents.get(entity_id, [])
            direct_parent_is_first_child = None
            for parent in parents:
                if parent and parent in all_first_children:
                    direct_parent_is_first_child = parent
                    break

            if direct_parent_is_first_child:
                # Denormalize to the first child parent (normalized)
                denormalization_map[entity_key] = normalize_cfihos_id(
                    direct_parent_is_first_child
                )
                continue

            # Priority 5: Find which first child this entity descends from (traverse up the tree)
            # This handles deeper descendants of first children
            # This should catch any remaining cases, including entities that might
            # have been missed by the direct parent check
            first_child = find_first_child_ancestor(entity_id, all_first_children)
            if first_child:
                # Denormalize to the first child (normalized)
                denormalization_map[entity_key] = normalize_cfihos_id(first_child)
                continue

            # If we get here, the entity doesn't descend from any first child
            # This shouldn't happen for entities that start with TCFIHOS- or ECFIHOS-
            # but we'll leave it unmapped rather than raising an error

        self.tag_and_equipment_classes_to_root_nodes = denormalization_map

    def _assign_root_nodes_to_tag_and_equipment_classes(
        self, enitity_id: str, property_id: str
    ) -> str:
        """Returns the root node entity ID assigned to the given tag or equipment class entity ID.

        Looks up the mapping from tag_and_equipment_classes_to_root_nodes using the provided entity ID.
        The mapping uses normalized CFIHOS IDs (without T/E prefix) for both keys and values.
        Returns None if the entity ID is not found in the mapping.

        Args:
            enitity_id (str): The tag or equipment class entity ID to look up (can be TCFIHOS- or ECFIHOS- format).

        Returns:
            str or None: The assigned root node entity ID in normalized CFIHOS- format, or None if not mapped.
        """
        # Normalize the entity ID (remove T/E prefix) for lookup
        normalized_id = enitity_id
        # NOTE CFIHOS addtions should not start with T or E in order not to confuse the below logic. E.g TOS-30000311 should not be treated as a tag.
        normalized_id = (
            enitity_id[1:] if enitity_id.lower().startswith(("t", "e")) else None
        )
        # Look up in the denormalization map (which uses normalized IDs)
        node_group_id = self.tag_and_equipment_classes_to_root_nodes.get(
            normalized_id, None
        )
        if property_id.lower().endswith("_uom"):
            return node_group_id.replace("-", "_") + "_UOM" if node_group_id else None
        return node_group_id.replace("-", "_") if node_group_id else None
