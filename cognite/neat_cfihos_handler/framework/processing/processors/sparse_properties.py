"""Sparse Properties Processor module.

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
class SparsePropertiesProcessor(BaseProcessor):
    """Processor class for handling model processing.

    Attributes:
        model_processors_config (List[dict]): Configuration for model processors.
        model_type (str): The type of model to process.
    """

    model_type: str = field(default=SparseModelType.CONTAINERS)
    model_processors: list[CfihosModelLoader] = field(default_factory=list, init=False)

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

        # Step 1: build the full inheritance of entities
        self._build_entities_full_inheritance()
        # Step 2: extend the direct relations properties with sclarified properties
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
        properties = []
        entities: dict[str, list[dict[str, str]]] = {}
        # unique_properties = self._df_entity_properties[PropertyStructure.ID].unique()
        unique_properties = self._df_entity_properties.loc[
            (~self._df_entity_properties[PropertyStructure.FIRSTCLASSCITIZEN])
            & (
                ~self._df_entity_properties[PropertyStructure.PROPERTY_TYPE].isin(
                    [Relations.EDGE, Relations.REVERSE]
                )
            )
        ][PropertyStructure.ID].unique()
        # Check that all target types are present
        for prop in unique_properties:
            df_property_subset = self._df_entity_properties.loc[
                self._df_entity_properties[PropertyStructure.ID] == prop
            ]
            df_property_subset_groups = df_property_subset.groupby(
                PropertyStructure.PROPERTY_TYPE
            )
            for idx, df_subset in df_property_subset_groups:
                if len(df_subset) > 0:
                    columns_to_check = {
                        PropertyStructure.NAME: df_subset[
                            PropertyStructure.NAME
                        ].unique(),
                        PropertyStructure.DMS_NAME: df_subset[
                            PropertyStructure.DMS_NAME
                        ].unique(),
                        PropertyStructure.TARGET_TYPE: (
                            df_subset[PropertyStructure.TARGET_TYPE].unique()
                            if idx == "BASIC_DATA_TYPE"
                            else [None]
                        ),
                        PropertyStructure.PROPERTY_TYPE: df_subset[
                            PropertyStructure.PROPERTY_TYPE
                        ].unique(),
                        PropertyStructure.MULTI_VALUED: df_subset[
                            PropertyStructure.MULTI_VALUED
                        ].unique(),
                    }
                    for col_name, data in columns_to_check.items():
                        # validate duplicate non fcc properties
                        if len(data) != 1:
                            raise NeatValueError(
                                f"Found properties '{col_name}' with lacking or multiple values: {data}"
                            )
                    prop_row = {}
                    if columns_to_check:
                        for key, value in columns_to_check.items():
                            if key not in [
                                PropertyStructure.FIRSTCLASSCITIZEN,
                                PropertyStructure.UNIQUE_VALIDATION_ID,
                            ]:
                                prop_row[key] = value[0]
                    # Always include ID and DESCRIPTION
                    prop_row[PropertyStructure.ID] = prop.replace("-", "_")
                    prop_row[PropertyStructure.DESCRIPTION] = df_subset[
                        PropertyStructure.DESCRIPTION
                    ].unique()[0]
                    property_group_id = self._assign_property_group(
                        prop_row[PropertyStructure.ID], CONTAINER_PROPERTY_LIMIT
                    )
                    entity_property_row = self._create_property_row(
                        prop_row, property_group=property_group_id
                    )
                    if property_group_id not in entities:
                        entities[property_group_id] = {
                            EntityStructure.ID: property_group_id,
                            EntityStructure.NAME: property_group_id,
                            EntityStructure.DMS_NAME: property_group_id,
                            EntityStructure.DESCRIPTION: None,
                            EntityStructure.INHERITS_FROM_ID: None,
                            EntityStructure.INHERITS_FROM_NAME: None,
                            EntityStructure.FULL_INHERITANCE: None,
                            EntityStructure.PROPERTIES: [],
                            EntityStructure.FIRSTCLASSCITIZEN: False,
                            EntityStructure.IMPLEMENTS_CORE_MODEL: None,
                            EntityStructure.VIEW_FILTER: None,
                        }
                        entities[property_group_id]["properties"].append(
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
                    entities[property_group_id]["properties"].append(
                        entity_property_row
                    )
                    properties.append(entity_property_row)

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

        property_extention_suffix_list = ["_rel", "_uom"]
        property_group_id = (
            f"{property_group_id}_ext"
            if any(
                propertyId.lower().endswith(ext)
                for ext in property_extention_suffix_list
            )
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
