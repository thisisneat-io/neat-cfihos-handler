"""Data sheets column mappings for CFIHOS processing."""
from dataclasses import dataclass

from cfihos_handler.framework.common.constants import PARENT_SUFFIX
from cfihos_handler.framework.common.generic_classes import (
    EntityStructure,
    PropertyStructure,
)

ENTITY_CDM_EXTENSION_MAPPING = {
    "CFIHOS ID": EntityStructure.ID,
    "inherited core model type": EntityStructure.IMPLEMENTS_CORE_MODEL,
}

ENTITY_COLUMN_MAPPING = {
    "entity name": EntityStructure.NAME,
    "definition": EntityStructure.DESCRIPTION,
    "is first class citizen": EntityStructure.FIRSTCLASSCITIZEN,
}

ENTITY_EDGE_COLUMN_MAPPING = {
    "source": PropertyStructure.EDGE_SOURCE,
    "destination": PropertyStructure.EDGE_TARGET,
    "edge unique id": PropertyStructure.ID,
    "edge name": PropertyStructure.NAME,
    "edge definition": PropertyStructure.DESCRIPTION,
    "source name": PropertyStructure.EDGE_SOURCE_NAME,
    "destination name": PropertyStructure.EDGE_TARGET_NAME,
    PropertyStructure.EDGE_EXTERNAL_ID: PropertyStructure.EDGE_EXTERNAL_ID,
}

ENTITY_EDGE_REVERSE_COLUMN_MAPPING = {
    "source": PropertyStructure.EDGE_TARGET,
    "destination": PropertyStructure.EDGE_SOURCE,
    "source name": PropertyStructure.EDGE_TARGET_NAME,
    "destination name": PropertyStructure.EDGE_SOURCE_NAME,
    "reverse edge unique id": PropertyStructure.ID,
    "reverse edge name": PropertyStructure.NAME,
    "reverse edge definition": PropertyStructure.DESCRIPTION,
    PropertyStructure.EDGE_EXTERNAL_ID: PropertyStructure.EDGE_EXTERNAL_ID,
}

ENTITY_PROPERTY_METADATA_MAPPING = {
    "CFIHOS unique code": PropertyStructure.ID,
    "name": PropertyStructure.NAME,
    "definition": PropertyStructure.DESCRIPTION,
    "format": PropertyStructure.TARGET_TYPE,
}

ENTITY_RAW_COLUMN_MAPPING = {
    EntityStructure.ID: "CFIHOS unique code",
    EntityStructure.NAME: "name",
    EntityStructure.INHERITS_FROM_ID: "parent CFIHOS unique ID",
    EntityStructure.INHERITS_FROM_NAME: "parent entity name",
}

# NOTE: reverese relations related stuff disabled for now
ENTITY_RELEVANT_PROPERTY_COLUMNS = {
    "CFIHOS unique code": PropertyStructure.ID,
    "entity name": EntityStructure.NAME,
    "property name": PropertyStructure.NAME,
    "constraint must be present in": PropertyStructure.PROPERTY_TYPE,
    "definition": PropertyStructure.DESCRIPTION,
    "identifier / mandatory / optional": PropertyStructure.IS_REQUIRED,
    "format": PropertyStructure.TARGET_TYPE,
    "CDF reverse property id": PropertyStructure.REV_THROUGH_PROPERTY,
    "CDF reverse property name": PropertyStructure.REV_PROPERTY_NAME,
    "CDF reverse property description": PropertyStructure.REV_PROPERTY_DESCRIPTION,
    "CDF isList": PropertyStructure.MULTI_VALUED,
    "in model": PropertyStructure.IN_MODEL,
}

TAG_OR_EQUIPMENT_PROPERTY_METADATA_MAPPING = {
    "CFIHOS unique code": PropertyStructure.ID,
    "property name": PropertyStructure.NAME,
    "property definition": PropertyStructure.DESCRIPTION,
    "property data type": PropertyStructure.TARGET_TYPE,
    "property picklist name": PropertyStructure.ENUMERATION_TABLE,
    "unit of measure dimension code": PropertyStructure.UOM,
    "in model": PropertyStructure.IN_MODEL,
}


@dataclass
class TagOrEquipment:
    """Tag or Equipment dataframe fields."""

    cfihos_type_object_name: str

    @property
    def column_renaming(self) -> dict[str, str]:
        """Column renaming for the Tag or Equipment dataframes."""
        return {
            f"{self.cfihos_type_object_name} class name": EntityStructure.NAME,
            f"{self.cfihos_type_object_name} class definition": EntityStructure.DESCRIPTION,
            f"{self.cfihos_type_object_name} class name{PARENT_SUFFIX}": EntityStructure.INHERITS_FROM_NAME,
            f"{EntityStructure.ID}{PARENT_SUFFIX}": EntityStructure.INHERITS_FROM_ID,
        }

    @property
    def raw_column_mapping(self) -> dict[str, str]:
        """Raw column mapping for the Tag or Equipment dataframes."""
        return {
            # CFIHOS unique code column naming is different for tag and equipment
            EntityStructure.ID: f"{self.cfihos_type_object_name} class CFIHOS unique code"
            if self.cfihos_type_object_name == "equipment"
            else "CFIHOS unique code",
            EntityStructure.NAME: f"{self.cfihos_type_object_name} class name",
            "parent_name_key": f"parent {self.cfihos_type_object_name} class name",
            "parent_join_key": "parent_join_key",
            "entity_join_key": EntityStructure.ID,
        }

    @property
    def property_mapping(self) -> dict[str, str]:
        """Property mapping for the Tag or Equipment dataframes."""
        return {
            f"{self.cfihos_type_object_name} class CFIHOS unique code": EntityStructure.ID,
            f"{self.cfihos_type_object_name} class name": EntityStructure.NAME,
            "property CFIHOS unique code": PropertyStructure.ID,
            "property name": PropertyStructure.NAME,
            # TODO: Below covers only SI unit of measure, CHECK if imperial unit of measure is also needed
            "SI unit of measure CFIHOS unique code": PropertyStructure.UOM,
            "in model": PropertyStructure.IN_MODEL,
        }

    @property
    def parent_entity_id_column(self) -> str:
        """Parent entity ID column name."""
        return f"{EntityStructure.ID}_parent"

    @property
    def parent_entity_name_column(self) -> str:
        """Parent entity name column name."""
        return f"{self.cfihos_type_object_name} class name{PARENT_SUFFIX}"
