"""This module defines generic classes and enumerations used throughout the CFIHOS framework.

It provides reusable structures for representing entities and properties, as well as any common
enums or base classes that are shared across the framework.
"""
from enum import Enum


# Entity Fields
class EntityStructure:
    """Defines field names and constants for entity structure representation.

    This class provides standardized field names used throughout the framework
    for representing entities, including their identifiers, names, inheritance
    relationships, and properties.
    """

    ID: str = "entityId"
    NAME: str = "entityName"
    DMS_NAME: str = "dmsEntityName"  # DMS name is used for the entity in the DMS
    DESCRIPTION: str = "description"
    INHERITS_FROM_NAME: str = "inheritsFromName"
    INHERITS_FROM_ID: str = "inheritsFromId"
    FULL_INHERITANCE: str = "fullInheritance"
    PROPERTIES: str = "properties"
    FCC_PREFIX = "FCC_"
    FIRSTCLASSCITIZEN = "firstClassCitizen"
    IMPLEMENTS_CORE_MODEL = (
        "implementsCoreModel"  # str in CSV -> list[dict] after processing
    )
    VIEW_FILTER: str = "viewFilters"


# Property Fields
class PropertyStructure:
    """Defines field names and constants for property structure representation.

    This class provides standardized field names used throughout the framework
    for representing properties, including their types, validation rules,
    relationships, and edge configurations.
    """

    ID: str = "propertyId"
    NAME: str = "propertyName"
    DMS_NAME: str = "dmsPropertyName"  # DMS name is used for the entity in the DMS
    DESCRIPTION: str = "description"
    TARGET_TYPE: str = "targetType"
    ORIGINAL_TARGET_TYPE: str = "originalTargetType"
    PROPERTY_TYPE: str = "propertyType"
    MULTI_VALUED: str = "multiValued"
    IS_REQUIRED: str = "isRequired"
    IS_UNIQUE: str = "isUnique"
    ENUMERATION_TABLE: str = "enumerationTableName"
    UOM: str = "unitOfMeasure"
    PROPERTY_GROUP: str = "propertyGroup"
    INHERITED: str = "inherited"
    INHERITED_FROM: str = "inheritedFrom"
    FIRSTCLASSCITIZEN = "firstClassCitizen"
    MAPPED_PROPERTY = "mapped_property"
    CUSTOM_PROPERTY = "custom_property"
    FCC_PREFIX = "fcc_"
    UNIQUE_ENTITYID_PROPERTYID = "entityIdPropertyId"
    UNIQUE_VALIDATION_ID = "unique_validation_id"
    EDGE_DIRECTION = "edgeDirection"
    EDGE_SOURCE = "edgeSource"
    EDGE_TARGET = "edgeTarget"
    EDGE_SOURCE_NAME = "edgeSourceName"
    EDGE_TARGET_NAME = "edgeTargetName"
    EDGE_SOURCE_DMS_NAME = "edgeSourceDmsName"
    EDGE_TARGET_DMS_NAME = "edgeTargetDmsName"
    EDGE_EXTERNAL_ID = "edgeExternalId"
    ENTITY_EDGE = "entityEdge"
    DIRECT_RELATION = "DirectRelation"
    REV_THROUGH_PROPERTY = "throughProperty"  # str
    REV_PROPERTY_NAME = "revPropertyName"  # str
    REV_PROPERTY_DMS_NAME = "revPropertyDmsName"  # str
    REV_PROPERTY_DESCRIPTION = "revPropertyDescription"  # str
    IN_MODEL = "inModel"


class NeatViewStructure:
    """Defines field names for NEAT view structure representation.

    This class provides standardized field names used for representing
    views in the NEAT model,
    including view metadata and implementation details.
    """

    VIEW: str = "View"
    NAME: str = "Name"
    DESCRIPTION: str = "Description"
    IMPLEMENTS: str = "Implements"
    FILTER: str = "Filter"
    IN_MODEL: str = "In Model"


class NeatPropertyStructure:
    """Defines field names for NEAT property structure representation.

    This class provides standardized field names used for representing
    properties within NEAT views, including connection types, value constraints, indexes,
    and container relationships.
    """

    VIEW: str = "View"
    VIEW_PROPERTY: str = "View Property"
    NAME: str = "Name"
    DESCRIPTION: str = "Description"
    CONNECTION: str = "Connection"
    VALUE_TYPE: str = "Value Type"
    IMMUTABLE: str = "Immutable"
    MIN_COUNT: str = "Min Count"
    MAX_COUNT: str = "Max Count"
    DEFAULT: str = "Default"
    REFERENCE: str = "Reference"
    CONTAINER: str = "Container"
    CONTAINER_PROPERTY: str = "Container Property"
    INDEX: str = "Index"
    CONSTRAINT: str = "Constraint"


class NeatContainerStructure:
    """Defines field names for NEAT container structure representation.

    This class provides standardized field names used for representing
    containers in the NEAT model, including container metadata and usage patterns.
    """

    CONTAINER: str = "Container"
    NAME: str = "Name"
    DESCRIPTION: str = "Description"
    CONSTRAINT: str = "Implements"
    USED_FOR: str = "Used For"


class GitHubAttributes:
    """Defines attribute names for GitHub integration.

    This class provides standardized attribute names used for GitHub-related
    configurations and branch management in the CFIHOS framework.
    """

    CFIHOS_EPC_GIT_BRANCH: str = "CFIHOS_EPC_GIT_BRANCH"


class DataSource(Enum):
    """Enumeration of supported sources for CFIHOS files.

    This enum defines the available source locations from which CFIHOS files
    can be retrieved and processed by the framework.

    Attributes:
        CDF: CFIHOS files stored in Cognite Data Fusion
        CSV: CFIHOS files in CSV format from local or remote file systems
        GITHUB: CFIHOS files stored in GitHub repositories
    """

    CDF = "cdf"
    CSV = "csv"
    GITHUB = "github"

    @classmethod
    def default(cls):
        """Return the default data source."""
        return cls.CSV

    @classmethod
    def get(cls, value):
        """Get the data source for a given value."""
        # lowercase for simplicity
        return cls.__members__.get(value.lower(), cls.default())


class ScopeConfig:
    """Defines scope configuration constants.

    This class provides standardized scope identifiers used for filtering
    and organizing data within different operational contexts.
    """

    ALL: str = "All"
    TAGS: str = "Tags"
    EQUIPMENT: str = "Equipment"
    SCOPED: str = "Scoped"


class Relations:
    """Relation Types.

    This class defines the different types of relationships that can exist
    between entities in the data model
    """

    DIRECT = "ENTITY_RELATION"
    REVERSE = "ENTITY_REVERSE_RELATION"
    EDGE = "EDGE_RELATION"


class SparseModelType:
    """Enumeration for Sparse data Model types."""

    CONTAINERS = "containers"
    VIEWS = "views"
