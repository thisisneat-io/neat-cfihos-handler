# Sparse Properties Processor Documentation

This document provides comprehensive documentation for the `SparsePropertiesProcessor` class, which is responsible for processing CFIHOS data models and transforming them into sparse container and view structures optimized for Cognite Data Fusion (CDF).

## Table of Contents

1. [Overview](#overview)
2. [Architecture and Design](#architecture-and-design)
3. [Container Model Processing](#container-model-processing)
4. [View Model Processing](#view-model-processing)
5. [Integration with Base Processor](#integration-with-base-processor)
6. [Integration with CfihosModelLoader](#integration-with-cfihosmodelloader)
7. [Key Processing Steps](#key-processing-steps)
8. [Property Grouping Strategy](#property-grouping-strategy)
9. [First-Class Citizen Entities](#first-class-citizen-entities)
10. [Additional Features](#additional-features)

---

## Overview

The `SparsePropertiesProcessor` is a specialized processor that transforms CFIHOS standard data into sparse, denormalized structures suitable for CDF. The processor implements a grouping strategy that significantly reduces the number of containers required while maintaining the semantic structure of CFIHOS models.

### Key Benefits

- **Efficiency**: Reduces thousands of potential containers to a manageable number through intelligent grouping
- **Scalability**: Handles large CFIHOS models efficiently by grouping non-first-class entities
- **Flexibility**: Supports both container-based and view-based model generation
- **Standards Compliance**: Maintains CFIHOS standard structure in view models

### Processing Modes

The processor supports two distinct processing modes:

1. **Container Mode** (`model_type="containers"`): Creates denormalized container structures with grouped properties
2. **View Mode** (`model_type="views"`): Creates CFIHOS-compliant view structures mapped to container models

---

## Architecture and Design

### Class Hierarchy

```
BaseProcessor (Abstract Base Class)
    └── SparsePropertiesProcessor (Concrete Implementation)
```

### Core Components

- **Model Processors**: List of `CfihosModelLoader` instances that process different CFIHOS sources
- **DataFrames**: Pandas DataFrames storing entities, properties, and metadata
- **Model Structures**: Dictionaries containing processed entities and properties
- **Mapping Tables**: Cross-references between entity IDs, DMS IDs, and names

### Template Method Pattern

The processor follows the template method pattern defined in `BaseProcessor`, implementing the following abstract methods:

- `_setup_model_processors()`: Initialize CFIHOS model loaders
- `_collect_processor_data()`: Gather data from all processors
- `_validate_collected_data()`: Validate data consistency
- `_process_collected_data()`: Transform and enrich data
- `_build_model_structures()`: Create final model structures

---

## Container Model Processing

Container model processing creates denormalized container structures where properties are grouped based on entity classification (first-class citizen vs. non-first-class citizen).

### Processing Flow

1. **Setup Phase**: Initialize model processors from configuration
2. **Collection Phase**: Gather entities and properties from all CFIHOS sources
3. **Validation Phase**: Ensure data consistency and uniqueness
4. **Transformation Phase**: Build inheritance chains, extend properties
5. **Structure Building Phase**: Create container entities and groups

### Entity Grouping Strategy

The processor uses a two-tier grouping strategy:

#### First-Class Citizen Entities

First-class citizen entities are assigned to **dedicated groups** with a one-to-one correspondence:

- **Group ID**: Same as the entity's CFIHOS code or name (with hyphens replaced by underscores)
- **Group Name**: Same as the entity's CFIHOS name, but sanitized to comply CDF permitted identifire formats
- **Container Creation**: Each first-class citizen entity gets its own container
- **View Creation**: Each first-class citizen entity gets its own view with the same name

**Example:**
```python
# Entity: CFIHOS_00000001 (Area)
# Group ID: CFIHOS_00000001
# Group Name: "Area"
# Result: Container "Area" and View "Area" are created
```

#### Non-First-Class Citizen Entities

Non-first-class citizen entities are grouped into **ranges** based on CFIHOS property codes:

- **Group Size**: Defined by `CONTAINER_PROPERTY_LIMIT` constant (default: 100)
- **Grouping Logic**: Properties are grouped in ranges of 100 based on their numeric CFIHOS code
- **Group ID Format**: `{PREFIX}_{START_CODE}_{END_CODE}` (e.g., `CFIHOS_1_10000001_10000100`)
- **Container Creation**: One container per group containing all properties in that range

**Example:**
```python
# Properties: CFIHOS_10000001, CFIHOS_10000050, CFIHOS_10000099
# Group ID: CFIHOS_1_10000001_10000100
# Result: Single container "CFIHOS_1_10000001_10000100" contains all three properties
```

### Container Model Creation Process

The `_create_container_model_entities()` method processes non-first-class citizen properties:

1. **Filter Properties**: Selects only non-first-class citizen properties (excluding edges and reverse relations)
2. **Validate Consistency**: Ensures each property has consistent attributes across all occurrences
3. **Assign Groups**: Groups properties using `_assign_property_group()` method
4. **Create Entity Structures**: Creates container entity structures for each group
5. **Add EntityType Property**: Adds an `entityType` property to each group for filtering

**Key Code Snippet:**
```python
def _create_container_model_entities(self):
    # Filter non-FCC properties
    unique_properties = self._df_entity_properties.loc[
        (~self._df_entity_properties[PropertyStructure.FIRSTCLASSCITIZEN])
        & (~self._df_entity_properties[PropertyStructure.PROPERTY_TYPE].isin(
            [Relations.EDGE, Relations.REVERSE]
        ))
    ][PropertyStructure.ID].unique()
    
    # Group properties and create containers
    for prop in unique_properties:
        property_group_id = self._assign_property_group(
            prop_row[PropertyStructure.ID], 
            CONTAINER_PROPERTY_LIMIT  # Default: 100
        )
        # Create container entity structure...
```

### First-Class Citizen Extension

The `_extend_container_model_first_class_citizens_entities()` method adds first-class citizen entities:

1. **Identify FCC Properties**: Filters properties belonging to first-class citizen entities
2. **Create Dedicated Groups**: Creates one group per first-class citizen entity
3. **Preserve Entity Metadata**: Maintains entity name, description, and core model implementations
4. **Validate Relations**: Ensures relationships target valid first-class citizen entities

**Key Code Snippet:**
```python
def _extend_container_model_first_class_citizens_entities(self):
    fcc_properties = self._df_entity_properties.loc[
        self._df_entity_properties[PropertyStructure.FIRSTCLASSCITIZEN]
    ]
    
    for _, prop in fcc_properties.iterrows():
        # Use entity ID as property group (one-to-one mapping)
        property_group_id = prop[EntityStructure.ID].replace("-", "_")
        
        # Create entity structure with same name/code as original entity
        entities[property_group_id] = {
            EntityStructure.ID: property_group_id,
            EntityStructure.NAME: fcc_entity[EntityStructure.NAME],
            EntityStructure.DMS_NAME: fcc_entity[EntityStructure.DMS_NAME],
            # ... preserves original entity metadata
        }
```

### EntityTypeGroup Container

A special container called `EntityTypeGroup` is created to support filtering:

- **Purpose**: Holds CFIHOS entity IDs for filtering instances in containers
- **Property**: Contains an `entityType` property (String, required)
- **Usage**: Allows querying which entity types are present in grouped containers
- **First-Class Citizen**: Marked as first-class citizen to avoid further denormalization

---

## View Model Processing

View model processing creates **CFIHOS-compliant view structures** that maintain the original CFIHOS entity hierarchy and relationships, while **mapping all properties to containers** created during container model processing. This dual approach ensures standards compliance while leveraging the efficiency of grouped containers.

### CFIHOS Compliance and Container Mapping

View models achieve CFIHOS compliance through:

1. **Preserved Entity Structure**: Views maintain the original CFIHOS entity hierarchy, inheritance relationships, and entity names
2. **Standards-Compliant Views**: Each view represents a CFIHOS entity with its proper structure, relationships, and metadata
3. **Container Property Mapping**: All properties in views reference containers from the container model, enabling efficient data storage

**Key Principle**: Views provide the CFIHOS-compliant interface, while containers provide the efficient storage mechanism. Properties in views map to container properties, creating a bridge between the standard structure and the optimized storage.

### Processing Flow

The `_create_views_model_entities()` method:

1. **Process Each Entity**: Iterates through all entities in the model
2. **Map Entity IDs**: Converts entity IDs to DMS IDs using mapping tables
3. **Collect Properties**: Gathers all properties for each entity
4. **Handle Inheritance**: Excludes inherited properties (handled through view inheritance)
5. **Assign Property Groups**: Links properties to their container groups (maps to containers)
6. **Create View Structures**: Builds view entities with property references to containers

### View-to-Container Mapping

Views reference containers through the `property_group` field, which maps to containers created in container mode:

- **First-Class Citizen Views**: 
  - Properties reference the entity's own container (same name as view)
  - Example: View "Area" → Container "Area" → Properties map to "Area" container
  
- **Non-First-Class Citizen Views**: 
  - Properties reference grouped containers based on property code ranges
  - Example: View "MeasurementPoint" → Properties map to grouped container "CFIHOS_1_10000001_10000100"

**Example:**
```python
# View: "Area" (CFIHOS_00000001) - CFIHOS compliant view structure
# Property: "areaCode" (CFIHOS_10000001)
# Property Group: "CFIHOS_1_10000001_10000100" (non-FCC property)
# Container Reference: "sp_containers:CFIHOS_1_10000001_10000100"
# 
# The view maintains CFIHOS structure, but the property maps to a grouped container
# from the container model for efficient storage.
```

### Property Group Assignment in Views

The processor uses different logic for assigning property groups in views:

```python
property_group = (
    prop_row[EntityStructure.ID].replace("-", "_")
    if row[EntityStructure.FIRSTCLASSCITIZEN]
    else self._assign_property_group(prop_row[PropertyStructure.ID])
)
```

- **First-Class Citizen**: Uses entity ID as property group (matches container name)
- **Non-First-Class Citizen**: Uses grouped property range (matches container group)

### Inheritance Handling

Views properly handle property inheritance:

1. **Compute Inherited Properties**: Identifies properties inherited from parent entities
2. **Exclude from View**: Inherited properties are not duplicated in child views
3. **View Inheritance**: Inheritance is represented through view hierarchy, not property duplication

**Key Code:**
```python
# Compute inherited properties
inherited_props = set().union(*[
    entity_props_lookup.get(parent_id, set())
    for parent_id in row[EntityStructure.FULL_INHERITANCE]
])

# Skip inherited properties
if prop_row[PropertyStructure.ID] in inherited_props:
    continue  # skip inherited property
```

### EntityType Property in Views

Non-first-class citizen views include an `entityType` property:

- **Purpose**: Identifies which entity type an instance represents in grouped containers
- **Property Group**: Assigned to `EntityTypeGroup` container
- **Required**: Marked as required for proper filtering

---

## Integration with Base Processor

The `SparsePropertiesProcessor` extends `BaseProcessor` and implements the template method pattern.

### Template Method: `process_and_collect_models()`

The base class defines the processing workflow:

```python
def process_and_collect_models(self):
    # Step 1: Setup model processors
    self._setup_model_processors()
    
    # Step 2: Collect data from all processors
    self._collect_processor_data()
    
    # Step 3: Validate collected data
    self._validate_collected_data()
    
    # Step 4: Process and transform data
    self._process_collected_data()
    
    # Step 5: Build final model structures
    self._build_model_structures()
```

### Implemented Abstract Methods

#### `_setup_model_processors()`

Initializes `CfihosModelLoader` instances from configuration:

```python
def _setup_model_processors(self):
    for model_processor in self.model_processors_config:
        for model_processor_type, processor_data in model_processor.items():
            processor_data["processor_config_name"] = model_processor_type
            self.model_processors.append(CfihosModelLoader(**processor_data))
            self._setup_property_groups(processor_data["id_prefix"])
    
    # Synchronize mapping tables across processors
    self._sync_processor_mapping_tables()
```

#### `_collect_processor_data()`

Gathers data from all processors and combines into unified DataFrames:

```python
def _collect_processor_data(self):
    for processor in self.model_processors:
        df_entities, df_properties, df_metadata = processor.process()
        # Combine data from all processors...
    
    # Mark FCC properties based on entity classification
    self._df_entity_properties.loc[
        self._df_entity_properties[EntityStructure.ID].isin(
            self._df_entities.loc[
                self._df_entities[EntityStructure.FIRSTCLASSCITIZEN],
                EntityStructure.ID
            ]
        ),
        EntityStructure.FIRSTCLASSCITIZEN
    ] = True
```

#### `_validate_collected_data()`

Ensures data consistency and uniqueness:

- Validates unique entity IDs
- Validates unique DMS names
- Validates unique property validation IDs
- Raises `NeatValueError` on conflicts

#### `_process_collected_data()`

Transforms and enriches the collected data:

1. Merges property metadata with entity properties
2. Builds full inheritance chains
3. Extends direct relations with scalar properties (if enabled)
4. Extends properties with UOM variants

#### `_build_model_structures()`

Creates final model structures based on model type:

```python
def _build_model_structures(self):
    if self.model_type == SparseModelType.CONTAINERS:
        self._create_container_model_entities()
        self._extend_container_model_first_class_citizens_entities()
    elif self.model_type == SparseModelType.VIEWS:
        self._create_views_model_entities()
```

### Inherited Attributes

The processor inherits useful attributes from `BaseProcessor`:

- `_df_entities`: DataFrame of all entities
- `_df_entity_properties`: DataFrame of all entity properties
- `_df_properties_metadata`: DataFrame of property metadata
- `_model_entities`: Dictionary of processed entities
- `_model_properties`: Dictionary of processed properties
- `_model_property_groups`: Dictionary of property groups
- Mapping tables for entity/ID conversions

---

## Integration with CfihosModelLoader

The processor leverages `CfihosModelLoader` to process CFIHOS data from various sources (CSV files, etc.).

### Multiple Processor Support

The processor can handle multiple `CfihosModelLoader` instances:

- **Standard CFIHOS**: Primary CFIHOS standard data
- **Extensions**: Additional CFIHOS extensions (e.g., Extention-CFIHOS, user-defined entities)
- **Combined Processing**: All processors are processed together to create a unified model

### Processor Initialization

Each processor is initialized with configuration from `model_processors_config`:

```python
for model_processor in self.model_processors_config:
    for model_processor_type, processor_data in model_processor.items():
        processor_data["processor_config_name"] = model_processor_type
        self.model_processors.append(CfihosModelLoader(**processor_data))
```

### Data Collection

The `process()` method of each `CfihosModelLoader` returns:

- **Entities DataFrame**: Contains entity definitions
- **Properties DataFrame**: Contains property definitions
- **Properties Metadata DataFrame**: Contains additional property metadata

These are combined into unified DataFrames:

```python
self._df_entities = pd.concat(list_of_entities)
self._df_entity_properties = pd.concat(list_of_properties)
self._df_properties_metadata = pd.concat(list_of_properties_metadata)
```

### Mapping Table Synchronization

The processor synchronizes mapping tables across all processors:

```python
def _sync_processor_mapping_tables(self):
    # Collect all mapping tables
    for processor in self.model_processors:
        self._map_entity_id_to_dms_id.update(processor._map_entity_id_to_dms_id)
        self._map_dms_id_to_entity_id.update(processor._map_dms_id_to_entity_id)
        self._map_entity_name_to_entity_id.update(
            processor._map_entity_name_to_entity_id
        )
    
    # Update all processors with combined mappings
    for processor in self.model_processors:
        processor._map_entity_id_to_dms_id = self._map_entity_id_to_dms_id.copy()
        # ... update other mappings
```

This ensures:
- **Global Uniqueness**: Entity IDs are unique across all processors
- **Consistent References**: Relationships can reference entities from any processor
- **Unified Model**: All processors contribute to a single, coherent model

### Property Group Prefixes

Each processor contributes property group prefixes:

```python
def _setup_property_groups(self, processor_id_prefix: str):
    self._property_groupings.extend(
        [f"{processor_id_prefix}_{idx}" for idx in range(0, 10)]
    )
```

This allows properties from different processors to be grouped separately while maintaining the same grouping logic.

---

## Key Processing Steps

### Step 1: Setup Model Processors

- Initialize `CfihosModelLoader` instances from configuration
- Set up property group prefixes for each processor
- Synchronize mapping tables across processors

### Step 2: Collect Processor Data

- Call `process()` on each `CfihosModelLoader`
- Combine entities, properties, and metadata DataFrames
- Mark first-class citizen properties based on entity classification

### Step 3: Validate Collected Data

- Check for duplicate entity IDs
- Check for duplicate DMS names
- Check for duplicate property validation IDs
- Raise errors on validation failures

### Step 4: Process Collected Data

- Merge property metadata with entity properties
- Build full inheritance chains for all entities
- Extend direct relations with scalar properties (optional)
- Extend properties with UOM variants

### Step 5: Build Model Structures

**For Container Mode:**
- Create grouped containers for non-first-class citizen properties
- Extend with first-class citizen entity containers
- Add EntityTypeGroup container

**For View Mode:**
- Create view entities with property references
- Map properties to container groups
- Handle inheritance properly
- Add entityType properties for non-FCC views

---

## Property Grouping Strategy

### Group Assignment Algorithm

The `_assign_property_group()` method groups properties based on their CFIHOS code:

```python
def _assign_property_group(
    self, propertyId: str, container_property_limit: int = 100
) -> str:
    propertyId = propertyId.replace("-", "_")
    id_number = int(self._get_property_id_number(propertyId))
    property_group_prefix = self._get_property_group_prefix(propertyId)
    
    # Calculate range boundaries
    if id_number % container_property_limit == 0:
        # Handle boundary case
        start = id_number - (id_number - 1) % container_property_limit
        end = start + container_property_limit - 1
    else:
        # Normal case
        start = id_number - id_number % container_property_limit + 1
        end = start + container_property_limit - 1
    
    return f"{property_group_prefix}_{start}_{end}"
```

### Group ID Format

- **Pattern**: `{PREFIX}_{START}_{END}`
- **Example**: `CFIHOS_1_10000001_10000100`
- **Special Cases**: 
  - UOM variants: `{GROUP_ID}_ext`
  - Direct relation variants: `{GROUP_ID}_ext` (if enabled)

### Property ID Number Extraction

The processor extracts numeric parts from property IDs:

```python
def _get_property_id_number(self, property_id: str) -> str:
    property_id_number = re.findall(r"\d+", property_id)[0]
    return property_id_number
```

**Examples:**
- `CFIHOS_10000005` → `10000005`
- `CFIHOS-10000050` → `10000050` (after hyphen replacement)

### Group Prefix Detection

The processor identifies property group prefixes:

```python
def _get_property_group_prefix(self, propertyId: str) -> str:
    for group_prefix in self._property_groupings:
        if propertyId.startswith(group_prefix):
            return group_prefix
    return None
```

**Example Prefixes:**
- `CFIHOS_0`, `CFIHOS_1`, ..., `CFIHOS_9`
- `EXTENTION_0`, `EXTENTION_1`, ..., `EXTENTION_9`

---

## First-Class Citizen Entities

### Definition

First-class citizen entities are entities that:
- Are marked with `firstClassCitizen=True` in the source data
- Typically represent important, frequently-accessed entities
- Deserve dedicated containers and views

### Identification

First-class citizen status is determined from the source CFIHOS data and propagated to properties:

```python
# Mark properties as FCC if their entity is FCC
self._df_entity_properties.loc[
    self._df_entity_properties[EntityStructure.ID].isin(
        self._df_entities.loc[
            self._df_entities[EntityStructure.FIRSTCLASSCITIZEN],
            EntityStructure.ID
        ]
    ),
    EntityStructure.FIRSTCLASSCITIZEN
] = True
```

### Processing Differences

**Container Mode:**

- **First-Class Citizen (FCC) Entities:**
  - FCC entities get dedicated containers with the same name/code
  - All properties belonging to an FCC entity are grouped within that entity's individual container
  - No range-based grouping is applied to FCC properties—each FCC gets its own container

- **Non-First-Class Citizen Entities:**
  - Properties of non-FCC entities are grouped into containers by property code ranges (e.g., batches of 100 properties)
  - A group/container may contain properties from multiple non-FCC entities within the range
  - Container/group ID format typically follows `{PREFIX}_{START_CODE}_{END_CODE}` pattern (e.g., `CFIHOS_1_10000001_10000100`)
  - No dedicated container per non-FCC entity; grouping is based on numeric ranges for scalability

**View Mode:**

View models are **CFIHOS-compliant** (maintaining standard CFIHOS entity structure, hierarchy, and relationships) while **mapping all properties to containers** from the container model:

- **First-Class Citizen (FCC) Entities:**
  - Views maintain CFIHOS-compliant entity structure (hierarchy, relationships, metadata)
  - FCC entities get dedicated views with the same name/code (CFIHOS compliant structure)
  - **All properties in the view map to the FCC's own container** (created in container mode)
  - There is a strict one-to-one mapping between each FCC entity and its view
  - The view maintains CFIHOS compliance, while properties reference the dedicated container for storage

- **Non-First-Class Citizen Entities:**
  - Views maintain CFIHOS-compliant entity structure (hierarchy, relationships, metadata)
  - **All properties in the view map to grouped containers** from container mode (based on property code ranges)
  - Each property references a container corresponding to its property group's range (e.g., `CFIHOS_1_10000001_10000100`)
  - No one-to-one entity-view mapping for non-FCCs; organization corresponds to property code groupings
  - The view provides CFIHOS compliance, while properties efficiently map to grouped containers for storage

**Key Point**: Both FCC and non-FCC views are CFIHOS-compliant in structure, but all their properties map to containers (either dedicated FCC containers or grouped containers) created during container model processing. This ensures standards compliance while leveraging efficient container-based storage.

### Relationship Validation

FCC entities have special relationship validation:

- **Direct Relations**: Must target first-class citizen entities
- **Reverse Relations**: The source and the target entity, and (through property) must be first-class citizen entities
- **Edge Relations**: Both source and target must be first-class citizen entities

```python
def _validate_relation_is_eligible(self, entity_property: dict) -> bool:
    if entity_property[PropertyStructure.PROPERTY_TYPE] == Relations.DIRECT:
        # Check if target is FCC
        return not self._df_entities.loc[
            (self._df_entities[EntityStructure.ID] == target_id)
            & (self._df_entities[EntityStructure.FIRSTCLASSCITIZEN])
        ].empty
    # ... similar checks for REVERSE and EDGE
```

---

## Additional Features

### Inheritance Chain Building

The processor builds full inheritance chains for all entities:

```python
def _build_entities_full_inheritance(self):
    def get_ancestors(eid):
        parents = entity_to_parents.get(eid, [])
        ancestors = []
        for parent in parents:
            if parent in entities_with_properties:
                ancestors.append(parent)
                ancestors.extend(get_ancestors(parent))
        return ancestors
    
    self._df_entities[EntityStructure.FULL_INHERITANCE] = self._df_entities[
        EntityStructure.ID
    ].apply(get_ancestors)
```

This enables:
- Proper view inheritance
- Property inheritance detection
- Hierarchical query support

### UOM Property Extension

Properties with unit of measure (UOM) are extended with string variants:

- **Original Property**: `CFIHOS_10000005` (e.g., "Temperature")
- **UOM Variant**: `CFIHOS_10000005_UOM` (e.g., "Temperature_UOM" as String)

This allows storing both the value and its unit separately.

### Direct Relation Scalar Properties

When `add_scalar_properties_for_direct_relations=True`:

- **Relation Property**: `CFIHOS_10000005_rel` (Entity Reference)
- **Scalar Variant**: `CFIHOS_10000005` (String, stores entity ID)

This provides alternative access patterns for relationships.

### Property Row Creation

The `_create_property_row()` method handles various property variants:

- **Standard Properties**: Basic data types
- **UOM Variants**: String properties for units
- **Relationship Variants**: Scalar versions of relations
- **Edge Properties**: Graph edge relationships
- **Reverse Relations**: Bidirectional relationship properties

### EntityType Property

Non-first-class citizen containers include an `entityType` property:

- **Purpose**: Identifies which CFIHOS entity type an instance represents
- **Type**: String (required)
- **Container**: EntityTypeGroup
- **Usage**: Enables filtering instances by entity type in grouped containers

---

## Summary

The `SparsePropertiesProcessor` is a sophisticated processor that:

1. **Groups Properties Efficiently**: Reduces container count through intelligent grouping
2. **Preserves CFIHOS Structure**: Maintains standard structure in view models
3. **Handles Multiple Sources**: Processes standard CFIHOS and extensions together
4. **Supports Two Modes**: Container mode for denormalized storage, view mode for standards compliance
5. **Respects Entity Hierarchy**: Properly handles inheritance and relationships
6. **Extends Functionality**: Adds UOM variants, scalar relations, and filtering support

The processor is designed to balance efficiency (fewer containers) with usability (maintainable structure) while preserving the semantic richness of CFIHOS models.

