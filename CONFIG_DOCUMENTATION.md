# Configuration File Documentation

This document provides comprehensive guidance for configuring the CFIHOS Handler using the `config.yaml` file. The configuration file defines how CFIHOS data models are processed, structured, and organized in Cognite Data Fusion (CDF).

## Table of Contents

1. [Overview](#overview)
2. [Container Model Configuration](#container-model-configuration)
3. [Model Spaces](#model-spaces)
4. [Model Processors Configuration](#model-processors-configuration)
5. [Scopes: Defining View Models](#scopes-defining-view-models)
6. [Container Indexes](#container-indexes)
7. [Additional Configuration Options](#additional-configuration-options)
8. [Complete Example](#complete-example)

---

## Overview

The configuration file (`config.yaml`) is a YAML file that controls how CFIHOS standards are processed and transformed into NEAT data models. It supports two main processing concepts:

- **Containers**: Complete CFIHOS models processed into denormalized container structures
- **Views**: Scoped subsets of CFIHOS models processed into view structures for specific use cases

---

## Container Model Configuration

The container model configuration defines the metadata and structure for the main container data model that will be created in CDF.

### Required Fields

```yaml
container_data_model_name: "Example DEV CFIHOS 2.0 Model"
container_data_model_external_id: "EXAMPLE_DEV_CFIHOS_Model_1"
container_data_model_description: "Generated NEAT Domain Model based on CFIHOS"
container_data_model_version: "1"
container_data_model_space: "sp_example_dev_cfihos_containers"
views_data_model_space: "sp_example_dev_cfihos_views"
model_creator: "John Doe"
```

### Field Descriptions

- **`container_data_model_name`** (string, required): The human-readable name of the container data model. This appears in CDF as the model's display name.

- **`container_data_model_external_id`** (string, required): A unique identifier for the container data model in CDF. Must be unique within your CDF instance. Use a consistent naming convention (e.g., `PROJECT_CFIHOS_Model_1`).

- **`container_data_model_description`** (string, required): A descriptive text explaining the purpose and content of the data model.

- **`container_data_model_version`** (string, required): The version number of the container data model. Use semantic versioning or simple incrementing numbers (e.g., "1", "2", "1.0").

- **`model_creator`** (string, required): The name or identifier of the person or system creating the model. Used for audit and tracking purposes.

---

## Model Spaces

Model spaces are logical separators in CDF that organize different types of data models. The configuration defines two distinct spaces:

### Container Data Model Space

```yaml
container_data_model_space: "sp_example_dev_cfihos_containers"
```

- **Purpose**: This space contains the complete container data model with all CFIHOS entities and their properties.
- **Naming Convention**: Use a prefix like `sp_` followed by a descriptive name (e.g., `sp_project_cfihos_containers`).
- **Content**: Contains the full, denormalized container structures representing the entire CFIHOS model.

### Views Data Model Space

```yaml
views_data_model_space: "sp_example_dev_cfihos_views"
```

- **Purpose**: This space contains scoped view models that represent subsets of the CFIHOS standard model.
- **Naming Convention**: Similar to container space, use a descriptive prefix (e.g., `sp_project_cfihos_views`).
- **Content**: Contains multiple view data models, each defined by a scope configuration.

### Best Practices

- Use consistent naming conventions across spaces
- Include environment indicators (e.g., `dev`, `test`) in space names

---

## Model Processors Configuration

The `model_processors_config` section defines one or more CFIHOS data sources to be processed. This allows you to combine multiple CFIHOS standards (e.g., standard CFIHOS and extensions like user defined entities, or tag/equipment classes) into a single unified enterprise CFIHOS model.

### Structure

```yaml
model_processors_config:
  [
    {
      STANDARD-CFIHOS: { ... },
    },
    {
      EXTENDED-CFIHOS: { ... },
    },
  ]
```

Each processor configuration is a dictionary where the key is the processor name (e.g., `STANDARD-CFIHOS`, `EXTENDED-CFIHOS`) and the value contains the processor-specific settings.

### Standard CFIHOS Reference Configuration

To define a standard CFIHOS reference, use the `STANDARD-CFIHOS` processor:

```yaml
{
  STANDARD-CFIHOS:
    {
      source: csv,
      id_prefix: CFIHOS,
      abs_fpath_model_raw_data_folder: ./src/2.0/,
      rdl_master_objects_fname: CFIHOS CORE RDL master object v2.0.csv,
      rdl_master_object_id_col_name: CFIHOS unique code,
      rdl_master_object_name_col_name: CFIHOS name,
      rdl_master_object_file_type_col_name: CFIHOS definition file,
      included_cfihos_types_config: [ ... ],
    }
}
```

### Field Descriptions

- **`source`** (string, required): The data source location. Currently supports `csv`, which reads CFIHOS sources from local CSV files.

- **`id_prefix`** (string, required): A prefix used to identify entities from this processor. This prefix is prepended to entity IDs to ensure uniqueness when combining multiple processors (e.g., `CFIHOS`, `XOM`).

- **`abs_fpath_model_raw_data_folder`** (string, required): Absolute or relative path to the folder containing the CFIHOS CSV files. Relative paths are resolved from the configuration file's location.

- **`rdl_master_objects_fname`** (string, required): Filename of the RDL (Reference Data Library) master objects CSV file. This file contains the master list of all CFIHOS objects.

- **`rdl_master_object_id_col_name`** (string, required): Column name in the RDL master objects file that contains the unique CFIHOS code/ID.

- **`rdl_master_object_name_col_name`** (string, required): Column name in the RDL master objects file that contains the CFIHOS name.

- **`rdl_master_object_file_type_col_name`** (string, required): Column name in the RDL master objects file that indicates the definition file type.

### Included CFIHOS Types Configuration

The `included_cfihos_types_config` array defines which types of CFIHOS entities to process. Each entry specifies the file locations for a particular entity type.

#### Entity Type Configuration

```yaml
included_cfihos_types_config:
  [
    {
      type: cfihosTypeEntity,
      data_folder_abs_fpath: ./src/2.0/,
      entities_fname: CFIHOS CORE data dictionary v2.0.csv,
      entities_attrib_fname: "",  # not used in CFIHOS 2.0
      entities_attrib_relation_fname: "",  # not used in CFIHOS 2.0
      entities_core_model: CFIHOS core models.csv,
      entities_edges: "",
    },
  ]
```

#### Equipment Type Configuration

```yaml
{
  type: cfihosTypeEquipment,
  data_folder_abs_fpath: ./src/2.0/,
  entities_fname: "CFIHOS CORE equipment class v2.0.csv",
  entities_attrib_fname: "CFIHOS CORE equipment class property v2.0.csv",
  property_metadata_fname: "CFIHOS CORE property v2.0.csv",
}
```

#### Tag Type Configuration

```yaml
{
  type: cfihosTypeTag,
  data_folder_abs_fpath: ./src/2.0/,
  entities_fname: "CFIHOS CORE tag class v2.0.csv",
  entities_attrib_fname: "CFIHOS CORE tag class property v2.0.csv",
  property_metadata_fname: "CFIHOS CORE property v2.0.csv",
}
```

### Field Descriptions for Type Configuration

- **`type`** (string, required): The type of CFIHOS entity. Valid values:
  - `cfihosTypeEntity`: General CFIHOS entities
  - `cfihosTypeEquipment`: Equipment classes
  - `cfihosTypeTag`: Tag classes

- **`data_folder_abs_fpath`** (string, required): Path to the folder containing the entity type CSV files.

- **`entities_fname`** (string, required): Filename of the CSV containing entity definitions.

- **`entities_attrib_fname`** (string): Filename of the CSV containing entity attributes/properties. For CFIHOS 2.0, this is typically used for Equipment and Tag types. For Entity types in CFIHOS 2.0, this is often empty.

- **`property_metadata_fname`** (string): Filename of the CSV containing property metadata. Used for Equipment and Tag types.

- **`entities_core_model`** (string): Filename of the CSV containing core model relationships. Used for Entity types.

- **`entities_edges`** (string): Filename of the CSV containing edge/relationship definitions. May be empty for standard CFIHOS but used for extensions.

### Adding CFIHOS Extensions (E.g: EXTENTION-CFIHOS)

To add CFIHOS extensions like XOM-CFIHOS, add an additional processor configuration to the `model_processors_config` array:

```yaml
{
  EXTENTION-CFIHOS:
    {
      source: csv,
      id_prefix: EXTENTION,
      abs_fpath_model_raw_data_folder: ./src/xom 2.0/,
      rdl_master_objects_fname: EXTENTION CORE RDL master object v2.0.csv,
      rdl_master_object_id_col_name: CFIHOS unique code,
      rdl_master_object_name_col_name: CFIHOS name,
      rdl_master_object_file_type_col_name: CFIHOS definition file,
      included_cfihos_types_config:
        [
          {
            type: cfihosTypeEntity,
            data_folder_abs_fpath: ./src/xom 2.0/,
            entities_fname: EXTENTION CORE data dictionary v2.0.csv,
            entities_attrib_fname: "",
            entities_attrib_relation_fname: "",
            entities_core_model: EXTENTION CDF core data model.csv,
            entities_edges: EXTENTION Edges.csv,  # Note: extensions may include edges
          },
          # ... additional types
        ],
    }
}
```

### Key Points for Extensions

1. **Unique ID Prefix**: Use a different `id_prefix` (e.g., `EXTENTION`) to distinguish extension entities from standard CFIHOS entities.

2. **Separate Data Folder**: Point to a different folder containing the extension's CSV files.

3. **Edge Files**: Extensions may include `entities_edges` files that define additional edge relationships. Edges are not part of CFIHOS. Therefore, edges will be considered an add-on to and should be configured in the etentions rather than Standard-CFIHOS

4. **Combined Processing**: All processors in the array are processed together, creating a unified model that combines standard CFIHOS and extensions.

---

## Scopes: Defining View Models

The `scopes` section defines one or more view models, each representing a scoped subset of the standard CFIHOS model. View models are useful for creating focused data models for specific use cases (e.g., a pump model, heat exchanger model).

### Structure

```yaml
scopes:
  [
    {
      scope_model_external_id: example_centrifugal_pump,
      scope_model_version: "1",
      scope_name: Example Centrifugal Pump,
      scope_description: "CFIHOS Centrifugal Pump CFIHOS Model",
      scope_subset: [ ... ],
    },
    {
      scope_name: test_heat_exchanger,
      scope_description: "Heat Exchanger CFIHOS Model",
      scope_subset: [ ... ],
    },
  ]
```

### Field Descriptions

- **`scope_name`** (string, required): A unique identifier for the scope. This is used when processing view models to select which scope to build. Must be unique within the configuration. Addtionaly, the scope_name will used to define the data model name in CDF

- **`scope_model_external_id`** (string, required for view generation): The external ID for the view data model in CDF. This will be the identifier of the generated view data model.

- **`scope_model_version`** (string, required for view generation): The version number of the view data model.

- **`scope_description`** (string, optional but recommended): A description of what the scope represents and its intended use case. Will be refelected in the description of the views data model

- **`scope_subset`** (array of strings, required): A list of CFIHOS entity IDs to include in this scope. The system will:
  1. Include the specified entities
  2. Automatically include all parent entities (through inheritance)
  3. Include all dependent entities (through relationships)
  4. Build a complete, self-contained view model

### Scope Subset IDs

The `scope_subset` array contains CFIHOS entity identifiers. These can be:

- **Standard CFIHOS IDs**: These must be prefixed with the processor's `id_prefix`. Examples: `CFIHOS_00000001`, `TCFIHOS_30000521`, `ECFIHOS_30000521`, `ECFIHOS_30000390`.
- **Extension IDs**: These must be prefixed with the extension's `id_prefix`. Examples: `EXTENTION_00000001`, `EXTENTION_00000004`.
- **Tag vs. Equipment Classes**: Tag and Equipment classes may share the same CFIHOS code number. To distinguish between them:
    - Prefix with `T` for Tag classes (e.g., `TCFIHOS_30000521`)
    - Prefix with `E` for Equipment classes (e.g., `ECFIHOS_30000521`)
    - For example: to include a Tag Centrifugal Pump, use `TCFIHOS_30000521`; to include an Equipment Centrifugal Pump, use `ECFIHOS_30000521`.
- **Entities (0xx series)**: For entities in the 0xx series, you do not need to add an extra tag or equipment class prefix. Use the standard CFIHOS prefix only (for example: `CFIHOS_00000001`, `CFIHOS_00000003`).
- **Formatting Requirement**: Always use an underscore (`_`) to separate the prefix and the code number (correct: `TCFIHOS_30000521`; incorrect: `TCFIHOS-30000521`). Do not use hyphens.
- **Summary**: When listing IDs in `scope_subset`, double-check:
    - The correct prefix (`T`, `E`, or your extension's prefix)
    - Underscore separation—never hyphens (For all Tag classes, Equipment classes and Entities)
    - The number matches the official CFIHOS code or your extension code

### Example: Multiple View Models

```yaml
scopes:
  [
    {
      scope_model_external_id: example_centrifugal_pump,
      scope_model_version: "1",
      scope_name: Example Centrifugal Pump,
      scope_description: "CFIHOS Centrifugal Pump CFIHOS Model",
      scope_subset:
        [
          TCFIHOS_30000521,  # Tag class for centrifugal pump
          CFIHOS_00000003,  # Area entity
          EXTENTION_00000001,      # Extension entity
          EXTENTION_00000004,      # Extension entity
        ],
    },
    {
      scope_model_external_id: example_heat_exchanger,
      scope_model_version: "1",
      scope_name: test_heat_exchanger,
      scope_description: "Heat Exchanger CFIHOS Model",
      scope_subset:
        [
          TCFIHOS_30000390,  # Tag class for heat exchanger
          ECFIHOS_30000390,  # Equipment class for heat exchanger
        ],
    },
  ]
```

### How Scopes Work

When a scope is processed:

1. **Entity Selection**: The system starts with the entities listed in `scope_subset`.

2. **Dependency Resolution**: The system automatically expands the list of included entities from `scope_subset` by resolving all dependencies necessary for a complete, connected model. This process works in two ways:
   - **All parent entities (inheritance chain)**: If an entity inherits from another (its parent), that parent entity is also included—even if not explicitly listed. This ensures class hierarchies are preserved in the view model.
   - **All related entities through relationships**: Any entities linked via relationships are automatically added if they are not already present.

   **For example:**
   - If you include `CFIHOS_00000001` (site) in your `scope_subset`, and `CFIHOS_00000075` (measurement system) is related to that site through a defined relationship, then `CFIHOS_00000075` will be automatically included in the view model even though you did not explicitly add it.
   - Similarly, if you add `TCFIHOS_30000521` (Tag Centrifugal Pump) to the scope, and it inherits from `TCFIHOS_30000550` (Tag Pump) in the type hierarchy, then `TCFIHOS_30000550` will be automatically included as part of the dependency resolution.

   In summary, specifying an entity in the scope will pull in all the required parents and related (dependent) entities so the final view model is self-contained and functional.

3. **View Generation**: A complete view data model is created containing only the scoped entities and their dependencies, stored in the `views_data_model_space`.

4. **Usage**: To generate a view model, specify the `scope_name` when processing with `model_type="views"`.

---

## Container Indexes

The `containers_indexes` section defines database indexes for container properties. Indexes improve query performance for frequently accessed properties and relationships.

### Structure

```yaml
containers_indexes:
  {
    ContainerName:
      [
        {
          index_id: CFIHOS_10000005_rel_index,
          index_type: btree,
          cursorable: false,
          properties: [CFIHOS_10000005_rel],
        },
        {
          index_id: CFIHOS_10000005_index,
          index_type: btree,
          cursorable: false,
          properties: [CFIHOS_10000005],
        },
      ],
    AnotherContainerName: [ ... ],
  }
```

### Field Descriptions

- **Container Key** (string, required): The name of the container to index. This can be:
  - A container name (e.g., `Area`)
  - A CFIHOS entity ID (e.g., `CFIHOS_00000003`)
  - A CFIHOS entity ID range (e.g., `CFIHOS_1_10000001_10000100`)
  
  **Note**: For simplicity, you can specify either the CFIHOS code (e.g., `CFIHOS_00000003`) or the human-readable name of the container (e.g., `Area`) as the container key.

- **`index_id`** (string, required): A unique identifier for the index within the container. This will be the index name in CDF.

- **`index_type`** (string, required): The type of index to create. Currently supports:
  - `btree`: B-tree index (most common, supports range queries)
  - `inverse`: Inverse index (used to index non scalar properties like "lists")

- **`cursorable`** (boolean, required): Whether the index supports cursor-based pagination.

- **`properties`** (array of strings, required): List of property names to include in the index. Can include:
  - Property IDs (e.g., `CFIHOS_10000005`)
  - Relationship property IDs (e.g., `CFIHOS_10000005_rel`)
  - Property names (e.g., `area_code_rel`)
> **Note:** For simplicity, you may define either CFIHOS codes (e.g., `CFIHOS_10000005`) *or* human-readable property names (e.g., `area_code`) forproperty keys throughout your `properties` configuration.

### Example: Multiple Container Indexes

```yaml
containers_indexes:
  {
    # Index for a named container
    Area:
      [
        {
          index_id: CFIHOS_10000005_rel_index,
          index_type: btree,
          cursorable: false,
          properties: [CFIHOS_10000005_rel],
        },
      ],
    
    # Index for a specific CFIHOS entity
    CFIHOS_00000031:
      [
        {
          index_id: CFIHOS_10000005_index,
          index_type: btree,
          cursorable: false,
          properties: [CFIHOS_10000005],
        },
      ],
    
    # Index for a wide container
    CFIHOS_1_10000001_10000100:
      [
        {
          index_id: area_code_rel_index,
          index_type: btree,
          cursorable: false,
          properties: [area_code_rel],
        },
      ],
  }
```

### Best Practices for Indexes

1. **Index Frequently Queried Properties**: Add indexes for properties used in common queries or filters.

2. **Index Relationship Properties**: Relationship properties (`*_rel`) are often good candidates for indexing.

3. **Avoid Over-Indexing**: Too many indexes can slow down write operations. Index only what's necessary.

4. **Use Descriptive Index IDs**: Choose clear, consistent naming for `index_id` values.

---

## Additional Configuration Options

### Processor Type

```yaml
processor_type: sparse
```

- **Purpose**: Specifies which processor implementation to use for model processing.
- **Valid Values**: Currently supports `sparse` (sparse properties processor).
- **Required**: Yes

### DMS Identifier

```yaml
dms_identifire: "cfihos_name"  # or "cfihos_code"
```

- **Purpose**: Determines which identifier is used as the primary identifier in the generated data model.
- **Valid Values**:
  - `cfihos_name`: Uses CFIHOS names as identifiers (more human-readable)
  - `cfihos_code`: Uses CFIHOS codes as identifiers (more stable, code-based)
- **Required**: Yes
- **Impact**: This affects how entities and properties are identified in the generated model. Choose based on whether you prefer human-readable names or stable code-based identifiers.

### Add Scalar Properties for Direct Relations

```yaml
add_scalar_properties_for_direct_relations: False
```

- **Purpose**: When `True`, creates additional scalar properties for direct relationships, providing alternative access patterns.
- **Default**: `False`
- **Required**: Yes

---

## Complete Example

Here's a complete example configuration file combining all the concepts:

```yaml
{
  # Container Model Configuration
  container_data_model_name: "Example DEV CFIHOS 2.0 Model",
  container_data_model_external_id: "EXAMPLE_DEV_CFIHOS_Model_1",
  container_data_model_description: "Generated NEAT Domain Model based on CFIHOS",
  container_data_model_version: "1",
  container_data_model_space: "sp_example_dev_cfihos_containers",
  views_data_model_space: "sp_example_dev_cfihos_views",
  model_creator: "John Doe",
  
  # Processor Configuration
  processor_type: sparse,
  dms_identifire: "cfihos_name",
  add_scalar_properties_for_direct_relations: False,
  
  # Model Processors: Standard CFIHOS + Extension
  model_processors_config:
    [
      {
        STANDARD-CFIHOS:
          {
            source: csv,
            id_prefix: CFIHOS,
            abs_fpath_model_raw_data_folder: ./src/2.0/,
            rdl_master_objects_fname: CFIHOS CORE RDL master object v2.0.csv,
            rdl_master_object_id_col_name: CFIHOS unique code,
            rdl_master_object_name_col_name: CFIHOS name,
            rdl_master_object_file_type_col_name: CFIHOS definition file,
            included_cfihos_types_config:
              [
                {
                  type: cfihosTypeEntity,
                  data_folder_abs_fpath: ./src/2.0/,
                  entities_fname: CFIHOS CORE data dictionary v2.0.csv,
                  entities_attrib_fname: "",
                  entities_attrib_relation_fname: "",
                  entities_core_model: CFIHOS core models.csv,
                  entities_edges: "",
                },
                {
                  type: cfihosTypeEquipment,
                  data_folder_abs_fpath: ./src/2.0/,
                  entities_fname: "CFIHOS CORE equipment class v2.0.csv",
                  entities_attrib_fname: "CFIHOS CORE equipment class property v2.0.csv",
                  property_metadata_fname: "CFIHOS CORE property v2.0.csv",
                },
                {
                  type: cfihosTypeTag,
                  data_folder_abs_fpath: ./src/2.0/,
                  entities_fname: "CFIHOS CORE tag class v2.0.csv",
                  entities_attrib_fname: "CFIHOS CORE tag class property v2.0.csv",
                  property_metadata_fname: "CFIHOS CORE property v2.0.csv",
                },
              ],
          }
      },
      {
        EXTENTION-CFIHOS:
          {
            source: csv,
            id_prefix: EXTENTION,
            abs_fpath_model_raw_data_folder: ./src/EXTENTION 2.0/,
            rdl_master_objects_fname: EXTENTION CORE RDL master object v2.0.csv,
            rdl_master_object_id_col_name: CFIHOS unique code,
            rdl_master_object_name_col_name: CFIHOS name,
            rdl_master_object_file_type_col_name: CFIHOS definition file,
            included_cfihos_types_config:
              [
                {
                  type: cfihosTypeEntity,
                  data_folder_abs_fpath: ./src/EXTENTION 2.0/,
                  entities_fname: EXTENTION CORE data dictionary v2.0.csv,
                  entities_attrib_fname: "",
                  entities_attrib_relation_fname: "",
                  entities_core_model: EXTENTION CDF core data model.csv,
                  entities_edges: EXTENTION Edges.csv,
                },
                {
                  type: cfihosTypeEquipment,
                  data_folder_abs_fpath: ./src/EXTENTION 2.0/,
                  entities_fname: "EXTENTION CORE equipment class v2.0.csv",
                  entities_attrib_fname: "EXTENTION CORE equipment class property v2.0.csv",
                  property_metadata_fname: "EXTENTION CORE property v2.0.csv",
                },
                {
                  type: cfihosTypeTag,
                  data_folder_abs_fpath: ./src/EXTENTION 2.0/,
                  entities_fname: "EXTENTION CORE tag class v2.0.csv",
                  entities_attrib_fname: "EXTENTION CORE tag class property v2.0.csv",
                  property_metadata_fname: "EXTENTION CORE property v2.0.csv",
                },
              ],
          }
      },
    ],
  
  # Scopes: Multiple View Models
  scopes:
    [
      {
        scope_model_external_id: example_centrifugal_pump,
        scope_model_version: "1",
        scope_name: Example Centrifugal Pump,
        scope_description: "CFIHOS Centrifugal Pump CFIHOS Model",
        scope_subset:
          [
            TCFIHOS_30000521,
            EXTENTION_00000001,
            EXTENTION_00000004,
          ],
      },
      {
        scope_model_external_id: example_heat_exchanger,
        scope_model_version: "1",
        scope_name: test_heat_exchanger,
        scope_description: "Heat Exchanger CFIHOS Model",
        scope_subset:
          [
            TCFIHOS_30000390,
            ECFIHOS_30000390,
          ],
      },
    ],
  
  # Container Indexes
  containers_indexes:
    {
      Area:
        [
          {
            index_id: CFIHOS_10000005_rel_index,
            index_type: btree,
            cursorable: false,
            properties: [CFIHOS_10000005_rel],
          },
          {
            index_id: CFIHOS_10000002_index,
            index_type: btree,
            cursorable: false,
            properties: [CFIHOS_10000002],
          },
        ],
      CFIHOS_00000031:
        [
          {
            index_id: CFIHOS_10000005_index,
            index_type: btree,
            cursorable: false,
            properties: [CFIHOS_10000005],
          },
        ],
    }
}
```

---

## Validation and Error Handling

The configuration file is validated when loaded. Common validation errors include:

- **Missing Required Fields**: All required fields must be present
- **Invalid Processor Type**: Must be a supported processor type (e.g., `sparse`)
- **Invalid DMS Identifier**: Must be either `cfihos_name` or `cfihos_code`
- **Missing Scope Fields**: When generating views, scopes must have `scope_name`, `scope_model_external_id`, and `scope_model_version`
- **File Not Found**: CSV files referenced in `model_processors_config` must exist

---

## Tips and Best Practices

1. **Version Control**: Keep your configuration files in version control to track changes over time.

2. **Environment-Specific Configs**: Use separate configuration files for different environments (dev, test, prod).

3. **Relative Paths**: Use relative paths for data folders to make configurations portable across machines.

4. **Naming Conventions**: Establish consistent naming conventions for:
   - Model external IDs
   - Space names
   - Scope names
   - Index IDs

5. **Incremental Testing**: Start with a single processor and simple scope, then gradually add complexity.

6. **Documentation**: Add comments in your configuration file (using YAML comment syntax `#`) to explain non-obvious choices.

7. **Scope Design**: Design scopes based on actual use cases. Each scope should represent a meaningful subset of the model.

8. **Index Strategy**: Add indexes based on actual query patterns. Monitor query performance and adjust indexes accordingly.

---

## Related Documentation

- See `README.md` for general usage instructions
- See `README_PYPI.md` for installation and basic usage
- Refer to CFIHOS standard documentation for entity and property definitions

