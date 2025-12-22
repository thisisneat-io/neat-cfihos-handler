# Model Managers Documentation

This document provides comprehensive documentation for the CFIHOS Model Managers framework, which orchestrates the processing of CFIHOS data models and provides a flexible, extensible architecture for different processing strategies.

## Table of Contents

1. [Overview](#overview)
2. [General Structure](#general-structure)
3. [Base Manager Architecture](#base-manager-architecture)
4. [Manager Registration](#manager-registration)
5. [Dynamic Loading from Registry](#dynamic-loading-from-registry)
6. [Sparse Model Manager](#sparse-model-manager)
7. [Creating a New Model Manager](#creating-a-new-model-manager)

---

## Overview

Model Managers are the orchestration layer in the CFIHOS Handler framework. They coordinate between configuration, processors, and the final data model output. The framework uses a registry-based system that allows for dynamic loading and extensibility of different processing strategies.

### Key Concepts

- **Model Manager**: Orchestrates the processing of CFIHOS data models using a specific processor
- **Processor**: Implements the actual data transformation logic (e.g., `SparsePropertiesProcessor`)
- **Registry**: Maps processor type names to their corresponding manager class implementations
- **Provider**: Dynamically loads and instantiates managers based on configuration

### Architecture Flow

```
Configuration File (config.yaml)
    ↓
CFIHOSImporter
    ↓
CfihosManagerProvider (reads registry)
    ↓
Dynamic Manager Loading
    ↓
Model Manager (e.g., SparseCfihosManager)
    ↓
Processor (e.g., SparsePropertiesProcessor)
    ↓
ReadResult (Properties, Containers, Views, Metadata)
```

---

## General Structure

### Directory Structure

```
model_managers/
├── __init__.py
├── base_cfihos_manager.py      # Base class for all managers
├── model_manager_provider.py   # Registry and dynamic loading
└── sparse_model_manager.py     # Sparse processing implementation
```

### Component Relationships

```
BaseCfihosManager (Abstract Base Class)
    ├── SparseCfihosManager (Concrete Implementation)
    └── [Future Managers] (e.g., HybridCfihosManager)

CfihosManagerProvider
    ├── Registry (CFIHOS_PROCESSOR_REGISTRY)
    └── Dynamic Class Resolution
```

### Key Components

1. **BaseCfihosManager**: Abstract base class defining the interface all managers must implement
2. **ReadResult**: Data structure containing the processed model output
3. **CfihosManagerProvider**: Factory class that loads managers from the registry
4. **CFIHOS_PROCESSOR_REGISTRY**: Dictionary mapping processor names to manager class paths

---

## Base Manager Architecture

### BaseCfihosManager Class

The `BaseCfihosManager` is an abstract base class that defines the interface all model managers must implement.

#### Class Definition

```python
class BaseCfihosManager:
    """Base class for CFIHOS data managers."""
    
    def __init__(self, processor_config: dict):
        """Initialize the base CFIHOS manager."""
        self.processor_config = processor_config
        self.processor_issue_list = IssueList()
    
    def _validate_config(self):
        """Validate the configuration dictionary."""
        raise NotImplementedError(...)
    
    def read_model(self) -> None | ReadResult:
        """Read and process the model data."""
        raise NotImplementedError(...)
```

#### Required Methods

**`_validate_config()`**
- **Purpose**: Validates that the configuration dictionary contains all required keys
- **Implementation**: Must be implemented by subclasses
- **Raises**: `NeatValueError` if required configuration is missing

**`read_model()`**
- **Purpose**: Main entry point for processing the CFIHOS model
- **Returns**: `ReadResult` object containing processed model data, or `None`
- **Implementation**: Must be implemented by subclasses with specific processing logic

#### ReadResult Structure

The `ReadResult` dataclass contains the processed model output:

```python
@dataclass
class ReadResult:
    """Result structure for CFIHOS data loading operations."""
    
    Properties: list[dict]      # List of property definitions
    Containers: list[dict]      # List of container definitions
    Views: list[dict]           # List of view definitions
    Metadata: dict              # Model metadata (name, version, space, etc.)
```

**Metadata Dictionary Structure:**
```python
{
    "role": "DMS Architect",
    "dataModelType": "enterprise",
    "schema": "complete",
    "space": "sp_container_space",      # Container or views space
    "name": "Model Name",                # Data model name
    "description": "Model Description",  # Data model description
    "external_id": "MODEL_EXTERNAL_ID", # Unique model identifier
    "version": "1",                      # Model version
    "creator": "Creator Name"            # Model creator
}
```

### Design Principles

1. **Separation of Concerns**: Managers orchestrate, processors transform
2. **Extensibility**: New managers can be added without modifying existing code
3. **Configuration-Driven**: Processing behavior is controlled through configuration
4. **Type Safety**: All managers must inherit from `BaseCfihosManager`

---

## Manager Registration

Managers are registered in the `CFIHOS_PROCESSOR_REGISTRY` constant located in `cognite/neat_cfihos_handler/framework/common/constants.py`.

### Registry Structure

```python
CFIHOS_PROCESSOR_REGISTRY = {
    "sparse": "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparseCfihosManager",
    # Add more when implemented:
    # "hybrid": "cognite.neat_cfihos_handler.framework.processing.model_managers.hybrid_model_manager.HybridCfihosManager",
    # ...
}
```

### Registration Format

- **Key**: Processor type name (string) - used in configuration files as `processor_type`
- **Value**: Fully qualified class path (string) - module path and class name

### Registration Requirements

1. **Unique Keys**: Each processor's manger type name must be unique
2. **Valid Path**: The class path must be importable
3. **Valid Class**: The class must inherit from `BaseCfihosManager`
4. **Naming Convention**: Use lowercase, descriptive names (e.g., `sparse`, `hybrid`)

### Example: Registering a New Manager

```python
# In constants.py
CFIHOS_PROCESSOR_REGISTRY = {
    "sparse": "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparseCfihosManager",
    "hybrid": "cognite.neat_cfihos_handler.framework.processing.model_managers.hybrid_model_manager.HybridCfihosManager",
}
```

Then use in configuration:
```yaml
processor_type: hybrid  # Must match the registry key
```

---

## Dynamic Loading from Registry

The `CfihosManagerProvider` class handles dynamic loading of managers from the registry using Python's `importlib` module.

### Provider Class

```python
class CfihosManagerProvider:
    """Provider for CFIHOS manager instances."""
    
    _registry: dict[str, type[BaseCfihosManager]] = {
        name: _resolve_manager_class(path)
        for name, path in CFIHOS_PROCESSOR_REGISTRY.items()
    }
    
    def __init__(self, manager_name: str, processor_config: dict, **kwargs):
        """Initialize the CFIHOS manager provider."""
        if manager_name not in self._registry:
            available = ", ".join(sorted(self._registry.keys()))
            raise NeatValueError(
                f"Unknown CFIHOS manager: {manager_name}. Available: {available}"
            )
        manager_cls = self._registry[manager_name]
        self.manager: BaseCfihosManager = manager_cls(processor_config, **kwargs)
    
    def get_manager(self) -> BaseCfihosManager:
        """Get the configured manager instance."""
        return self.manager
```

### Dynamic Class Resolution

The `_resolve_manager_class()` function dynamically imports and validates manager classes:

```python
def _resolve_manager_class(path: str) -> type[BaseCfihosManager]:
    """Resolve a manager class from its fully qualified path.
    
    Args:
        path: Fully qualified class path (e.g., "module.path.ClassName")
    
    Returns:
        The manager class (subclass of BaseCfihosManager)
    
    Raises:
        NeatValueError: If path is invalid or class is not a BaseCfihosManager
    """
    module_path, _, class_name = path.rpartition(".")
    if not module_path or not class_name:
        raise NeatValueError(f"Invalid manager path: {path}")
    
    # Dynamically import the module
    module = import_module(module_path)
    
    # Get the class from the module
    cls = getattr(module, class_name)
    
    # Validate it's a BaseCfihosManager subclass
    if not issubclass(cls, BaseCfihosManager):
        raise NeatValueError(f"Manager '{class_name}' is not a BaseCfihosManager")
    
    return cls
```

### Loading Process

1. **Registry Initialization**: When `CfihosManagerProvider` is first accessed, it builds the registry by resolving all class paths
2. **Path Parsing**: The fully qualified path is split into module path and class name
3. **Dynamic Import**: The module is imported using `importlib.import_module()`
4. **Class Retrieval**: The class is retrieved from the module using `getattr()`
5. **Validation**: The class is validated to ensure it inherits from `BaseCfihosManager`

### Error Handling

The provider handles several error cases:

- **Unknown Manager**: Raises `NeatValueError` with available manager names
- **Invalid Path**: Raises `NeatValueError` if the path format is incorrect
- **Import Errors**: Python's import system raises `ImportError` if module/class not found
- **Type Validation**: Raises `NeatValueError` if class doesn't inherit from `BaseCfihosManager`

### Usage Flow

```python
# 1. Configuration specifies processor type
config = {"processor_type": "sparse", ...}

# 2. Provider is created with manager name
provider = CfihosManagerProvider(
    manager_name=config["processor_type"],
    processor_config=config,
    **kwargs
)

# 3. Manager is retrieved
manager = provider.get_manager()

# 4. Manager processes the model
result = manager.read_model()
```

---

## Sparse Model Manager

The `SparseCfihosManager` is the current implementation that orchestrates sparse property processing for CFIHOS models.

### Class Overview

```python
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
        # ... initialization logic
```

### Initialization Process

1. **Configuration Validation**: Validates required configuration keys
2. **Parameter Validation**: Validates `model_type` and `scope` parameters
3. **Processor Initialization**: Creates and runs `SparsePropertiesProcessor`
4. **Data Extraction**: Extracts processed entities, properties, and mappings
5. **Scope Indexing**: Builds scope lookup dictionary for view generation

### Key Attributes

- **`model_type`**: Processing mode (`"containers"` or `"views"`)
- **`scope`**: Scope name for view generation (required when `model_type="views"`)
- **`model_processor`**: Instance of `SparsePropertiesProcessor`
- **`_model_entities`**: Dictionary of processed entities
- **`_model_properties`**: Dictionary of processed properties
- **`_scopes_by_name`**: Dictionary mapping scope names to scope configurations
- **`_containers_indexes`**: Container index definitions from configuration

### Configuration Requirements

The manager requires the following configuration keys:

```python
required_keys = [
    "model_processors_config",           # CFIHOS data source configurations
    "containers_indexes",                # Container index definitions
    "container_data_model_space",        # Container model space
    "views_data_model_space",            # Views model space
    "model_creator",                     # Creator name
    "container_data_model_version",      # Model version    
    "container_data_model_name",         # Model name
    "container_data_model_description",  # Model description
    "container_data_model_external_id",  # Model external ID
    "dms_identifire",                    # DMS identifier type
    "scope_config",                      # Scope configuration
    "processor_type",                    # Processor type (must be "sparse")
    "add_scalar_properties_for_direct_relations",  # Feature flag
]
```

### Processing Methods

#### `_build_containers_model()`

Builds the container model from processed entities:

```python
def _build_containers_model(self) -> ReadResult:
    """Build container model from processed entities."""
    lst_views, lst_properties, lst_containers = build_neat_model_from_entities(
        entities=self._model_entities,
        dms_identifire=self.dms_identifire,
        include_containers=True,
        include_cdm=True,
        containers_indexes=self._containers_indexes,
        containers_space=self._container_space,
        force_code_as_view_id=False,
    )
    
    return ReadResult(
        Properties=lst_properties,
        Containers=lst_containers,
        Views=lst_views,
        Metadata={...}
    )
```

**Key Features:**
- Includes containers in the model
- Includes CDM (Cognite Data Model) concepts
- Applies container indexes
- Uses container space for container references

#### `_build_scoped_views_models(scope)`

Builds a scoped view model for a specific scope:

```python
def _build_scoped_views_models(self, scope) -> ReadResult:
    """Build scoped view model for a specific scope."""
    # Get scope configuration
    views_scope = self._scopes_by_name.get(scope)
    
    # Collect model subset based on scope
    scoped_model = collect_model_subset(
        full_model=self.model_processor.model_entities,
        scope=views_scope["scope_subset"],
        containers_space=self._container_space,
    )
    
    # Build views from scoped entities
    lst_entity_views, lst_entity_properties, [] = build_neat_model_from_entities(
        containers_space=self._container_space,
        entities=scoped_model,
        dms_identifire=self.dms_identifire,
        include_containers=False,  # Views don't include containers
        include_cdm=True,
    )
    
    return ReadResult(
        Properties=lst_entity_properties,
        Containers=[],  # Views don't have containers
        Views=lst_entity_views,
        Metadata={...}
    )
```

**Key Features:**
- Collects entities based on scope subset
- Automatically includes dependencies (parents, relationships)
- Creates CFIHOS-compliant views
- Maps properties to containers from container model
- Uses views space for view references

### Main Entry Point: `read_model()`

The `read_model()` method is the main entry point that routes to the appropriate processing method:

```python
def read_model(self) -> None | ReadResult:
    """Read and process the model according to the configured model_type."""
    if self.model_type == SparseModelType.CONTAINERS:
        return self._build_containers_model()
    elif self.model_type == SparseModelType.VIEWS:
        return self._build_scoped_views_models(self.scope)
    else:
        return None
```

### Validation Methods

#### `_validate_config()`

Validates that all required configuration keys are present:

```python
def _validate_config(self) -> None:
    """Validate the configuration dictionary."""
    required_keys = [...]
    missing_keys = [key for key in required_keys if key not in self.processor_config]
    if missing_keys:
        raise NeatValueError(f"Missing required keys: {', '.join(missing_keys)}")
```

#### `_validate_important_parameters()`

Validates `model_type` and `scope` parameters:

```python
def _validate_important_parameters(self) -> None:
    """Validate model_type and scope parameters."""
    # Validate model_type is not empty
    if not self.model_type or self.model_type.strip() == "":
        raise NeatValueError("model_type cannot be None or empty string")
    
    # Validate model_type has valid values
    valid_model_types = [SparseModelType.CONTAINERS, SparseModelType.VIEWS]
    if self.model_type not in valid_model_types:
        raise NeatValueError(f"Invalid model_type: {self.model_type}")
    
    # Validate scope is provided for views
    if self.model_type == SparseModelType.VIEWS:
        if not self.scope or self.scope.strip() == "":
            raise NeatValueError("scope is required when model_type is 'views'")
```

### Integration with SparsePropertiesProcessor

The manager delegates actual processing to `SparsePropertiesProcessor`:

1. **Initialization**: Creates processor with configuration
2. **Processing**: Calls `process_and_collect_models()` to process CFIHOS data
3. **Data Extraction**: Extracts processed entities, properties, and mappings
4. **Model Building**: Uses processed data to build NEAT model structures

---

## Creating a New Model Manager

To create a new model manager, follow these steps:

### Step 1: Create the Manager Class

Create a new file (e.g., `hybrid_model_manager.py`) in the `model_managers` directory:

```python
from cognite.neat.core._issues.errors import NeatValueError
from cognite.neat_cfihos_handler.framework.processing.model_managers.base_cfihos_manager import (
    BaseCfihosManager,
    ReadResult,
)
from cognite.neat_cfihos_handler.framework.processing.processors.your_processor import (
    YourProcessor,
)

class HybridCfihosManager(BaseCfihosManager):
    """Hybrid CFIHOS manager for processing hybrid data models."""
    
    def __init__(self, processor_config: dict, **kwargs):
        """Initialize the hybrid CFIHOS manager."""
        super().__init__(processor_config)
        self._validate_config()
        
        # Extract configuration
        self._model_name = processor_config["data_model_name"]
        # ... extract other configuration
        
        # Initialize processor
        self.processor = YourProcessor(**processor_config)
        self.processor.process_and_collect_models()
        
        # Extract processed data
        self._model_entities = self.processor.model_entities
        self._model_properties = self.processor.model_properties
    
    def _validate_config(self) -> None:
        """Validate the configuration dictionary."""
        required_keys = [
            "data_model_name",
            # ... other required keys
        ]
        missing_keys = [
            key for key in required_keys 
            if key not in self.processor_config
        ]
        if missing_keys:
            raise NeatValueError(
                f"Missing required keys: {', '.join(missing_keys)}"
            )
    
    def read_model(self) -> ReadResult:
        """Read and process the model."""
        # Implement your processing logic
        # ...
        
        return ReadResult(
            Properties=lst_properties,
            Containers=lst_containers,
            Views=lst_views,
            Metadata={
                "role": "DMS Architect",
                "dataModelType": "enterprise",
                "schema": "complete",
                "space": self._model_space,
                "name": self._model_name,
                "description": self._model_description,
                "external_id": self._model_external_id,
                "version": self._model_version,
                "creator": self._model_creator,
            },
        )
```

### Step 2: Register in Registry

Add your manager to `CFIHOS_PROCESSOR_REGISTRY` in `constants.py`:

```python
CFIHOS_PROCESSOR_REGISTRY = {
    "sparse": "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparseCfihosManager",
    "hybrid": "cognite.neat_cfihos_handler.framework.processing.model_managers.hybrid_model_manager.HybridCfihosManager",
}
```

### Step 3: Create Processor 

Create your custom processor following the `BaseProcessor` pattern (see `SparsePropertiesProcessor` documentation).

### Step 4: Update Configuration

Users can now use your manager in their configuration:

```yaml
processor_type: hybrid  # Must match registry key
data_model_name: "Hybrid CFIHOS Model"
# ... other configuration
```

### Best Practices

1. **Inherit from BaseCfihosManager**: Always inherit from the base class
2. **Validate Configuration**: Implement thorough configuration validation
3. **Error Handling**: Provide clear error messages for missing/invalid configuration
4. **Documentation**: Document your manager's specific requirements and behavior
5. **Testing**: Create unit tests for your manager
6. **Consistent Interface**: Follow the same patterns as `SparseCfihosManager`

---

## Summary

The Model Managers framework provides:

1. **Extensible Architecture**: Easy to add new processing strategies
2. **Dynamic Loading**: Managers loaded from registry at runtime
3. **Type Safety**: All managers inherit from `BaseCfihosManager`
4. **Configuration-Driven**: Behavior controlled through configuration files
5. **Separation of Concerns**: Managers orchestrate, processors transform
6. **Standardized Output**: All managers return `ReadResult` with consistent structure

The framework enables the CFIHOS Handler to support multiple processing strategies while maintaining a consistent interface and extensible architecture.

