# CFIHOS Handler Plugin

## Overview

The CFIHOS Handler is a flexible framework that supports multiple processor implementations for handling CFIHOS industrial standard data. Processors can be added and configured through the `CFIHOS_PROCESSOR_REGISTRY` in the constants file, allowing for extensible processing capabilities.

## Environment Setup

Install dependencies using UV:

```bash
uv sync --all-extras
```

This will install all required dependencies including optional extras for the project.



## Architecture

The CFIHOS processor architecture consists of:

1. **Processors**: Core processing logic that handles CFIHOS data transformation (e.g., `sparse_properties.py`)
2. **Model Managers**: Classes that orchestrate processors and manage model lifecycle (e.g., `sparse_model_manager.py`)
3. **Registry**: Configuration mapping processor types to their model manager implementations

Each processor requires a corresponding model manager that loads and orchestrates the processor. For example, `SparseCfihosManager` (in `cfihos_handler/framework/processing/model_managers/sparse_model_manager.py`) loads the `SparsePropertiesProcessor` (in `cfihos_handler/framework/processing/processors/sparse_properties.py`).

## Available Processors

### Sparse Properties Processor

The `sparse_properties_processor` is the currently available processor. It is based on two processing concepts:

1. **Containers**: Processes the complete CFIHOS model into container structures
2. **Views**: Processes scoped subsets of the CFIHOS model into view structures

Users select which concept to process by passing the `model_type` parameter:
- `model_type="containers"` - for container-based processing
- `model_type="views"` - for view-based processing (requires a `scope` parameter)

**Examples:**
- See `examples/cfihos_plugin_containers_debugging.ipynb` for containers processing
- See `examples/cfihos_plugin_views_debugging.ipynb` for views processing

## Adding a New Processor

To add a new processor implementation, follow these steps:

### Step 1: Register the Processor

Add your processor to the `CFIHOS_PROCESSOR_REGISTRY` in `cfihos_handler/framework/common/constants.py`:

```python
CFIHOS_PROCESSOR_REGISTRY = {
    "sparse": "cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparseCfihosManager",
    "your_processor_name": "cfihos_handler.framework.processing.model_managers.your_manager.YourCfihosManager",
    # Add more when implemented:
    # "hybrid": "cfihos_handler.framework.processing.model_managers.hybrid_model_manager.HybridCfihosManager",
}
```

### Step 2: Create the Model Manager

Create a new model manager class that:
- Inherits from `BaseCfihosManager` (located in `cfihos_handler/framework/processing/model_managers/base_cfihos_manager.py`)
- Implements the required abstract methods
- Loads and orchestrates your processor

Example structure:
```python
from cfihos_handler.framework.processing.model_managers.base_cfihos_manager import (
    BaseCfihosManager,
    ReadResult,
)
from cfihos_handler.framework.processing.processors.your_processor import (
    YourProcessor,
)

class YourCfihosManager(BaseCfihosManager):
    def __init__(self, processor_config: dict, **kwargs):
        super().__init__(processor_config)
        self.processor = YourProcessor(**processor_config)
        # ... initialization logic
    
    def read_model(self) -> ReadResult:
        # ... processing logic
        return ReadResult(...)
```

### Step 3: Create the Processor

Create your processor class that:
- Inherits from `BaseProcessor` (located in `cfihos_handler/framework/processing/processors/base_processor.py`)
- Implements the required processing logic

### Step 4: Configure in Config File

Configure your processor in the configuration YAML file (e.g., `examples/configuration/config.yaml`):

```yaml
processor_type: your_processor_name  # Must match the key in CFIHOS_PROCESSOR_REGISTRY
data_model_name: "Your CFIHOS Model"
data_model_external_id: "YOUR_CFIHOS_MODEL_1"
data_model_description: "Generated NEAT Domain Model based on CFIHOS"
container_data_model_space: "sp_your_cfihos_containers"
views_data_model_space: "sp_your_cfihos_views"
model_version: "1"
model_creator: "Your Name"
dms_identifire: "cfihos_name"  # or "cfihos_code"
model_processors_config:
  # ... your processor-specific configuration
```

## Configuration

The processor type is specified in the configuration file using the `processor_type` field:

```yaml
processor_type: sparse  # or your custom processor name
```

This value must match a key in the `CFIHOS_PROCESSOR_REGISTRY`. The framework will automatically load the corresponding model manager and processor based on this configuration.

## Usage Examples

### Processing Containers

```python
from cognite.neat import NeatSession, get_cognite_client

client = get_cognite_client(".env")
neat = NeatSession(client)

# Read and process the CFIHOS containers model
neat.plugins.data_model.read(
    "cfihos",
    model_type="containers",
    configurationDir="configuration/config.yaml"
)

# Deploy to CDF
neat.to.cdf.data_model()
```

### Processing Views

```python
from cognite.neat import NeatSession, get_cognite_client

client = get_cognite_client(".env")
neat_views = NeatSession(client)

# Read and process the CFIHOS views model
# NOTE: Always deploy containers before deploying views
neat_views.plugins.data_model.read(
    "cfihos",
    model_type="views",
    scope="Example Centrifugal Pump",  # Required for views
    configurationDir="configuration/config.yaml",
)

# Deploy to CDF
neat_views.to.cdf.data_model()
```

For complete examples, see:
- `examples/cfihos_plugin_containers_debugging.ipynb`
- `examples/cfihos_plugin_views_debugging.ipynb`

...

Under development