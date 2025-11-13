# CFIHOS Handler Plugin

## Overview

The CFIHOS Handler is a flexible framework that supports multiple processor implementations for handling CFIHOS industrial standard data. This plugin integrates with Cognite NEAT to process and transform CFIHOS data models.

## Installation

Install the package from PyPI using pip:

```bash
pip install cognite-neat-cfihos-handler
```

## Architecture

The CFIHOS Handler plugin supports multiple processor implementations. Each `processor_type` maintains a specific CFIHOS data model in Cognite Data Fusion (CDF), allowing you to choose the processing approach that best fits your use case.

## Available Processors

### Sparse Properties Processor

The `sparse_properties_processor` is the currently available processor. It is based on two processing concepts:

1. **Containers**: Processes the complete CFIHOS model into denormalized container structures.
2. **Views**: Processes scoped subsets of the CFIHOS model into CFIHOS view structures based on configured scopes that fit different use cases.

Users select which concept to process by passing the `model_type` parameter:
- `model_type="containers"` - for container-based processing
- `model_type="views"` - for view-based processing (requires a `scope` parameter)

## Configuration

The processor type is specified in the configuration file using the `processor_type` field:

```yaml
processor_type: sparse  # or other available processor types
```

The framework will automatically load the corresponding processor based on this configuration.

Example configuration file (`config.yaml`):

```yaml
processor_type: sparse
data_model_name: "Your CFIHOS Model"
data_model_external_id: "YOUR_CFIHOS_MODEL_1"
data_model_description: "Generated NEAT Domain Model based on CFIHOS"
container_data_model_space: "sp_your_cfihos_containers"
views_data_model_space: "sp_your_cfihos_views"
model_version: "1"
model_creator: "Your Name"
dms_identifire: "cfihos_name"  # or "cfihos_code"
model_processors_config:
  # ... processor-specific configuration
```

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

## Requirements

- Python >= 3.10
- cognite-neat >= 0.123.33

## License

Apache-2.0

