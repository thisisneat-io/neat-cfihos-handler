"""Constants for the CFIHOS framework."""
# DMS
MODEL_VERSION_LENGTH = 4
MAX_DMS_MODEL_NAME = 50 + MODEL_VERSION_LENGTH
MAX_DMS_FIELD_NAME = 50 + MODEL_VERSION_LENGTH
CONTAINER_PROPERTY_LIMIT = 100

# CDF CDM
CDF_CDM_VERSION = "v1"
CDF_CDM_SPACE = "cdf_cdm"

CFIHOS_PROCESSOR_REGISTRY = {
    "sparse": "cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparseCfihosManager",
    # Add more when implemented:
    # "hybrid": "processor.cfihos.framework.processing.model_managers.HybridCfihosLoader",
    # ...
}

"""Constants for CFIHOS processing."""
CFIHOS_TYPE_ENTITY = "entity"
CFIHOS_TYPE_ENTITY_ATTRIBUTE = "entity attribute"
CFIHOS_TYPE_EQUIPMENT = "equipment"
CFIHOS_TYPE_TAG = "tag"
CFIHOS_TYPE_PROPERTY = "property"
CFIHOS_TYPE_TAG_OR_EQUIPMENT_CLASS = "tag or equipment class"


CFIHOS_TYPE_ENTITY_PREFIX = ""
CFIHOS_TYPE_EQUIPMENT_PREFIX = "E"
CFIHOS_TYPE_TAG_PREFIX = "T"

CFIHOS_ID_ENTITY_PREFIX = "CFIHOS-0"
CFIHOS_ID_EQUIPMENT_PREFIX = "CFIHOS-3"
CFIHOS_ID_TAG_PREFIX = "CFIHOS-3"
CFIHOS_ID_PREFIX = "CFIHOS-"

PARENT_SUFFIX = "_parent"

CFIHOS_TYPE_EQUIPMENT_AND_TAG = (CFIHOS_TYPE_EQUIPMENT, CFIHOS_TYPE_TAG)
