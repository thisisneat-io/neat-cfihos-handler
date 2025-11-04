"""Unit tests for sparse_properties.py."""

import pandas as pd
import pytest
from cognite.neat.core._issues.errors import NeatValueError

from cfihos_handler.framework.common.generic_classes import (
    EntityStructure,
    PropertyStructure,
    Relations,
)
from cfihos_handler.framework.processing.processors.sparse_properties import (
    SparsePropertiesProcessor,
)


class TestSparsePropertiesProcessorCreateContainerModelEntities:
    """Test suite for _create_container_model_entities method."""

    @pytest.fixture
    def minimal_processor_config(self):
        """Create a minimal processor config for testing."""
        return {
            "model_processors_config": [{"test_processor": {"id_prefix": "TEST"}}],
        }

    @pytest.fixture
    def processor(self, minimal_processor_config):
        """Create a SparsePropertiesProcessor instance for testing."""
        processor = SparsePropertiesProcessor(**minimal_processor_config)
        processor._df_entity_properties = pd.DataFrame()
        processor._model_properties = {}
        processor._model_entities = {}
        # Property groupings that match the test property IDs
        processor._property_groupings = ["TEST_0", "TEST_1", "TEST_2", "CFIHOS_1"]
        processor._map_entity_name_to_dms_name = {}
        processor._map_entity_id_to_dms_id = {}
        processor._map_entity_name_to_entity_id = {}
        return processor

    def test_create_container_model_entities_raises_error_on_multiple_names(
        self, processor
    ):
        """Test that validation raises error when property has multiple NAME values."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000001"],
                PropertyStructure.NAME: ["Property1", "Property2"],  # Multiple values
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False],
            }
        )

        with pytest.raises(
            NeatValueError,
            match=r"Found properties 'propertyName' with lacking or multiple values:",
        ):
            processor._create_container_model_entities()

    def test_create_container_model_entities_raises_error_on_multiple_dms_names(
        self, processor
    ):
        """Test that validation raises error when property has multiple DMS_NAME values."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS-10000001", "CFIHOS-10000001"],
                PropertyStructure.NAME: ["Property1", "Property1"],
                PropertyStructure.DMS_NAME: [
                    "dms_prop_1",
                    "dms_prop_2",
                ],  # Multiple values
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False],
            }
        )

        with pytest.raises(
            NeatValueError,
            match=r"Found properties 'dmsPropertyName' with lacking or multiple values:",
        ):
            processor._create_container_model_entities()

    def test_create_container_model_entities_raises_error_on_multiple_target_types_for_basic_data_type(
        self, processor
    ):
        """Test that validation raises error when BASIC_DATA_TYPE has multiple TARGET_TYPE values."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000001"],
                PropertyStructure.NAME: ["Property1", "Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "Integer"],  # Multiple values
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False],
            }
        )

        with pytest.raises(
            NeatValueError,
            match=r"Found properties 'targetType' with lacking or multiple values:",
        ):
            processor._create_container_model_entities()

    def test_create_container_model_entities_raises_error_on_multiple_multi_valued_flags(
        self, processor
    ):
        """Test that validation raises error when property has multiple MULTI_VALUED values."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000001"],
                PropertyStructure.NAME: ["Property1", "Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, True],  # Multiple values
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False],
            }
        )

        with pytest.raises(
            NeatValueError,
            match=r"Found properties 'multiValued' with lacking or multiple values:",
        ):
            processor._create_container_model_entities()

    def test_create_container_model_entities_creates_entities_and_properties_successfully(
        self, processor
    ):
        """Test that entities and properties are created and assigned correctly."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000002"],
                PropertyStructure.NAME: ["Property1", "Property2"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_2"],
                PropertyStructure.DESCRIPTION: ["Description1", "Description2"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "Integer"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False],
            }
        )

        processor._create_container_model_entities()

        # Verify EntityTypeGroup is always created
        assert "EntityTypeGroup" in processor._model_entities
        entity_type_group = processor._model_entities["EntityTypeGroup"]
        assert entity_type_group[EntityStructure.ID] == "EntityTypeGroup"
        assert len(entity_type_group[EntityStructure.PROPERTIES]) == 1
        assert (
            entity_type_group[EntityStructure.PROPERTIES][0][PropertyStructure.ID]
            == "entityType"
        )

        # Verify properties were created and assigned to entities
        # Collect all properties from all entities (excluding EntityTypeGroup)
        all_properties = []
        for entity_id, entity in processor._model_entities.items():
            if entity_id != "EntityTypeGroup":
                all_properties.extend(entity.get(EntityStructure.PROPERTIES, []))

        # Filter out entityType properties (they're added automatically)
        user_properties = [
            p for p in all_properties if p.get(PropertyStructure.ID) != "entityType"
        ]

        # Verify we have 2 user properties
        assert len(user_properties) == 2
        property_ids = [p[PropertyStructure.ID] for p in user_properties]
        assert "CFIHOS_10000001" in property_ids
        assert "CFIHOS_10000002" in property_ids

        # Verify entities were created for property groups
        # Properties should be grouped based on their IDs
        assert (
            len(processor._model_entities) >= 2
        )  # At least EntityTypeGroup + property groups

    def test_create_container_model_entities_properties_assigned_to_correct_entity_groups(
        self, processor
    ):
        """Test that properties are assigned to the correct property groups/entities."""
        # Use property IDs that will fall into the same group (first 100)
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000050"],
                PropertyStructure.NAME: ["Property1", "Property2"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_2"],
                PropertyStructure.DESCRIPTION: ["Description1", "Description2"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "Integer"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False],
            }
        )

        processor._create_container_model_entities()

        # Collect all properties from all entities (excluding EntityTypeGroup)
        all_properties = []
        property_to_entity = {}
        for entity_id, entity in processor._model_entities.items():
            if entity_id != "EntityTypeGroup":
                properties = entity.get(EntityStructure.PROPERTIES, [])
                for prop in properties:
                    prop_id = prop.get(PropertyStructure.ID)
                    if prop_id != "entityType":  # Skip entityType properties
                        all_properties.append(prop)
                        property_to_entity[prop_id] = entity_id

        # Verify we have 2 user properties
        assert len(all_properties) == 2
        property_ids = [p[PropertyStructure.ID] for p in all_properties]
        assert "CFIHOS_10000001" in property_ids
        assert "CFIHOS_10000050" in property_ids

        # Verify properties have property groups assigned
        for prop in all_properties:
            assert PropertyStructure.PROPERTY_GROUP in prop
            assert prop[PropertyStructure.PROPERTY_GROUP] is not None
            # Verify property group matches the entity ID it's assigned to
            assert (
                prop[PropertyStructure.PROPERTY_GROUP]
                == property_to_entity[prop[PropertyStructure.ID]]
            )

    def test_create_container_model_entities_entity_contains_entitytype_property_when_created(
        self, processor
    ):
        """Test that when an entity is created, it automatically gets an entityType property."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001"],
                PropertyStructure.NAME: ["Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Description1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String"],
                PropertyStructure.MULTI_VALUED: [False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False],
            }
        )

        processor._create_container_model_entities()

        # Find the property group entity (not EntityTypeGroup)
        property_group_entities = {
            k: v for k, v in processor._model_entities.items() if k != "EntityTypeGroup"
        }

        # At least one property group entity should exist
        assert len(property_group_entities) > 0

        # Each property group entity should have an entityType property
        for entity_key, entity_value in property_group_entities.items():
            properties = entity_value.get(EntityStructure.PROPERTIES, [])
            entity_type_props = [
                p for p in properties if p.get(PropertyStructure.ID) == "entityType"
            ]
            assert (
                len(entity_type_props) == 1
            ), f"Entity {entity_key} missing entityType property"

    def test_create_container_model_entities_filters_out_first_class_citizen_properties(
        self, processor
    ):
        """Test that first class citizen properties are filtered out."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000002"],
                PropertyStructure.NAME: ["Property1", "FCCProperty"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_fcc_prop"],
                PropertyStructure.DESCRIPTION: ["Description1", "FCC Description"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, True],  # Second is FCC
            }
        )

        processor._create_container_model_entities()

        # Collect all user properties from all entities (excluding EntityTypeGroup and entityType props)
        all_properties = []
        for entity_id, entity in processor._model_entities.items():
            if entity_id != "EntityTypeGroup":
                properties = entity.get(EntityStructure.PROPERTIES, [])
                for prop in properties:
                    prop_id = prop.get(PropertyStructure.ID)
                    if prop_id != "entityType":  # Skip entityType properties
                        all_properties.append(prop)

        # Only non-FCC property should be processed
        assert len(all_properties) == 1
        assert all_properties[0][PropertyStructure.ID] == "CFIHOS_10000001"

    def test_create_container_model_entities_filters_out_edge_and_reverse_relations(
        self, processor
    ):
        """Test that EDGE and REVERSE relation types are filtered out."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: [
                    "CFIHOS_10000001",
                    "CFIHOS_10000002",
                    "CFIHOS_10000003",
                ],
                PropertyStructure.NAME: ["Property1", "EdgeProp", "ReverseProp"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_edge", "dms_reverse"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Edge Desc", "Reverse Desc"],
                PropertyStructure.PROPERTY_TYPE: [
                    "BASIC_DATA_TYPE",
                    Relations.EDGE,
                    Relations.REVERSE,
                ],
                PropertyStructure.TARGET_TYPE: ["String", None, None],
                PropertyStructure.MULTI_VALUED: [False, False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False, False],
            }
        )

        processor._create_container_model_entities()

        # Collect all user properties from all entities (excluding EntityTypeGroup and entityType props)
        all_properties = []
        for entity_id, entity in processor._model_entities.items():
            if entity_id != "EntityTypeGroup":
                properties = entity.get(EntityStructure.PROPERTIES, [])
                for prop in properties:
                    prop_id = prop.get(PropertyStructure.ID)
                    if prop_id != "entityType":  # Skip entityType properties
                        all_properties.append(prop)

        # Only BASIC_DATA_TYPE property should be processed
        assert len(all_properties) == 1
        assert all_properties[0][PropertyStructure.ID] == "CFIHOS_10000001"

    def test_create_container_model_entities_property_id_dashes_replaced_with_underscores(
        self, processor
    ):
        """Test that property IDs with dashes are replaced with underscores."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS-10000001", "CFIHOS-10000002"],
                PropertyStructure.NAME: ["Property1", "Property2"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_2"],
                PropertyStructure.DESCRIPTION: ["Description1", "Description2"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "Integer"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False],
            }
        )

        processor._create_container_model_entities()

        # Collect all user properties from all entities (excluding EntityTypeGroup and entityType props)
        all_properties = []
        for entity_id, entity in processor._model_entities.items():
            if entity_id != "EntityTypeGroup":
                properties = entity.get(EntityStructure.PROPERTIES, [])
                for prop in properties:
                    prop_id = prop.get(PropertyStructure.ID)
                    if prop_id != "entityType":  # Skip entityType properties
                        all_properties.append(prop)

        # Verify dashes are replaced with underscores in property IDs
        property_ids = []
        for prop in all_properties:
            property_id = prop.get(PropertyStructure.ID, "")
            assert "-" not in property_id, f"Property ID {property_id} contains dashes"
            property_ids.append(property_id)

        # Verify the converted IDs are present
        assert "CFIHOS_10000001" in property_ids
        assert "CFIHOS_10000002" in property_ids

    def test_create_container_model_entities_multiple_properties_same_group_added_to_same_entity(
        self, processor
    ):
        """Test that multiple properties in the same group are added to the same entity."""
        # Create properties that will be in the same group (same prefix, sequential IDs)
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000002"],
                PropertyStructure.NAME: ["Property1", "Property2"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_2"],
                PropertyStructure.DESCRIPTION: ["Description1", "Description2"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False],
            }
        )

        processor._create_container_model_entities()

        # Find property group entities (exclude EntityTypeGroup)
        property_group_entities = {
            k: v for k, v in processor._model_entities.items() if k != "EntityTypeGroup"
        }

        # Properties should be grouped, so we should have at least one entity with multiple properties
        # (entityType + 2 user properties = at least 3 properties in the entity)
        entities_with_multiple_props = [
            e
            for e in property_group_entities.values()
            if len(e.get(EntityStructure.PROPERTIES, [])) > 1
        ]
        assert len(entities_with_multiple_props) > 0


class TestSparsePropertiesProcessorExtendContainerModelFirstClassCitizensEntities:
    """Test suite for _extend_container_model_first_class_citizens_entities method."""

    @pytest.fixture
    def minimal_processor_config(self):
        """Create a minimal processor config for testing."""
        return {
            "model_processors_config": [{"test_processor": {"id_prefix": "TEST"}}],
        }

    @pytest.fixture
    def processor(self, minimal_processor_config):
        """Create a SparsePropertiesProcessor instance for testing."""
        processor = SparsePropertiesProcessor(**minimal_processor_config)
        processor._df_entity_properties = pd.DataFrame()
        processor._df_entities = pd.DataFrame()
        processor._model_properties = {}
        processor._model_entities = {}
        processor._property_groupings = ["TEST_0", "TEST_1", "TEST_2", "CFIHOS"]
        processor._map_entity_name_to_dms_name = {}
        processor._map_entity_id_to_dms_id = {}
        processor._map_entity_name_to_entity_id = {}
        return processor

    def test_extend_container_model_first_class_citizens_entities_handles_empty_fcc_properties(
        self, processor
    ):
        """Test that function handles empty FCC properties gracefully."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001"],
                PropertyStructure.NAME: ["Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String"],
                PropertyStructure.MULTI_VALUED: [False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False],  # Not FCC
                PropertyStructure.UNIQUE_VALIDATION_ID: ["CFIHOS_10000001_validation"],
            }
        )

        # Should not raise an error, just return
        processor._extend_container_model_first_class_citizens_entities()

        # No entities should be added
        assert len(processor._model_entities) == 0

    def test_extend_container_model_first_class_citizens_entities_raises_error_on_multiple_names(
        self, processor
    ):
        """Test that validation raises error when FCC property has multiple NAME values."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000001"],
                PropertyStructure.NAME: ["Property1", "Property2"],  # Multiple values
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [True, True],  # FCC
                PropertyStructure.UNIQUE_VALIDATION_ID: [
                    "CFIHOS_10000001_validation",
                    "CFIHOS_10000001_validation",
                ],
                EntityStructure.ID: ["CFIHOS_00000001", "CFIHOS_00000001"],
            }
        )

        with pytest.raises(
            NeatValueError,
            match=r"Found properties 'propertyName' with lacking or multiple values:",
        ):
            processor._extend_container_model_first_class_citizens_entities()

    def test_extend_container_model_first_class_citizens_entities_raises_error_on_multiple_dms_names(
        self, processor
    ):
        """Test that validation raises error when FCC property has multiple DMS_NAME values."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000001"],
                PropertyStructure.NAME: ["Property1", "Property1"],
                PropertyStructure.DMS_NAME: [
                    "dms_prop_1",
                    "dms_prop_2",
                ],  # Multiple values
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [True, True],  # FCC
                PropertyStructure.UNIQUE_VALIDATION_ID: [
                    "CFIHOS_10000001_validation",
                    "CFIHOS_10000001_validation",
                ],
                EntityStructure.ID: ["CFIHOS_00000001", "CFIHOS_00000001"],
            }
        )

        with pytest.raises(
            NeatValueError,
            match=r"Found properties 'dmsPropertyName' with lacking or multiple values:",
        ):
            processor._extend_container_model_first_class_citizens_entities()

    def test_extend_container_model_first_class_citizens_entities_raises_error_on_multiple_target_types(
        self, processor
    ):
        """Test that validation raises error when FCC property has multiple TARGET_TYPE values."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000001"],
                PropertyStructure.NAME: ["Property1", "Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "Integer"],  # Multiple values
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [True, True],  # FCC
                PropertyStructure.UNIQUE_VALIDATION_ID: [
                    "CFIHOS_10000001_validation",
                    "CFIHOS_10000001_validation",
                ],
                EntityStructure.ID: ["CFIHOS_00000001", "CFIHOS_00000001"],
            }
        )

        with pytest.raises(
            NeatValueError,
            match=r"Found properties 'targetType' with lacking or multiple values:",
        ):
            processor._extend_container_model_first_class_citizens_entities()

    def test_extend_container_model_first_class_citizens_entities_raises_error_on_multiple_multi_valued(
        self, processor
    ):
        """Test that validation raises error when FCC property has multiple MULTI_VALUED values."""
        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000001"],
                PropertyStructure.NAME: ["Property1", "Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, True],  # Multiple values
                PropertyStructure.FIRSTCLASSCITIZEN: [True, True],  # FCC
                PropertyStructure.UNIQUE_VALIDATION_ID: [
                    "CFIHOS_10000001_validation",
                    "CFIHOS_10000001_validation",
                ],
                EntityStructure.ID: ["CFIHOS_00000001", "CFIHOS_00000001"],
            }
        )

        with pytest.raises(
            NeatValueError,
            match=r"Found properties 'multiValued' with lacking or multiple values:",
        ):
            processor._extend_container_model_first_class_citizens_entities()

    def test_extend_container_model_first_class_citizens_entities_creates_multiple_properties_for_entity(
        self, processor
    ):
        """Test that multiple FCC properties are added to the same entity."""
        entity_id = "CFIHOS_00000001"
        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["FCC Entity"],
                EntityStructure.DMS_NAME: ["dms_fcc_entity"],
                EntityStructure.DESCRIPTION: ["FCC Entity Description"],
                EntityStructure.FIRSTCLASSCITIZEN: [True],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000002"],
                PropertyStructure.NAME: ["Property1", "Property2"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_2"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc2"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "Integer"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [True, True],  # FCC
                PropertyStructure.UNIQUE_VALIDATION_ID: [
                    "CFIHOS_10000001_validation",
                    "CFIHOS_10000002_validation",
                ],
                EntityStructure.ID: [entity_id, entity_id],
            }
        )

        processor._extend_container_model_first_class_citizens_entities()

        # Verify entity was created with both properties
        entity_key = entity_id.replace("-", "_")
        assert entity_key in processor._model_entities

        entity = processor._model_entities[entity_key]
        assert len(entity[EntityStructure.PROPERTIES]) == 2

        property_ids = [
            p[PropertyStructure.ID] for p in entity[EntityStructure.PROPERTIES]
        ]
        assert "CFIHOS_10000001" in property_ids
        assert "CFIHOS_10000002" in property_ids

    def test_extend_container_model_first_class_citizens_entities_creates_multiple_entities(
        self, processor
    ):
        """Test that multiple FCC entities are created correctly."""
        entity_id_1 = "CFIHOS_00000001"
        entity_id_2 = "CFIHOS_00000002"

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id_1, entity_id_2],
                EntityStructure.NAME: ["FCC Entity 1", "FCC Entity 2"],
                EntityStructure.DMS_NAME: ["dms_fcc_entity_1", "dms_fcc_entity_2"],
                EntityStructure.DESCRIPTION: ["Desc 1", "Desc 2"],
                EntityStructure.FIRSTCLASSCITIZEN: [True, True],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None, None],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000002"],
                PropertyStructure.NAME: ["Property1", "Property2"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_2"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc2"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "Integer"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [True, True],  # FCC
                PropertyStructure.UNIQUE_VALIDATION_ID: [
                    "CFIHOS_10000001_validation",
                    "CFIHOS_10000002_validation",
                ],
                EntityStructure.ID: [entity_id_1, entity_id_2],
            }
        )

        processor._extend_container_model_first_class_citizens_entities()

        # Verify both entities were created
        entity_key_1 = entity_id_1.replace("-", "_")
        entity_key_2 = entity_id_2.replace("-", "_")

        assert entity_key_1 in processor._model_entities
        assert entity_key_2 in processor._model_entities

        entity_1 = processor._model_entities[entity_key_1]
        entity_2 = processor._model_entities[entity_key_2]

        assert len(entity_1[EntityStructure.PROPERTIES]) == 1
        assert len(entity_2[EntityStructure.PROPERTIES]) == 1
        assert (
            entity_1[EntityStructure.PROPERTIES][0][PropertyStructure.ID]
            == "CFIHOS_10000001"
        )
        assert (
            entity_2[EntityStructure.PROPERTIES][0][PropertyStructure.ID]
            == "CFIHOS_10000002"
        )

    def test_extend_container_model_first_class_citizens_entities_replaces_dashes_in_entity_id(
        self, processor
    ):
        """Test that entity IDs with dashes are replaced with underscores."""
        entity_id = "CFIHOS-00000001"
        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["FCC Entity"],
                EntityStructure.DMS_NAME: ["dms_fcc_entity"],
                EntityStructure.DESCRIPTION: ["FCC Entity Description"],
                EntityStructure.FIRSTCLASSCITIZEN: [True],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001"],
                PropertyStructure.NAME: ["Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Property Description"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String"],
                PropertyStructure.MULTI_VALUED: [False],
                PropertyStructure.FIRSTCLASSCITIZEN: [True],  # FCC
                PropertyStructure.UNIQUE_VALIDATION_ID: ["CFIHOS_10000001_validation"],
                EntityStructure.ID: [entity_id],
            }
        )

        processor._extend_container_model_first_class_citizens_entities()

        # Verify entity key has dashes replaced
        entity_key = "CFIHOS_00000001"
        assert entity_key in processor._model_entities
        # Verify the original entity_id with dashes is NOT in model_entities (it was replaced)
        assert entity_id not in processor._model_entities

    def test_extend_container_model_first_class_citizens_entities_handles_implements_core_model(
        self, processor
    ):
        """Test that IMPLEMENTS_CORE_MODEL is correctly handled when it's a list."""
        entity_id = "CFIHOS_00000001"
        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["FCC Entity"],
                EntityStructure.DMS_NAME: ["dms_fcc_entity"],
                EntityStructure.DESCRIPTION: ["FCC Entity Description"],
                EntityStructure.FIRSTCLASSCITIZEN: [True],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [
                    ["CoreModel1", "CoreModel2"]
                ],  # List
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001"],
                PropertyStructure.NAME: ["Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Property Description"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String"],
                PropertyStructure.MULTI_VALUED: [False],
                PropertyStructure.FIRSTCLASSCITIZEN: [True],  # FCC
                PropertyStructure.UNIQUE_VALIDATION_ID: ["CFIHOS_10000001_validation"],
                EntityStructure.ID: [entity_id],
            }
        )

        processor._extend_container_model_first_class_citizens_entities()

        # Verify entity was created with IMPLEMENTS_CORE_MODEL as list
        entity_key = entity_id.replace("-", "_")
        entity = processor._model_entities[entity_key]
        assert entity[EntityStructure.IMPLEMENTS_CORE_MODEL] == [
            "CoreModel1",
            "CoreModel2",
        ]


class TestSparsePropertiesProcessorCreateViewsModelEntities:
    """Test suite for _create_views_model_entities method."""

    @pytest.fixture
    def minimal_processor_config(self):
        """Create a minimal processor config for testing."""
        return {
            "model_processors_config": [{"test_processor": {"id_prefix": "TEST"}}],
        }

    @pytest.fixture
    def processor(self, minimal_processor_config):
        """Create a SparsePropertiesProcessor instance for testing."""
        processor = SparsePropertiesProcessor(**minimal_processor_config)
        # Create empty DataFrame with required columns for groupby and filtering operations
        processor._df_entity_properties = pd.DataFrame(
            columns=[
                PropertyStructure.ID,
                PropertyStructure.NAME,
                PropertyStructure.IN_MODEL,
                PropertyStructure.FIRSTCLASSCITIZEN,
                PropertyStructure.PROPERTY_TYPE,
                PropertyStructure.TARGET_TYPE,
                EntityStructure.ID,
            ]
        )
        processor._df_entities = pd.DataFrame()
        processor._model_properties = {}
        processor._model_entities = {}
        processor._model_property_groups = {}
        processor._property_groupings = ["CFIHOS_1"]
        processor._map_entity_name_to_dms_name = {}
        processor._map_entity_id_to_dms_id = {}
        processor._map_entity_name_to_entity_id = {}
        processor._map_dms_id_to_entity_id = {}
        return processor

    def test_create_views_model_entities_raises_error_on_duplicate_property_id(
        self, processor
    ):
        """Test that validation raises error when duplicate property IDs are found in an entity."""
        entity_id = "CFIHOS_00000001"
        dms_id = "dms_CFIHOS_00000001"
        property_id = "CFIHOS_10000001"

        processor._map_entity_id_to_dms_id = {entity_id: dms_id}

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["Entity1"],
                EntityStructure.DMS_NAME: ["dms_entity_1"],
                EntityStructure.DESCRIPTION: ["Desc1"],
                EntityStructure.FIRSTCLASSCITIZEN: [False],
                EntityStructure.INHERITS_FROM_ID: [None],
                EntityStructure.INHERITS_FROM_NAME: [None],
                EntityStructure.FULL_INHERITANCE: [[]],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None],
                "type": ["EntityType1"],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: [property_id, property_id],  # Duplicate
                PropertyStructure.NAME: ["Property1", "Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False],
                PropertyStructure.IN_MODEL: [True, True],
                EntityStructure.ID: [entity_id, entity_id],
            }
        )

        with pytest.raises(
            NeatValueError,
            match=rf"Found duplicate property id '{property_id}' in {dms_id}",
        ):
            processor._create_views_model_entities()

    def test_create_views_model_entities_raises_error_on_duplicate_fcc_property_id(
        self, processor
    ):
        """Test that validation raises error when duplicate FCC property IDs are found."""
        entity_id = "CFIHOS_00000001"
        dms_id = "dms_CFIHOS_00000001"
        property_id = "CFIHOS_10000001"

        processor._map_entity_id_to_dms_id = {entity_id: dms_id}

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["Entity1"],
                EntityStructure.DMS_NAME: ["dms_entity_1"],
                EntityStructure.DESCRIPTION: ["Desc1"],
                EntityStructure.FIRSTCLASSCITIZEN: [True],  # FCC
                EntityStructure.INHERITS_FROM_ID: [None],
                EntityStructure.INHERITS_FROM_NAME: [None],
                EntityStructure.FULL_INHERITANCE: [[]],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None],
                "type": ["EntityType1"],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: [property_id, property_id],  # Duplicate
                PropertyStructure.NAME: ["Property1", "Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [True, True],  # FCC
                PropertyStructure.IN_MODEL: [True, True],
                EntityStructure.ID: [entity_id, entity_id],
            }
        )

        with pytest.raises(
            NeatValueError,
            match=rf"Found duplicate property id '{property_id}' in FCC {dms_id}",
        ):
            processor._create_views_model_entities()

    def test_create_views_model_entities_creates_entities_and_properties(
        self, processor
    ):
        """Test that entities and properties are created correctly."""
        entity_id = "CFIHOS_00000001"
        dms_id = "dms_CFIHOS_00000001"
        property_id = "CFIHOS_10000001"

        processor._map_entity_id_to_dms_id = {entity_id: dms_id}
        processor._map_entity_id_to_dms_name = {}

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["Entity1"],
                EntityStructure.DMS_NAME: ["dms_entity_1"],
                EntityStructure.DESCRIPTION: ["Desc1"],
                EntityStructure.FIRSTCLASSCITIZEN: [False],
                EntityStructure.INHERITS_FROM_ID: [None],
                EntityStructure.INHERITS_FROM_NAME: [None],
                EntityStructure.FULL_INHERITANCE: [[]],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None],
                "type": ["EntityType1"],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: [property_id],
                PropertyStructure.NAME: ["Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String"],
                PropertyStructure.MULTI_VALUED: [False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False],
                PropertyStructure.IN_MODEL: [True],
                EntityStructure.ID: [entity_id],
            }
        )

        processor._create_views_model_entities()

        # Verify entity was created
        assert dms_id in processor._model_entities
        entity = processor._model_entities[dms_id]
        assert entity[EntityStructure.ID] == dms_id
        assert entity[EntityStructure.NAME] == "Entity1"
        assert entity[EntityStructure.DMS_NAME] == "dms_entity_1"
        assert entity[EntityStructure.DESCRIPTION] == "Desc1"
        assert entity[EntityStructure.FIRSTCLASSCITIZEN] is False
        assert entity["cfihosType"] == "EntityType1"
        assert entity["cfihosId"] == entity_id

        # Verify properties were added (including entityType for non-FCC)
        assert len(entity[EntityStructure.PROPERTIES]) == 2  # Property1 + entityType
        property_ids = [
            p[PropertyStructure.ID] for p in entity[EntityStructure.PROPERTIES]
        ]
        assert property_id in property_ids
        assert "entityType" in property_ids

    def test_create_views_model_entities_does_not_add_entitytype_to_fcc_entities(
        self, processor
    ):
        """Test that entityType property is NOT added to FCC entities."""
        entity_id = "CFIHOS_00000001"
        dms_id = "dms_CFIHOS_00000001"

        processor._map_entity_id_to_dms_id = {entity_id: dms_id}
        processor._map_entity_id_to_dms_name = {}

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["Entity1"],
                EntityStructure.DMS_NAME: ["dms_entity_1"],
                EntityStructure.DESCRIPTION: ["Desc1"],
                EntityStructure.FIRSTCLASSCITIZEN: [True],  # FCC
                EntityStructure.INHERITS_FROM_ID: [None],
                EntityStructure.INHERITS_FROM_NAME: [None],
                EntityStructure.FULL_INHERITANCE: [[]],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None],
                "type": ["EntityType1"],
            }
        )

        # Create empty DataFrame with required columns for groupby and filtering operations
        processor._df_entity_properties = pd.DataFrame(
            columns=[
                PropertyStructure.ID,
                PropertyStructure.NAME,
                PropertyStructure.IN_MODEL,
                PropertyStructure.FIRSTCLASSCITIZEN,
                PropertyStructure.PROPERTY_TYPE,
                PropertyStructure.TARGET_TYPE,
                EntityStructure.ID,
            ]
        )

        processor._create_views_model_entities()

        entity = processor._model_entities[dms_id]
        # Should NOT have entityType property for FCC entities
        assert len(entity[EntityStructure.PROPERTIES]) == 0

    def test_create_views_model_entities_filters_by_in_model(self, processor):
        """Test that only properties with IN_MODEL=True are processed."""
        entity_id = "CFIHOS_00000001"
        dms_id = "dms_CFIHOS_00000001"

        processor._map_entity_id_to_dms_id = {entity_id: dms_id}
        processor._map_entity_id_to_dms_name = {}

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["Entity1"],
                EntityStructure.DMS_NAME: ["dms_entity_1"],
                EntityStructure.DESCRIPTION: ["Desc1"],
                EntityStructure.FIRSTCLASSCITIZEN: [False],
                EntityStructure.INHERITS_FROM_ID: [None],
                EntityStructure.INHERITS_FROM_NAME: [None],
                EntityStructure.FULL_INHERITANCE: [[]],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None],
                "type": ["EntityType1"],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001", "CFIHOS_10000002"],
                PropertyStructure.NAME: ["Property1", "Property2"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_2"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc2"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False],
                PropertyStructure.IN_MODEL: [
                    True,
                    False,
                ],  # Second property not in model
                EntityStructure.ID: [entity_id, entity_id],
            }
        )

        processor._create_views_model_entities()

        entity = processor._model_entities[dms_id]
        # Should only have Property1 + entityType (Property2 filtered out)
        assert len(entity[EntityStructure.PROPERTIES]) == 2
        property_ids = [
            p[PropertyStructure.ID] for p in entity[EntityStructure.PROPERTIES]
        ]
        assert "CFIHOS_10000001" in property_ids
        assert "CFIHOS_10000002" not in property_ids

    def test_create_views_model_entities_excludes_inherited_properties(self, processor):
        """Test that inherited properties are excluded from entities."""
        entity_id_1 = "CFIHOS_00000001"  # Parent
        entity_id_2 = "CFIHOS_00000002"  # Child
        dms_id_1 = "dms_CFIHOS_00000001"
        dms_id_2 = "dms_CFIHOS_00000002"

        processor._map_entity_id_to_dms_id = {
            entity_id_1: dms_id_1,
            entity_id_2: dms_id_2,
        }
        processor._map_entity_id_to_dms_name = {}

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id_1, entity_id_2],
                EntityStructure.NAME: ["ParentEntity", "ChildEntity"],
                EntityStructure.DMS_NAME: ["dms_parent", "dms_child"],
                EntityStructure.DESCRIPTION: ["Parent", "Child"],
                EntityStructure.FIRSTCLASSCITIZEN: [False, False],
                EntityStructure.INHERITS_FROM_ID: [None, [entity_id_1]],
                EntityStructure.INHERITS_FROM_NAME: [None, ["ParentEntity"]],
                EntityStructure.FULL_INHERITANCE: [
                    [],
                    [entity_id_1],
                ],  # Child inherits from parent
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None, None],
                "type": ["ParentType", "ChildType"],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: [
                    "CFIHOS_10000001",
                    "CFIHOS_10000001",
                ],  # Same property ID
                PropertyStructure.NAME: ["Property1", "Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1", "dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1", "Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE", "BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String", "String"],
                PropertyStructure.MULTI_VALUED: [False, False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False, False],
                PropertyStructure.IN_MODEL: [True, True],
                EntityStructure.ID: [
                    entity_id_1,
                    entity_id_2,
                ],  # Property on both entities
            }
        )

        processor._create_views_model_entities()

        # Parent should have the property
        entity_1 = processor._model_entities[dms_id_1]
        property_ids_1 = [
            p[PropertyStructure.ID] for p in entity_1[EntityStructure.PROPERTIES]
        ]
        assert "CFIHOS_10000001" in property_ids_1

        # Child should NOT have the inherited property (only entityType)
        entity_2 = processor._model_entities[dms_id_2]
        property_ids_2 = [
            p[PropertyStructure.ID] for p in entity_2[EntityStructure.PROPERTIES]
        ]
        assert "CFIHOS_10000001" not in property_ids_2
        assert "entityType" in property_ids_2

    def test_create_views_model_entities_handles_implements_core_model(self, processor):
        """Test that IMPLEMENTS_CORE_MODEL is correctly handled."""
        entity_id = "CFIHOS_00000001"
        dms_id = "dms_CFIHOS_00000001"

        processor._map_entity_id_to_dms_id = {entity_id: dms_id}
        processor._map_entity_id_to_dms_name = {}

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["Entity1"],
                EntityStructure.DMS_NAME: ["dms_entity_1"],
                EntityStructure.DESCRIPTION: ["Desc1"],
                EntityStructure.FIRSTCLASSCITIZEN: [False],
                EntityStructure.INHERITS_FROM_ID: [None],
                EntityStructure.INHERITS_FROM_NAME: [None],
                EntityStructure.FULL_INHERITANCE: [[]],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [
                    ["CoreModel1", "CoreModel2"]
                ],  # List
                "type": ["EntityType1"],
            }
        )

        # Create empty DataFrame with required columns for groupby and filtering operations
        processor._df_entity_properties = pd.DataFrame(
            columns=[
                PropertyStructure.ID,
                PropertyStructure.NAME,
                PropertyStructure.IN_MODEL,
                PropertyStructure.FIRSTCLASSCITIZEN,
                PropertyStructure.PROPERTY_TYPE,
                PropertyStructure.TARGET_TYPE,
                EntityStructure.ID,
            ]
        )

        processor._create_views_model_entities()

        entity = processor._model_entities[dms_id]
        assert entity[EntityStructure.IMPLEMENTS_CORE_MODEL] == [
            "CoreModel1",
            "CoreModel2",
        ]

    def test_create_views_model_entities_assigns_property_groups_correctly(
        self, processor
    ):
        """Test that property groups are assigned correctly for non-FCC entities."""
        entity_id = "CFIHOS_00000001"
        dms_id = "dms_CFIHOS_00000001"
        property_id = "CFIHOS_1_10000001"  # Matches property grouping prefix

        processor._map_entity_id_to_dms_id = {entity_id: dms_id}
        processor._map_entity_id_to_dms_name = {}

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["Entity1"],
                EntityStructure.DMS_NAME: ["dms_entity_1"],
                EntityStructure.DESCRIPTION: ["Desc1"],
                EntityStructure.FIRSTCLASSCITIZEN: [False],
                EntityStructure.INHERITS_FROM_ID: [None],
                EntityStructure.INHERITS_FROM_NAME: [None],
                EntityStructure.FULL_INHERITANCE: [[]],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None],
                "type": ["EntityType1"],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: [property_id],
                PropertyStructure.NAME: ["Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String"],
                PropertyStructure.MULTI_VALUED: [False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False],
                PropertyStructure.IN_MODEL: [True],
                EntityStructure.ID: [entity_id],
            }
        )

        processor._create_views_model_entities()

        entity = processor._model_entities[dms_id]
        # Find the property (not entityType)
        properties = [
            p
            for p in entity[EntityStructure.PROPERTIES]
            if p[PropertyStructure.ID] == property_id
        ]
        assert len(properties) == 1
        assert PropertyStructure.PROPERTY_GROUP in properties[0]
        assert properties[0][PropertyStructure.PROPERTY_GROUP] is not None

    def test_create_views_model_entities_uses_entity_id_as_property_group_for_fcc(
        self, processor
    ):
        """Test that FCC entities use their entity ID as property group."""
        entity_id = "CFIHOS_00000001"
        dms_id = "dms_CFIHOS_00000001"
        property_id = "CFIHOS_10000001"

        processor._map_entity_id_to_dms_id = {entity_id: dms_id}
        processor._map_entity_id_to_dms_name = {}

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["Entity1"],
                EntityStructure.DMS_NAME: ["dms_entity_1"],
                EntityStructure.DESCRIPTION: ["Desc1"],
                EntityStructure.FIRSTCLASSCITIZEN: [True],  # FCC
                EntityStructure.INHERITS_FROM_ID: [None],
                EntityStructure.INHERITS_FROM_NAME: [None],
                EntityStructure.FULL_INHERITANCE: [[]],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None],
                "type": ["EntityType1"],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: [property_id],
                PropertyStructure.NAME: ["Property1"],
                PropertyStructure.DMS_NAME: ["dms_prop_1"],
                PropertyStructure.DESCRIPTION: ["Desc1"],
                PropertyStructure.PROPERTY_TYPE: ["BASIC_DATA_TYPE"],
                PropertyStructure.TARGET_TYPE: ["String"],
                PropertyStructure.MULTI_VALUED: [False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False],
                PropertyStructure.IN_MODEL: [True],
                EntityStructure.ID: [entity_id],
            }
        )

        processor._create_views_model_entities()

        entity = processor._model_entities[dms_id]
        properties = [
            p
            for p in entity[EntityStructure.PROPERTIES]
            if p[PropertyStructure.ID] == property_id
        ]
        assert len(properties) == 1
        # FCC entities use entity ID (with dashes replaced) as property group
        assert properties[0][PropertyStructure.PROPERTY_GROUP] == entity_id.replace(
            "-", "_"
        )

    def test_create_views_model_entities_validates_target_type_with_cfihos_entity_code(
        self, processor
    ):
        """Test that ENTITY_RELATION properties with valid CFIHOS entity code target types are processed."""
        entity_id = "CFIHOS_00000001"
        target_entity_id = "CFIHOS_00000002"
        dms_id = "dms_CFIHOS_00000001"
        dms_target_id = "dms_CFIHOS_00000002"

        processor._map_entity_id_to_dms_id = {
            entity_id: dms_id,
            target_entity_id: dms_target_id,
        }
        processor._map_entity_id_to_dms_name = {}
        # Map DMS ID back to entity ID - this validates that the target entity CFIHOS code exists
        processor._map_dms_id_to_entity_id = {dms_target_id: target_entity_id}

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["Entity1"],
                EntityStructure.DMS_NAME: ["dms_entity_1"],
                EntityStructure.DESCRIPTION: ["Desc1"],
                EntityStructure.FIRSTCLASSCITIZEN: [False],
                EntityStructure.INHERITS_FROM_ID: [None],
                EntityStructure.INHERITS_FROM_NAME: [None],
                EntityStructure.FULL_INHERITANCE: [[]],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None],
                "type": ["EntityType1"],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001"],
                PropertyStructure.NAME: ["RelationProperty"],
                PropertyStructure.DMS_NAME: ["dms_relation"],
                PropertyStructure.DESCRIPTION: ["Relation Desc"],
                PropertyStructure.PROPERTY_TYPE: ["ENTITY_RELATION"],
                PropertyStructure.TARGET_TYPE: [
                    dms_target_id
                ],  # DMS ID that maps to CFIHOS_00000002
                PropertyStructure.MULTI_VALUED: [False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False],
                PropertyStructure.IN_MODEL: [True],
                EntityStructure.ID: [entity_id],
            }
        )

        processor._create_views_model_entities()

        entity = processor._model_entities[dms_id]
        property_ids = [
            p[PropertyStructure.ID] for p in entity[EntityStructure.PROPERTIES]
        ]
        # Should have the relation property since target CFIHOS entity code (CFIHOS_00000002) is valid
        assert "CFIHOS_10000001" in property_ids
        # Verify the property was created with correct target
        properties = [
            p
            for p in entity[EntityStructure.PROPERTIES]
            if p[PropertyStructure.ID] == "CFIHOS_10000001"
        ]
        assert len(properties) == 1
        assert properties[0][PropertyStructure.PROPERTY_TYPE] == "ENTITY_RELATION"
        # Verify TARGET_TYPE is set to the DMS target ID
        assert properties[0][PropertyStructure.TARGET_TYPE] == dms_target_id

    def test_create_views_model_entities_handles_edge_properties(self, processor):
        """Test that edge properties are correctly marked."""
        entity_id = "CFIHOS_00000001"
        dms_id = "dms_CFIHOS_00000001"

        processor._map_entity_id_to_dms_id = {entity_id: dms_id}
        processor._map_entity_id_to_dms_name = {}
        processor._map_dms_id_to_entity_id = {}

        processor._df_entities = pd.DataFrame(
            {
                EntityStructure.ID: [entity_id],
                EntityStructure.NAME: ["Entity1"],
                EntityStructure.DMS_NAME: ["dms_entity_1"],
                EntityStructure.DESCRIPTION: ["Desc1"],
                EntityStructure.FIRSTCLASSCITIZEN: [False],
                EntityStructure.INHERITS_FROM_ID: [None],
                EntityStructure.INHERITS_FROM_NAME: [None],
                EntityStructure.FULL_INHERITANCE: [[]],
                EntityStructure.IMPLEMENTS_CORE_MODEL: [None],
                "type": ["EntityType1"],
            }
        )

        processor._df_entity_properties = pd.DataFrame(
            {
                PropertyStructure.ID: ["CFIHOS_10000001"],
                PropertyStructure.NAME: ["EdgeProperty"],
                PropertyStructure.DMS_NAME: ["dms_edge"],
                PropertyStructure.DESCRIPTION: ["Edge Desc"],
                PropertyStructure.PROPERTY_TYPE: [Relations.EDGE],
                PropertyStructure.TARGET_TYPE: ["String"],
                PropertyStructure.MULTI_VALUED: [False],
                PropertyStructure.FIRSTCLASSCITIZEN: [False],
                PropertyStructure.IN_MODEL: [True],
                EntityStructure.ID: [entity_id],
                PropertyStructure.EDGE_EXTERNAL_ID: ["edge_external_id"],
                PropertyStructure.EDGE_SOURCE: ["CFIHOS_00000001"],
                PropertyStructure.EDGE_TARGET: ["CFIHOS_00000002"],
                PropertyStructure.EDGE_SOURCE_DMS_NAME: ["dms_source"],
                PropertyStructure.EDGE_TARGET_DMS_NAME: ["dms_target"],
                PropertyStructure.EDGE_DIRECTION: ["directed"],
            }
        )

        processor._create_views_model_entities()

        entity = processor._model_entities[dms_id]
        properties = [
            p
            for p in entity[EntityStructure.PROPERTIES]
            if p[PropertyStructure.ID] == "CFIHOS_10000001"
        ]
        assert len(properties) == 1
        # Edge property should be marked correctly (checking through property creation)
        assert PropertyStructure.PROPERTY_GROUP in properties[0]


class TestSparsePropertiesProcessorAssignPropertyGroup:
    """Test suite for _assign_property_group method."""

    @pytest.fixture
    def minimal_processor_config(self):
        """Create a minimal processor config for testing."""
        return {
            "model_processors_config": [{"test_processor": {"id_prefix": "CFIHOS"}}],
        }

    @pytest.fixture
    def processor(self, minimal_processor_config):
        """Create a SparsePropertiesProcessor instance for testing."""
        processor = SparsePropertiesProcessor(**minimal_processor_config)
        # Set up property groupings for multiple prefixes
        processor._property_groupings = ["CFIHOS_1", "CFIHOS_4"]
        return processor

    @pytest.mark.parametrize(
        "property_id,expected_group",
        [
            ("CFIHOS_10000001", "CFIHOS_1_10000001_10000101"),
            ("CFIHOS_10000023", "CFIHOS_1_10000001_10000101"),  # Same group as 10000001
            ("CFIHOS_40000023", "CFIHOS_4_40000001_40000101"),
            ("CFIHOS_10000150", "CFIHOS_1_10000101_10000201"),
            (
                "CFIHOS_10000001_rel",
                "CFIHOS_1_10000001_10000101_ext",
            ),  # With _ext suffix
        ],
    )
    def test_assign_property_group(self, processor, property_id, expected_group):
        """Test that _assign_property_group correctly assigns property groups for different property IDs."""
        result = processor._assign_property_group(property_id)
        assert result == expected_group
