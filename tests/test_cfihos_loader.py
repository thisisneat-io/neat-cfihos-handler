"""Unit tests for cfihos_loader.py."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from cfihos_handler.framework.common import constants
from cfihos_handler.framework.importer.cfihos_loader import (
    CfihosModelLoader,
    cfihosTypeEntity,
    cfihosTypeEquipment,
    cfihosTypeTag,
)


class TestCfihosTypeTag:
    """Test suite for cfihosTypeTag dataclass."""

    def test_init(self):
        """Test cfihosTypeTag initialization."""
        tag_type = cfihosTypeTag(
            data_folder_abs_fpath="/data/",
            entities_fname="entities.csv",
            entities_attrib_fname="attributes.csv",
            property_metadata_fname="metadata.csv",
        )

        assert tag_type.data_folder_abs_fpath == "/data/"
        assert tag_type.entities_fname == "entities.csv"
        assert tag_type.type == constants.CFIHOS_TYPE_TAG
        assert tag_type.type_prefix == constants.CFIHOS_TYPE_TAG_PREFIX

    def test_post_init_sets_paths(self):
        """Test that __post_init__ sets absolute file paths."""
        tag_type = cfihosTypeTag(
            data_folder_abs_fpath="/data/",
            entities_fname="entities.csv",
            entities_attrib_fname="attributes.csv",
            property_metadata_fname="metadata.csv",
        )

        assert tag_type.entities_abs_fpath == "/data/entities.csv"
        assert tag_type.entities_attrib_abs_fpath == "/data/attributes.csv"
        assert tag_type.property_metadata_abs_fpath == "/data/metadata.csv"

    def test_post_init_with_base_folder(self):
        """Test that __post_init__ sets base folder paths when provided."""
        tag_type = cfihosTypeTag(
            data_folder_abs_fpath="/data/",
            entities_fname="entities.csv",
            entities_attrib_fname="attributes.csv",
            property_metadata_fname="metadata.csv",
            base_folder_abs_fpath="/base/",
            base_entities_attrib_fname="base_attributes.csv",
            base_property_metadata_fname="base_metadata.csv",
        )

        assert tag_type.base_entities_attrib_abs_fpath == "/base/base_attributes.csv"
        assert tag_type.base_property_metadata_abs_fpath == "/base/base_metadata.csv"


class TestCfihosTypeEquipment:
    """Test suite for cfihosTypeEquipment dataclass."""

    def test_init(self):
        """Test cfihosTypeEquipment initialization."""
        equipment_type = cfihosTypeEquipment(
            data_folder_abs_fpath="/data/",
            entities_fname="equipment.csv",
            entities_attrib_fname="equipment_attrs.csv",
            property_metadata_fname="equipment_metadata.csv",
        )

        assert equipment_type.type == constants.CFIHOS_TYPE_EQUIPMENT
        assert equipment_type.type_prefix == constants.CFIHOS_TYPE_EQUIPMENT_PREFIX


class TestCfihosTypeEntity:
    """Test suite for cfihosTypeEntity dataclass."""

    def test_init(self):
        """Test cfihosTypeEntity initialization."""
        entity_type = cfihosTypeEntity(
            data_folder_abs_fpath="/data/",
            entities_fname="entities.csv",
            entities_attrib_fname="attributes.csv",
        )

        assert entity_type.type == constants.CFIHOS_TYPE_ENTITY
        assert entity_type.type_prefix == constants.CFIHOS_TYPE_ENTITY_PREFIX

    def test_post_init_with_edges_and_core_model(self):
        """Test __post_init__ with edges and core model."""
        entity_type = cfihosTypeEntity(
            data_folder_abs_fpath="/data/",
            entities_fname="entities.csv",
            entities_edges="edges.csv",
            entities_core_model="core_model.csv",
        )

        assert entity_type.entities_edges_abs_fpath == "/data/edges.csv"
        assert entity_type.entities_core_model_abs_fpath == "/data/core_model.csv"


class TestCfihosModelLoader:
    """Test suite for CfihosModelLoader."""

    @pytest.fixture
    def minimal_config(self):
        """Create minimal configuration for CfihosModelLoader."""
        return {
            "included_cfihos_types_config": [
                {
                    "type": "cfihosTypeEntity",
                    "data_folder_abs_fpath": "/data/",
                    "entities_fname": "entities.csv",
                }
            ],
            "abs_fpath_model_raw_data_folder": Path("/data/"),
            "rdl_master_objects_fname": "master.csv",
            "rdl_master_object_id_col_name": "id",
            "rdl_master_object_name_col_name": "name",
            "rdl_master_object_file_type_col_name": "type",
            "processor_config_name": "test_processor",
            "id_prefix": "TEST",
        }

    @pytest.fixture
    def mock_read_input_sheet(self):
        """Mock read_input_sheet function."""
        with patch(
            "cfihos_handler.framework.importer.cfihos_loader.read_input_sheet"
        ) as mock_read:
            yield mock_read

    @patch("cfihos_handler.framework.importer.cfihos_loader.read_input_sheet")
    def test_init_sets_up_cfihos_types(self, mock_read, minimal_config):
        """Test that initialization sets up cfihos types correctly."""
        # Mock read_input_sheet to return empty DataFrame with required columns
        mock_df = pd.DataFrame(
            {
                "id": [],
                "name": [],
                "type": [],
            }
        )
        mock_read.return_value = mock_df

        loader = CfihosModelLoader(**minimal_config)

        assert len(loader.includes_cfihos_types) == 1
        assert loader.includes_cfihos_types[0].type == constants.CFIHOS_TYPE_ENTITY

    @patch("cfihos_handler.framework.importer.cfihos_loader.read_input_sheet")
    def test_init_raises_error_on_invalid_cfihos_type(self, mock_read):
        """Test that initialization raises error on invalid cfihos type."""
        config = {
            "included_cfihos_types_config": [
                {
                    "type": "cfihosTypeInvalid",
                    "data_folder_abs_fpath": "/data/",
                    "entities_fname": "entities.csv",
                }
            ],
            "abs_fpath_model_raw_data_folder": Path("/data/"),
            "rdl_master_objects_fname": "master.csv",
            "rdl_master_object_id_col_name": "id",
            "rdl_master_object_name_col_name": "name",
            "rdl_master_object_file_type_col_name": "type",
            "processor_config_name": "test_processor",
            "id_prefix": "TEST",
        }

        mock_df = pd.DataFrame({"id": [], "name": [], "type": []})
        mock_read.return_value = mock_df

        with pytest.raises(KeyError, match="Provided CFIHOS type.*is not supported"):
            CfihosModelLoader(**config)

    @patch("cfihos_handler.framework.importer.cfihos_loader.read_input_sheet")
    def test_init_sets_cfihos_type_metadata(self, mock_read, minimal_config):
        """Test that initialization sets cfihos_type_metadata."""
        mock_df = pd.DataFrame({"id": [], "name": [], "type": []})
        mock_read.return_value = mock_df

        loader = CfihosModelLoader(**minimal_config)

        assert constants.CFIHOS_TYPE_ENTITY in loader.cfihos_type_metadata
        assert (
            "type_prefix" in loader.cfihos_type_metadata[constants.CFIHOS_TYPE_ENTITY]
        )
        assert (
            "type_id_prefix"
            in loader.cfihos_type_metadata[constants.CFIHOS_TYPE_ENTITY]
        )

    def test_sanitize_as_dms_string_snake_case(self):
        """Test sanitize_as_dms_string with snake_case."""
        config = {
            "included_cfihos_types_config": [],
            "abs_fpath_model_raw_data_folder": Path("/data/"),
            "rdl_master_objects_fname": "master.csv",
            "rdl_master_object_id_col_name": "id",
            "rdl_master_object_name_col_name": "name",
            "rdl_master_object_file_type_col_name": "type",
            "processor_config_name": "test_processor",
            "id_prefix": "TEST",
        }

        with patch(
            "cfihos_handler.framework.importer.cfihos_loader.read_input_sheet"
        ) as mock_read:
            mock_df = pd.DataFrame({"id": [], "name": [], "type": []})
            mock_read.return_value = mock_df

            loader = CfihosModelLoader(**config)
            result = loader.sanitize_as_dms_string(
                "Test String", case_style="snake_case"
            )

            assert result == "test_string"
            assert "_" in result

    def test_sanitize_as_dms_string_pascal_case(self):
        """Test sanitize_as_dms_string with PascalCase."""
        config = {
            "included_cfihos_types_config": [],
            "abs_fpath_model_raw_data_folder": Path("/data/"),
            "rdl_master_objects_fname": "master.csv",
            "rdl_master_object_id_col_name": "id",
            "rdl_master_object_name_col_name": "name",
            "rdl_master_object_file_type_col_name": "type",
            "processor_config_name": "test_processor",
            "id_prefix": "TEST",
        }

        with patch(
            "cfihos_handler.framework.importer.cfihos_loader.read_input_sheet"
        ) as mock_read:
            mock_df = pd.DataFrame({"id": [], "name": [], "type": []})
            mock_read.return_value = mock_df

            loader = CfihosModelLoader(**config)
            result = loader.sanitize_as_dms_string(
                "test string", case_style="PascalCase"
            )

            assert result == "TestString"

    def test_sanitize_as_dms_string_with_entity_relation(self):
        """Test sanitize_as_dms_string with is_entity_relation=True."""
        config = {
            "included_cfihos_types_config": [],
            "abs_fpath_model_raw_data_folder": Path("/data/"),
            "rdl_master_objects_fname": "master.csv",
            "rdl_master_object_id_col_name": "id",
            "rdl_master_object_name_col_name": "name",
            "rdl_master_object_file_type_col_name": "type",
            "processor_config_name": "test_processor",
            "id_prefix": "TEST",
        }

        with patch(
            "cfihos_handler.framework.importer.cfihos_loader.read_input_sheet"
        ) as mock_read:
            mock_df = pd.DataFrame({"id": [], "name": [], "type": []})
            mock_read.return_value = mock_df

            loader = CfihosModelLoader(**config)
            result = loader.sanitize_as_dms_string(
                "test", case_style="snake_case", is_entity_relation=True
            )

            assert result.endswith("_rel")

    def test_model_interpreter_name_property(self):
        """Test model_interpreter_name property."""
        assert CfihosModelLoader.model_interpreter_name == "CfihosModelLoader"

    def test_interpreting_model_name_property(self):
        """Test interpreting_model_name property."""
        config = {
            "included_cfihos_types_config": [],
            "abs_fpath_model_raw_data_folder": Path("/data/"),
            "rdl_master_objects_fname": "master.csv",
            "rdl_master_object_id_col_name": "id",
            "rdl_master_object_name_col_name": "name",
            "rdl_master_object_file_type_col_name": "type",
            "processor_config_name": "test_processor",
            "id_prefix": "TEST",
        }

        with patch(
            "cfihos_handler.framework.importer.cfihos_loader.read_input_sheet"
        ) as mock_read:
            mock_df = pd.DataFrame({"id": [], "name": [], "type": []})
            mock_read.return_value = mock_df

            loader = CfihosModelLoader(**config)
            assert loader.interpreting_model_name == "CFIHOS"
