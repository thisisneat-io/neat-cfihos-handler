"""Unit tests for sparse_model_manager.py."""

from unittest.mock import Mock, patch

import pytest
from cognite.neat.core._issues.errors import NeatValueError

from cognite.neat_cfihos_handler.framework.common.generic_classes import SparseModelType
from cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager import (
    SparseCfihosManager,
)


class TestSparseCfihosManager:
    """Test suite for SparseCfihosManager."""

    @pytest.fixture
    def minimal_processor_config(self):
        """Create a minimal processor config for testing."""
        return {
            "model_processors_config": [{"test_processor": {"id_prefix": "TEST"}}],
            "containers_indexes": {},
            "container_data_model_space": "test_space",
            "views_data_model_space": "test_views_space",
            "model_version": "1.0",
            "model_creator": "test_creator",
            "data_model_name": "test_model",
            "data_model_description": "test description",
            "data_model_external_id": "test_external_id",
            "dms_identifire": "test_dms",
            "processor_type": "sparse",
            "scope_config": {},
            "scopes": [],
        }

    @pytest.fixture
    def processor_config_with_scopes(self):
        """Create a processor config with scopes."""
        config = {
            "model_processors_config": [{"test_processor": {"id_prefix": "TEST"}}],
            "containers_indexes": {},
            "container_data_model_space": "test_space",
            "views_data_model_space": "test_views_space",
            "model_version": "1.0",
            "model_creator": "test_creator",
            "data_model_name": "test_model",
            "data_model_description": "test description",
            "data_model_external_id": "test_external_id",
            "dms_identifire": "test_dms",
            "processor_type": "sparse",
            "scope_config": {},
            "scopes": [
                {
                    "scope_model_external_id": "test_scope_model_external_id",
                    "scope_model_version": "test_scope_model_version",
                    "scope_name": "test_scope",
                    "scope_description": "test scope description",
                    "scope_subset": [],
                }
            ],
        }
        return config

    @patch(
        "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
    )
    def test_init_with_containers_model_type(
        self, mock_sparse_processor, minimal_processor_config
    ):
        """Test initialization with containers model type."""
        mock_processor_instance = Mock()
        mock_processor_instance.model_properties = {}
        mock_processor_instance.map_dms_id_to_entity_id = {}
        mock_processor_instance.map_entity_id_to_dms_id = {}
        mock_processor_instance.model_entities = {}
        mock_processor_instance.issue_list = Mock()
        mock_sparse_processor.return_value = mock_processor_instance

        manager = SparseCfihosManager(
            minimal_processor_config, model_type=SparseModelType.CONTAINERS
        )

        assert manager.model_type == SparseModelType.CONTAINERS
        assert manager.scope == ""
        mock_sparse_processor.assert_called_once()
        mock_processor_instance.process_and_collect_models.assert_called_once()

    @patch(
        "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
    )
    def test_init_with_views_model_type(
        self, mock_sparse_processor, processor_config_with_scopes
    ):
        """Test initialization with views model type and scope."""
        mock_processor_instance = Mock()
        mock_processor_instance.model_properties = {}
        mock_processor_instance.map_dms_id_to_entity_id = {}
        mock_processor_instance.map_entity_id_to_dms_id = {}
        mock_processor_instance.model_entities = {}
        mock_processor_instance.issue_list = Mock()
        mock_sparse_processor.return_value = mock_processor_instance

        manager = SparseCfihosManager(
            processor_config_with_scopes,
            model_type=SparseModelType.VIEWS,
            scope="test_scope",
        )

        assert manager.model_type == SparseModelType.VIEWS
        assert manager.scope == "test_scope"

    def test_init_raises_error_if_model_type_empty(self, minimal_processor_config):
        """Test that initialization raises error if model_type is empty."""
        with pytest.raises(
            NeatValueError, match=r"model_type cannot be None or empty( string)?"
        ):
            with patch(
                "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
            ):
                SparseCfihosManager(minimal_processor_config, model_type="")

    def test_init_raises_error_if_model_type_invalid(self, minimal_processor_config):
        """Test that initialization raises error if model_type is invalid."""
        with pytest.raises(
            NeatValueError, match="Invalid model_type.*Valid values are"
        ):
            with patch(
                "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
            ):
                SparseCfihosManager(minimal_processor_config, model_type="invalid")

    def test_init_raises_error_if_views_without_scope(
        self, processor_config_with_scopes
    ):
        """Test that initialization raises error if views model_type without scope."""
        with pytest.raises(
            NeatValueError,
            match=r"scope cannot be None or empty( string)? when model_type is 'views'",
        ):
            with patch(
                "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
            ):
                SparseCfihosManager(
                    processor_config_with_scopes,
                    model_type=SparseModelType.VIEWS,
                    scope="",
                )

    def test_init_missing_required_keys(self):
        """Test that initialization raises error if required config keys are missing."""
        incomplete_config = {"model_processors_config": []}

        with pytest.raises(
            NeatValueError, match="Missing required keys in configuration"
        ):
            with patch(
                "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
            ):
                SparseCfihosManager(
                    incomplete_config, model_type=SparseModelType.CONTAINERS
                )

    @patch(
        "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.build_neat_model_from_entities"
    )
    @patch(
        "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
    )
    def test_read_model_containers(
        self, mock_sparse_processor, mock_build_model, minimal_processor_config
    ):
        """Test read_model method for containers type."""
        mock_processor_instance = Mock()
        mock_processor_instance.model_properties = {}
        mock_processor_instance.map_dms_id_to_entity_id = {}
        mock_processor_instance.map_entity_id_to_dms_id = {}
        mock_processor_instance.model_entities = {}
        mock_processor_instance.issue_list = Mock()
        mock_sparse_processor.return_value = mock_processor_instance

        mock_build_model.return_value = ([], [], [])

        manager = SparseCfihosManager(
            minimal_processor_config, model_type=SparseModelType.CONTAINERS
        )
        result = manager.read_model()

        assert result is not None
        assert result.Properties == []
        assert result.Containers == []
        assert result.Views == []
        mock_build_model.assert_called_once()

    @patch(
        "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.collect_model_subset"
    )
    @patch(
        "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.build_neat_model_from_entities"
    )
    @patch(
        "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
    )
    def test_read_model_views(
        self,
        mock_sparse_processor,
        mock_build_model,
        mock_collect_subset,
        processor_config_with_scopes,
    ):
        """Test read_model method for views type."""
        mock_processor_instance = Mock()
        mock_processor_instance.model_properties = {}
        mock_processor_instance.map_dms_id_to_entity_id = {}
        mock_processor_instance.map_entity_id_to_dms_id = {}
        mock_processor_instance.model_entities = {"entity1": {}}
        mock_processor_instance.issue_list = Mock()
        mock_sparse_processor.return_value = mock_processor_instance

        mock_collect_subset.return_value = {"entity1": {}}
        mock_build_model.return_value = ([], [], [])

        manager = SparseCfihosManager(
            processor_config_with_scopes,
            model_type=SparseModelType.VIEWS,
            scope="test_scope",
        )
        result = manager.read_model()

        assert result is not None
        assert result.Properties == []
        assert result.Containers == []
        assert result.Views == []
        mock_collect_subset.assert_called_once()
        mock_build_model.assert_called_once()

    @patch(
        "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
    )
    def test_read_model_raises_error_if_model_type_empty(
        self, mock_sparse_processor, minimal_processor_config
    ):
        """Test that read_model raises error if model_type is empty."""
        mock_processor_instance = Mock()
        mock_processor_instance.model_properties = {}
        mock_processor_instance.map_dms_id_to_entity_id = {}
        mock_processor_instance.map_entity_id_to_dms_id = {}
        mock_processor_instance.model_entities = {}
        mock_processor_instance.issue_list = Mock()
        mock_sparse_processor.return_value = mock_processor_instance

        manager = SparseCfihosManager(
            minimal_processor_config, model_type=SparseModelType.CONTAINERS
        )
        manager.model_type = ""

        with pytest.raises(NeatValueError, match="model_type is a required parameter"):
            manager.read_model()

    @patch(
        "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
    )
    def test_read_model_raises_error_if_model_type_invalid(
        self, mock_sparse_processor, minimal_processor_config
    ):
        """Test that read_model raises error if model_type is invalid.

        Note: SparsePropertiesProcessor is patched because it gets instantiated during
        SparseCfihosManager.__init__, not in read_model(). The patch allows us to create
        the manager in the test without creating a real processor instance.
        """
        mock_processor_instance = Mock()
        mock_processor_instance.model_properties = {}
        mock_processor_instance.map_dms_id_to_entity_id = {}
        mock_processor_instance.map_entity_id_to_dms_id = {}
        mock_processor_instance.model_entities = {}
        mock_processor_instance.issue_list = Mock()
        mock_sparse_processor.return_value = mock_processor_instance

        manager = SparseCfihosManager(
            minimal_processor_config, model_type=SparseModelType.CONTAINERS
        )
        manager.model_type = "invalid"

        with pytest.raises(NeatValueError, match=r"Invalid model_type: invalid"):
            manager.read_model()

    @patch(
        "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
    )
    def test_get_scope_by_name(
        self, mock_sparse_processor, processor_config_with_scopes
    ):
        """Test get_scope_by_name method."""
        mock_processor_instance = Mock()
        mock_processor_instance.model_properties = {}
        mock_processor_instance.map_dms_id_to_entity_id = {}
        mock_processor_instance.map_entity_id_to_dms_id = {}
        mock_processor_instance.model_entities = {}
        mock_processor_instance.issue_list = Mock()
        mock_sparse_processor.return_value = mock_processor_instance

        manager = SparseCfihosManager(
            processor_config_with_scopes,
            model_type=SparseModelType.VIEWS,
            scope="test_scope",
        )

        scope = manager.get_scope_by_name("test_scope")
        assert scope["scope_name"] == "test_scope"
        assert scope["scope_description"] == "test scope description"

    @patch(
        "cognite.neat_cfihos_handler.framework.processing.model_managers.sparse_model_manager.SparsePropertiesProcessor"
    )
    def test_get_scope_by_name_not_found(
        self, mock_sparse_processor, processor_config_with_scopes
    ):
        """Test get_scope_by_name raises error when scope not found."""
        mock_processor_instance = Mock()
        mock_processor_instance.model_properties = {}
        mock_processor_instance.map_dms_id_to_entity_id = {}
        mock_processor_instance.map_entity_id_to_dms_id = {}
        mock_processor_instance.model_entities = {}
        mock_processor_instance.issue_list = Mock()
        mock_sparse_processor.return_value = mock_processor_instance

        manager = SparseCfihosManager(
            processor_config_with_scopes,
            model_type=SparseModelType.VIEWS,
            scope="not_found_scope",
        )

        with pytest.raises(NeatValueError, match="Scope 'nonexistent' not found"):
            manager.get_scope_by_name("nonexistent")
