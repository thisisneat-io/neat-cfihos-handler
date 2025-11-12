"""Unit tests for plugin.py."""

from pathlib import Path
from unittest.mock import Mock, patch

from cognite.neat_cfihos_handler.plugin import CFIHOS_2DataModelImporter


class TestCFIHOS_2DataModelImporter:
    """Test suite for CFIHOS_2DataModelImporter."""

    def test_configure_with_configuration_dir(self):
        """Test configure method with configurationDir provided."""
        plugin = CFIHOS_2DataModelImporter()
        config_dir = "/path/to/config"
        kwargs = {"model_type": "containers", "scope": "test_scope"}

        with patch(
            "cognite.neat_cfihos_handler.plugin.CFIHOSProcessor"
        ) as mock_processor:
            mock_instance = Mock()
            mock_processor.return_value = mock_instance
            result = plugin.configure(configurationDir=config_dir, **kwargs)

            mock_processor.assert_called_once_with(
                configurationDir=Path(config_dir), **kwargs
            )
            assert result == mock_instance

    def test_configure_without_configuration_dir(self):
        """Test configure method without configurationDir."""
        plugin = CFIHOS_2DataModelImporter()
        kwargs = {"model_type": "containers"}

        with patch(
            "cognite.neat_cfihos_handler.plugin.CFIHOSProcessor"
        ) as mock_processor:
            mock_instance = Mock()
            mock_processor.return_value = mock_instance
            result = plugin.configure(**kwargs)

            mock_processor.assert_called_once_with(configurationDir=None, **kwargs)
            assert result == mock_instance

    def test_configure_passes_all_kwargs(self):
        """Test that all kwargs are passed to CFIHOSProcessor."""
        plugin = CFIHOS_2DataModelImporter()
        kwargs = {
            "model_type": "views",
            "scope": "test_scope",
            "custom_param": "custom_value",
        }

        with patch(
            "cognite.neat_cfihos_handler.plugin.CFIHOSProcessor"
        ) as mock_processor:
            mock_instance = Mock()
            mock_processor.return_value = mock_instance
            plugin.configure(**kwargs)

            mock_processor.assert_called_once()
            call_kwargs = mock_processor.call_args[1]
            assert call_kwargs["model_type"] == "views"
            assert call_kwargs["scope"] == "test_scope"
            assert call_kwargs["custom_param"] == "custom_value"
