"""Unit tests for _reader.py."""

from pathlib import Path
from unittest.mock import Mock, patch

from cognite.neat.core._data_model._shared import ImportedDataModel
from cognite.neat.core._data_model.models import UnverifiedPhysicalDataModel

from cfihos_handler._reader import CFIHOSReader


class TestCFIHOSReader:
    """Test suite for CFIHOSReader."""

    @patch("cfihos_handler._reader.CFIHOSImporter")
    def test_init(self, mock_importer):
        """Test CFIHOSReader initialization."""
        filepath = Path("/test/config.yaml")
        kwargs = {"model_type": "containers", "scope": "test_scope"}

        mock_importer_instance = Mock()
        mock_importer.return_value = mock_importer_instance

        reader = CFIHOSReader(filepath, **kwargs)

        mock_importer.assert_called_once_with(configFilePath=filepath, **kwargs)
        assert reader.config == filepath
        assert reader.addtional_parameters_dict == kwargs
        assert reader.importer == mock_importer_instance

    @patch("cfihos_handler._reader.CFIHOSImporter")
    def test_read(self, mock_importer):
        """Test read method."""
        filepath = Path("/test/config.yaml")
        mock_data_model = Mock(spec=UnverifiedPhysicalDataModel)
        mock_imported = ImportedDataModel(mock_data_model, {})

        mock_importer_instance = Mock()
        mock_importer_instance.to_data_model.return_value = mock_imported
        mock_importer.return_value = mock_importer_instance

        reader = CFIHOSReader(filepath, model_type="containers")
        result = reader.read()

        mock_importer_instance.to_data_model.assert_called_once()
        assert result == mock_imported

    @patch("cfihos_handler._reader.CFIHOSImporter")
    def test_read_returns_imported_data_model(self, mock_importer):
        """Test that read returns an ImportedDataModel instance."""
        filepath = Path("/test/config.yaml")
        mock_importer_instance = Mock()
        mock_data_model = Mock(spec=UnverifiedPhysicalDataModel)
        mock_imported = ImportedDataModel(mock_data_model, {})

        mock_importer_instance.to_data_model.return_value = mock_imported
        mock_importer.return_value = mock_importer_instance

        reader = CFIHOSReader(filepath)
        result = reader.read()

        assert isinstance(result, ImportedDataModel)
        # Verify the result equals the mock_imported which was constructed with mock_data_model
        assert result == mock_imported

    @patch("cfihos_handler._reader.CFIHOSImporter")
    def test_init_passes_all_kwargs_to_importer(self, mock_importer):
        """Test that all kwargs are passed to CFIHOSImporter."""
        filepath = Path("/test/config.yaml")
        kwargs = {
            "model_type": "views",
            "scope": "test_scope",
            "custom_param": "custom_value",
        }

        mock_importer_instance = Mock()
        mock_importer.return_value = mock_importer_instance

        CFIHOSReader(filepath, **kwargs)

        mock_importer.assert_called_once()
        call_kwargs = mock_importer.call_args[1]
        assert call_kwargs["model_type"] == "views"
        assert call_kwargs["scope"] == "test_scope"
        assert call_kwargs["custom_param"] == "custom_value"
