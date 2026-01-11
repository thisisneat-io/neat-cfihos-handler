"""Test for denormalization mapping in RootContainersProcessor."""

import pandas as pd
import pytest

from cognite.neat_cfihos_handler.framework.common.generic_classes import EntityStructure
from cognite.neat_cfihos_handler.framework.processing.processors.root_containers import (
    RootContainersProcessor,
)


@pytest.fixture
def sample_entities_df():
    """Create a sample entities dataframe for testing."""
    data = {
        EntityStructure.ID: [
            "TCFIHOS-30000311",  # Root
            "TCFIHOS-30000101",  # First child
            "TCFIHOS-30000397",  # Direct child of first child (the bug case)
            "TCFIHOS-30000667",  # First child
            "TCFIHOS-30000680",  # Child of first child (has sub-children)
            "TCFIHOS-30000681",  # Sub-child of TCFIHOS-30000680
        ],
        EntityStructure.NAME: [
            "Root",
            "FirstChild1",
            "DirectChild",
            "FirstChild2",
            "ChildWithSubChildren",
            "SubChild",
        ],
        EntityStructure.INHERITS_FROM_ID: [
            None,
            ["TCFIHOS-30000311"],
            ["TCFIHOS-30000101"],
            ["TCFIHOS-30000311"],
            ["TCFIHOS-30000667"],
            ["TCFIHOS-30000680"],
        ],
        EntityStructure.FIRSTCLASSCITIZEN: [False] * 6,
        "type": ["tag"] * 6,
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_properties_df():
    """Create a sample properties dataframe for testing."""
    return pd.DataFrame(
        {
            "propertyId": [],
            "entityId": [],
        }
    )


def test_direct_child_of_first_child_is_mapped(
    sample_entities_df, sample_properties_df
):
    """Test that direct children of first children are included in denormalization_map.

    This test specifically addresses the bug where direct children of first children
    (like CFIHOS-30000397) were not being added to the denormalization_map.
    """
    processor = RootContainersProcessor(
        model_processors_config=[],
        model_type="containers",
    )
    processor._df_entities = sample_entities_df
    processor._df_entity_properties = sample_properties_df

    # Create the denormalization mapping
    processor._create_denormalization_mapping()

    # Get the mapping
    mapping = processor.tag_and_equipment_classes_to_root_nodes

    # Verify all entities are in the mapping
    print(f"\nMapping keys: {sorted(mapping.keys())}")
    print(
        "Expected entities: CFIHOS-30000397, CFIHOS-30000101, CFIHOS-30000667, CFIHOS-30000680, CFIHOS-30000681"
    )

    # TCFIHOS-30000397 should be in the mapping (THE BUG CASE)
    # It should map to its parent first child (TCFIHOS-30000101)
    assert "CFIHOS-30000397" in mapping, (
        f"Direct child of first child (CFIHOS-30000397) should be in mapping. "
        f"Current mapping keys: {sorted(mapping.keys())}"
    )
    assert (
        mapping["CFIHOS-30000397"] == "CFIHOS-30000101"
    ), f"Direct child should map to its first child parent. Got: {mapping.get('CFIHOS-30000397')}"

    # TCFIHOS-30000101 (first child) should map to itself
    assert "CFIHOS-30000101" in mapping, "First child should be in mapping"
    assert (
        mapping["CFIHOS-30000101"] == "CFIHOS-30000101"
    ), "First child should map to itself"

    # TCFIHOS-30000667 (first child) should map to itself
    assert "CFIHOS-30000667" in mapping, "First child should be in mapping"
    assert (
        mapping["CFIHOS-30000667"] == "CFIHOS-30000667"
    ), "First child should map to itself"

    # TCFIHOS-30000680 should also be in the mapping
    assert "CFIHOS-30000680" in mapping, "Child with sub-children should be in mapping"
    assert (
        mapping["CFIHOS-30000680"] == "CFIHOS-30000667"
    ), "Child should map to its first child parent"

    # TCFIHOS-30000681 (sub-child) should map to first child
    assert "CFIHOS-30000681" in mapping, "Sub-child should be in mapping"
    assert (
        mapping["CFIHOS-30000681"] == "CFIHOS-30000667"
    ), "Sub-child should map to first child ancestor"


def test_exact_user_scenario():
    """Test the exact scenario from the user's bug report.

    User reported: CFIHOS-00000028-> CFIHOS-30000311 -> CFIHOS-30000101 ->CFIHOS-30000397
    CFIHOS-30000397 is not in the denormalization_map dictionary
    """
    data = {
        EntityStructure.ID: [
            "TCFIHOS-00000028",  # Root ancestor
            "TCFIHOS-30000311",  # Root
            "TCFIHOS-30000101",  # First child
            "TCFIHOS-30000397",  # Direct child of first child (THE BUG)
        ],
        EntityStructure.NAME: [
            "RootAncestor",
            "Root",
            "FirstChild",
            "DirectChild",
        ],
        EntityStructure.INHERITS_FROM_ID: [
            None,
            ["TCFIHOS-00000028"],
            ["TCFIHOS-30000311"],
            ["TCFIHOS-30000101"],  # Direct child of first child
        ],
        EntityStructure.FIRSTCLASSCITIZEN: [False] * 4,
        "type": ["tag"] * 4,
    }
    entities_df = pd.DataFrame(data)
    properties_df = pd.DataFrame({"propertyId": [], "entityId": []})

    processor = RootContainersProcessor(
        model_processors_config=[],
        model_type="containers",
    )
    processor._df_entities = entities_df
    processor._df_entity_properties = properties_df

    # Create the denormalization mapping
    processor._create_denormalization_mapping()

    # Get the mapping
    mapping = processor.tag_and_equipment_classes_to_root_nodes

    # THE BUG: CFIHOS-30000397 should be in the mapping
    assert "CFIHOS-30000397" in mapping, (
        f"BUG: CFIHOS-30000397 (direct child of first child) should be in mapping. "
        f"Current keys: {sorted(mapping.keys())}"
    )
    assert (
        mapping["CFIHOS-30000397"] == "CFIHOS-30000101"
    ), f"CFIHOS-30000397 should map to CFIHOS-30000101, got {mapping.get('CFIHOS-30000397')}"

    # CFIHOS-30000101 should map to itself
    assert "CFIHOS-30000101" in mapping
    assert mapping["CFIHOS-30000101"] == "CFIHOS-30000101"


def test_equipment_scenario():
    """Test the exact ECFIHOS scenario from the user.

    ECFIHOS-30000311 --> ECFIHOS-30000101 --> ECFIHOS-30000397
    Getting the denormalized parent for ECFIHOS-30000397 should return ECFIHOS-30000101
    """
    data = {
        EntityStructure.ID: [
            "ECFIHOS-30000311",  # Equipment Root
            "ECFIHOS-30000101",  # First child
            "ECFIHOS-30000397",  # Direct child of first child
        ],
        EntityStructure.NAME: [
            "EquipmentRoot",
            "FirstChild",
            "DirectChild",
        ],
        EntityStructure.INHERITS_FROM_ID: [
            None,
            ["ECFIHOS-30000311"],
            ["ECFIHOS-30000101"],  # Direct child of first child
        ],
        EntityStructure.FIRSTCLASSCITIZEN: [False] * 3,
        "type": ["equipment"] * 3,
    }
    entities_df = pd.DataFrame(data)
    properties_df = pd.DataFrame({"propertyId": [], "entityId": []})

    processor = RootContainersProcessor(
        model_processors_config=[],
        model_type="containers",
    )
    processor._df_entities = entities_df
    processor._df_entity_properties = properties_df

    # Create the denormalization mapping
    processor._create_denormalization_mapping()

    # Get the mapping
    mapping = processor.tag_and_equipment_classes_to_root_nodes

    print(f"\nEquipment scenario mapping keys: {sorted(mapping.keys())}")
    print("Looking for CFIHOS-30000397 in mapping...")

    # THE BUG: CFIHOS-30000397 should be in the mapping
    assert "CFIHOS-30000397" in mapping, (
        f"BUG: ECFIHOS-30000397 (direct child of first child) should be in mapping. "
        f"Current keys: {sorted(mapping.keys())}"
    )

    # It should map to CFIHOS-30000101 (normalized from ECFIHOS-30000101)
    expected_parent = "CFIHOS-30000101"
    actual_parent = mapping["CFIHOS-30000397"]
    assert actual_parent == expected_parent, (
        f"ECFIHOS-30000397 should map to {expected_parent}, "
        f"but got {actual_parent}. Full mapping: {mapping}"
    )

    # CFIHOS-30000101 should map to itself
    assert "CFIHOS-30000101" in mapping
    assert mapping["CFIHOS-30000101"] == "CFIHOS-30000101"

    # Test the helper function
    result = processor._assign_root_nodes_to_tag_and_equipment_classes(
        "ECFIHOS-30000397", "some_property"
    )
    assert result == "CFIHOS_30000101", (
        f"_assign_root_nodes_to_tag_and_equipment_classes should return "
        f"CFIHOS_30000101, but got {result}"
    )


def test_equipment_scenario_ecfihos_30000397():
    """Test the exact equipment scenario from the user's bug report.

    User reported: ECFIHOS-30000311 --> ECFIHOS-30000101 --> ECFIHOS-30000397
    The denormalized parent for ECFIHOS-30000397 should return ECFIHOS-30000101.
    """
    data = {
        EntityStructure.ID: [
            "ECFIHOS-30000311",  # Equipment Root
            "ECFIHOS-30000101",  # First child
            "ECFIHOS-30000397",  # Direct child of first child (THE BUG)
        ],
        EntityStructure.NAME: [
            "EquipmentRoot",
            "FirstChild",
            "DirectChild",
        ],
        EntityStructure.INHERITS_FROM_ID: [
            None,
            ["ECFIHOS-30000311"],
            ["ECFIHOS-30000101"],  # Direct child of first child
        ],
        EntityStructure.FIRSTCLASSCITIZEN: [False] * 3,
        "type": ["equipment"] * 3,
    }
    entities_df = pd.DataFrame(data)
    properties_df = pd.DataFrame({"propertyId": [], "entityId": []})

    processor = RootContainersProcessor(
        model_processors_config=[],
        model_type="containers",
    )
    processor._df_entities = entities_df
    processor._df_entity_properties = properties_df

    # Debug: Check the dataframe
    print("\n=== DEBUG INFO ===")
    print("Entities in dataframe:")
    for _, row in entities_df.iterrows():
        print(
            f"  {row[EntityStructure.ID]} -> parents: {row[EntityStructure.INHERITS_FROM_ID]}"
        )

    # Create the denormalization mapping
    processor._create_denormalization_mapping()

    # Get the mapping
    mapping = processor.tag_and_equipment_classes_to_root_nodes

    print(f"\nEquipment mapping: {mapping}")
    print("Looking for: CFIHOS-30000397 (normalized from ECFIHOS-30000397)")

    # THE BUG: ECFIHOS-30000397 should be in the mapping (normalized to CFIHOS-30000397)
    if "CFIHOS-30000397" not in mapping:
        print("\nERROR: CFIHOS-30000397 is NOT in mapping!")
        print(f"Available keys: {sorted(mapping.keys())}")
        # Let's check what entities were processed
        entities_to_check = ["ECFIHOS-30000311", "ECFIHOS-30000101", "ECFIHOS-30000397"]
        for eid in entities_to_check:
            normalized = (
                "CFIHOS-" + eid.split("-", 1)[1] if eid.startswith(("T", "E")) else eid
            )
            print(
                f"  {eid} -> normalized: {normalized} -> in mapping: {normalized in mapping}"
            )

    assert "CFIHOS-30000397" in mapping, (
        f"BUG: ECFIHOS-30000397 (direct child of first child) should be in mapping. "
        f"Current keys: {sorted(mapping.keys())}"
    )

    # It should map to ECFIHOS-30000101 (normalized to CFIHOS-30000101)
    expected_parent = "CFIHOS-30000101"
    actual_parent = mapping["CFIHOS-30000397"]
    assert actual_parent == expected_parent, (
        f"ECFIHOS-30000397 should map to ECFIHOS-30000101 (normalized: {expected_parent}), "
        f"but got {actual_parent}"
    )

    # Also test using the helper function
    result = processor._assign_root_nodes_to_tag_and_equipment_classes(
        "ECFIHOS-30000397", "some_property"
    )
    assert result == "CFIHOS_30000101", (
        f"_assign_root_nodes_to_tag_and_equipment_classes should return "
        f"CFIHOS_30000101, but got {result}"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
