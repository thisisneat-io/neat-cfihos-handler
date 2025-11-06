"""Utility functions for the CFIHOS framework.

This module provides a collection of utility functions that are used throughout
the framework to perform various operations such as reading and writing files,
converting strings to different cases, and collecting subsets of the model.
"""
import json
import pathlib
import re

import chardet
import pandas as pd
from cognite.neat.core._issues.errors import NeatValueError

from cfihos_handler.framework.common import constants

from .generic_classes import (
    DataSource,
    EntityStructure,
    NeatContainerStructure,
    NeatPropertyStructure,
    NeatViewStructure,
    PropertyStructure,
    Relations,
    ScopeConfig,
)
from .log import log_init

logging = log_init(f"{__name__}", "i")


def read_scope(fpath: str, id_col: str) -> list[str]:
    """Read scope from a CSV file and return a list of unique IDs.

    Args:
        fpath (str): Path to the CSV file containing scope data.
        id_col (str): Name of the column containing the IDs.

    Raises:
        ValueError: If the file is not a CSV file or contains duplicate IDs.
        KeyError: If the specified id column is not present in the file.

    Returns:
        list[str]: List of unique IDs from the scope file.
    """
    if not fpath.endswith(".csv"):
        raise ValueError(f"Provided scope file {fpath} must be '.csv'")

    df = pd.read_csv(fpath)
    if id_col not in df.columns:
        raise KeyError(f"Given id column '{id_col}' is not present in scope file")
    scope = df[id_col].values
    if len(scope) != len(set(scope)):
        duplicate_ids = set([x for x in scope if list(scope).count(x) > 1])
        raise ValueError(
            f"Provided scope file with id column '{id_col}' contains duplicates: {duplicate_ids}"
        )
    return scope


def check_file_content_encoding(fpath: str) -> str:
    """Detect the encoding of a file and return it with fallback handling.

    Args:
        fpath (str): Path to the file to check encoding for.

    Returns:
        str: The detected encoding, or 'cp1252' as a fallback for low confidence detections.
    """
    with open(fpath, "rb") as f:
        raw = f.read(100_000)
        result = chardet.detect(raw)
        encoding = result.get("encoding", "")
        confidence = result.get("confidence", 0)

        # Heuristic fallbacks
        if not encoding or encoding.lower() == "ascii" or confidence < 0.7:
            return "cp1252"  # very tolerant for Excel/Windows files

        return encoding


def read_input_sheet(
    fpath: str,
    source: DataSource = DataSource.default(),
    **kwargs,
) -> pd.DataFrame:
    """Read input sheet from a given data source `source`.

    Args:
        fpath (str): file path to the input sheet.
        source (DataSource, optional): source where input sheet resides. Defaults to DataSource.default() ("csv").

    Raises:
        ValueError: if `source` is not a valid DataSource. Could also consider this as a NotImplementedError.

    Returns:
        pd.DataFrame: pandas dataframe of the input sheet.
    """
    match source:
        case DataSource.CSV.value:
            new_kwargs = dict(kwargs)
            new_kwargs["encoding"] = check_file_content_encoding(fpath)
            return pd.read_csv(fpath, **new_kwargs)
        case DataSource.GITHUB.value:
            return pd.read_csv(
                fpath, **kwargs
            )  # TODO: add the original github integration code
        case _:
            raise ValueError(f"Unknown data source {source}")


def create_folder_structure_if_missing(path: str):
    """Create folder structure if it doesn't exist.

    Args:
        path (str): Path to the folder structure to create.
    """
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)


def to_pascal_case(s: str) -> str:
    """Convert a string to PascalCase format.

    Args:
        s (str): Input string to convert.

    Returns:
        str: String converted to PascalCase format.
    """
    s = re.sub(r"(_|-|,|\)|\(|/)+", " ", s).title().replace(" ", "")
    return "".join([s[0].upper(), s[1:]])


def to_camel_case(s: str) -> str:
    """Convert a string to camelCase format.

    Args:
        s (str): Input string to convert.

    Returns:
        str: String converted to camelCase format.
    """
    s = re.sub(r"(_|-|,|\)|\(|/)+", " ", s).title().replace(" ", "")
    return "".join([s[0].lower(), s[1:]])


def is_camel_case(s: str) -> bool:
    """Check if a string is in camelCase format.

    Args:
        s (str): String to check.

    Returns:
        bool: True if the string is in camelCase format, False otherwise.
    """
    if not s[0].islower():
        return False
    s = s[0].upper() + s[1:]
    return s != s.lower() and s != s.upper()


def is_pascal_case(s: str) -> bool:
    """Check if a string is in PascalCase format.

    Args:
        s (str): String to check.

    Returns:
        bool: True if the string is in PascalCase format, False otherwise.
    """
    return s[0].isupper() and is_camel_case(s=s[0].lower() + s[1:])


def generate_dms_friendly_name(name: str, max_length: int) -> str:
    """Generate a DMS-friendly name by converting to PascalCase and truncating if necessary.

    Args:
        name (str): Original name to convert.
        max_length (int): Maximum allowed length for the name.

    Raises:
        ValueError: If the generated name exceeds the maximum length plus model version length.

    Returns:
        str: DMS-friendly name in PascalCase format.
    """
    name = to_pascal_case(name)
    if len(name) < max_length:
        return name
    # TODO - hacky solution
    offset_value = 10
    new_name = name[: max_length - offset_value] + "".join(
        [c for c in name[max_length - offset_value :] if c.isupper()]
    )
    if len(new_name) > max_length + constants.MODEL_VERSION_LENGTH:
        raise ValueError(f"entity: New-name: {new_name} old name {name}")
    # print(f"[WARNING] - dms name: {name} was shorten to {new_name}")
    return new_name


def generate_dms_friendly_property_name(name: str, max_length: int):
    """Generate a DMS-friendly property name by converting to camelCase and truncating if necessary.

    Args:
        name (str): Original property name to convert.
        max_length (int): Maximum allowed length for the property name.

    Raises:
        ValueError: If the generated property name exceeds the maximum length plus model version length.

    Returns:
        str: DMS-friendly property name in camelCase format.
    """
    name = to_camel_case(name)
    if len(name) < max_length:
        return name
    # TODO - hacky solution
    offset_value = 10
    new_name = name[: max_length - offset_value] + "".join(
        [c for c in name[max_length - offset_value :] if c.isupper()]
    )

    if len(new_name) > max_length + constants.MODEL_VERSION_LENGTH:
        raise ValueError(f"prop: New-name: {new_name} old name {name}")
    return new_name


def dfs(visited: set, entity_id: str, full_model: dict):
    """Perform depth-first search to traverse entity relationships and collect visited entities.

    Args:
        visited (set): Set of already visited entity IDs.
        entity_id (str): Current entity ID to process.
        full_model (dict): Complete model containing all entities and their relationships.

    Returns:
        set: Updated set of visited entity IDs after traversal.
    """
    if entity_id not in visited:
        visited.add(entity_id)
        entity_data = full_model[entity_id]
        extends = entity_data.get(EntityStructure.INHERITS_FROM_ID, [])

        if entity_data[EntityStructure.PROPERTIES]:
            properties_to_extend = set()
            for property in entity_data[EntityStructure.PROPERTIES]:
                if property[PropertyStructure.PROPERTY_TYPE] == "ENTITY_RELATION":
                    prop_target_type = property[PropertyStructure.TARGET_TYPE]
                    if prop_target_type is False:
                        continue
                    if prop_target_type not in visited:
                        properties_to_extend.add(prop_target_type)
            for prop_to_extend in properties_to_extend:
                dfs(visited, prop_to_extend, full_model)
        if extends is None:
            return visited
        for parent in extends:
            parent_entity_id = parent
            dfs(visited, parent_entity_id, full_model)
    return visited


def collect_model_subset(
    full_model: dict,
    scope_config: str,
    scope: list[str],
    containers_space: str = None,
):
    """Collect a subset of the full model based on scope configuration and scope list.

    Args:
        full_model (dict): Complete model containing all entities.
        scope_config (str): Configuration defining how to scope the model (SCOPED, TAGS, EQUIPMENT).
        scope (list[str]): List of entity IDs to include when using SCOPED configuration.
        containers_space (str): The space identifier where the containers reside.

    Returns:
        dict: Subset of the full model containing only the entities within scope and their dependencies.
    """
    visited: set[str] = set()  # Set to keep track of visited nodes of the graph
    # entities = {scope_model_id: full_model[scope_model_id] for scope_model_id in scope}
    entities = {
        cfihos_id: full_model[cfihos_id]
        for cfihos_id in full_model
        if (
            (scope_config == ScopeConfig.SCOPED and cfihos_id in scope)
            or (
                scope_config == ScopeConfig.TAGS
                and cfihos_id.startswith(constants.CFIHOS_TYPE_TAG_PREFIX)
            )
            or (
                scope_config == ScopeConfig.EQUIPMENT
                and cfihos_id.startswith(constants.CFIHOS_TYPE_EQUIPMENT_PREFIX)
            )
            or full_model[cfihos_id][EntityStructure.FIRSTCLASSCITIZEN]
        )
    }

    for entity_id in entities:
        visited = visited.union(
            dfs(
                visited,
                entity_id,
                full_model,
            )
        )

    scoped_entities = {key: full_model[key] for key in visited}

    logging.info(f"Selected {len(visited)} objects from {len(full_model)}")
    inheritance_tree = create_inheritance_tree_from_root_node(scoped_entities)
    scoped_entities = assign_view_filters_by_inheritance_tree(
        entities=scoped_entities,
        inheritance_tree=inheritance_tree,
        containers_space=containers_space,
    )
    return scoped_entities


def collect_property_subset(subset_model: dict, property_space: dict) -> dict:
    """Collect a subset of properties based on the entities in the subset model.

    Args:
        subset_model (dict): Subset of the model containing selected entities.
        property_space (dict): Complete property space containing all available properties.

    Returns:
        dict: Subset of properties that are used by entities in the subset model.
    """
    subset_model_properties = set()
    for entity_id, entity_data in subset_model.items():
        for prop in entity_data["properties"]:
            subset_model_properties.add(prop[PropertyStructure.ID])

    return {key: property_space[key] for key in subset_model_properties}


def create_inheritance_tree_from_root_node(entities: dict) -> dict:
    """Build a mapping of all parent entities to their descendant entities, based on inheritance relationships.

    Args:
        entities (dict): Dictionary where each key is an entity ID and each value is a dictionary of attributes of the entity.

    Returns:
        dict: A dictionary where each key is a parent entity ID and each value is a list of all descendant entity IDs
              (including indirect descendants via transitive inheritance).
    """
    parent_child_dict: dict[str, set[str]] = {}
    for entity_id, entity_data in entities.items():
        if entity_data[EntityStructure.INHERITS_FROM_ID]:
            for inheritance_id in entity_data[EntityStructure.INHERITS_FROM_ID]:
                try:
                    parent_child_dict[inheritance_id].add(
                        entity_data[EntityStructure.ID]
                    )
                except KeyError:
                    parent_child_dict[inheritance_id] = set()
                    parent_child_dict[inheritance_id].add(
                        entity_data[EntityStructure.ID]
                    )

    def visit(childs, parent_child_dict, inheritance_tree, found_childs):
        for child in childs:
            found_childs.add(child)
            if child in inheritance_tree:
                found_childs.update(set(inheritance_tree[child]))
                continue
            if parent_child_dict.get(child):
                visit(
                    parent_child_dict[child],
                    parent_child_dict,
                    inheritance_tree,
                    found_childs,
                )
        return

    inheritance_tree: dict[str, list[str]] = {}
    for entity_id, children in parent_child_dict.items():
        found_children: set[str] = set()
        visit(children, parent_child_dict, inheritance_tree, found_children)
        inheritance_tree[entity_id] = list(found_children)
    return inheritance_tree


def create_view_filter(
    space: str, entity_id: str, inherited_entities: list[str]
) -> str:
    """Create a view filter for an entity in a given space.

    The filter incorporates inherited entities and is used to determine which entities
    should be included based on their IDs and type groups.

    Args:
        space (str): The space identifier where the entities reside.
        entity_id (str): The unique identifier of the entity.
        inherited_entities (list[str]): A list of inherited entity IDs.

    Returns:
        dict: A dictionary representing the view filter in the form of a nested dictionary structure.
    """
    return (
        "rawFilter("
        + json.dumps(
            {
                "and": [
                    {
                        "in": {
                            "property": [space, "EntityTypeGroup", "entityType"],
                            "values": list(
                                set(
                                    [
                                        id
                                        if id.startswith("ECFIHOS")
                                        or id.startswith("TCFIHOS")
                                        else id
                                        for id in inherited_entities
                                        if id
                                    ]
                                    + [
                                        entity_id
                                        if entity_id.startswith("ECFIHOS")
                                        or entity_id.startswith("TCFIHOS")
                                        else entity_id
                                    ]
                                )
                            ),
                        }
                    }
                ]
            }
        )
        + ")"
    )


def assign_view_filters_by_inheritance_tree(
    entities: dict, inheritance_tree: dict, containers_space: str = None
) -> dict:
    """Assign view filters to the entities based on the inheritance tree.

    Args:
        entities (dict): A dictionary containing entity information.
        inheritance_tree (dict): A dictionary containing the inheritance tree.
        containers_space (str): The space identifier where the containers reside.

    Returns:
        dict: A dictionary containing the entities with assigned view filters.
    """
    for entity_id, entity_data in entities.items():
        if entity_data[EntityStructure.FIRSTCLASSCITIZEN]:
            entity_data[EntityStructure.VIEW_FILTER] = None
            continue
        view_filter = create_view_filter(
            containers_space, entity_id, inheritance_tree.get(entity_id, [])
        )
        entity_data[EntityStructure.VIEW_FILTER] = view_filter
    return entities


def save_json(data: list, fpath: str):
    """Save data as JSON to a file with camelCase formatting.

    Args:
        data (list): List of objects to save, each must have a dump method.
        fpath (str): File path where the JSON data will be saved.
    """
    # TODO: Deterministic serialization with predictable ordering of dict keys
    data_dump = [c.dump(camel_case=True) for c in data]
    # data_dump = [v.update({"properties": dict(sorted(v["properties"].items()))}) for v in data_dump]
    json.dump(data_dump, open(fpath, "w"), ensure_ascii=False, indent=4)


def get_entity_relation_target(
    Property_id, entity_id, entities, property_type
) -> str | None:
    """Get the target entity for a relation property based on the property type.

    Args:
        Property_id: ID of the property to find the target for.
        entity_id: ID of the entity containing the property.
        entities: Dictionary of all entities in the model.
        property_type: Type of relation (DIRECT, EDGE, or REVERSE).

    Raises:
        NeatValueError: If edge or reverse property targets are not first class citizens.

    Returns:
        str | None: Target entity ID or "#N/A" if not found or not eligible.
    """
    container_entity_properties = entities[entity_id]["properties"]
    for property in container_entity_properties:
        if property[PropertyStructure.ID] == Property_id:
            if (
                property_type == Relations.DIRECT
                and entities[property[PropertyStructure.TARGET_TYPE]][
                    EntityStructure.FIRSTCLASSCITIZEN
                ]
                and entities[entity_id][EntityStructure.FIRSTCLASSCITIZEN]
            ):
                return property[PropertyStructure.TARGET_TYPE]
            elif property_type == Relations.EDGE:
                if (
                    entities[property[PropertyStructure.EDGE_SOURCE]][
                        EntityStructure.FIRSTCLASSCITIZEN
                    ]
                    and entities[property[PropertyStructure.EDGE_TARGET]][
                        EntityStructure.FIRSTCLASSCITIZEN
                    ]
                ):
                    return property[PropertyStructure.EDGE_TARGET]
                else:
                    raise NeatValueError(
                        f"Edge property {property[PropertyStructure.ID]} has a source or target type that is not a first class citizen"
                    )
            elif property_type == Relations.REVERSE:
                if (
                    entities[property[PropertyStructure.TARGET_TYPE]][
                        EntityStructure.FIRSTCLASSCITIZEN
                    ]
                    and entities[entity_id][EntityStructure.FIRSTCLASSCITIZEN]
                ):
                    return property[PropertyStructure.TARGET_TYPE]
                else:
                    raise NeatValueError(
                        f"Reverse property {property[PropertyStructure.ID]} has a through property that is not a first class citizen"
                    )

    return "#N/A"  # None TODO: check if this is correct, there should be a proper type for N/A


# TODO: add data types to the parameters in the below function
def get_relation_target_if_eligible(
    key, container_external_id, entities, property_type
) -> str | None:
    """Determine the relation target only if the container meets the eligibility criteria.

    Args:
        key: Property key to find the target for.
        container_external_id: External ID of the container entity.
        entities: Dictionary of all entities in the model.
        property_type: Type of relation (DIRECT, EDGE, or REVERSE).

    Returns:
        str | None: Target entity ID if eligible, "#N/A" otherwise.
    """
    if (
        container_external_id in entities.keys()
        and container_external_id != "EntityTypeGroup"
        and property_type in (Relations.DIRECT, Relations.EDGE, Relations.REVERSE)
    ):
        return get_entity_relation_target(
            key, container_external_id, entities, property_type
        )
    return "#N/A"  # None TODO: check if this is correct, there should be a proper type for N/A


def create_neat_view_structure(
    view: str,
    view_name: str = None,
    view_description: str = None,
    implements: str = None,
    filter: str = None,
    in_model: bool = True,
) -> dict:
    """Create a dictionary representing a NEAT view structure with the specified parameters.

    Args:
        view (str): The identifier of the view.
        view_name (str, optional): The name of the view. Defaults to None.
        view_description (str, optional): A description of the view. Defaults to None.
        implements (list[str], optional): A list of views that this view implements. Defaults to None.
        filter (str, optional): An optional filter expression to apply to the view. Defaults to None.
        in_model (bool, optional): Indicates whether the view is included in the model. Defaults to True.

    Returns:
        dict: A dictionary containing the NEAT view structure with the provided parameters.
    """
    return {
        NeatViewStructure.VIEW: view,
        NeatViewStructure.NAME: view_name,
        NeatViewStructure.DESCRIPTION: view_description,
        NeatViewStructure.IMPLEMENTS: implements,
        NeatViewStructure.FILTER: filter,
        NeatViewStructure.IN_MODEL: in_model,
    }


def create_neat_property_structure(
    view: str,
    property: str,
    name: str = None,
    description: str = None,
    connection: str = None,
    value_type: str = None,
    min_count: int = 0,
    max_count: int | str = None,
    immutable: bool = False,
    default: str = None,
    reference: str = None,
    container: str = None,
    container_property: str = None,
    index: str = None,
    constraint: str = None,
) -> dict:
    """Create a dictionary representing a NEAT property structure with the specified attributes.

    Args:
        view (str): The name of the view to which the property belongs.
        property (str): The property identifier within the view.
        name (str, optional): The display name of the property. Defaults to None.
        description (str, optional): A description of the property. Defaults to None.
        connection (str, optional): Connection information or identifier. Defaults to None.
        value_type (str, optional): The type of value the property holds. Defaults to None.
        min_count (int, optional): The minimum number of values allowed. Defaults to 0.
        max_count (int, optional): The maximum number of values allowed. Defaults to None.
        immutable (bool, optional): Whether the property is immutable. Defaults to False.
        default (str, optional): The default value for the property. Defaults to None.
        reference (str, optional): Reference to another entity or property. Defaults to None.
        container (str, optional): The container to which the property belongs. Defaults to None.
        container_property (str, optional): The property within the container. Defaults to None.
        index (str, optional): Index information for the property. Defaults to None.
        constraint (str, optional): container constraints applied to the property. Defaults to None.

    Returns:
        dict: A dictionary representing the NEAT property structure with the specified attributes.
    """
    return {
        NeatPropertyStructure.VIEW: view,
        NeatPropertyStructure.VIEW_PROPERTY: property,
        NeatPropertyStructure.NAME: name,
        NeatPropertyStructure.DESCRIPTION: description,
        NeatPropertyStructure.CONNECTION: connection,
        NeatPropertyStructure.VALUE_TYPE: value_type,
        NeatPropertyStructure.MIN_COUNT: min_count,
        NeatPropertyStructure.MAX_COUNT: max_count,
        NeatPropertyStructure.IMMUTABLE: immutable,
        NeatPropertyStructure.DEFAULT: default,
        NeatPropertyStructure.REFERENCE: reference,
        NeatPropertyStructure.CONTAINER: container,
        NeatPropertyStructure.CONTAINER_PROPERTY: container_property,
        NeatPropertyStructure.INDEX: index,
        NeatPropertyStructure.CONSTRAINT: constraint,
    }


def create_neat_container_structure(
    container: str,
    container_name: str = "",
    container_description: str = "",
    constraint: str = None,
    used_for: str = "node",
) -> dict:
    """Create a dictionary representing the structure of a NEAT container with specified attributes.

    Args:
        container (str): The identifier or type of the container.
        container_name (str, optional): The name of the container. Defaults to an empty string.
        container_description (str, optional): A description of the container. Defaults to an empty string.
        constraint (str, optional): Any constraint associated with the container. Defaults to None.
        used_for (str, optional): The intended use of the container (e.g., "node"). Defaults to "node".

    Returns:
        dict: A dictionary containing the container structure with the specified attributes.
    """
    return {
        NeatContainerStructure.CONTAINER: container,
        NeatContainerStructure.NAME: container_name,
        NeatContainerStructure.DESCRIPTION: container_description,
        NeatContainerStructure.CONSTRAINT: constraint,
        NeatContainerStructure.USED_FOR: used_for,
    }
