"""Views model module for creating data model views from entities."""
from cognite.neat.core._constants import COGNITE_CONCEPTS

from cfihos_handler.framework.common.constants import CDF_CDM_SPACE, CDF_CDM_VERSION
from cfihos_handler.framework.common.generic_classes import (
    EntityStructure,
    PropertyStructure,
    Relations,
)
from cfihos_handler.framework.common.log import log_init
from cfihos_handler.framework.common.utils import (
    create_neat_container_structure,
    create_neat_property_structure,
    create_neat_view_structure,
)

logging = log_init(f"{__name__}", "i")

map_property_identifier = {
    "cfihos_name": PropertyStructure.DMS_NAME,
    "cfihos_code": PropertyStructure.ID,
}
map_reverse_direct_relation_identifier = {
    "cfihos_name": PropertyStructure.REV_PROPERTY_DMS_NAME,
    "cfihos_code": PropertyStructure.REV_THROUGH_PROPERTY,
}
map_entity_identifier = {
    "cfihos_name": EntityStructure.DMS_NAME,
    "cfihos_code": EntityStructure.ID,
}
map_source_edge_identifier = {
    "cfihos_name": PropertyStructure.EDGE_SOURCE_DMS_NAME,
    "cfihos_code": PropertyStructure.EDGE_SOURCE,
}
map_target_edge_identifier = {
    "cfihos_name": PropertyStructure.EDGE_TARGET_DMS_NAME,
    "cfihos_code": PropertyStructure.EDGE_TARGET,
}


def build_neat_model_from_entities(
    entities: dict,
    dms_identifire: str,
    include_containers: bool = False,
    include_cdm: bool = False,
    containers_space: str = None,
    containers_indexes: dict[str, list[dict[str, str | list[str]]]] = None,
    force_code_as_view_id: bool = False,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Build a NEAT model from the provided entities.

    Args:
        entities (dict): A dictionary containing entity information.
        dms_identifire (str): The DMS identifier used for mapping entity and property identifiers, where the identifire could be "cfihos_name" or "cfihos_code".
        include_containers (bool): Whether to include containers in the model.
        include_cdm (bool): Whether to include CDM concepts in the model.
        containers_space (str): The space identifier where the containers reside.
        containers_indexes (dict): A dictionary containing container indexes.
        force_code_as_view_id (bool): Whether to force the code as view id.

    Returns:
        tuple[list[dict], list[dict], list[dict]]: A tuple containing a list of created view dictionaries,
        a list of property dictionaries, and a list of container dictionaries.
    """
    views = []
    containers = []
    properties = []

    if include_cdm:
        for cdm_view in COGNITE_CONCEPTS:
            views.append(
                create_neat_view_structure(
                    view=f"{CDF_CDM_SPACE}:"
                    + cdm_view
                    + f"(version={CDF_CDM_VERSION})",
                    view_name=cdm_view,
                )
            )
    for _, entity_data in entities.items():
        parents_ext_ids: list[str] = []
        inherits_from = entity_data.get(EntityStructure.INHERITS_FROM_ID)
        if inherits_from:
            parents_ext_ids.extend(
                [
                    entities[parent_id][map_entity_identifier[dms_identifire]]
                    if not force_code_as_view_id
                    else entities[parent_id][map_entity_identifier["cfihos_code"]]
                    for parent_id in inherits_from
                ]
                if inherits_from
                else []
            )

        # add views for those entities that implements core models if include_cdm is True
        if include_cdm:
            parents_ext_ids.extend(
                [
                    f"{CDF_CDM_SPACE}:"
                    + view_id["external_id"]
                    + f"(version={CDF_CDM_VERSION})"
                    for view_id in entity_data[EntityStructure.IMPLEMENTS_CORE_MODEL]
                ]
                if entity_data[EntityStructure.IMPLEMENTS_CORE_MODEL] is not None
                else []
            )
        for prop_data in entity_data[EntityStructure.PROPERTIES]:
            lst_property_container_indexes = []
            index_order = 0
            if (
                containers_indexes is not None
                and entity_data[map_entity_identifier["cfihos_code"]]
                in containers_indexes.keys()
            ):
                for container_index in containers_indexes[
                    entity_data[map_entity_identifier["cfihos_code"]]
                ]:
                    if prop_data[PropertyStructure.ID] in container_index["properties"]:
                        lst_property_container_indexes.append(
                            f'{container_index["index_type"]}:{container_index["index_id"]}(cursorable={container_index["cursorable"]}, order={index_order})'
                        )
                        index_order += 1
            if prop_data[PropertyStructure.PROPERTY_TYPE] in [
                Relations.DIRECT,
                Relations.REVERSE,
                Relations.EDGE,
            ]:
                max_count_property: int | str
                match prop_data[PropertyStructure.PROPERTY_TYPE]:
                    case Relations.EDGE:
                        container_reference = None
                        value_type_property = (
                            entities[prop_data[PropertyStructure.EDGE_TARGET]][
                                map_entity_identifier[dms_identifire]
                                if not force_code_as_view_id
                                else map_entity_identifier["cfihos_code"]
                            ]
                            if prop_data[PropertyStructure.EDGE_DIRECTION] == "inwards"
                            else entities[prop_data[PropertyStructure.EDGE_SOURCE]][
                                map_entity_identifier[dms_identifire]
                                if not force_code_as_view_id
                                else map_entity_identifier["cfihos_code"]
                            ]
                        )
                        container_property = None
                        connection_property = f"edge(type={prop_data[PropertyStructure.EDGE_EXTERNAL_ID]}, direction={prop_data[PropertyStructure.EDGE_DIRECTION]})"
                        max_count_property = "inf"
                    case Relations.REVERSE:
                        value_type_property = entities[
                            prop_data[PropertyStructure.TARGET_TYPE]
                        ][
                            map_entity_identifier[dms_identifire]
                            if not force_code_as_view_id
                            else map_entity_identifier["cfihos_code"]
                        ]
                        container_reference = None
                        container_property = None
                        connection_property = f"reverse(property={prop_data[map_reverse_direct_relation_identifier[dms_identifire]]})"
                        max_count_property = "inf"
                    case Relations.DIRECT:
                        try:
                            value_type_property = (
                                entities[prop_data[PropertyStructure.TARGET_TYPE]][
                                    map_entity_identifier[dms_identifire]
                                    if not force_code_as_view_id
                                    else map_entity_identifier["cfihos_code"]
                                ]
                                if prop_data[PropertyStructure.TARGET_TYPE] is not None
                                else None
                            )
                        except KeyError:
                            value_type_property = None
                            logging.warning(
                                f"Target type {prop_data[PropertyStructure.TARGET_TYPE]} for property {prop_data[PropertyStructure.ID]} not found, skipping."
                            )
                            continue
                        container_reference = (
                            containers_space
                            + ":"
                            + prop_data[PropertyStructure.PROPERTY_GROUP]
                        )
                        container_property = prop_data[PropertyStructure.ID]
                        connection_property = "direct"
                        max_count_property = 1
                    case _:
                        logging.warning(
                            f"Unknown property type: {prop_data[PropertyStructure.PROPERTY_TYPE]} for property {prop_data[PropertyStructure.ID]}"
                        )
                        continue

                properties.append(
                    create_neat_property_structure(
                        view=entity_data[map_entity_identifier[dms_identifire]]
                        if not force_code_as_view_id
                        else entity_data[map_entity_identifier["cfihos_code"]],
                        property=prop_data[map_property_identifier[dms_identifire]],
                        name=prop_data[PropertyStructure.NAME],
                        description=prop_data[PropertyStructure.DESCRIPTION],
                        connection=connection_property,
                        value_type=value_type_property
                        if value_type_property is not None
                        else "#N/A",
                        min_count=0,
                        max_count=max_count_property,
                        container=container_reference,
                        container_property=container_property,
                        index=",".join(lst_property_container_indexes)
                        if len(lst_property_container_indexes) > 0
                        and include_containers
                        else None,
                    )
                )

            else:
                if (
                    prop_data[PropertyStructure.PROPERTY_GROUP] is None
                    or prop_data[PropertyStructure.PROPERTY_GROUP] == ""
                ):
                    logging.warning(
                        f"Property {prop_data[PropertyStructure.NAME]} with ID {prop_data[PropertyStructure.ID]} has no container assigned, skipping."
                    )
                    continue
                properties.append(
                    create_neat_property_structure(
                        view=entity_data[map_entity_identifier[dms_identifire]]
                        if not force_code_as_view_id
                        else entity_data[map_entity_identifier["cfihos_code"]],
                        property=prop_data[map_property_identifier[dms_identifire]],
                        name=prop_data[PropertyStructure.NAME],
                        description=prop_data[PropertyStructure.DESCRIPTION],
                        value_type=prop_data[PropertyStructure.TARGET_TYPE],
                        min_count=0,
                        max_count=1,
                        container=containers_space
                        + ":"
                        + prop_data[PropertyStructure.PROPERTY_GROUP],
                        container_property=prop_data[PropertyStructure.ID],
                    )
                )
        views.append(
            create_neat_view_structure(
                view=entity_data[map_entity_identifier[dms_identifire]]
                if not force_code_as_view_id
                else entity_data[map_entity_identifier["cfihos_code"]],
                view_name=entity_data[EntityStructure.NAME],
                view_description=entity_data[EntityStructure.DESCRIPTION],
                implements=",".join(parents_ext_ids) if parents_ext_ids else None,
                filter=entity_data[EntityStructure.VIEW_FILTER]
                if entity_data[EntityStructure.VIEW_FILTER]
                else None,
            )
        )
        # add container for the entity if include_containers is True, by default, we use CFIHOS code as container external id
        if include_containers:
            containers.append(
                create_neat_container_structure(
                    container=entity_data[map_entity_identifier["cfihos_code"]]
                )
            )

    # Find all entities that have their 'properties' field as None or an empty list.
    entities_with_empty_or_none_properties = {}
    for entity_id, entity_data in entities.items():
        props = entity_data.get("properties")
        if props is None or props == []:
            entities_with_empty_or_none_properties[entity_id] = entity_data
    return views, properties, containers
