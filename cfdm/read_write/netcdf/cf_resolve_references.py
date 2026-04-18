from dataclasses import dataclass
from typing import Callable


class NetCDFError(Exception):
    """Error raised when file can't be viewed as netCDF."""

    pass


class AttributeParsingError(Exception):
    """TODO."""

    pass


def resolve_references(f):
    """TODO."""
    resolvable_attributes = set(resolving_rules)
    for variable in f.all_variables.values():
        attrs = variable.attrs
        for name in resolvable_attributes.intersection(attrs):
            resolver = resolving_rules[name].resolver
            attrs[name] = resolver(attrs[name], variable)


def search_by_absolute_or_relative_path(ref, variable, search_type):
    """TODO."""
    g = variable.group()
    parts = ref.split("/")
    path = "/".join(parts[:-1])
    try:
        g = g[path]
    except KeyError:
        return

    name = parts[-1]

    if search_type == "dim":
        x = g.dimensions.get(name)
    else:
        x = g.variables.get(name)

    if x is not None:
        return x.path


def search_by_proximity(ref, variable, search_type):
    """TODO."""
    if search_type == "var" and variable.dimensions == (ref,):
        return coordinate_search_by_proximity(ref, variable)

    g = variable.group()
    while g is not None:
        if search_type == "dim":
            x = g.dimensions.get(ref)
        else:
            x = g.variables.get(ref)

        if x is not None:
            return x.path

        g = g.parent


def coordinate_search_by_proximity(ref, variable):
    """TODO."""
    # Find the local apex group: The ancestor group that contains a
    # dimension with the same name as the variable
    g = variable.group()
    dim = None
    depth = 0
    while g is not None:
        dim = g.dimensions.get(ref)
        if dim is not None:
            break

        depth += 1
        g = g.parent

    local_apex_group = g
    if local_apex_group is None:
        return

    return lateral_search(ref, local_apex_group, depth)


def lateral_search(ref, group, depth):
    """TODO.

    Performs a lateral search starting in *group* and proceeding to
    *depth* layers of sub-groups. If *depth* is less than 0 then no
    search is done; if *depth* is 0, only *group* is searched; if
    *depth* is 1 then direct sub-groups are also searched; if *depth* is
    2 then direct sub-groups of those sub-groups are also searched; etc.

    """
    if depth < 0:
        # Not found in the tree from 'group' down to the given depth
        return

    var = group.variables.get(ref)
    if var is not None:
        # Found
        return var.path

    if not depth:
        return

    for g in group.groups():
        var = g.variables.get(ref)
        if var is not None:
            # Found
            return var.path

        path = lateral_search(ref, g, depth - 1)
        if path is not None:
            # Found
            return path

    # Not found in the tree from 'group' down to the given depth


def resolve_pattern_1(value, variable):
    """TODO.

    Resolve references in an attribute whose value has one of the
    following patterns:

    * ''
    * 'var'
    * 'var var'

    E.g. ``coordinates``, ``ancillary_variables``,
    ``edge_node_connectivity``

    """
    resolved = [
        resolve_reference(x, variable, var=True) for x in value.split()
    ]
    return " ".join(resolved)


def resolve_pattern_1b(value, variable):
    """TODO.

    Resolve references in an attribute whose value has one of the
    following patterns:

    * ''
    * 'dim'
    * 'dim dim'

    E.g. ``dimensions``, ``face_dimension``

    """
    resolved = [
        resolve_reference(x, variable, dim=True) for x in value.split()
    ]
    return " ".join(resolved)


def resolve_pattern_2(value, variable):
    """TODO.

    Resolve references in an attribute whose value has one of the
    following patterns:

    * ''
    * 'key: var'
    * 'key: var key: var'

    E.g. ``cell_measures``, ``aggregated_data``, ``formula_terms``,
    ``interpolation_parameters``

    """
    resolved = []

    for ref in value.split():
        if not ref.endswith(":"):
            ref = resolve_reference(ref, variable, var=True)

        resolved.append(ref)

    return " ".join(resolved)


def resolve_pattern_3(value, variable):
    """TODO.

    Resolve references in an attribute whose value has one of the
    following patterns:

    * ''
    * 'var: var'
    * 'var: var var'
    * 'var: var var: var var'
    * 'var: var var var: var var'

    E.g. ``grid_mapping``

    """
    resolved = []
    for ref in value.split():
        if ref.endswith(":"):
            ref = resolve_reference(ref[:-1], variable, var=True)
            ref += ":"
        else:
            ref = resolve_reference(ref, variable, var=True)

        resolved.append(ref)

    return " ".join(resolved)


def resolve_pattern_4(value, variable):
    """Parse a CF cell_methods string.

    .. versionadded:: (cfdm) 1.7.0

    :Parameters:

        cell_methods_string: `str`
            A CF cell methods string.

        field_ncvar: `str`, optional
            The netCDF name of the data variable that contains the
            cell methods.

    :Returns:

        `list` of `dict`

    **Examples**

    >>> c = parse_cell_methods('t: minimum within years '
    ...                        't: mean over ENSO years)')

    """
    import re

    resolved = []
    # ------------------------------------------------------------
    # Split the cell_methods string into a list of strings ready
    # for parsing. For example:
    #
    #   'lat: lon: mean (interval: 1 hour) time: max'
    #
    # would be split up into:
    #
    #   ['lat:', 'lon:', 'mean', '(interval: 1 hour)', 'time:', 'max']
    # ------------------------------------------------------------
    cell_methods = re.findall(r"\([^)]*\)|\S+", value)

    previous_ref = None
    for ref in cell_methods:
        if ref.endswith(":"):
            ref = resolve_reference(ref[:-1], variable, dim=True)
            resolved.append(ref + ":")
            previous_ref = "axis"
            continue

        if previous_ref == "axis":
            resolved.append(ref)
            previous_ref = "method"
            continue

        if previous_ref == "method":
            if ref == "within":
                resolved.append(ref)
                previous_ref = "climatological within"
                continue

            if ref == "where":
                resolved.append(ref)
                previous_ref = "where"
                continue

        if previous_ref == "climatological within":
            resolved.append(ref)
            previous_ref = "years|days"
            continue

        if previous_ref == "years|days" and ref == "over":
            resolved.append(ref)
            previous_ref = "climatological over"
            continue

        if previous_ref == "climatological over":
            resolved.append(ref)
            previous_ref = "years|days"
            continue

        if previous_ref == "where":
            ref = resolve_reference(ref, variable, var=True)
            resolved.append(ref)
            previous_ref = "type1"
            continue

        if previous_ref == "type1" and ref == "over":
            resolved.append(ref)
            previous_ref = "over"
            continue

        if previous_ref == "over":
            ref = resolve_reference(ref, variable, var=True)
            resolved.append(ref)
            previous_ref = "type2"
            continue

        # Still here?
        #        if ref.startswith("(") and ref.endswith(")"):
        resolved.append(ref)
        previous_ref = None

    return " ".join(resolved)


def resolve_pattern_5(value, variable):
    """TODO.

    Resolve references in an attribute whose value has one of the
    following patterns:

    * ''
    * 'dim: var dim'
    * 'dim: var dim dim: var dim'
    * 'dim: var dim dim dim: var dim dim'

    E.g. ``tie_point_mapping``

    """
    resolved = []

    next_ref = None
    for ref in value.split():
        if ref.endswith(":"):
            ref = resolve_reference(ref[:-1], variable, dim=True)
            ref += ":"
            next_ref = "variable"
        elif next_ref == "variable":
            ref = resolve_reference(ref, variable, var=True)
            next_ref = "dimension"
        elif next_ref == "dimension":
            ref = resolve_reference(ref, variable, dim=True)
            next_ref = "dimension"

        resolved.append(ref)

    return " ".join(resolved)


def resolve_reference(ref, variable, dim=False, var=False, dim_then_var=True):
    """Resolve a reference.

    Resolves the absolute path to a coordinate variable within the
    group structure.

    .. versionadded:: (cfdm) 1.11.2.0

    :Parameters:

        ref: `str`
            The reference to resolve.

        variable: `Variable`
            The original variable object containing the reference.

        rules: `Rules`
            The flattening rules that apply to the reference.

    :Returns:

        `str`
            The absolute path to the reference.

    """
    resolved_ref = None

    second_search = None
    if dim:
        if var:
            first_search = "dim" if dim_then_var else "var"
            second_search = "var" if first_search == "dim" else "dim"
        else:
            first_search = "dim"
    else:
        first_search = "var"

    # Reference is given by relative path
    if "/" in ref:
        # First tentative as dim or var
        resolved_ref = search_by_absolute_or_relative_path(
            ref, variable, first_search
        )

        # If failed and alternative possible, second tentative
        if resolved_ref is None and second_search:
            resolved_ref = search_by_absolute_or_relative_path(
                ref, variable, second_search
            )

    # Reference is to be searched by proximity
    else:
        resolved_ref = search_by_proximity(ref, variable, first_search)
        if resolved_ref is None and second_search:
            resolved_ref = search_by_proximity(ref, variable, second_search)

    if resolved_ref is not None:
        return resolved_ref

    return ref


@dataclass
class Rules:
    """Rules for resolving references in a netCDF attribute.

    TODO For a named netCDF attribute, the rules a define how the contents
    of the attribute are flattened. For instance, it has to be defined
    that the ``ancillary_variables`` attribute contains the names of
    other netCDF variables.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    # name: The name of attribute containing the references to be
    #       resolved
    name: str
    # resolver: The function that will do the resolving
    resolver: Callable


# --------------------------------------------------------------------
# Define the flattening rules for named CF attributes
# --------------------------------------------------------------------
resolving_rules = {
    attr.name: attr
    for attr in (
        # ------------------------------------------------------------
        # Coordinates
        # ------------------------------------------------------------
        Rules(name="coordinates", resolver=resolve_pattern_1),
        # ------------------------------------------------------------
        # Bounds
        # ------------------------------------------------------------
        Rules(name="bounds", resolver=resolve_pattern_1),
        Rules(name="climatology", resolver=resolve_pattern_1),
        # ------------------------------------------------------------
        # Cell methods
        # ------------------------------------------------------------
        Rules(name="cell_methods", resolver=resolve_pattern_4),
        # ------------------------------------------------------------
        # Cell measures
        # ------------------------------------------------------------
        Rules(name="cell_measures", resolver=resolve_pattern_2),
        # ------------------------------------------------------------
        # Coordinate references
        # ------------------------------------------------------------
        Rules(name="formula_terms", resolver=resolve_pattern_2),
        Rules(name="grid_mapping", resolver=resolve_pattern_3),
        # ------------------------------------------------------------
        # Ancillary variables
        # ------------------------------------------------------------
        Rules(name="ancillary_variables", resolver=resolve_pattern_1),
        # ------------------------------------------------------------
        # Compression by gathering
        # ------------------------------------------------------------
        Rules(name="compress", resolver=resolve_pattern_1b),
        # ------------------------------------------------------------
        # Discrete sampling geometries
        # ------------------------------------------------------------
        Rules(name="instance_dimension", resolver=resolve_pattern_1b),
        Rules(name="sample_dimension", resolver=resolve_pattern_1b),
        # ------------------------------------------------------------
        # Domain variables
        # ------------------------------------------------------------
        Rules(name="dimensions", resolver=resolve_pattern_1b),
        # ------------------------------------------------------------
        # Aggregation variables
        # ------------------------------------------------------------
        Rules(name="aggregated_dimensions", resolver=resolve_pattern_1b),
        Rules(name="aggregated_data", resolver=resolve_pattern_2),
        # ------------------------------------------------------------
        # Cell geometries
        # ------------------------------------------------------------
        Rules(name="geometry", resolver=resolve_pattern_1),
        Rules(name="interior_ring", resolver=resolve_pattern_1),
        Rules(name="node_coordinates", resolver=resolve_pattern_1),
        Rules(name="node_count", resolver=resolve_pattern_1),
        Rules(name="nodes", resolver=resolve_pattern_1),
        Rules(name="part_node_count", resolver=resolve_pattern_1),
        # ------------------------------------------------------------
        # UGRID variables
        # ------------------------------------------------------------
        Rules(name="mesh", resolver=resolve_pattern_1),
        Rules(name="edge_coordinates", resolver=resolve_pattern_1),
        Rules(name="face_coordinates", resolver=resolve_pattern_1),
        Rules(name="edge_node_connectivity", resolver=resolve_pattern_1),
        Rules(name="face_node_connectivity", resolver=resolve_pattern_1),
        Rules(name="face_face_connectivity", resolver=resolve_pattern_1),
        Rules(name="edge_face_connectivity", resolver=resolve_pattern_1),
        Rules(name="face_edge_connectivity", resolver=resolve_pattern_1),
        Rules(name="edge_dimension", resolver=resolve_pattern_1b),
        Rules(name="face_dimension", resolver=resolve_pattern_1b),
        # ------------------------------------------------------------
        # Compression by coordinate subsampling
        # ------------------------------------------------------------
        Rules(name="coordinate_interpolation", resolver=resolve_pattern_3),
        Rules(name="tie_point_mapping", resolver=resolve_pattern_5),
        Rules(name="interpolation_parameters", resolver=resolve_pattern_2),
        # ------------------------------------------------------------
        # Quantization
        # ------------------------------------------------------------
        Rules(name="quantization", resolver=resolve_pattern_1),
    )
}
