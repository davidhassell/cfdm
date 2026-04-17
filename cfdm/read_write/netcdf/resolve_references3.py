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


def search_by_relative_path(ref, variable, search_type):
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

    return coordinate_search_by_proximity(ref, variable)


def coordinate_search_by_proximity(ref, variable):
    # find the local apex group - the ancestor group that contains a
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

    if dim is None:
        return

    return search_for_coordinate_from_local_apex(ref, g, depth)


def search_for_coordinate_from_local_apex(ref, group, depth):
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

        path = search_for_coordinate_from_local_apex(ref, g, depth - 1)
        if path is not None:
            # Found
            return path

    # Not found in the tree from 'group' down to the given depth


def resolve_pattern_1(value, variable):
    """TODO.

    Resolve attributes whose values are
    * ''
    * 'var1'
    * 'var1 var2'

    E.g. ``coordinates``, ``ancillary_variables``,
    ``edge_node_connectivity``

    """
    resolved = [
        resolve_reference(x, variable, var=True) for x in value.split()
    ]
    return " ".join(resolved)


def resolve_pattern_1b(value, variable):
    """TODO.

    Resolve attributes whose values are
    * ''
    * 'dim1'
    * 'dim1 dim2'

    E.g. ``dimensions``, ``face_dimension``

    """
    resolved = [
        resolve_reference(x, variable, dim=True) for x in value.split()
    ]
    return " ".join(resolved)


def resolve_pattern_2(value, variable):
    """TODO.

    Resolve attributes whose values are

    * 'var1: var2',
    * 'var1: var2 var3',
    * 'var1: var2 var3: var4 var5',

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


def resolve_pattern_3(value, variable):
    """TODO.

    Resolve attributes whose values are

    * 'key1: var1',
    * 'key1: var1 key2: var2'

    E.g. ``cell_measures``, ``aggregated_data``, ``formula_terms``,
    ``interpolation_parameters``

    """
    resolved = []

    for ref in value.split():
        if not ref.endswith(":"):
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
    #   ['lat:', 'lon:', 'mean', '(interval: 1 hour)', 'time', 'max']
    # ------------------------------------------------------------
    cell_methods = re.findall(r"\([^)]*\)|\S+", value)

    previous = "axis"
    for ref in cell_methods:
        if ref.endswith(":"):
            ref = resolve_reference(ref[:-1], variable, dim=True)
            resolved.append(ref + ":")
            previous = "axis"
            continue

        if previous == "axis":
            resolved.append(ref)
            previous = "method"
            continue

        if previous == "method":
            if ref == "within":
                resolved.append(ref)
                previous = "within"
                continue

            if ref == "where":
                resolved.append(ref)
                previous = "where"
                continue

        if previous == "within":
            resolved.append(ref)
            previous = "years|days"
            continue

        if previous == "years|days" and ref == "over":
            resolved.append(ref)
            previous = "climatological over"
            continue

        if previous == "climatological over":
            resolved.append(ref)
            previous = "years|days"
            continue

        if previous == "where":
            ref = resolve_reference(ref, variable, var=True)
            resolved.append(ref)
            previous = "type1"
            continue

        if previous == "type1" and ref == "over":
            resolved.append(ref)
            previous = "over"
            continue

        if previous == "over":
            ref = resolve_reference(ref, variable, var=True)
            resolved.append(ref)
            previous = "type2"
            continue

        # Still here?
        #        if ref.startswith("(") and ref.endswith(")"):
        resolved.append(ref)
        previous = None

    return " ".join(resolved)


def resolve_pattern_5(value, variable):
    """TODO.

    Resolve attributes whose values are "interpolated_dimension:
    tie_point_index_variable subsampled_dimension
    [interpolation_subarea_dimension] [interpolated_dimension: ...]",

    * 'dim1: var1 dim2'
    * 'dim1: var1 dim2 dim3'

    E.g. ``te_point_mapping``

    """
    resolved = []

    ref_type = None
    for ref in value.split():
        if ref.endswith(":"):
            ref = resolve_reference(ref[:-1], variable, dim=True)
            ref += ":"
            ref_type = "variable"
        elif ref_type == "variable":
            ref = resolve_reference(ref, variable, var=True)
            ref_type = "dimension"
        elif ref_type == "dimension":
            ref = resolve_reference(ref, variable, dim=True)
            ref_type = "dimension"

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
        resolved_ref = search_by_relative_path(ref, variable, first_search)

        # If failed and alternative possible, second tentative
        if resolved_ref is None and second_search:
            resolved_ref = search_by_relative_path(
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
        Rules(name="grid_mapping", resolver=resolve_pattern_2),
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
        Rules(name="coordinate_interpolation", resolver=resolve_pattern_2),
        Rules(name="tie_point_mapping", resolver=resolve_pattern_5),
        Rules(name="interpolation_parameters", resolver=resolve_pattern_2),
        # ------------------------------------------------------------
        # Quantization
        # ------------------------------------------------------------
        Rules(name="quantization", resolver=resolve_pattern_1),
    )
}
