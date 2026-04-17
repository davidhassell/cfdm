import numpy as np

from .flatten.config import flattening_rules


class NetCDFError(Exception):
    """Error raised when file can't be viewed as netCDF."""

    pass


class AttributeParsingError(Exception):
    """TODO."""

    pass


def resolve_references2(f):
    """TODO."""
    for variable in f.all_variables.values():
        resolve_references(variable)

    return f


def parse_attribute(name, attribute):
    """Parse variable attribute of any form into a dict:

     * 'time' -> {'time': []}
     * 'lat lon' -> {'lat': [], 'lon': []}
     * 'area: time volume: lat lon' -> {'area': ['time'], 'volume':
       ['lat', 'lon']}

    .. versionadded:: (cfdm) 1.11.2.0

    :Parameters:

        name: `str`
            The attribute name (e.g. ``'cell_methods'``).

        attribute: `str`
            The attribute value to parse.

    :Returns:

        `dict`
            The parsed string.

    """
    import re

    def subst(s):
        """Substitute tokens for WORD and SEP."""
        return s.replace("WORD", r"[A-Za-z0-9_#/.\(\)]+").replace(
            "SEP", r"(\s+|$)"
        )

    # Regex for 'dict form': "k1: v1 v2 k2: v3"
    pat_value = subst(r"(?P<value>WORD)SEP")
    pat_values = f"({pat_value})*"
    pat_mapping = subst(rf"(?P<mapping_name>WORD):SEP(?P<values>{pat_values})")
    pat_mapping_list = f"({pat_mapping})+"

    # Regex for 'list form': "v1 v2 v3" (including single-item form)
    pat_list_item = subst(r"(?P<list_item>WORD)SEP")
    pat_list = f"({pat_list_item})+"

    # Regex for any form:
    pat_all = subst(
        rf"((?P<list>{pat_list})|(?P<mapping_list>{pat_mapping_list}))$"
    )

    m = re.match(pat_all, attribute)

    # Output is always a dict. If input form is a list, dict values
    # are set as empty lists
    out = {}

    if m is not None:
        list_match = m.group("list")
        # Parse as a list
        if list_match:
            for mapping in re.finditer(pat_list_item, list_match):
                item = mapping.group("list_item")
                out[item] = None

        # Parse as a dict:
        else:
            mapping_list = m.group("mapping_list")
            for mapping in re.finditer(pat_mapping, mapping_list):
                term = mapping.group("mapping_name")
                values = [
                    value.group("value")
                    for value in re.finditer(
                        pat_value, mapping.group("values")
                    )
                ]
                out[term] = values
    else:
        raise AttributeParsingError(
            f"Error parsing {name!r} attribute with value {attribute!r}"
        )

    return out


def resolve_references(variable):
    """TODO."""
    attrs = variable.attrs
    for name in set(flattening_rules).intersection(attrs):
        parsed_attribute = parse_attribute(name, attrs[name])

        # Resolved references in parsed as required by attribute
        # properties
        resolved_parsed_attr = {}

        rules = flattening_rules[name]
        resolve_key = rules.resolve_key
        resolve_value = rules.resolve_value

        for k, v in parsed_attribute.items():
            if resolve_key:
                k = resolve_reference(k, variable, rules)

            if resolve_value and v is not None:
                v = [resolve_reference(x, variable, rules) for x in v]

            resolved_parsed_attr[k] = v

        # Re-generate attribute value string with resolved
        # references
        attrs[name] = generate_var_attr_str(resolved_parsed_attr)


def generate_var_attr_str(d):
    """Re-generate the attribute string from a dictionary.

    .. versionadded:: (cfdm) 1.11.2.0

    :Parameters:

        d: `dict`
            A resolved and parsed attribute.

    :Returns:

        `str`
            The flattened attribute value.

    """
    parsed_list = []
    for k, v in d.items():
        if v is None:
            parsed_list.append(k)
        elif not v:
            parsed_list.append(f"{k}:")
        else:
            parsed_list.append(f"{k}: {' '.join(v)}")

    return " ".join(parsed_list)


def resolve_reference(ref, variable, rules):
    """Resolve a reference.

    Resolves the absolute path to a coordinate variable within the
    group structure.

    .. versionadded:: (cfdm) 1.11.2.0

    :Parameters:

        ref: `str`
            The reference to resolve.

        variable: `Variable`
            The original variable object containing the reference.

        rules: `FlatteningRules`
            The flattening rules that apply to the reference.

    :Returns:

        `str`
            The absolute path to the reference.

    """
    absolute_ref = None
    ref_type = ""

    ref_to_dim = rules.ref_to_dim
    ref_to_var = rules.ref_to_var

    # Resolve first as dim (True), or var (False)
    resolve_dim_or_var = ref_to_dim > ref_to_var

    # Resolve var (resp. dim) if resolving as dim (resp. var) failed
    resolve_alt = ref_to_dim and ref_to_var

    # Reference is given by relative path
    if "/" in ref:
        method = "Relative"

        # First tentative as dim OR var
        absolute_ref = search_by_relative_path(
            ref, variable, resolve_dim_or_var
        )

        # If failed and alternative possible, second tentative
        if absolute_ref is None and resolve_alt:
            absolute_ref = search_by_relative_path(
                ref, variable, not resolve_dim_or_var
            )

    # Reference is to be searched by proximity
    else:
        method = "Proximity"
        absolute_ref = search_by_proximity(
            ref,
            variable,
            resolve_dim_or_var,
        )
        if absolute_ref is None and resolve_alt:
            absolute_ref = search_by_proximity(
                ref,
                variable,
                not resolve_dim_or_var,
            )

    if absolute_ref is not None:
        return absolute_ref

    return ref


def search_by_relative_path(ref, variable, search_dim):
    g = variable.group()
    parts = ref.split("/")
    path = "/".join(parts[:-1])
    try:
        g = g[path]
    except KeyError:
        return

    name = parts[-1]

    if search_dim:
        x = g.dimensions.get(name)
    else:
        x = g.variables.get(name)

    if x is not None:
        return x.path


def search_by_proximity(ref, variable, search_dim):
    """TODO."""
    if not search_dim and variable.dimensions == (ref,):
        return coordinate_search_by_proximity(ref, variable)

    g = variable.group()
    while g is not None:
        if search_dim:
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

        path = search_for_coordinate_local_apex(ref, g, depth - 1)
        if path is not None:
            # Found
            return path

    # Not found in the tree from 'group' down to the given depth
