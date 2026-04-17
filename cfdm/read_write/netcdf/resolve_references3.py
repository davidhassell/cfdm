import numpy as np

from dataclasses import dataclass


@dataclass()
class FlatteningRules:
    """Define the flattening rules for a netCDF attribute.

    For a named netCDF attribute, the rules a define how the contents
    of the attribute are flattened. For instance, it has to be defined
    that the ``ancillary_variables`` attribute contains the names of
    other netCDF variables.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    # name: The name of attribute containing the reference to be
    #       flattened
    name: str
    # ref_to_dim: Positive integer if contains references to
    #             dimensions. If ref_to_dim and ref_to_var are both
    #             positive then the rule with the greater value is
    #             tested first.
#    ref_to_dim: int = 0
#    # ref_to_var: Positive integer if contains references to
#    #             variables. If ref_to_dim and ref_to_var are both
#    #             positive then the rule with the greater value is
#    #             tested first.
#    ref_to_var: int = 0
#    # resolve_key: True if 'keys' have to be resolved in 'key1: value1
#    #              key2: value2 value3' or 'key1 key2'
#    resolve_key: bool = False
#    # resolve_value: True if 'values' have to be resolved in 'key1:
#    #                value1 key2: value2 value3'
#    resolve_value: bool = False
#    # stop_at_local_apex: True if upward research in the hierarchy has
#    #                     to stop at local apex.
#    stop_at_local_apex: bool = False
#    # accept_standard_names: True if any standard name is valid in
#    #                        place of references (in which case no
#    #                        exception is raised if a reference cannot
#    #                        be resolved, and the standard name is
#    #                        used in place)
#    accept_standard_names: bool = False
#    # limit_to_scalar_coordinates: True if references to variables are
#    #                              only resolved if present as well in
#    #                              the 'coordinates' attributes of the
#    #                              variable, and they are scalar.
#    limit_to_scalar_coordinates: bool = False
    #
    resolver: Any = None
    

# --------------------------------------------------------------------
# Define the flattening rules for named CF attributes
# --------------------------------------------------------------------
flattening_rules = {
    attr.name: attr
    for attr in (
        # ------------------------------------------------------------
        # Coordinates
        # ------------------------------------------------------------
        FlatteningRules(name="coordinates", resolver=resolve_pattern_1),
        # ------------------------------------------------------------
        # Bounds            
        # ------------------------------------------------------------
        FlatteningRules(name="bounds", resolver=resolve_pattern_1),
        FlatteningRules(name="climatology", resolver=resolve_pattern_1),
        # ------------------------------------------------------------
        # Cell methods
        # ------------------------------------------------------------
        FlatteningRules(name="cell_methods", resolver=resolve_pattern_3),
        # ------------------------------------------------------------
        # Cell measures
        # ------------------------------------------------------------
        FlatteningRules(name="cell_measures", resolver=resolve_pattern_2),
        # ------------------------------------------------------------
        # Coordinate references
        # ------------------------------------------------------------
        FlatteningRules(name="formula_terms", resolver=resolve_pattern_2),
        FlatteningRules(name="grid_mapping", resolver=resolve_pattern_2),
        # ------------------------------------------------------------
        # Ancillary variables
        # ------------------------------------------------------------
        FlatteningRules(name="ancillary_variables", resolver=resolve_pattern_1),
        # ------------------------------------------------------------
        # Compression by gathering
        # ------------------------------------------------------------
        FlatteningRules(name="compress", resolver=resolve_pattern_1b),
        # ------------------------------------------------------------
        # Discrete sampling geometries
        # ------------------------------------------------------------
        FlatteningRules(name="instance_dimension", resolver=resolve_pattern_1b),
        FlatteningRules(name="sample_dimension", resolver=resolve_pattern_1b),
        # ------------------------------------------------------------
        # Domain variables
        # ------------------------------------------------------------
        FlatteningRules(name="dimensions", resolver=resolve_pattern_1b),
        # ------------------------------------------------------------
        # Aggregation variables
        # ------------------------------------------------------------
        FlatteningRules(name="aggregated_dimensions", resolver=resolve_pattern_1b),
        FlatteningRules(name="aggregated_data", resolver=resolve_pattern_2),
        # ------------------------------------------------------------
        # Cell geometries
        # ------------------------------------------------------------
        FlatteningRules(name="geometry", resolver=resolve_pattern_1),
        FlatteningRules(name="interior_ring",resolver=resolve_pattern_1),
        FlatteningRules(name="node_coordinates", resolver=resolve_pattern_1),
        FlatteningRules(name="node_count", resolver=resolve_pattern_1),
        FlatteningRules(name="nodes", resolver=resolve_pattern_1),
        FlatteningRules(name="part_node_count", resolver=resolve_pattern_1),
        # ------------------------------------------------------------
        # UGRID variables
        # ------------------------------------------------------------
        FlatteningRules(name="mesh", resolver=resolve_pattern_1),
        FlatteningRules(name="edge_coordinates", resolver=resolve_pattern_1),
        FlatteningRules(name="face_coordinates", resolver=resolve_pattern_1),
        FlatteningRules(name="edge_node_connectivity", resolver=resolve_pattern_1),
        FlatteningRules(name="face_node_connectivity", resolver=resolve_pattern_1),
        FlatteningRules(name="face_face_connectivity", resolver=resolve_pattern_1),
        FlatteningRules(name="edge_face_connectivity", resolver=resolve_pattern_1),
        FlatteningRules(name="face_edge_connectivity", resolver=resolve_pattern_1),
        FlatteningRules(name="edge_dimension", resolver=resolve_pattern_1b),
        FlatteningRules(name="face_dimension", resolver=resolve_pattern_1b),
        # ------------------------------------------------------------
        # Compression by coordinate subsampling
        # ------------------------------------------------------------
        FlatteningRules(name="coordinate_interpolation", resolver=resolve_pattern_2),
        FlatteningRules(name="tie_point_mapping", resolver=resolve_pattern_4),
        FlatteningRules(name="interpolation_parameters", resolver=resolve_pattern_2),
        # ------------------------------------------------------------
        # Quantization
        # ------------------------------------------------------------
        FlatteningRules(name="quantization", resolver=resolve_pattern_1),
}


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

def _parse_x(
    self,
    parent_ncvar,
    string,
    keys_are_variables=False,
    keys_are_dimensions=False,
):
    """Parse CF-netCDF strings.

    Handling of CF-compliant strings:
    ---------------------------------

    'area: areacello' ->
        [{'area': ['areacello']}]

    'area: areacello volume: volumecello' ->
        [{'area': ['areacello']}, {'volume': ['volumecello']}]

    'rotated_latitude_longitude' ->
        [{'rotated_latitude_longitude': []}]

    'rotated_latitude_longitude: x y latitude_longitude: lat lon' ->
        [{'rotated_latitude_longitude': ['x', 'y']},
         {'latitude_longitude': ['lat', 'lon']}]

    'rotated_latitude_longitude: x latitude_longitude: lat lon' ->
        [{'rotated_latitude_longitude': ['x']},
         {'latitude_longitude': ['lat', 'lon']}]

    'a: A b: B orog: OROG' ->
        [{'a': ['A']}, {'b': ['B']}, {'orog': ['OROG']}]

    Handling of non-CF-compliant strings:
    -------------------------------------

    'area' ->
        [{'area': []}]

    'a: b: B orog: OROG' ->
        []

    'rotated_latitude_longitude:' ->
        []

    'rotated_latitude_longitude zzz' ->
        []

    .. versionadded:: (cfdm) 1.7.0

    """
    # ============================================================
    # Thanks to Alan Iwi for creating these regular expressions
    # ============================================================
    import re

    def subst(s):
        """Substitutes WORD and SEP tokens for regular expressions.

        All WORD tokens are replaced by the expression for a space
        and all SEP tokens are replaced by the expression for the
        end of string.

        """
        return s.replace("WORD", r"[A-Za-z0-9_#/]+").replace(
            "SEP", r"(\s+|$)"
        )

    out = []

    pat_value = subst(r"(?P<value>WORD)SEP")
    pat_values = f"({pat_value})+"

    pat_mapping = subst(
        rf"(?P<mapping_name>WORD):SEP(?P<values>{pat_values})"
    )
    pat_mapping_list = f"({pat_mapping})+"

    pat_all = subst(
        rf"((?P<sole_mapping>WORD)|(?P<mapping_list>{pat_mapping_list}))$"
    )

    m = re.match(pat_all, string)
    if m is None:
        return []

    sole_mapping = m.group("sole_mapping")
    if sole_mapping:
        out.append({sole_mapping: []})
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
            out.append({term: values})

    return out


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
        rules = flattening_rules[name]
        attrs[name] = rules.resolver(name, variable, rules)
        
#        parsed_attribute = parse_attribute(name, attrs[name])
#
#        # Resolved references in parsed as required by attribute
#        # properties
#        resolved_parsed_attr = {}
#
#        rules = flattening_rules[name]
#        resolve_key = rules.resolve_key
#        resolve_value = rules.resolve_value
#
#        for k, v in parsed_attribute.items():
#            if resolve_key:
#                k = resolve_reference(k, variable, rules)
#
#            if resolve_value and v is not None:
#                v = [resolve_reference(x, variable, rules) for x in v]
#
#            resolved_parsed_attr[k] = v
#
#        # Re-generate attribute value string with resolved
#        # references
#        attrs[name] = generate_var_attr_str(resolved_parsed_attr)


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


def search_by_relative_path(ref, variable, search_type):
    g = variable.group()
    parts = ref.split("/")
    path = "/".join(parts[:-1])
    try:
        g = g[path]
    except KeyError:
        return ref

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
        return ref

    return search_for_coordinate_from_local_apex(ref, g, depth)


def search_for_coordinate_from_local_apex(ref, group, depth):
    if depth < 0:
        # Not found in the tree from 'group' down to the given depth
        return ref

    var = group.variables.get(ref)
    if var is not None:
        # Found
        return var.path

    if not depth:
        return ref

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

def resolve_pattern_1(name, value, variable, rules):
    """TODO

    Resolve attributes whose values are
    * ''
    * 'var1'
    * 'var1 var2'

    E.g. ``coordinates``, ``ancillary_variables``,
    ``edge_node_connectivity``

    """
    resolved = [
        resolve_reference(x, variable, rules, var=True) for x in value.split()
    ]
    variable.attrs[name] = ' '.join(resolved)

def resolve_pattern_1b(name, value, variable, rules):
    """TODO

    Resolve attributes whose values are
    * ''
    * 'dim1'
    * 'dim1 dim2'

    E.g. ``dimensions``, ``face_dimension``

    """
    resolved = [
        resolve_reference(x, variable, rules, dim=True) for x in value.split()
    ]
    variable.attrs[name] = ' '.join(resolved)

def resolve_pattern_2(name, value, variable, rules):
    """TODO

    Resolve attributes whose values are
    
    * 'var1: var2',
    * 'var1: var2 var3',
    * 'var1: var2 var3: var4 var5',

    """
    resolved = []
    for ref in value.split():
        ref =  parsed_attr.pop(0)
        if rules.resolve_key:
            if ref.endswith(":"):           
                ref = resolve_reference(ref[:-1], variable, rules, var=True)
                ref += ":"
        else:
            ref = resolve_reference(ref, variable, rules, var=True)
            
        resolved.append(ref)
        
    variable.attrs[name] = ' '.join(resolved)

def resolve_pattern_3(name, value, variable, rules):
    """TODO

    Resolve attributes whose values are
    
    * 'key1: var1',
    * 'key1: var1 key2: var2'

    E.g. ``cell_measures``, ``aggregated_data``, ``formula_terms``,
    ``interpolation_parameters``

    """
    resolved = []

    ref_type = "variable"
    for ref in value.split():
        if not ref.endswith(":"):           
            ref = resolve_reference(ref, variable, rules, var=True)

        resolved.append(ref)
        
    variable.attrs[name] = ' '.join(resolved)

def resolve_pattern_4(name, value, variable, rules):
    """TODO

    Resolve attributes whose values are "interpolated_dimension:
    tie_point_index_variable subsampled_dimension
    [interpolation_subarea_dimension] [interpolated_dimension: ...]",
    
    * 'dim1: var1 dim2'
    * 'dim1: var1 dim2 dim3'

    E.g. ``te_point_mapping``

    """
    resolved = []

    ref_type = "variable"
    for ref in value.split():
        if ref.endswith(":"):           
            ref = resolve_reference(ref[:-1], variable, rules, dim=True)
            ref += ":"
            ref_type =  "variable"
        elif ref_type  == "variable":
            ref = resolve_reference(ref, variable, rules, var=True)
            ref_type =  "dimension"
        elif ref_type == "dimension":
            ref = resolve_reference(ref, variable, rules, dim=True)
            ref_type =  "dimension"

        resolved.append(ref)
        
    variable.attrs[name] = ' '.join(resolved)

def resolve_pattern_3(name, value, variable, rules):
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
    #   'lat: mean (interval: 1 hour)'
    #
    # would be split up into:
    #
    #   ['lat:', 'mean', '(interval: 1 hour)']
    # ------------------------------------------------------------
    pattern = r'\([^)]*\)|\S+'
    cell_methods = re.findall(pattern, ref)

    previous = "axis"
    for ref in cell_methods:
        if ref.endswith(":"):
            ref = resolve_reference(ref[:-1], variable, rules, dim=True)
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
            ref = resolve_reference(ref, variable, rules, var=True)
            resolved.append(ref)
            previous = "type1"
            continue
                
        if previous == "type1" and ref == "over":
            resolved.append(ref)
            previous = "over"
            continue

        if previous == "over":
            ref = resolve_reference(ref, variable, rules, var=True)
            resolved.append(ref)
            previous = "type2"
            continue

        # Still here?
#        if ref.startswith("(") and ref.endswith(")"):
        resolved.append(ref)
        previous = None
    
    variable.attrs[name] = " ".join(resolved)

def resolve_reference(ref, variable, rules, dim=False, var=False,
                      dim_then_var=True, coordinate=False):
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
        method = "Relative"

        # First tentative as dim OR var
        absolute_ref = search_by_relative_path(
            ref, variable, first_search
        )

        # If failed and alternative possible, second tentative
        if absolute_ref is None and second_search :
            absolute_ref = search_by_relative_path(
                ref, variable, second_search 
            )

    # Reference is to be searched by proximity
    else:
        method = "Proximity"
        absolute_ref = search_by_proximity(
            ref,
            variable,
            first_search
        )
        if absolute_ref is None and second_search:
            absolute_ref = search_by_proximity(
                ref,
                variable,,
                second_search

            )

    if absolute_ref is not None:
        return absolute_ref

    return ref

