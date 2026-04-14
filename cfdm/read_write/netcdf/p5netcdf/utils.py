import numpy as np

# Ignore netCDF internal attributes
_IGNORED_ATTRS = {
    "CLASS",
    "NAME",
    "REFERENCE_LIST",
    "DIMENSION_LIST",
    "DIMENSION_LABELS",
}
_IGNORED_PREFIXES = ("_Netcdf4", "_nc", "_NC")


class NetCDFError(Exception):
    """Error raised when file can't be viewed as netCDF."""
    pass


class         AttributeParsingError(Exception):
    """TODO"""
    pass


def _format_attr(lib, value):
    """Format an attribute according to netCDF-4.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        lib:
            The library that created the variable or group that owns
            the attribute value.

        value:
            The raw attribute value.

    :Returns:

            The formatted attribute value adhering to netCDF-4.

    """
    # Handle strings/bytes immediately
    if isinstance(value, (bytes, np.bytes_)):
        return value.decode("utf-8")

    try:
        if isinstance(value, lib.Empty):
            dtype = value.dtype
            if dtype.kind in "SUT":
                return ""

            return np.array([], dtype=value)
    except AttributeError:
        pass

    if isinstance(value, str):
        return value

    if np.isscalar(value):
        return value

    # A Python or numpy sequence attribute
    is_numpy = False
    try:
        size = value.size  # Works for numpy
        is_numpy = True
    except AttributeError:
        try:
            size = len(value)  # Works for lists
        except TypeError:
            return value

    # Empty sequence
    if not size:
        if isinstance(value, (bytes, str)) or (
            isinstance(value, np.ndarray) and value.dtype.kind in "SUT"
        ):
            return ""

        return value

    if is_numpy:
        item = value.flat[0]
    else:
        item = value[0]

    # Single-element sequence
    if size == 1:
        # If size == 1 and it's an array, then treat it as a scalar.
        if isinstance(item, (bytes, np.bytes_)):
            # Return as a string
            return item.decode("utf-8")

        # Return as a numpy scalar
        if is_numpy:
            return item

        return getattr(value, "dtype", np.array(item).dtype).type(item)

    # Multi-element sequence: Return as a numeric numpy array, or as a
    # list of strings.

    # String sequence
    if isinstance(item, (bytes, np.bytes_)):
        return [v.decode("utf-8") for v in value]

    if isinstance(item, str):
        return list(value)

    # Numeric sequence
    if is_numpy:
        return value

    return np.array(value)


def _parse_attributes(obj, raw_attributes):
    """Format raw attributes attributes according to netCDF-4.

    * Strings return as pure Python strings.
    * Single numeric values return as true numpy scalars (preserving
      data type).
    * Multi-element numeric values return as numpy arrays.
    * Multi-element string values return as Python lists.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        obj: `Group` or `Variable`
            The object that owns the raw attributes.

        raw_attributes: `dict`
            The raw attributes value from the file.

    :Returns:

        `dict`
            The formatted attributes

    """
    lib = obj.lib
    return {
        k: _format_attr(lib, v)
        for k, v in raw_attributes.items()
        if k not in _IGNORED_ATTRS and not k.startswith(_IGNORED_PREFIXES)
    }




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
    """TODO"""

    attrs = variable.attrs
    for name in referencing_attributes.intersection(attrs):
        parsed_attribute = parse_attribute(name, attrs[name])
        
        # Resolved references in parsed as required by attribute
        # properties
        resolved_parsed_attr = {}

        rules = flattening_rules[name]
        resolve_key = rules.resolve_key
        resolve_value = rules.resolve_value

        for k, v in parsed_attribute.items():
            if resolve_key:
                k = self.resolve_reference(k, old_var, rules)

            if resolve_value and v is not None:
                v = [self.resolve_reference(x, old_var, rules) for x in v]

            resolved_parsed_attr[k] = v

        # Re-generate attribute value string with resolved
        # references
        attrs[name] = generate_var_attr_str(resolved_parsed_attr)

def search_by_relative_path(ref, group, search_dim):
    if ref.startswith('/'):
        path = '/'.join(ref.split('/')[:-1])
        path =group[path].path/name
