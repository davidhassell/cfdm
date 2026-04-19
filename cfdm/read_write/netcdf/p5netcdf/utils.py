import numpy as np

# Ignore netCDF internal attributes
_IGNORED_ATTRS = {
    "CLASS",
    "NAME",
    "REFERENCE_LIST",
    "DIMENSION_LIST",
    "DIMENSION_LABELS","_ARRAY_DIMENSIONS"
}
_IGNORED_PREFIXES = ("_Netcdf4", "_nc", "_NC")


class NetCDFError(Exception):
    """Error raised when file can't be viewed as netCDF."""

    pass


class AttributeParsingError(Exception):
    """TODO."""

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
