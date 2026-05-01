import numpy as np

# Ignore netCDF internal attributes
_IGNORED_ATTRS = {
    "CLASS",
    "NAME",
    "REFERENCE_LIST",
    "DIMENSION_LIST",
    "DIMENSION_LABELS",
    "_ARRAY_DIMENSIONS",
}
_IGNORED_PREFIXES = ("_Netcdf4", "_nc", "_NC")


class NetCDFError(Exception):
    """Error raised when dataset can't be viewed as netCDF."""

    pass


def _format_attr(attr, value, lib):
    """Format an attribute according to netCDF-4.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        attr: `str`
            The name of the attribute.

        value:
            The raw attribute value.

        lib:
            The backend library that created the variable or group
            that owns the attribute value.

    :Returns:

            The formatted attribute value according to netCDF-4.

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


def _parse_attributes(obj, raw_attrs):
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

        raw_attrs: `dict`
            The raw attributes from the dataset.

    :Returns:

        `dict`
            The attributes formatted according to netCDF-4.

    """
    lib = obj.lib
    return {
        k: _format_attr(k, v, lib)
        for k, v in raw_attrs.items()
        if k not in _IGNORED_ATTRS and not k.startswith(_IGNORED_PREFIXES)
    }


# --------------------------------------------------------------------
# Conversion to CDL methods
# --------------------------------------------------------------------
def _cdl_is_string_list(value):
    """Helper to detect if we need the 'string' attribute prefix in CDL.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        value:
            The attribute value.

    :Returns:

         `bool`
             `True` if we need the 'string' prefix.

    """
    if isinstance(value, (list, np.ndarray, tuple)):
        return np.array(value).dtype.kind in ("S", "U")

    return False


def _cdl_type(dtype):
    """Maps numpy dtypes to CDL type keywords.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        dtype: `numpy.dtype`
            The data type.

    :Returns:

         `str`
             The formatted data type.

    """
    s = dtype.kind
    if s == "f":
        return "double" if dtype.itemsize == 8 else "float"

    if s in ("i", "u"):
        if dtype.itemsize == 1:
            return "byte"

        if dtype.itemsize == 2:
            return "short"

        if dtype.itemsize == 8:
            return "int64"

        return "int"

    if s in ("S", "U"):
        return "string"

    return str(dtype)


def _cdl_value(value):
    """Format an attribute value.

    Formats an attribute value with CDL suffixes based on its numpy
    dtype.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        value:
            The attribute value.

    :Returns:

         `str`
             The formatted value.

    """
    # Handle empty arrays/lists first
    if isinstance(value, (list, np.ndarray, tuple)) and not len(value):
        return '""'

    # Ensure we are working with a numpy-friendly type to check dtypes
    dtype = getattr(value, "dtype", np.array(value).dtype)
    kind = dtype.kind
    is_array = isinstance(value, (list, np.ndarray, tuple))

    def format_el(v, k, dt):
        """Helper to apply suffixes to individual elements."""
        if k in ("S", "U"):  # Strings
            return f'"{v}"'

        if k == "f":  # Floats
            # ncdump uses 'f' for float32, and just a '.' for float64
            res = str(float(v))
            if res.endswith(".0"):
                res = res[:-1]  # "49.0" -> "49."

            return f"{res}f" if dt.itemsize == 4 else res

        if k in ("i", "u"):  # Integers
            suffix = {
                ("i", 1): "b",  # int8
                ("i", 2): "s",  # int16
                ("i", 4): "",  # int32 (default)
                ("i", 8): "LL",  # int64
                ("u", 1): "UB",  # uint8
                ("u", 2): "US",  # uint16
                ("u", 4): "U",  # uint32
                ("u", 8): "ULL",  # uint64
            }.get((k, dt.itemsize), "")
            return f"{int(v)}{suffix}"

        return str(v)

    if is_array:
        return ", ".join(format_el(x, kind, dtype) for x in value)

    return format_el(value, kind, dtype)
