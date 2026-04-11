from collections.abc import Mapping
from itertools import chain
from math import prod
from os.path import expanduser, expandvars

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
    """Error raised when file can't be parsed as netCDF."""

    pass


def _format_attr(lib, value):
    """Format an attribute according to netCDF-4.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        obj: `Group` or `Variable`
            The object that owns the raw attribute.

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


class Dimension:
    """A netCDF dimension.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    __hash__ = None

    def __init__(self, name, size, isunlimited, parent):
        """**Initialisation**

        :Parameters:

            name: `str`
                The name of the dimension in its parent group.

            size: `int`
                The size of the dimension.

            isunlimited: `bool`
                True if the dimension is unlimited.

            parent: `Group` or `File`
                The group in which this dimension is defined.

        """
        self._name = name
        self._size = size
        self._isunlimited = isunlimited
        self._parent = parent

    def __len__(self):
        """The size of the dimension.

        Returns the length of the dimension when interrogated by the
        builtin `len` function.

        """
        return self.size

    def __repr__(self):
        """Called by the `repr` built-in function.

        x.__repr__() <==> repr(x)

        """
        unlimited = ", unlimited" if self.isunlimited() else ""
        return (
            f"<netcdf.{self.__class__.__name__}: "
            f"{self.path}, size={self.size}{unlimited}>"
        )

    @property
    def name(self):
        """The name of the dimension in its parent group.

        .. seealso:: `path`

        :Returns:

            `str`
                The relative netCDF name (e.g. ``'time'``).

        """
        return self._name

    @property
    def parent(self):
        """The parent group.

        .. seealso:: `group`

        :Returns:

            `Group` or `File`
                The parent group.

        """
        return self.group()

    @property
    def path(self):
        """The full absolute path of the dimension.

        .. seealso:: `name`

        :Returns:

            `str`
                The absolute netCDF path (e.g. ``'/lat'`` or
                ``'/group/time'``).

        """
        path = getattr(self, "_path", None)
        if path is None:
            path = self.parent.path
            if path == "/":
                path = ""

            path += f"/{self.name}"
            self._path = path

        return path

    @property
    def size(self):
        """The size of the dimension.

        :Returns:

            `int`
                The size.

        """
        return self._size

    def dump(
        self,
        display=True,
        _prefix=None,
    ):
        """A full description of the dimension.

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            `None` or `str`
                The description. If *display* is True then the
                description is printed and `None` is
                returned. Otherwise the description is returned as a
                string.

        """
        if _prefix is None:
            _prefix = f"{self.name}: "

        lines = [f"{_prefix}{self!r}"]

        out = "\n".join(lines)
        if display:
            print(out)
            return

        return out

    def group(self):
        """The parent group that defines this dimension.

        .. seealso:: `parent`

        :Returns:

            `Group` or `File`
                The parent group.

        """
        return self._parent

    def isunlimited(self):
        """Whether the dimension is unlimited.

        :Returns:

            `bool`
                `True` if the dimension is unlimited, `False`
                otherwise.

        """
        return self._isunlimited


class Variable:
    """A netCDF variable.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    __hash__ = None

    def __init__(self, name, parent, var, var_attrs):
        """**Initialisation**

        :Parameters:

            name: `str`
                The name of the variable in its parent group.

            parent: `Group` or `File`
                The parent group containing this variable.

            var:
                The underlying (subclass of) `pyfive.Dataset` or
                `scipy.io.netcdf_variable` object.

            var_attrs: `dict`
                The raw attributes of *var*.

        """
        self._name = name
        self._var = var
        self._parent = parent
        self._var_attrs = var_attrs

        root = parent.root
        self._backend = root.backend
        self._lib = root.lib

        self._attrs = _parse_attributes(self, var_attrs)

    def __getitem__(self, index):
        """Return a subspace of the data array defined by indices."""
        array = self._var[index]
        match self.backend:
            case "pyfive" | "netCDF4" | "zarr" | "h5py":
                return array
            case "netcdf_file":
                # Need to copy the numpy array returned by
                # scipy.io.netcdf_file with mmap=True. See
                # `dataset_close` and the scipy.io.netcdf_file docs
                # for details.
                return array.copy()

    def __len__(self):
        """The size of leading data array dimension."""
        shape = self.shape
        if shape:
            return shape[0]

        raise TypeError("len() of unsized object (scalar variable)")

    def __repr__(self):
        """Called by the `repr` built-in function.

        x.__repr__() <==> repr(x)

        """
        # Resolve the dimension objects to get their full paths
        try:
            dim_paths = [d.path for d in self.get_dims()]
            if len(dim_paths) == 1:
                dims = f"({dim_paths[0]},)"
            else:
                dims = f"({', '.join(dim_paths)})"
        except Exception:
            # Fallback if resolution fails for any reason
            dims = self.dimensions

        return (
            f"<netcdf.{self.__class__.__name__}: "
            f"{self.path}, shape={self.shape}, dimensions={dims}>"
        )

    def _get_dimensions(self):
        """Get the variable dimension names.

        Raises a `NetCDFError` exception if the DIMENSION_LIST
        attribute is not appropriately set.

        :Returns:

            `tuple`
                The dimension names, relative to their parent groups.

        """
        var_attrs = self._var_attrs
        match self.backend:
            case "pyfive" | "h5py":
                # ----------------------------------------------------
                # Backend: pyfive
                # ----------------------------------------------------

                # Case 1: It's a coordinate variable that has the same
                #         name as its dimension.
                if var_attrs.get("CLASS") == b"DIMENSION_SCALE":
                    return (self.name,)

                ndim = self.ndim

                # Case 2: It's a scalar variable with no dimensions
                if not ndim:
                    return ()

                # Case 3: It's an N-d variable (N>=1) not covered by
                #         case 1.
                dim_list = var_attrs.get("DIMENSION_LIST", ())
                if len(dim_list) != ndim:
                    raise NetCDFError(
                        f"Variable {self.path!r} requires {ndim} "
                        f"DIMENSION_LIST links, found {len(dim_list)}"
                    )

                dim_names = []

                for ref in dim_list:
                    try:
                        if hasattr(ref, "item"):
                            ref = ref.item()
                        elif isinstance(ref, (list, tuple)) and len(ref) > 0:
                            ref = ref[0]

                        root_grp = self.parent.root._grp
                        dim_dataset = root_grp[ref]
                        dim_names.append(dim_dataset.name.split("/")[-1])
                    except (KeyError, ValueError, TypeError):
                        continue

                return tuple(dim_names)

            case "netCDF4" | "netcdf_file":
                # ----------------------------------------------------
                # netCDF4 | netcdf_file
                # ----------------------------------------------------
                return self._var.dimensions

            case "zarr":
                # ----------------------------------------------------
                # zarr
                # ----------------------------------------------------
                if hasattr(self.root, "_var_to_dims"):
                    return tuple(
                        dim.name for dim in self.root._var_to_dims[self.path]
                    )

                return

    @property
    def backend(self):
        """The TODO variable attributes.

        :Returns:

            `dict`
                The attribute values, keyed by their names.

        """
        return self._backend

    @property
    def attrs(self):
        """The variable attributes.

        :Returns:

            `dict`
                The attribute values, keyed by their names.

        """
        return self._attrs

    @property
    def chunks(self):
        """The data chunk shape.

        .. seealso:: `chunks`

        :Returns:

            `tuple` or `None`
                The chunk shape, e.g. ``(5, 6, 7)``. If the data is
                contiguous then `None` is returned.

        """
        chunks = getattr(self, "_chunks", None)
        if chunks is None:
            match self.backend:
                case "pyfive" | "zarr" | "h5py":
                    chunks = self._var.chunks
                case "netCDF4":
                    chunks = self._var.chunking()
                    if chunks == "contiguous":
                        chunks = None
                    elif chunks is not None:
                        chunks = tuple(chunks)
                case "netcdf_file":
                    chunks = None

            self._chunks = chunks

        return chunks

    @property
    def dimensions(self):
        """The variable dimensions.

        .. seealso:: `get_dims`

        :Returns:

            `tuple`
                The dimension names, in the order of the data array
                dimensions.

        """
        dimensions = getattr(self, "_dimensions", None)
        if dimensions is None:
            dimensions = self._get_dimensions()
            self._dimensions = dimensions
            if dimensions is not None:
                del self._var_attrs

        return dimensions

    @property
    def dtype(self):
        """The numpy data type of the variable's dataset."""
        dtype = getattr(self, "_dtype", None)
        if dtype is None:
            match self.backend:
                case "pyfive" | "zarr" | "netCDF4" | "h5py":
                    dtype = self._var.dtype
                    # TODO zarr dtype is endianny
                case "netcdf_file":
                    dtype = (
                        self._var[(slice(0, 1),) * len(self.shape)]
                        .flat[0]
                        .dtype
                    )

            self._dtype = dtype

        return dtype

    @property
    def filename(self):
        """The TODOvariable dimensions.

        .. seealso:: `get_dims`

        :Returns:

            `tuple`
                The dimension names, in the order of the data array
                dimensions.

        """
        return self.root.filename

    @property
    def lib(self):
        """The TODO variable attributes.

        :Returns:

            `dict`
                The attribute values, keyed by their names.

        """
        return self._lib

    @property
    def maxshape(self):
        """The maximum dimension lengths of the variable.

        :Returns:

            `tuple`
                The dimension lengths (e.g. ``(180, 360)``). Unlimited
                dimensions are represented by `None` (e.g. ``(None,
                96, 73)``)

        """
        maxshape = getattr(self, "_maxshape", None)
        if maxshape is None:
            match self.backend:
                case "pyfive" | "h5py":
                    maxshape = self._var.maxshape
                case "netCDF4":
                    maxshape = [
                        None if dim.isunlimited() else size
                        for size, dim in zip(self.shape, self.get_dims())
                    ]
                    maxshape = tuple(maxshape)
                case "zarr" | "netcdf_file":
                    maxshape = self.shape

            self._maxshape = maxshape

        return maxshape

    @property
    def name(self):
        """The name of the variable in its parent group.

        .. seealso:: `path`

        :Returns:

            `str`
                The relative netCDF name (e.g. ``'latitude'``).

        """
        return self._name

    @property
    def ndim(self):
        """The number of dimensions for the variable.

        :Returns:

            `int`
                The number of dimensions.

        """
        return len(self.shape)

    @property
    def parent(self):
        """The parent group.

        .. seealso:: `group`

        :Returns:

            `Group` or `File`
                The parent group.

        """
        return self._parent

    @property
    def path(self):
        """The full absolute path of the variable.

        .. seealso:: `name`

        :Returns:

            `str`
                The absolute netCDF path (e.g. ``'/time'`` or
                ``'/group/latitude'``).

        """
        path = getattr(self, "_path", None)
        if path is None:
            match self.backend:
                case "pyfive" | "zarr" | "h5py":
                    path = self._var.name
                case "netCDF4":
                    path = self.parent.path
                    if path == "/":
                        path = ""

                    path += f"/{self.name}"
                case "netcdf_file":
                    path = f"/{self.name}"

            self._path = path

        return path

    @property
    def root(self):
        """The parent group.

        .. seealso:: `group`

        :Returns:

            `Group` or `File`
                The parent group.

        """
        return self.parent.root

    @property
    def shape(self):
        """The dimension lengths of the variable.

        :Returns:

            `tuple` of `int`
                The dimension lengths (e.g. ``(12, 96, 73)``).

        """
        shape = getattr(self, "_shape", None)
        if shape is None:
            shape = self._var.shape
            self._shape = shape

        return shape

    @property
    def size(self):
        """The total number of elements in the variable's data.

        :Returns:

            `int`
                The number of elements.

        """
        size = getattr(self, "_size", None)
        if size is None:
            size = prod(self.shape)
            self._size = size

        return size

    def dump(
        self,
        display=True,
        _prefix=None,
        _level=0,
        _structure=False,
    ):
        """A full description of the variable.

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            `None` or `str`
                The description. If *display* is True then the
                description is printed and `None` is
                returned. Otherwise the description is returned as a
                string.

        """
        if _prefix is None:
            _prefix = f"{self.name}: "

        lines = [f"{_prefix}{self!r}"]

        indent = "    "
        i1 = indent * (_level + 1)
        i2 = indent * (_level + 2)

        # Attributes
        if not _structure and self.attrs:
            lines.append(f"{i1}Attributes:")
            lines.extend(
                f"{i2}{name}: {value!r}" for name, value in self.attrs.items()
            )

        out = "\n".join(lines)
        if display:
            print(out)
            return

        return out

    def get_dims(self):
        """Return the dimensions of the variable.

        .. seealso:: `dimensions`

        :Returns:

            `tuple` of `Dimension`
                The dimensions for the variable.

        """
        dims = getattr(self, "_dims", None)
        if dims is not None:
            return dims

        dims = []
        for dim_name in self.dimensions:
            current_group = self.parent
            found = False

            # Walk up the tree to find where the dimension is defined
            while current_group is not None:
                dim = current_group.dimensions.get(dim_name)
                if dim is not None:
                    dims.append(dim)
                    found = True
                    break

                current_group = current_group.parent

            if not found:
                raise NetCDFError(
                    f"Dimension {dim_name!r} not found in the group hierarchy."
                )

        dims = tuple(dims)
        self._dims = dims
        return dims

    def group(self):
        """The parent group that defines this variable.

        .. seealso:: `parent`

        :Returns:

            `Group`
                The parent group.

        """
        return self._parent

    def structure(
        self,
        display=True,
        _prefix=None,
        _level=0,
    ):
        """A full description of the `Variable`.

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            `None` or `str`
                The description. If *display* is True then the
                description is printed and `None` is
                returned. Otherwise the description is returned as a
                string.

        """
        return self.dump(display, _prefix, _level, True)


class Group(Mapping):
    """A netCDF group.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    __hash__ = None

    def __init__(self, name, parent, root, grp, grp_attrs):
        """**Initialisation**

        :Parameters:

            name: `str`
                The name of the group in its parent group. The root
                group has the name ``''``.

            parent: `Group` or `None`
                The parent group. Set to `None` if there is no parent
                (i.e. the group is the root group).

            root: `File`
                The root group.

            grp:
                The underlying (subclass of) `pyfive.Group`, (subclass
                of) `pyfive.File`, or `scipy.io.netcdf_file` object.

            grp_attrs: `dict`
                The raw attributes of *grp*.

        """
        self._name = name
        self._parent = parent
        self._root = root
        self._backend = root.backend
        self._lib = root.lib
        self._grp = grp

        self._attrs = _parse_attributes(self, grp_attrs)

        self._dimensions = {}
        self._variables = {}
        self._groups = {}
        self._parse_group_structure(root)

    def __getitem__(self, key):
        """Get a variable or group.

        Absolute and relative nested paths are allowed, which may
        include ``.`` (current group) and ``..`` (parent group)
        elements. A trailing ``/`` in the path is ignored. An empty
        path (``''``) is equivalent to ``'/'``.

        """
        if key == "":
            return self

        # Still here? Determine the starting point
        current = self
        if key.startswith("/"):
            current = self.root
        else:
            current = self

        # Split the path into parts (ignoring empty strings from
        # double-slashes)
        segments = [s for s in key.split("/") if s]

        # Handle a key of "/", "//", "///", etc.
        if not segments:
            return current

        # Still here? Then loop through the segments
        for i, part in enumerate(segments):
            if part == "..":
                # Move up one group
                current = current.parent
                if current is None:
                    if key.startswith("/"):
                        start = ""
                    else:
                        start = f" from group {self.path}"

                    raise KeyError(
                        f"Invalid path {key!r}{start}: Attempted to "
                        "navigate above the root group."
                    )

                continue

            if part == ".":
                continue

            # Group/Variable navigation
            if part in current.groups:
                current = current.groups[part]
            elif part in current.variables:
                # A variable must be the final element in a path
                if i == len(segments) - 1:
                    return current.variables[part]

                if key.startswith("/"):
                    start = ""
                else:
                    start = f" from group {self.path}"

                raise KeyError(
                    f"Invalid path {key!r}{start}: "
                    f"{current.variables[part].path} is a variable "
                    "and cannot have children"
                )
            else:
                if key.startswith("/"):
                    start = ""
                else:
                    start = f" from group {self.path}"

                raise KeyError(
                    f"Invalid path {key!r}{start}: Path element {part!r} "
                    f"not found in group {current.path}"
                )

        return current

    def __iter__(self):
        """The variables and sub-groups."""
        return chain(self.groups, self.variables)

    def __len__(self):
        """The number of variables and sub-groups."""
        return len(self.variables) + len(self.groups)

    def __repr__(self):
        """Called by the `repr` built-in function."""
        pd = "" if len(self.dimensions) == 1 else "s"
        pv = "" if len(self.variables) == 1 else "s"
        pg = "" if len(self.groups) == 1 else "s"

        path = self.path
        if path == "/":
            path = ""
        else:
            path += ", "

        return (
            f"<netcdf.{self.__class__.__name__}: "
            f"{path}"
            f"{len(self.dimensions)} dimension{pd}, "
            f"{len(self.variables)} variable{pv}, "
            f"{len(self.groups)} group{pg}>"
        )

    def __str__(self):
        """Called by the `str` built-in function."""
        return self.dump(display=False, _recursive=False, _structure=True)

    def _parse_group_structure(self, root):
        """Parse the group structure.

        Parses variables, dimensions, and subgroups, recursively.

        :Parameters:

            root: `File`
                The root group.

        :Returns:

            `None`

        """
        match self.backend:
            case "pyfive" | "h5py":
                # ----------------------------------------------------
                # pyfive | h5py
                # ----------------------------------------------------
                raw_dims = {}
                subgroups = []
                datasets = []
                dataset_attrs = {}

                # Categorise objects without double-reading items from
                # the dataset
                lib = root.lib
                for name, h5 in self._grp.items():
                    if isinstance(h5, lib.Group):
                        subgroups.append((name, h5))
                    elif isinstance(h5, lib.Dataset):
                        datasets.append((name, h5))

                # Extract dimension scales (strictly ignoring scalars)
                for name, var in datasets:
                    shape = var.shape
                    attrs = var.attrs
                    dataset_attrs[name] = attrs

                    if shape and attrs.get("CLASS") == b"DIMENSION_SCALE":
                        # Get ID: Use `None` if missing to push to end
                        # of sort
                        dim_id = attrs.get("_Netcdf4Dimid")
                        if dim_id is not None:
                            dim_id = int(dim_id)

                        dim_name = name.split("/")[-1]

                        is_unlimited = False
                        maxshape = var.maxshape
                        if maxshape and len(maxshape) > 0:
                            is_unlimited = maxshape[0] is None

                        raw_dims[dim_name] = {
                            "id": dim_id,
                            "size": shape[0],
                            "is_unlimited": is_unlimited,
                            "is_stub": (
                                b"not a netCDF variable"
                                in attrs.get("NAME", b"")
                            ),
                        }

                # Sort and create Dimension objects
                #
                # Sorting ensures consistency with netCDF4-python,
                # which preserves the creation order of its dimensions
                # in an ordered dictionary.
                #
                # We sort by (ID, Name). If ID is `None` (pure HDF5), it's
                # treated as infinity tqo ensure it appears after the
                # netCDF-indexed dimensions.
                sorted_items = sorted(
                    raw_dims.items(),
                    key=lambda x: (
                        x[1]["id"] if x[1]["id"] is not None else float("inf"),
                        x[0],
                    ),
                )

                for d_name, d_info in sorted_items:
                    self._dimensions[d_name] = Dimension(
                        name=d_name,
                        size=d_info["size"],
                        isunlimited=d_info["is_unlimited"],
                        parent=self,
                    )

                # Create variables (skipping internal netCDF stubs)
                for name, var in datasets:
                    dim_name = name.split("/")[-1]

                    # If it's in 'raw_dims' and flagged as a stub then
                    # skip it. Otherwise - whether it's a coordinate
                    # variable, normal data, or a scalar pretending to
                    # be a scale - it becomes a Variable.
                    is_stub = raw_dims.get(dim_name, {}).get("is_stub", False)

                    if not is_stub:
                        self._variables[name] = Variable(
                            name=name,
                            parent=self,
                            var=var,
                            var_attrs=dataset_attrs[name],
                        )

                # Create subgroups
                for name, grp in subgroups:
                    self._groups[name] = Group(
                        name=name,
                        parent=self,
                        root=root,
                        grp=grp,
                        grp_attrs=grp.attrs,
                    )

            case "netCDF4":
                # ----------------------------------------------------
                # netCDF4
                # ----------------------------------------------------
                # Create dimensions in this group
                for name, dim in self._grp.dimensions.items():
                    self._dimensions[name] = Dimension(
                        name=name,
                        size=dim.size,
                        isunlimited=dim.isunlimited(),
                        parent=self,
                    )

                # Create variables in this group
                for name, var in self._grp.variables.items():
                    attrs = {
                        attr: var.getncattr(attr) for attr in var.ncattrs()
                    }
                    self._variables[name] = Variable(
                        name=name,
                        parent=self,
                        var=var,
                        var_attrs=attrs,
                    )

                # Create subgroups
                for name, grp in self._grp.groups.items():
                    attrs = {
                        attr: grp.getncattr(attr) for attr in grp.ncattrs()
                    }
                    self._groups[name] = Group(
                        name=name,
                        parent=self,
                        root=root,
                        grp=grp,
                        grp_attrs=attrs,
                    )

            case "netcdf_file":
                # ----------------------------------------------------
                # netcdf_file
                # ----------------------------------------------------
                # Create dimensions in this group
                for name, size in self._grp.dimensions.items():
                    self._dimensions[name] = Dimension(
                        name=name,
                        size=size,
                        isunlimited=False,
                        parent=self,
                    )

                # Create variables in this group
                for name, var in self._grp.variables.items():
                    self._variables[name] = Variable(
                        name=name,
                        parent=self,
                        var=var,
                        var_attrs=var._attributes,
                    )

            case "zarr":
                # ----------------------------------------------------
                # zarr
                # ----------------------------------------------------
                # Create variables in this group
                for name, var in dict(self._grp.arrays()).items():
                    self._variables[name] = Variable(
                        name=name, parent=self, var=var, var_attrs=var.attrs
                    )

                # Create subgroups
                for name, grp in dict(self._grp.groups()).items():
                    self._groups[name] = Group(
                        name=name,
                        parent=self,
                        root=root,
                        grp=grp,
                        grp_attrs=grp.attrs,
                    )

                # Create dimensions in all groups, starting with the
                # root group.
                if self.path == "/":
                    root._group_to_dims = {}
                    root._var_to_dims = {}
                    self._populate_dimension_maps(self)

                    for path, dims in root._group_to_dims.items():
                        group = self[path]
                        for name, dim in dims.items():
                            group._dimensions[name] = dim

    def _populate_dimension_maps(self, group):
        """Populate the dimension map dictionaries.

        For the given group and all of its child groups, a mapping of
        full-path group names to the unique dimensions implied by the
        variables therein will be added to `_group_to_dims`. For
        instance::

           {'/': {},
            'bounds2': <ZarrDimension: bounds2, size(2)>,
            'x': <ZarrDimension: x, size(9)>},
            '/forecast': {'y': <ZarrDimension: y, size(10)>},
            '/forecast/model': {}}


        For the given group and all of its child groups, a mapping of
        full-path variables names to their dimensions will be added to
        `_var_to_dims`. For instance::

           {'/latitude_longitude': (),
            '/x': (<ZarrDimension: x, size(9)>,),
            '/x_bnds': (<ZarrDimension: x, size(9)>
                        <ZarrDimension: bounds2, size(2)>),
            '/forecast/cell_measure': (<ZarrDimension: x, size(9)>,
                                       <ZarrDimension: y, size(10)>),
            '/forecast/latitude': (<ZarrDimension: y, size(10)>,
                                   <ZarrDimension: x, size(9)>),
            '/forecast/longitude': (<ZarrDimension: x, size(9)>,
                                    <ZarrDimension: y, size(10)>),
            '/forecast/rotated_latitude_longitude': (),
            '/forecast/time': (),
            '/forecast/y': (<ZarrDimension: y, size(10)>,),
            '/forecast/y_bnds': (<ZarrDimension: y, size(10)>,
                                 <ZarrDimension: bounds2, size(2)>),
            '/forecast/model/ta': (<ZarrDimension: y, size(10)>,
                                   <ZarrDimension: x, size(9)>)}

        **Zarr datasets**

        Populating the `_group_to_dims` dictionary is currently only
        required for a Zarr grouped dataset, for which this
        information is not explicitly defined in the format's data
        model (unlike for netCDF and HDF5 datasets).

        See `dataset_flatten` for details.

        .. versionadded:: (cfdm) 1.13.0.0

        :Parameters:

            group: `Group`
                The group object.

        :Returns:

            `None`

        """
        group_path = group.path
        root = group.root
        group_to_dims = root._group_to_dims
        var_to_dims = root._var_to_dims
        group_dimension_search = ( #root._group_dimension_search
            "closest_ancestor"  # root._group_dimension_search
        )

        # Initialise the mapping from this group to its `Dimension`
        # objects. Use 'setdefault' because a previous call to
        # `_populate_dimension_maps` might already have done this.
        group_to_dims.setdefault(group_path, {})

        # Loop over variables in this group, sorted by variable name.
        #        for v in dict(sorted(group.arrays())).values():
        for v in group.variables.values():
            # Initialise mapping from the variable to its Dimension
            # objects
            var_path = v.path
            var_to_dims[var_path] = ()

            raw_dimension_names = self._zarr_raw_dimension_names(v)
            if not raw_dimension_names:
                # A scalar variable has no dimensions
                continue

            # Loop over this variable's dimension names
            for name, size in zip(raw_dimension_names, v.shape):
                name_split = name.split("/")
                basename = name_split[-1]

                # ----------------------------------------------------
                # Define 'g' as the absolute path name of the group in
                # which to register the logical dimension object for
                # this dimension.
                #
                # Which group is defined will depend on the nature of
                # the dimension's 'name'.
                # ----------------------------------------------------
                if "/" not in name:
                    # ------------------------------------------------
                    # Raw dimension name which contains no '/'
                    # characters
                    #
                    # The behaviour depends on the search algorithm
                    # defined by 'group_dimension_search'.
                    #
                    # E.g. "dim"
                    # ------------------------------------------------
                    if group_dimension_search in (
                        "closest_ancestor",
                        "furthest_ancestor",
                    ):
                        # Find the names of all ancestor groups, in
                        # the appropriate order for searching.
                        group_split = group_path.split("/")
                        ancestor_names = [
                            "/".join(group_split[:n])
                            for n in range(1, len(group_split))
                        ]
                        ancestor_names[0] = "/"
                        # E.g. if the current group is /g1/g2/g3 then
                        #      the ancestor group names are [/, /g1,
                        #      /g1/g2]

                        if group_dimension_search == "closest_ancestor":
                            # "closest_ancestor" searching requires
                            # the ancestor group order to be reversed,
                            # e.g. [/g1/g2, /g1, /]
                            ancestor_names = ancestor_names[::-1]

                        # Search through the ancestors in order,
                        # stopping if we find a matching dimension.
                        found_dim_in_ancestor = False
                        for g in ancestor_names:
                            zarr_dim = group_to_dims[g].get(basename)
                            if zarr_dim is not None and zarr_dim.size == size:
                                # Found a dimension in this ancestor
                                # group 'g' with the right name and
                                # size
                                found_dim_in_ancestor = True
                                break

                        if not found_dim_in_ancestor:
                            # Dimension 'basename' could not be
                            # matched to any ancestor group
                            # dimensions, so define it in the current
                            # group.
                            g = group_path

                    elif group_dimension_search == "local":
                        # Assume that the dimension is different to
                        # any with same name and size defined in any
                        # ancestor group.
                        g = group_path

                    else:
                        raise NetCDFError(
                            "Bad 'group_dimension_search' value: "
                            f"{group_dimension_search!r}"
                        )
                else:
                    # ------------------------------------------------
                    # Raw dimension name contains '/' characters
                    # ------------------------------------------------
                    if name.endswith("/"):
                        raise NetCDFError(
                            "Dimension names can't end with '/': "
                            f"dataset={self.dataset_name()} "
                            f"variable={var_path} "
                            f"dimension_name={name}"
                        )

                    g = "/".join(name_split[:-1])
                    try:
                        g = self[g].path
                    except KeyError:
                        raise NetCDFError("Bad dimension name TODO")

                # TODO
                zarr_dim = None
                if g in group_to_dims:
                    # Group 'g' is already registered in the mapping
                    zarr_dim = group_to_dims[g].get(basename)
                    if zarr_dim is not None:
                        # Dimension 'basename' is already registered
                        # in group 'g'
                        if zarr_dim.size != size:
                            raise NetCDFError(
                                f"Zarr dimension has the wrong size: {size}. "
                                f"Expected size {zarr_dim.size} defined "
                                f"by Zarr dimension {zarr_dim.name!r} "
                                f"in group {zarr_dim.group().path!r}"
                            )
                else:
                    # Initialise group 'g' in the mapping
                    group_to_dims[g] = {}

                if zarr_dim is None:
                    # Register a new Dimension in a group
                    parent = root.get(g)
                    if parent is None:
                        # Must be the root group
                        parent = root

                    zarr_dim = Dimension(basename, size, False, parent)
                    group_to_dims[g][basename] = zarr_dim

                # Map the variable to the `Dimension` object
                var_to_dims[var_path] += (zarr_dim,)

        # ------------------------------------------------------------
        # Recursively scan all child groups
        # ------------------------------------------------------------
        for g in group.groups.values():
            self._populate_dimension_maps(g)

    def _zarr_raw_dimension_names(self, variable):
        """Return the raw dimension names for a variable.

        Currently this is only required for, and only works for, Zarr
        variables. An `AttributeError` will be raised if called for
        any other type of variable.

        :Parameters:

            var: `Variable`
                The variable object.

        :Returns:

            `list` of `str`
                The variable's raw dimension names. A scalar variable
                will have an empty list.

        """
        zarr_var = variable._var
        metadata = zarr_var.metadata

        zarr_format = metadata.zarr_format
        match zarr_format:
            case 3:
                dimensions = metadata.dimension_names
            case 2:
                dimensions = metadata.attrs.get("_ARRAY_DIMENSIONS")
            case _:
                raise NetCDFError(
                    f"Can't parse a Zarr v{zarr_format} dataset. "
                    "Only Zarr v3 and v2 can be parsed."
                )

        if dimensions is None:
            if variable.shape:
                raise NetCDFError(
                    f"Non-scalar Zarr v{zarr_format} variable has no "
                    f"dimension names: {variable.path}"
                )

            dimensions = []

        return dimensions

    @property
    def attrs(self):
        """The group attributes.

        :Returns:

            `dict`
                The attribute values, keyed by their names.

        """
        return self._attrs

    @property
    def backend(self):
        """The TODO variable attributes.

        :Returns:

            `dict`
                The attribute values, keyed by their names.

        """
        return self._backend

    @property
    def dimensions(self):
        """The dimensions defined in this group.

        :Returns:

            `dict`
                The `Dimension` objects, keyed by their names.

        """
        return self._dimensions

    @property
    def groups(self):
        """The sub-groups.

        :Returns:

            `dict`
                The `Group` objects, keyed by their names.

        """
        return self._groups

    @property
    def lib(self):
        """The TODO variable attributes.

        :Returns:

            `dict`
                The attribute values, keyed by their names.

        """
        return self._lib

    @property
    def name(self):
        """The name of the group in its parent group.

        :Returns:

            `str`
                The relative netCDF name (e.g. ``'subgroup'``).

        """
        return self._name

    @property
    def parent(self):
        """The parent group.

        :Returns:

            `Group` or `File` or `None`
                The parent group, or `None` if there is no parent
                (i.e. this group is the root group).

        """
        return self._parent

    @property
    def path(self):
        """The full absolute path of the group.

        :Returns:

            `str`
                The absolute netCDF path (e.g. ``'/'``, ``'/model'``,
                or ``'/group/forecast'``).

        """
        path = getattr(self, "_path", None)
        if path is None:
            match self.backend:
                case "pyfive" | "netCDF4" | "zarr" | "h5py":
                    # TODO is this OK for netCDF3?
                    path = self._grp.name
                case "netcdf_file":
                    path = "/"
                    # TODO is this OK for netCDF3?

            self._path = path

        return path

    @property
    def root(self):
        """The root group.

        :Returns:

            `File`
                The root group.

        """
        return self._root

    @property
    def variables(self):
        """The dimensions defined in this group.

        :Returns:

            `dict`
                The `Variable` objects, keyed by their names.

        """
        return self._variables

    def dump(
        self,
        display=True,
        _prefix=None,
        _level=0,
        _recursive=True,
        _structure=False,
    ):
        """A full description of the group.

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            `None` or `str`
                The description. If *display* is True then the
                description is printed and `None` is
                returned. Otherwise the description is returned as a
                string.

        """
        if _prefix is None:
            _prefix = f"{self.name}: "

        lines = [f"{_prefix}{self!r}"]

        indent = "    "
        i1 = indent * (_level + 1)
        i2 = indent * (_level + 2)

        # Attributes
        if not _structure and self.attrs:
            lines.append(f"{i1}Attributes:")
            lines.extend(
                f"{i2}{name}: {value!r}" for name, value in self.attrs.items()
            )

        # Dimensions
        if self.dimensions:
            lines.append(f"{i1}Dimensions:")
            lines.extend(
                f"{i2}{dim.dump(display=False)}"
                for name, dim in self.dimensions.items()
            )
        # Variables
        if self.variables:
            lines.append(f"{i1}Variables:")
            lines.extend(
                f"{i2}{var.dump(display=False, _level=_level + 2, _structure=_structure)}"
                for name, var in self.variables.items()
            )

        # Groups
        if self.groups:
            lines.append(f"{i1}Groups:")
            if _recursive:
                lines.extend(
                    f"{i2}{group.dump(display=False, _level=_level + 2, _recursive=True, _structure=_structure)}"
                    for group in self.groups.values()
                )
            else:
                lines.extend(
                    f"{i2}{name}: {group!r}"
                    for name, group in self.groups.items()
                )

        out = "\n".join(lines)
        if display:
            print(out)
            return

        return out

    def structure(
        self,
        display=True,
        _prefix=None,
        _level=0,
    ):
        """A full description of the `Group`.

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            `None` or `str`
                The description. If *display* is True then the
                description is printed and `None` is
                returned. Otherwise the description is returned as a
                string.

        """
        return self.dump(display, _prefix, _level, True, True)


class File(Group):
    """A netCDF dataset.

    A `File` represents the netCDF dataset as a collection of groups
    (`Group` objects), dimensions (`Dimension` objects), variables
    (`Variable` objects), and attributes.

    A `File` may be intantiated from a `str` or `pathlib.Path` path, a
    file-like object, a `pyfive.File` object, or a subclass of a
    `pyfive.File` object.

    **Instantiation from a subclass of `pyfive.File`**

    A subclass of `pyfive.File` (e.g. `my_pyfive.File`) must reference
    classes `my_pyfive.Group` and `my_pyfive.Dataset` that inherit
    repectively from `pyfive.Group` and pyfive.Dataset`. The
    sublcasses must expose following attributes and methods:

    * `my_pyfive.File.attrs`
    * `my_pyfive.File.close`
    * `my_pyfive.File.filename`
    * `my_pyfive.File.items`
    * `my_pyfive.Group.attrs`
    * `my_pyfive.Group.items`
    * `my_pyfive.Dataset.attrs`
    * `my_pyfive.Dataset.chunks`
    * `my_pyfive.Dataset.dtype`
    * `my_pyfive.Dataset.maxshape`
    * `my_pyfive.Dataset.name`
    * `my_pyfive.Dataset.shape`

    **Performance**

    `netcdf` is "structure- and attribute-eager", meaning that
    during `File` instantiation, the entire netCDF-4 group, variable,
    and dimension structure is parsed; along with all group and
    variable attributes. Variable data access is always via access to
    the underlying (subclass of a) `pyfive.Dataset` object. Some
    `Variable` and `Group` properties and methods also access an
    underlying (subclass of a) `pyfive.Dataset` or `pyfive.Group`
    object, but only for the first request, after which the result is
    cached. For instance, the first time `Variable.shape` is requested
    it is retrieved from the underlying (subclass of)
    `pyfive.Dataset`, and subsequent requests access the shape that is
    cached inside the `Variable` object.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    _netcdf = True

    def __init__(self, dataset, mode="r", backend=None):
        """**Initialisation**

        :Parameters:

            dataset:
                The netCDF dataset to be read.

                May be a `str` or `pathlib.Path` path, a file-like
                object (such as `io.BufferedReader` or the result of
                an `fsspec` file system open), a `pyfive.File` object,
                or a subclass of a `pyfive.File` object.

                If *dataset* is path or file-like object, then a
                `pyfive.File` object is automatically created
                internally. E.g ``nc = p5netcdf.File('file.nc')`` is
                equivalent to ``py5 = pyfive.File('file.nc'); nc =
                p5netcdf.File(py5)`` (see `close`).

            mode: `str`, optional
                The access mode used when using `pyfive.File` to open
                the *dataset*. The only allowed value is ``'r'``
                (read-only), and this is the default. Ignored if
                *dataset* is already a (subclass of a) `pyfive.File`
                object.

            pyfive_options: `dict` or `None`, optional
                Keyword arguments that are passed to `pyfive.File` to
                be used when opening a netCDF-4 *dataset*. Setting to
                `None` (the default) is equivalent to providing an
                empty dictionary. Ignored if *dataset* is already a
                (subclass of a) `pyfive.File` object.

        """
        import pyfive

        if mode != "r":
            raise ValueError("mode must be 'r'. Got: mode={mode!r}")

        if isinstance(dataset, pyfive.File):
            nc = dataset
            attrs = dataset.attrs
            self._backend = "pyfive"
            self._lib = pyfive
            # The opened dataset is owned externally
            self._owns_nc = False

        else:
            # Map backend names to dataset-open functions
            open_functions = {
                "pyfive": self._open_pyfive,
                "netcdf_file": self._open_netcdf_file,
                "netCDF4": self._open_netCDF4,
                "zarr": self._open_zarr,
                "h5py": self._open_h5py,
            }

            if backend is not None:
                if isinstance(backend, str):
                    backend = (backend,)

                open_functions = {b: open_functions[b] for b in backend}

            try:
                # Try to expand `str` or `pathlib.Path`
                dataset = expanduser(expandvars(dataset))
            except TypeError:
                # Likely file-like or directory-like object
                pass

            nc = None
            open_log = []
            for name, func in open_functions.items():
                try:
                    nc, attrs = func(dataset)
                except Exception as error:
                    open_log.append(
                        f"{name}:\n{error.__class__.__name__}: {error}"
                    )
                else:
                    open_log.append(f"{name}:\nSuccessfully opened")
                    break

            self._open_log = open_log
            if nc is None:
                error = "\n\n".join(open_log)
                raise NetCDFError(
                    f"Can't interpret {dataset} as a netCDF dataset "
                    f"with any of the backends {tuple(open_functions)}:\n\n"
                    f"{error}"
                )

            # The opened dataset is owned internally
            self._owns_nc = True

        # ------------------------------------------------------------
        # Initialise the group structure
        # ------------------------------------------------------------
        super().__init__(
            name="", parent=None, root=self, grp=nc, grp_attrs=attrs
        )

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context and close the file."""
        self.close()

    @property
    def filename(self):
        """The name of the file on disk.

        :Returns:

            `str`
                The filename of the dataset.

        """
        filename = getattr(self, "_filename", None)
        if filename is None:
            match self.backend:
                case "pyfive" | "h5py" | "netcdf_file":
                    filename = self._grp.filename
                case "netCDF4":
                    filename = self._grp.filepath()
                case "zarr":
                    filename = str(self._grp.store_path)

            self._filename = filename

        return filename

    def close(self):
        """Close the dataset.

        Closes the underlying netCDF dataset, but not if the dataset
        was originally defined by a (subclass of a) `pyfive.File`
        object.

        :Returns:

            `None`

        """
        if self._owns_nc:
            if self.backend == "netcdf_file":
                # We can't close a scipy.io.netcdf_file instance
                # opened with mmap=True when any variable still
                # exists, or when an array referring to a variable's
                # data still exists (see scipy.io.netcdf_file docs for
                # details). So, rather than attempting to hunt down
                # all such references (messy!), the hack of setting
                # the '_mm_buf' attribute to `None` allows the file to
                # be closed. We get away with this because we know
                # that we've copied all memory mapped data into memory
                # inside `Variable.__getitem__`.
                self._grp._mm_buf = None

            self._grp.close()

    def dump(
        self,
        display=True,
        _prefix=None,
        _level=0,
        _recursive=True,
        _structure=False,
    ):
        """A full description of the group.

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            `None` or `str`
                The description. If *display* is True then the
                description is printed and `None` is
                returned. Otherwise the description is returned as a
                string.

        """
        if _prefix is None:
            _prefix = ""

        out = "\n".join(
            (
                self.filename,
                super().dump(False, _prefix, _level, _recursive, _structure),
            )
        )
        if display:
            print(out)
            return

        return out

    def _open_zarr(self, dataset):
        """Return an open `zarr.Group`.

        :Parameters:

            dataset:
                The dataset. May be a string-valued path, a file-like
                object, or a directory-like object.

        :Returns:

            `zarr.Group`

        """
        import zarr

        nc = zarr.open(dataset, mode="r")
        self._backend = "zarr"
        self._lib = zarr
        return nc, nc.attrs

    def _open_netCDF4(self, filename):
        """Return an open `netCDF4.Dataset`.

        :Parameters:

            filename: `str`
                The file to open.

        :Returns:

            `netCDF4.Dataset`

        """
        import netCDF4

        nc = netCDF4.Dataset(filename, mode="r")
        self._backend = "netCDF4"
        self._lib = netCDF4
        return nc, {attr: nc.getncattr(attr) for attr in nc.ncattrs()}

    def _open_netcdf_file(self, filename):
        """Return an open `scipy.io.netcdf_file`.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            filename: `str`
                The file to open.

        :Returns:

            `scipy.io.netcdf_file`

        """
        from scipy.io import netcdf_file

        nc = netcdf_file(filename, mode="r", mmap=True)
        self._backend = "netcdf_file"
        self._lib = netcdf_file
        return nc, nc._attributes

    def _open_pyfive(self, dataset):
        """Open a dataset with `pyfive`

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            dataset:
                May be a `str` or `pathlib.Path` path, a file-like
                object (such as `io.BufferedReader` or the result of
                an `fsspec` file system open), a `pyfive.File` object,
                or a subclass of a `pyfive.File` object.

        :Returns:

            `p5netcdf.File`

        """
        import pyfive

        nc = pyfive.File(dataset, mode="r")
        self._backend = "pyfive"
        self._lib = pyfive
        return nc, nc.attrs

    def _open_h5py(self, dataset):
        """Open a dataset with `pyfive`

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            dataset:
                May be a `str` or `pathlib.Path` path, a file-like
                object (such as `io.BufferedReader` or the result of
                an `fsspec` file system open), a `pyfive.File` object,
                or a subclass of a `pyfive.File` object.

        :Returns:

            `p5netcdf.File`

        """
        import h5py

        nc = h5py.File(
            dataset,
            mode="r",
            rdcc_nbytes=16777216,
            rdcc_w0=0.75,
            rdcc_nslots=4133,
        )
        self._backend = "h5py"
        self._lib = h5py
        return nc, nc.attrs
