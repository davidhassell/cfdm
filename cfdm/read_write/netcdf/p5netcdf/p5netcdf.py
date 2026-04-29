from collections.abc import Mapping
from itertools import chain
from math import prod
from os.path import expanduser, expandvars

import numpy as np

from .utils import NetCDFError, _parse_attributes
from .utils_hdf5 import (
    h5py_open,
    hdf5_dimension_names,
    hdf5_parse_group_structure,
    pyfive_open,
)
from .utils_netcdf import (
    netCDF4_open,
    netCDF4_parse_group_structure,
    netcdf_file_close,
    netcdf_file_dtype,
    netcdf_file_open,
    netcdf_file_parse_group_structure,
)
from .utils_zarr import zarr_open, zarr_parse_group_structure

np.set_printoptions(floatmode="maxprec")

_iam = "p5netcdf"


class Mixin:
    """Mixin class for methods in common.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    # Quantum of indentation for `dump` and `structure`
    __indent = "    "

    @property
    def backend(self):
        """The name of the library that provides the backend.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `lib`

        :Returns:

            `str`
                The name of the library that provides the backend.

        """
        return self.root._backend

    @property
    def dataset(self):
        """The dataset definition, as passed to `File`.

        If an original string-like dataset definition contained tilde
        or environment variables, then these are expanded in the
        returned string.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `dataset_name`

        :Returns:

            string-like or file-like or directory-like or `pyfive.File`-like
                The dataset deinition.

        """
        return self.root._dataset

    @property
    def dataset_name(self):
        """The name of the dataset.

        This is an alias for `filename`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `dataset`, `filename`

        :Returns:

            `str`
                The name of the dataset. If the name is not known then
                an empty string is returned.

        """
        return self.root._dataset_name

    @property
    def filename(self):
        """The name of the dataset.

        This is an alias for `dataset_name`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `dataset_name`

        :Returns:

            `str`
                The name of the dataset. If the name is not known then
                an empty string is returned.

        """
        return self.dataset_name

    @property
    def is_local(self):
        """Whether the input dataset is on the local file system.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `protocol`

        :Returns:

            `bool` or `None`
                `True` if the input dataset is on the local file
                system, `False` otherwise.

                When the input dataset is provided as a file-like,
                directory-like, or (subclass of a) `pyfive.File`
                object, it is generally possible to glean whether or
                not the underlying dataset is on the local file
                system, but in those cases when it is not possible,
                `is_local` will return `None` (and `protocol` will
                raise an `AttributeError`).

        """
        return self.root._is_local

    @property
    def lib(self):
        """The library that provides the backend.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `backend`

        :Returns:

                The library that provides the backend.

        """
        return self.root._lib

    @property
    def parent(self):
        """The parent group.

        This is an alias for `group`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `group`

        :Returns:

            `Group` or `File` or `None`
                The parent group, or `None` if there is no parent
                group.

        """
        return self._parent

    @property
    def protocol(self):
        """TODO.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `is_local`

        """
        try:
            return self._protocol
        except AttributeError:
            raise AttributeError(
                "Could not determine the file system protocol"
            )

    @property
    def root(self):
        """The root group.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `parent`

        :Returns:

            `File`
                The root group.

        """
        root = getattr(self, "_root", None)
        if root is None:
            return self.parent.root

        return root

    @property
    def thread_safe(self):
        """Whether the backend allows thread-safe dataset access.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `bool`
                `True` if the backend allow thread-safe dataset
                access, otherwise `False`.

        """
        thread_safe = getattr(self.root, "_thread_safe", None)
        if thread_safe is None:
            thread_safe = self.backend not in ("netCDF4", "h5py")
            self.root._thread_safe = thread_safe

        return thread_safe

    def group(self):
        """The parent group.

        This is an alias for `parent`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `parent`

        :Returns:

            `Group` or `File` or `None`
                The parent group, or `None` if there is no parent
                group.

        """
        return self.parent

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
        return self.dump(
            display, _prefix, _level, _recursive=True, _structure=True
        )


class Dimension(Mixin):
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
            f"<{_iam}.{self.__class__.__name__}: "
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
    def path(self):
        """The full absolute path of the dimension.

        .. seealso:: `name`

        :Returns:

            `str`
                The absolute netCDF path, e.g. ``'/lat'`` or
                ``'/group/time'``.

        """
        path = getattr(self, "_path", None)
        if path is None:
            parent = self.parent
            if parent.is_root:
                path = f"/{self.name}"
            else:
                path = f"{parent.path}/{self.name}"

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
        _level=0,
        _recursive=True,
        _structure=False,
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
        indent = self._Mixin__indent
        i0 = indent * _level

        if _prefix is None:
            _prefix = f"{self.name}: "

        lines = [f"{i0}{_prefix}{self!r}"]

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


class Variable(Mixin):
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
        self._attrs = _parse_attributes(self, var_attrs)

    def __getitem__(self, indices):
        """Return a subspace of the data array defined by indices."""
        array = self._var[indices]
        if self.backend == "netcdf_file":
            # Need to copy the numpy array returned by
            # scipy.io.netcdf_file with mmap=True. See
            # `netcdf_file_close` for details.
            array = array.copy()

        return array

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
            f"<{_iam}.{self.__class__.__name__}: "
            f"{self.path}, shape={self.shape}, dimensions={dims}>"
        )

    @property
    def __orthogonal_indexing__(self):
        """Flag to indicate whether indexing is orthogonal.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        orthogonal_indexing = getattr(self, "_orthogonal_indexing", None)
        if orthogonal_indexing is None:
            orthogonal_indexing = getattr(
                self._var, "__orthogonal_indexing__", False
            )
            self._orthogonal_indexing = orthogonal_indexing

        return orthogonal_indexing

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
                case "pyfive" | "h5py":
                    chunks = self._var.chunks

                case "netCDF4":
                    chunks = self._var.chunking()
                    if chunks == "contiguous":
                        chunks = None
                    elif chunks is not None:
                        chunks = tuple(chunks)

                case "netcdf_file":
                    chunks = None

                case "zarr":
                    chunks = self._var.chunks
                    if not chunks:
                        chunks = None

            self._chunks = chunks

        return chunks

    @property
    def dimension_paths(self):
        """The variable dimensions.

        .. seealso:: `dimensions`, `get_dims`

        :Returns:

            `tuple`
                The dimension paths, in the order of the data array
                dimensions.

        """
        paths = getattr(self, "_dimension_paths", None)
        if paths is None:
            paths = tuple(dim.path for dim in self.get_dims())
            self._dimension_paths = paths

        return paths

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
            dimensions = tuple(dim.name for dim in self.get_dims())
            self._dimensions = dimensions

        return dimensions

    @property
    def dtype(self):
        """The numpy data type of the variable."""
        dtype = getattr(self, "_dtype", None)
        if dtype is None:
            match self.backend:
                case "pyfive" | "zarr" | "netCDF4" | "h5py":
                    dtype = self._var.dtype
                    # TODO zarr dtype is endianny
                case "netcdf_file":
                    dtype = netcdf_file_dtype(self)

            if dtype is not str and dtype != np.dtypes.StringDType():
                dtype = np.dtype(f"{dtype.kind}{dtype.itemsize}")

            self._dtype = dtype

        return dtype

    @property
    def maxshape(self):
        """The maximum dimension lengths of the variable.

        :Returns:

            `tuple`
                The maximum dimension lengths (e.g. ``(180,
                360)``). Unlimited dimensions are represented by
                `None` (e.g. ``(None, 96, 73)``)

        """
        maxshape = getattr(self, "_maxshape", None)
        if maxshape is None:
            maxshape = tuple(
                None if dim.isunlimited() else dim.size
                for dim in self.get_dims()
            )
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
            parent = self.parent
            if parent.is_root:
                path = f"/{self.name}"
            else:
                path = f"{parent.path}/{self.name}"

            self._path = path

        return path

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
    def shards(self):
        """The TODO dimension lengths of the variable.

        :Returns:

            `tuple` of `int`
                The dimension lengths (e.g. ``(12, 96, 73)``).

        """

        if hasattr(self, "_shards"):
            return self._shards

        match self.backend:
            case "zarr":
                shards = self._var.shards
            case _:
                shards = None

        self._shards = shards
        return shards

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

    def chunking(self):
        """The data chunk shape.

        .. seealso:: `chunks`

        :Returns:

            `tuple` or `None`
                The chunk shape, e.g. ``(5, 6, 7)``. If the data is
                contiguous then `None` is returned.

        """
        chunking = getattr(self, "_chunking", None)
        if chunking is None:
            chunks = self.chunks
            match self.backend:
                case "pyfive" | "zarr" | "h5py":
                    if chunks is None:
                        chunking = "contiguous"
                    else:
                        chunking = list(chunks)

                case "netCDF4":
                    if chunks is None:
                        if self.parent._grp.data_model == "NETCDF3_CLASSIC":
                            chunking = None
                        else:
                            chunking = "contiguous"
                    else:
                        chunking = list(chunks)

                case "netcdf_file":
                    chunking = None

            self._chunking = chunking

        return chunking

    def dump(
        self,
        display=True,
        _prefix=None,
        _level=0,
        _recursive=True,
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
        indent = self._Mixin__indent
        i0 = indent * _level

        if _prefix is None:
            _prefix = f"{self.name}: "

        lines = [f"{i0}{_prefix}{self!r}"]

        # Attributes
        if not _structure and self.attrs:
            i1 = indent * (_level + 1)
            i2 = indent * (_level + 2)

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
        # Note: This is not called in `__init__`, because for some
        #       backends (e.g. `zarr`) the `Dimension` objects are
        #       only available after the entire group and variable
        #       structure has been parsed.
        dims = getattr(self, "_dims", None)
        if dims is not None:
            return dims

        match self.backend:
            case "pyfive" | "h5py":
                dims = []
                for dim_name in hdf5_dimension_names(self):
                    # Walk up the tree to find where the dimension is
                    # defined
                    current_group = self.parent
                    found = False
                    while current_group is not None:
                        dim = current_group.dimensions.get(dim_name)
                        if dim is not None:
                            dims.append(dim)
                            found = True
                            break

                        current_group = current_group.parent

                    if not found:
                        raise NetCDFError(
                            f"Dimension {dim_name!r} not found in the "
                            "group hierarchy."
                        )

            case "netCDF4":
                root = self.root
                dims = [
                    root[ndim.group().path].dimensions[ndim.name]
                    for ndim in self._var.get_dims()
                ]

            case "netcdf_file":
                dimensions = self.root.dimensions
                dims = [dimensions[dim] for dim in self._var.dimensions]

            case "zarr":
                raise RuntimeError(
                    "_dims should have already been set to something other "
                    "than None by the zarr_parse_group_structure function"
                )

        dims = tuple(dims)
        self._dims = dims
        return dims


class Group(Mixin, Mapping):
    """A netCDF group.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    __hash__ = None

    # Store classes for creating dimensions, variables and sub-groups
    #
    # Note: __Group will be re-set to `Group` after the `Group` class
    #       has finished defining itself
    __Dimension = Dimension
    __Variable = Variable
    __Group = None

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
        self._grp = grp
        self._is_root = parent is None

        self._attrs = _parse_attributes(self, grp_attrs)

        self._dimensions = {}
        self._variables = {}
        self._groups = {}
        self._parse_group_structure()

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

        parent = self.parent
        if parent is None:
            # Root group
            path = ""
        else:
            path = f"{self.path}, "

        return (
            f"<{_iam}.{self.__class__.__name__}: "
            f"{path}"
            f"{len(self.dimensions)} dimension{pd}, "
            f"{len(self.variables)} variable{pv}, "
            f"{len(self.groups)} group{pg}>"
        )

    def __str__(self):
        """Called by the `str` built-in function."""
        return self.dump(display=False, _recursive=False, _structure=True)

    def _create_dimension(self, name, size, isunlimited):
        """Create a new dimension in this group.

        :Parameters:

             Parameters *name*, *size*, and *isunlimited* are
             identical those parameter for `Dimension.__init__`.

        :Returns:

            `Dimension`
                The new dimension.

        """
        dimension = self.__Dimension(name, size, isunlimited, self)
        self._dimensions[name] = dimension
        return dimension

    def _create_group(self, name, grp, grp_attrs):
        """Create a new sub-group in this group.

        :Parameters:

             Parameters *name*, *grp*, and *grp_attrs* are identical
             those parameter for `Group.__init__`.

        :Returns:

            `Group`
                The new group.

        """
        group = self.__Group(name, self, self.root, grp, grp_attrs)
        self._groups[name] = group
        return group

    def _create_variable(self, name, var, var_attrs):
        """Create a new variable in this group.

        :Parameters:

             Parameters *name*, *var*, and *var_attrs* are identical
             those parameter for `Variable.__init__`.

        :Returns:

            `Variable`
                The new variable.

        """
        variable = self.__Variable(name, self, var, var_attrs)
        self._variables[name] = variable
        return variable

    def _populate_all(self):
        """Populate the 'all_*' dictionaries.

        Populates the dictionaries of all dimensions, variables, and
        groups.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        root = self.root

        if self.is_root:
            # Initialise the 'all_*' dictionaries
            root._all_dimensions = {}
            root._all_variables = {}
            root._all_groups = {}

        for dimension in self._dimensions.values():
            root._all_dimensions[dimension.path] = dimension

        for variable in self._variables.values():
            root._all_variables[variable.path] = variable

        root._all_groups[self.path] = self

        # Recursively populate from sub-groups
        for group in self.groups.values():
            group._populate_all()

    def _parse_group_structure(self):
        """Parse the group structure.

        Parses variables, dimensions, and subgroups, recursively.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            root: `File`
                The root group.

        :Returns:

            `None`

        """
        match self.backend:
            case "pyfive" | "h5py":
                hdf5_parse_group_structure(self)

            case "netCDF4":
                netCDF4_parse_group_structure(self)

            case "netcdf_file":
                netcdf_file_parse_group_structure(self)

            case "zarr":
                zarr_parse_group_structure(self)

    @property
    def attrs(self):
        """The group attributes.

        :Returns:

            `dict`
                The attribute values, keyed by their names.

        """
        return self._attrs

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
    def is_root(self):
        """Whether or not this is the root group.

        :Returns:

            `bool`
                `True` if this is the root group, otherwise `False`.

        """
        return self._is_root

    @property
    def name(self):
        """The name of the group in its parent group.

        :Returns:

            `str`
                The relative netCDF name (e.g. ``'subgroup'``).

        """
        return self._name

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
                case "pyfive" | "zarr" | "h5py":
                    path = self._grp.name
                case "netCDF4":
                    path = self._grp.path
                case "netcdf_file":
                    path = "/"

            self._path = path

        return path

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
        indent = self._Mixin__indent
        i0 = indent * _level
        i1 = indent * (_level + 1)
        i2 = indent * (_level + 2)

        if _prefix is None:
            _prefix = f"{self.name}: "

        lines = [f"{i0}{_prefix}{self!r}"]

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
                f"{dim.dump(display=False, _level=_level + 2)}"
                for name, dim in self.dimensions.items()
            )
        # Variables
        if self.variables:
            lines.append(f"{i1}Variables:")
            lines.extend(
                f"{var.dump(display=False, _level=_level + 2, _structure=_structure)}"
                for name, var in self.variables.items()
            )

        # Groups
        if self.groups:
            lines.append(f"{i1}Groups:")
            if _recursive:
                lines.extend(
                    f"{group.dump(display=False, _level=_level + 2, _recursive=True, _structure=_structure)}"
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

    def is_sub_group(self, other):
        """Return True if the group is a subgroup of, or is, 'other'.

        If `True`, then *other* is an ancestor of this group.

        'other' should be another Group (or File) object.

        """
        group = self
        while group is not None:
            if group is other:
                return True

            try:
                group = group.parent
            except AttributeError:
                return False

        return False

    def is_ancestor_group(self, other):
        """Return True if the group is an ancestor of, or is, 'other'.

        If `True`, then *other* is a sub-group of this group.

        'other' should be another Group (or File) object.

        """
        while other is not None:
            if self is other:
                return True

            try:
                other = other.parent
            except AttributeError:
                return False

        return False


# Set __Group to `Group`, now that `Group` has been defined.
Group._Group__Group = Group


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

    `netcdf` is "structure- and attribute-eager", meaning that during
    `File` instantiation, the entire netCDF-4 group, variable, and
    dimension structure is parsed; along with all group and variable
    attributes. Variable data access is always via access to the
    underlying (subclass of a) `pyfive.Dataset` object. Some
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

    def __init__(
        self,
        dataset,
        mode="r",
        backend=None,
        metadata_strategy="minimal",
        pyfive_options=None,
        h5py_options=None,
        zarr_dimension_search="closest_ancestor",
        verbose=0,
    ):
        """**Initialisation**

        :Parameters:

            dataset:
                The definition of the netCDF dataset to be read.

                May be one of:

                * string-like (such as `str` or `pathlib.Path`)

                * file-like (such as `io.BufferedReader` or the result
                             of an `fsspec` file system open)

                * directory-like (such as `fsspec.mapping.FSMap`)

                * `pyfive.File`-like (`pyfive.File` or a subclass of
                                      `pyfive.File`)

                Note that::

                   >>> nc = p5netcdf.File('file.nc', backend='pyfive')

                is identical to::

                   >>> py5 = pyfive.File('file.nc')
                   >>> nc = p5netcdf.File(py5)

            mode: `str`, optional
                The access mode used when using `pyfive.File` to open
                the *dataset*. The only allowed value is ``'r'``
                (read-only), and this is the default.

            backend: `None` or (sequence of) `str`, optional
                Which library or libraries to use for reading the
                dataset. An attempt to open the dataset is made by the
                given backends in the order given, stopping after the
                first successful read.

                The available backends are:

                =================  ======================
                Backend            Library
                =================  ======================
                ``'pyfive'``       `pyfive`
                ``'zarr'``         `zarr`
                ``'netCDF4'``      `netCDF4`
                ``'netcdf_file'``  `scipy.io.netcdf_file`
                ``'h5py'``         `h5py`
                =================  ======================

                By default *backend* is `None`, which is equivalent to
                providing the ordered sequence of backends:

                ``('pyfive', 'zarr', 'netCDF4', 'netcdf_file', 'h5py')``

            metadata_strategy: `str`, optional
                The strategy used for retrieving metadata from the
                dataset, and caching it, during the initial parsing of
                the *dataset*. Must be one of:

                * ``'minimal'``

                  This is the default. Only the minimum amount of
                  metadata required to parse the dataset is retrieved
                  from the dataset and cached. This includes, for
                  instance, all of the variable and group attributes,
                  but may (depending on the backend library) exclude
                  the variable shapes.


                * ``'maximal'``

                  All relevant metadata is retrieved from the dataset
                  and cached. The dataset then never needs to
                  revisited except to access the variable data arrays.

                Maximal metadata retrieval can also be applied to an
                existing `File` instance with its
                `cache_maximal_metadata` method.

            pyfive_options: `dict` or `None`, optional
                Keyword arguments that are passed to `pyfive.File` to
                be used when opening a netCDF-4 *dataset*. Setting to
                `None` (the default) is equivalent to providing an
                empty dictionary. Ignored if *dataset* is already a
                (subclass of a) `pyfive.File` object.

            h5py_options: `dict` or `None`, optional
                Keyword arguments that are passed to `h5py.File` to
                be used when opening a netCDF-4 *dataset*. Setting to
                `None` (the default) is equivalent to providing an
                empty dictionary.

            zarr_dimension_search: `str`, optional
                How to interpret a Zarr or Kerchunk dimension name
                that contains no group-separator characters, such as
                ``dim`` (as opposed to ``group/dim``, ``/group/dim``,
                ``../dim``, etc.).

                For a Zarr or Kerchunk dataset, setting this parameter
                may be necessary for the correct interpretation of the
                dataset in the event that its dimensions are named
                inconsistently with CF conventions (section 2.7
                Groups).

                The *zarr_dimension_search* parameter must be one of:

                * ``'closest_ancestor'``

                  This is the default and is the behaviour defined by
                  the CF conventions (section 2.7 Groups).

                  Assume that the sub-group dimension is the same as
                  the dimension with the same name and size in an
                  ancestor group, if one exists. If multiple such
                  dimensions exist, then the correspondence is with
                  the dimension in the ancestor group that is
                  **closest** to the sub-group (i.e. that is furthest
                  away from the root group).

                * ``'furthest_ancestor'``

                  This behaviour is different to that defined by the
                  CF conventions (section 2.7 Groups).

                  Assume that the sub-group dimension is the same as
                  the one with the same name and size in an ancestor
                  group, if one exists. If multiple such dimensions
                  exist, then the correspondence is with the dimension
                  in the ancestor group that is **furthest away** from
                  the sub-group (i.e. that is closest to the root
                  group).

                * ``'local'``

                  This behaviour is different to that defined by the
                  CF conventions (section 2.7 Groups).

                  Assume that the sub-group dimension is different to
                  any with the same name and size in all ancestor
                  groups.

            verbose: `int`, optional
                 Set the verbosity. If *verbose* is less than ``1``
                 then there is no verbose output; more output is
                 produced for progressively larger values of
                 *verbose*. Values of ``3`` and higher produce the
                 same maximally verbose output.

        """
        import pyfive

        if mode != "r":
            raise ValueError("mode must be 'r'. Got: mode={mode!r}")

        _open_options = {}
        if h5py_options:
            _open_options["h5py"] = h5py_options

        if pyfive_options:
            _open_options["pyfive"] = pyfive_options

        self._zarr_dimension_search = zarr_dimension_search

        self._open_log = []
        dataset_name = ""
        protocol = -1

        if isinstance(dataset, pyfive.File):
            # --------------------------------------------------------
            # Input (subclass of) `pyfive.File`
            # --------------------------------------------------------
            nc = dataset
            attrs = dataset.attrs
            self._backend = "pyfive"
            self._lib = pyfive

            # The opened dataset is owned externally
            self._owns_nc = False

            # Attempt to set the dataset name and file system protocol
            try:
                # fsspec file-like
                dataset_name = dataset._fh.path
            except AttributeError:
                try:
                    # BinaryIO
                    dataset_name = dataset._fh.name
                except AttributeError:
                    pass
                else:
                    # BinaryIO
                    protocol = "file"
            else:
                try:
                    # fsspec file-like
                    protocol = dataset._fh.fs.protocol
                except AttributeError:
                    pass

        else:
            # --------------------------------------------------------
            # Input string-like, file-like, or directory-like
            # --------------------------------------------------------
            # Attempt to set the dataset name and protocol
            try:
                # string-like: Expand tilde and environment variables
                dataset = expanduser(expandvars(dataset))
            except TypeError:
                try:
                    # fsspec file-like
                    dataset_name = dataset.path
                except AttributeError:
                    try:
                        # BinaryIO
                        dataset_name = dataset.name
                    except AttributeError:
                        try:
                            # fsspec kerchunk dictionary-like
                            dataset_name = dataset.fs.storage_options.get("fo")
                        except AttributeError:
                            # Can't find dataset name
                            pass
                    else:
                        # BinaryIO
                        protocol = "file"
                else:
                    try:
                        # fsspec file-like
                        protocol = dataset.fs.protocol
                    except AttributeError:
                        pass
            else:
                # string-like
                dataset_name = dataset

                from urllib.parse import urlparse

                protocol = urlparse(dataset_name).scheme

            self._dataset = dataset

            # Map backend names to dataset-open functions
            open_functions = {
                "pyfive": pyfive_open,
                "zarr": zarr_open,
                "netCDF4": netCDF4_open,
                "netcdf_file": netcdf_file_open,
                "h5py": h5py_open,
            }
            if backend is not None:
                # Select backends
                if isinstance(backend, str):
                    backend = (backend,)

                open_functions = {b: open_functions[b] for b in backend}

            nc = None
            for backend, func in open_functions.items():
                options = _open_options.get(backend, {})
                try:
                    nc, attrs, lib = func(dataset, options)
                except Exception as error:
                    self._open_log.append(
                        f"{backend}: {error.__class__.__name__}: {error}"
                    )
                else:
                    self._open_log.append(f"{backend}: Successfully opened")
                    break

            if nc is None:
                try:
                    # Rewind file-like
                    dataset.seek(0)
                except Exception:
                    pass

                raise NetCDFError(
                    f"Can't interpret {dataset} as a netCDF dataset "
                    f"with any of the backends {tuple(open_functions)}:\n\n"
                    f"{self.open_log(display=False)}"
                )

            self._backend = backend
            self._lib = lib

            # The opened dataset is owned internally
            self._owns_nc = True

        self._dataset = dataset
        self._dataset_name = dataset_name

        # Set the file system protocol, but only if we've found out
        # what it is.
        if protocol == -1:
            # -1 is a non-string and non-None code for an unknown file
            # system protocol
            is_local = None
        else:
            if isinstance(protocol, tuple):
                protocol = protocol[0]

            if not protocol:
                protocol = "file"

            self._protocol = protocol
            is_local = protocol in ("file", "local", None)

        self._is_local = is_local

        # ------------------------------------------------------------
        # Initialise the group structure
        # ------------------------------------------------------------
        super().__init__(
            name="", parent=None, root=self, grp=nc, grp_attrs=attrs
        )

        if metadata_strategy == "maximal":
            self.cache_maximal_metadata()
        elif metadata_strategy != "minimal":
            raise ValueError(
                "Invalid value for metadata_strategy. "
                f"Got {metadata_strategy!r}, expected one of "
                "'minimal', 'maximal'"
            )

        # Verbose output
        if verbose >= 1:
            self.open_log()

        if verbose >= 3:
            print()
            self.dump()
        elif verbose >= 2:
            print()
            self.structure()

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context and close the file."""
        self.close()

    @property
    def all_dimensions(self):
        """A dictionary of all dimensions.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `dict`
                The dimensions are keyed by their absolute paths.

        **Example**

        >>> n.all_dimensions
        {'/bounds2': <p5netcdf.Dimension: /bounds2, size=2>,
         '/forecast/lon': <p5netcdf.Dimension: /forecast/lon, size=8, unlimited>,
         '/forecast/model/lat': <p5netcdf.Dimension: /forecast/model/lat, size=5>}

        """
        if getattr(self, "_all_dimensions", None) is None:
            self._populate_all()

        return self._all_dimensions

    @property
    def all_groups(self):
        """A dictionary of all groups.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `dict`
                The groups are keyed by their absolute paths.

        **Example**

        >>> n.all_groups
        {'/': <p5netcdf.File: 1 dimension, 1 variable, 1 group>,
         '/forecast': <p5netcdf.Group: /forecast, 1 dimension, 2 variables, 1 group>,
         '/forecast/model': <p5netcdf.Group: /forecast/model, 1 dimension, 3 variables, 0 groups>}

        """
        if getattr(self, "_all_groups", None) is None:
            self._populate_all()

        return self._all_groups

    @property
    def all_variables(self):
        """A dictionary of all variables.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `dict`
                The variables are keyed by their absolute paths.

        **Example**

        >>> n.all_variables
        {'/time': <p5netcdf.Variable: /time, shape=(), dimensions=()>,
         '/forecast/lon_bnds': <p5netcdf.Variable: /forecast/lon_bnds, shape=(8, 2), dimensions=(/forecast/lon, /bounds2)>,
         '/forecast/lon': <p5netcdf.Variable: /forecast/lon, shape=(8,), dimensions=(/forecast/lon,)>,
         '/forecast/model/lat_bnds': <p5netcdf.Variable: /forecast/model/lat_bnds, shape=(5, 2), dimensions=(/forecast/model/lat, /bounds2)>,
         '/forecast/model/lat': <p5netcdf.Variable: /forecast/model/lat, shape=(5,), dimensions=(/forecast/model/lat,)>,
         '/forecast/model/q': <p5netcdf.Variable: /forecast/model/q, shape=(5, 8), dimensions=(/forecast/model/lat, /forecast/lon)>}

        """
        if getattr(self, "_all_variables", None) is None:
            self._populate_all()

        return self._all_variables

    def close(self):
        """Close the dataset.

        Closes the underlying netCDF dataset, but not if it is not
        owned by this `File` instance.

        :Returns:

            `None`

        """
        if not self._owns_nc:
            return

        if self.backend == "netcdf_file":
            netcdf_file_close(self)
        else:
            try:
                self._grp.close()
            except AttributeError:
                pass

    def dump(
        self,
        display=True,
        _prefix=None,
        _level=0,
        _recursive=True,
        _structure=False,
    ):
        """A full description of the dataset.

        .. versionadded:: (cfdm) NEXTVERSION

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

    def open_log(self, display=True):
        """The dataset-open log.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            display: `bool`, optional
                If False then return the log as a string. By default
                the log is printed.

        :Returns:

            `None` or `str`
                The open log. If *display* is True then the log is
                printed and `None` is returned. Otherwise the log is
                returned as a string.

        """
        log = "\n\n".join(self._open_log)
        if not display:
            return log

        print(log)

    def cache_maximal_metadata(self):
        """TODO.

        Eagerly execute all methods that still might need to visit the
        dataset to get metadata. These methods cache their results.

        """
        for group in self.all_groups.values():
            group.path

        for variable in self.all_variables.values():
            variable.__orthogonal_indexing__
            variable.dtype
            variable.shape
            variable.shards
            variable.get_dims()
            variable.chunking()
