from collections.abc import Mapping
from itertools import chain
from math import prod
from os.path import expanduser, expandvars

import numpy as np

from .utils import (
    NetCDFError,
    cdl_format,
    get_dimensions_from_source,
    get_library,
    h5py_open,
    hdf5_dimension_names,
    hdf5_parse_group_structure,
    netCDF4_open,
    netCDF4_parse_group_structure,
    netcdf_file_close,
    netcdf_file_dtype,
    netcdf_file_open,
    netcdf_file_parse_group_structure,
    parse_attributes,
    ppfive_open,
    pyfive_open,
    xarray_open,
    xarray_parse_group_structure,
    zarr_open,
    zarr_parse_group_structure,
)

np.set_printoptions(floatmode="maxprec")

_iam = "p5netcdf"


class Mixin:
    """Mixin class for common methods.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    # Quantum of indentation for the `dump` and `structure` methods
    __indent = "    "

    @property
    def backend(self):
        """The name of the library that provides the backend.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `library`

        :Returns:

            `str`
                The name of the library that provides the backend.

        """
        return self.root._backend

    @property
    def dataset(self):
        """The dataset definition, as originally passed to `File`.

        If an original string-like dataset definition contained tilde
        or environment variables, then these are expanded in the
        returned string.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `dataset_name`

        :Returns:

                The dataset definition. One of string-like, file-like,
                directory-like, `pyfive.File`-like, or
                xarray.Dataset`-like, or `xarray.DataTree`-like.

        """
        return self.root._dataset

    @property
    def dataset_name(self):
        """The name of the dataset.

        This is an alias for `filename`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `filename`, `dataset`

        :Returns:

            `str`
                The name of the dataset. If the dataset name is not
                known then an empty string is returned.

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
                The name of the dataset. If the dataset name is not
                known then an empty string is returned.

        """
        return self.dataset_name

    @property
    def is_local(self):
        """Whether the dataset is on the local file system.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `protocol`

        :Returns:

            `bool` or `None`
                `True` if the dataset is on the local file system
                (i.e. `protocol` returns ``'file'``), and `False`
                otherwise.

                It is usually possible to ascertain the file system
                protocol from the input dataset, but in those cases
                when it is not possible, `is_local` will return `None`
                and `protocol` will raise an `AttributeError`.

        """
        return self.root._is_local

    @property
    def library(self):
        """The backend library.

        The backend library is the interface to the dataset, and
        provides access to the dataset contents.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `backend`

        :Returns:

                The library that provides the backend.

        """
        return self.root._library

    @property
    def parent(self):
        """The parent group.

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
        """The file system protocol for the dataset.

        It is usually possible to ascertain the file system protocol
        from the input dataset, but in those cases when it is not
        possible, `protocol` will raise an `AttributeError` and
        `is_local` will return `None`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `is_local`

        :Returns:

            `str` or `None`
                The file system protocol. The local file system is
                indicated by ``'file'``, ``'local'``, or `None`.

        """
        try:
            return self.root._protocol
        except AttributeError:
            raise AttributeError("Can't determine the file system protocol")

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

    def structure(
        self,
        display=True,
        depth=None,
        _prefix=None,
        _level=0,
    ):
        """A purely structural description.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `dump`

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
            display=display,
            data=False,
            depth=depth,
            _prefix=_prefix,
            _level=_level,
            _structure=True,
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

        .. versionadded:: (cfdm) NEXTVERSION

        """
        return self.size

    def __repr__(self):
        """Called by the `repr` built-in function.

        x.__repr__() <==> repr(x)

        .. versionadded:: (cfdm) NEXTVERSION

        """
        unlimited = ", unlimited" if self.isunlimited() else ""
        return (
            f"<{_iam}.{self.__class__.__name__}: "
            f"{self.path}, size={self.size}{unlimited}>"
        )

    @property
    def name(self):
        """The name of the dimension in its parent group.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `path`

        :Returns:

            `str`
                The relative name (e.g. ``'time'``).

        """
        return self._name

    @property
    def path(self):
        """The absolute path of the dimension.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `name`

        :Returns:

            `str`
                The absolute path of the dimension, e.g. ``'/lat'`` or
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

        .. versionadded:: (cfdm) NEXTVERSION

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
        _structure=False,
    ):
        """A full description of the dimension.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `structure`

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
        if not display:
            return out

        print(out)

    def group(self):
        """The parent group that defines this dimension.

        This is an alias for `parent`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `parent`

        :Returns:

            `Group` or `File`
                The parent group.

        """
        return self.parent

    def isunlimited(self):
        """Whether the dimension is unlimited.

        .. versionadded:: (cfdm) NEXTVERSION

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

    def __init__(self, name, parent, var, var_attrs, shape=None):
        """**Initialisation**

        :Parameters:

            name: `str`
                The name of the variable in its parent group.

            parent: `Group` or `File`
                The parent group in which this variable is defined.

            var:
                The underlying dataset variable object provided by the
                backend library.

            var_attrs: `dict`
                The raw attributes of *var*.

            shape: `None` or `tuple` of `int`, optional
                The shape of the variable's data array. If `None` (the
                default) then the shape is retrieved later from *var*,
                if required.

        """
        self._name = name
        self._var = var
        self._parent = parent
        self._var_attrs = var_attrs
        self._attrs = parse_attributes(self, var_attrs)
        if shape is not None:
            self._shape = shape

    def __getitem__(self, indices):
        """Return a subspace of the data array defined by indices.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        array = self._var[indices]
        match self.backend:
            case "netcdf_file":
                # Need to copy the numpy array returned by
                # scipy.io.netcdf_file with mmap=True. See
                # `netcdf_file_close` for details.
                array = array.copy()
            case "xarray":
                array = array.values

        return array

    def __len__(self):
        """The size of leading data array dimension.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        shape = self.shape
        if shape:
            return shape[0]

        raise TypeError("len() of unsized object (scalar variable)")

    def __repr__(self):
        """Called by the `repr` built-in function.

        x.__repr__() <==> repr(x)

        .. versionadded:: (cfdm) NEXTVERSION

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

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `dict`
                The attribute values, keyed by their names.

        """
        return self._attrs

    @property
    def chunks(self):
        """The data chunk shape.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `shards`, `chunking`

        :Returns:

            `None` or `tuple` of `int`
                The chunk shape, e.g. ``(5, 6, 7)``. If the data is
                contiguous then `None` is returned.

        """
        chunks = getattr(self, "_chunks", None)
        if chunks is None:
            match self.backend:
                case "pyfive" | "h5py" | "ppfive":
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

                case "xarray":
                    chunks = self._var.encoding.get("chunksizes")

                case _:
                    raise NotImplementedError(
                        "Need to implement 'chunks' for backend "
                        f"{self.backend!r}"
                    )

            self._chunks = chunks

        return chunks

    @property
    def dimension_paths(self):
        """The variable dimensions.

        .. versionadded:: (cfdm) NEXTVERSION

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

        .. versionadded:: (cfdm) NEXTVERSION

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
        """The numpy data type of the variable.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        dtype = getattr(self, "_dtype", None)
        if dtype is None:
            match self.backend:
                case (
                    "pyfive"
                    | "zarr"
                    | "netCDF4"
                    | "h5py"
                    | "xarray"
                    | "ppfive"
                ):
                    dtype = self._var.dtype

                case "netcdf_file":
                    dtype = netcdf_file_dtype(self)

                case _:
                    raise NotImplementedError(
                        "Need to implement 'dtype' for backend "
                        f"{self.backend!r}"
                    )

            if dtype is not str and dtype != np.dtypes.StringDType():
                dtype = np.dtype(f"{dtype.kind}{dtype.itemsize}")

            self._dtype = dtype

        return dtype

    @property
    def maxshape(self):
        """The maximum dimension lengths of the variable.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `shape`

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

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `path`

        :Returns:

            `str`
                The relative  name (e.g. ``'latitude'``).

        """
        return self._name

    @property
    def ndim(self):
        """The number of dimensions for the variable.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `size`, `shape`

        :Returns:

            `int`
                The number of dimensions.

        """
        ndim = getattr(self, "_ndim", None)
        if ndim is None:
            ndim = len(self.shape)
            self._ndim = ndim

        return ndim

    @property
    def path(self):
        """The absolute path of the variable.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `name`

        :Returns:

            `str`
                The absolute path of the variable, e.g. ``'/time'`` or
                ``'/group/latitude'``.

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

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `ndim`, `size`, `maxshape`

        :Returns:

            `tuple` of `int`
                The dimension lengths, e.g. ``(12, 96, 73)``.

        """
        shape = getattr(self, "_shape", None)
        if shape is None:
            shape = self._var.shape
            self._shape = shape

        return shape

    @property
    def shards(self):
        """The data shard shape.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `chunks`, `chunking`

        :Returns:

            `None` or `tuple` of `int`
                The shard shape (e.g. ``(12, 96, 73)``) for a sharded
                Zarr variable, or `None` for a non-sharded Zarr
                variable or a variable in a non-Zarr dataset.

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

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `ndim`, `shape`

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

        This method has the same API as `netCDF4.Variable.chunking`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `chunks`, `shards`

        :Returns:

            `list` or ``'contiguous'`` or `None`
                The chunk shape, e.g. ``[5, 6, 7]``. For contiguous
                data in a dataset that does support chunking,
                ``'contiguous'`` is returned. If the dataset doesn't
                support chunking (such as netCDF-3) then `None` is
                returned.

        """
        chunking = getattr(self, "_chunking", None)
        if chunking is None:
            chunks = self.chunks
            match self.backend:
                case "pyfive" | "zarr" | "xarray" | "h5py" | "ppfive":
                    if chunks is None:
                        chunking = "contiguous"
                    else:
                        chunking = list(chunks)

                case "netCDF4":
                    if chunks is None:
                        if self.root._grp.data_model.startswith("NETCDF3"):
                            chunking = None
                        else:
                            chunking = "contiguous"
                    else:
                        chunking = list(chunks)

                case "netcdf_file":
                    chunking = None

                case _:
                    raise NotImplementedError(
                        "Need to implement 'chunking' for backend "
                        f"{self.backend!r}"
                    )

            self._chunking = chunking

        return chunking

    def dump(
        self,
        display=True,
        data=False,
        _prefix=None,
        _level=0,
        _structure=False,
    ):
        """A full description of the variable.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `structure`

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

            data: `bool`, optional
                If True then include a summary of the variable's data
                array. If False (the default) then don't include a
                data summary.

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

        if _structure:
            data = False

        if data:
            # Set numpy linewidth
            linewidth0 = np.get_printoptions()["linewidth"]
            np.set_printoptions(linewidth=len(lines[0]))

        # Attributes
        if not _structure and self.attrs:
            lines.append(f"{i1}Attributes:")
            lines.extend(
                f"{i2}{name}: {value!r}" for name, value in self.attrs.items()
            )

        if data:
            lines.append(f"{i1}Data {self.dtype.name}:")
            lines.append(
                f"{i2}{np.array2string(self[...], separator=', ', prefix=i2)}"
            )
            # Reset numpy linewidth
            np.set_printoptions(linewidth=linewidth0)

        out = "\n".join(lines)
        if not display:
            return out

        print(out)

    def get_dims(self):
        """Return the dimensions of the variable.

        .. versionadded:: (cfdm) NEXTVERSION

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
        if dims is None:
            match self.backend:
                case "pyfive" | "h5py" | "ppfive":
                    dims = get_dimensions_from_source(
                        self, hdf5_dimension_names(self)
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

                case "xarray":
                    dims = get_dimensions_from_source(self, self._var.dims)

                case "zarr":
                    raise RuntimeError(
                        "self._dims should have already been set to  "
                        "something other than None by the "
                        "zarr_parse_group_structure function"
                    )

                case _:
                    raise NotImplementedError(
                        "Need to implement 'get_dims' for backend "
                        f"{self.backend!r}"
                    )

            dims = tuple(dims)
            self._dims = dims

        return dims

    def getncattr(self, name):
        """Get an attribute value by name.

        This method has the same API as `netCDF4.Variable.getncattr`.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            name: `str`
                The attribute name.

        :Returns:

                The attribute value.

        """
        try:
            return self.attrs[name]
        except KeyError:
            raise AttributeError(
                f"{self.__class__.__name__!r} object has no attribute "
                f"{name!r}"
            )

    def group(self):
        """The parent group that defines this variable.

        This is an alias for `parent`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `parent`

        :Returns:

            `Group` or `File`
                The parent group.

        """
        return self.parent

    def ncattrs(self):
        """Return a list of attribute names.

        This method has the same API as `netCDF4.Variable.ncattrs`.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `list` of `str`
                The attribute names.

        """
        return list(self.attrs)


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
                The underlying group object provided by the
                backend library.

            grp_attrs: `dict`
                The raw attributes of *grp*.

        """
        self._name = name
        self._parent = parent
        self._root = root
        self._grp = grp
        self._is_root = parent is None

        self._attrs = parse_attributes(self, grp_attrs)

        self._dimensions = {}
        self._variables = {}
        self._groups = {}
        self._parse_group_structure()

    def __getitem__(self, path):
        """Get a variable or group from its path.

        Absolute and relative nested paths are allowed, which may
        include ``.`` (current group) and ``..`` (parent group)
        elements. The empty path ``''`` is equivalent to ``'.'``. A
        trailing ``/`` in non-empty path is ignored.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        if path == "":
            return self

        # Still here? Determine the starting point
        current = self
        if path.startswith("/"):
            current = self.root
        else:
            current = self

        # Split the path into parts (ignoring empty strings from
        # double-slashes)
        segments = [s for s in path.split("/") if s]

        # Handle a path of "/", "//", "///", etc.
        if not segments:
            return current

        # Still here? Then loop through the segments
        for i, part in enumerate(segments):
            if part == "..":
                if current.is_root:
                    if path.startswith("/"):
                        start = ""
                    else:
                        start = f" from group {self.path}"

                    raise KeyError(
                        f"Invalid path {path!r}{start}: Attempted to "
                        "navigate above the root group."
                    )

                # Move up one group
                current = current.parent
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

                if path.startswith("/"):
                    start = ""
                else:
                    start = f" from group {self.path}"

                raise KeyError(
                    f"Invalid path {path!r}{start}: "
                    f"{current.variables[part].path} is a variable "
                    "and cannot have children"
                )
            else:
                if path.startswith("/"):
                    start = ""
                else:
                    start = f" from group {self.path}"

                raise KeyError(
                    f"Invalid path {path!r}{start}: Path element {part!r} "
                    f"not found in group {current.path}"
                )

        return current

    def __iter__(self):
        """The variables and sub-groups.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        return chain(self.groups, self.variables)

    def __len__(self):
        """The number of variables and sub-groups.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        return len(self.variables) + len(self.groups)

    def __repr__(self):
        """Called by the `repr` built-in function.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        pd = "" if len(self.dimensions) == 1 else "s"
        pv = "" if len(self.variables) == 1 else "s"
        pg = "" if len(self.groups) == 1 else "s"

        if self.is_root:
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
        """Called by the `str` built-in function.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        return self.dump(display=False, data=False, depth=0, _structure=True)

    def _create_dimension(self, name, size, isunlimited):
        """Create a new dimension in this group.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

             Parameters *name*, *size*, and *isunlimited* are
             identical those parameters for `Dimension.__init__`.

        :Returns:

            `Dimension`
                The new dimension.

        """
        dimension = self.__Dimension(name, size, isunlimited, self)
        self._dimensions[name] = dimension
        return dimension

    def _create_group(self, name, grp, grp_attrs):
        """Create a new sub-group in this group.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

             Parameters *name*, *grp*, and *grp_attrs* are identical
             those parameters for `Group.__init__`.

        :Returns:

            `Group`
                The new group.

        """
        group = self.__Group(name, self, self.root, grp, grp_attrs)
        self._groups[name] = group
        return group

    def _create_variable(self, name, var, var_attrs, shape=None):
        """Create a new variable in this group.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

             Parameters *name*, *var*, *var_attrs*, and *shape* are
             identical those parameters for `Variable.__init__`.

        :Returns:

            `Variable`
                The new variable.

        """
        variable = self.__Variable(name, self, var, var_attrs, shape)
        self._variables[name] = variable
        return variable

    def _populate_all(self):
        """Populate the 'all_*' dictionaries.

        Populates the root dictionaries of all dimensions, variables,
        and groups.

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
            case "pyfive" | "h5py" | "ppfive":
                hdf5_parse_group_structure(self)

            case "netCDF4":
                netCDF4_parse_group_structure(self)

            case "netcdf_file":
                netcdf_file_parse_group_structure(self)

            case "zarr":
                zarr_parse_group_structure(self)

            case "xarray":
                xarray_parse_group_structure(self)

            case _:
                raise NotImplementedError(
                    "Need a '*_parse group structure' function for backend "
                    f"{self.backend!r}"
                )

    @property
    def attrs(self):
        """The group attributes.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `dict`
                The attribute values, keyed by their names.

        """
        return self._attrs

    @property
    def dimensions(self):
        """The dimensions defined in this group.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `dict`
                The `Dimension` objects, keyed by their names.

        """
        return self._dimensions

    @property
    def groups(self):
        """The sub-groups defined in this group.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `dict`
                The `Group` objects, keyed by their names.

        """
        return self._groups

    @property
    def is_root(self):
        """Whether or not this is the root group.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `bool`
                `True` if this is the root group, otherwise `False`.

        """
        return self._is_root

    @property
    def name(self):
        """The name of the group in its parent group.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `str`
                The relative name (e.g. ``'subgroup'``).

        """
        return self._name

    @property
    def path(self):
        """The absolute path of the group.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `str`
                The absolute path of the group, e.g. ``'/'``,
                ``'/model'``, or ``'/group/forecast'``.

        """
        path = getattr(self, "_path", None)
        if path is None:
            if self.is_root:
                path = "/"
            else:
                parent = self.parent
                if parent.is_root:
                    path = f"/{self.name}"
                else:
                    path = f"{parent.path}/{self.name}"

            self._path = path

        return path

    @property
    def variables(self):
        """The variables defined in this group.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `dict`
                The `Variable` objects, keyed by their names.

        """
        return self._variables

    def dump(
        self,
        display=True,
        data=False,
        depth=None,
        _prefix=None,
        _level=0,
        _structure=False,
    ):
        """A full description of the group.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `structure`

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

            data: `bool`, optional
                If True then include a summary of each variable's data
                array. If False (the default) then don't include these
                data summaries.

            depth: `int` or `None`, optional
                TODOP5

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
                f"{var.dump(display=False, data=data, _level=_level + 2, _structure=_structure)}"
                for name, var in self.variables.items()
            )

        # Groups
        if self.groups:
            lines.append(f"{i1}Groups:")
            if depth is None or depth >= self.path.count("/"):
                if depth is not None:
                    depth = depth - 1

                lines.extend(
                    f"{group.dump(display=False, data=data, depth=depth, _level=_level + 2, _structure=_structure)}"
                    for group in self.groups.values()
                )
            else:
                lines.extend(
                    f"{i2}{name}: {group!r}"
                    for name, group in self.groups.items()
                )

        out = "\n".join(lines)
        if not display:
            return out

        print(out)

    def getncattr(self, name):
        """Get an attribute value by name.

        This method has the same API as `netCDF4.Group.getncattr`.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            name: `str`
                The attribute name.

        :Returns:

                The attribute value.

        """
        try:
            return self.attrs[name]
        except KeyError:
            raise AttributeError(
                f"{self.__class__.__name__!r} object has no attribute "
                f"{name!r}"
            )

    def is_ancestor_group(self, other):
        """Return True if this group is an ancestor of another group.

        A group is considered to be an ancestor of itself.

        If `True`, then *other* is a sub-group of this group.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            other: `Group` or `File`
                Another group to test against.

        :Returns:

            `bool`
                `True` if this group is an ancestor of *other*, or if
                this group is *other* itself. `False` otherwise.

        """
        while other is not None:
            if self is other:
                return True

            try:
                other = other.parent
            except AttributeError:
                return False

        return False

    def is_sub_group(self, other):
        """Return True if this group is a subgroup of another group.

        A group is considered to be a sub-group of itself.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            other: `Group` or `File`
                Another group to test against.

        :Returns:

            `bool`
                `True` if this group is a subgroup of *other*, or if
                this group is *other* itself. `False` otherwise.

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

    def ncattrs(self):
        """Return a list of attribute names.

        This method has the same API as `netCDF4.Group.ncattrs`.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `list` of `str`
                The attribute names.

        """
        return list(self.attrs)


# Set __Group to `Group`, now that `Group` has been defined. This is
# used to create sub-groups.
Group._Group__Group = Group


class File(Group):
    """A dataset viewed as netCDF.

    The dataset (a `File` object) is represented as a collection of
    netCDF groups (`Group` objects), dimensions (`Dimension` objects),
    variables (`Variable` objects), and attributes.

    **Performance**

    `p5netcdf` is "structure- and attribute-eager", meaning that
    during `File` instantiation, the entire netCDF group, variable,
    and dimension structure is parsed; along with all group and
    variable attributes. Variable data access is always via access to
    the underlying backend library (see the *backend* parameter). Some
    `Variable` and `Group` properties and methods might also access
    the underlying backend but only for the first request, after which
    the result is cached (see the *metadata_strategy* parameter).

    .. versionadded:: (cfdm) NEXTVERSION

    """

    _netcdf = True

    def __init__(
        self,
        dataset,
        backend=None,
        metadata_strategy="minimal",
        pyfive_options=None,
        h5py_options=None,
        xarray_options=None,
        zarr_options=None,
        zarr_dimension_search="closest_ancestor",
        verbose=0,
    ):
        """**Initialisation**

        :Parameters:

            dataset:
                The definition of the netCDF dataset to be read. One
                of:

                * string-like (such as `str` or `pathlib.Path`)

                * file-like (such as `io.BufferedReader` or the result
                             of an `fsspec` file system open)

                * directory-like (such as `fsspec.mapping.FSMap`)

                * `pyfive.File`-like

                   Note that::

                      >>> nc = p5netcdf.File('dataset.nc', backend='pyfive')

                   is identical to::

                      >>> p = pyfive.File('dataset.nc')
                      >>> nc = p5netcdf.File(p)

                   A subclass of `pyfive.File` must expose the
                   following classes, attributes and methods from the
                   `pyfive` API: `!File.attrs` `!File.close`,
                   `!File.filename`, `!File.items`, `!File._fh`,
                   `!Group.attrs`, `!Group.items`, `Dataset.attrs`,
                   `Dataset.chunks`, `Dataset.dtype`, `Dataset.name`,
                   `Dataset.shape`; and its `!File` and `!Group`
                   classes must be (registered as) subclasses of
                   `pyfive.File` and `pyfive.Group` respectively.

                * `xarray.Dataset`-like or `xarray.DataTree`-like

                   Note that::

                      >>> nc = p5netcdf.File('dataset', backend='xarray')

                   is identical to::

                      >>> x = xarray.open_datatree(
                      ...     'dataset', mask_and_scale=False, decode_cf=False
                      ... )
                      >>> nc = p5netcdf.File(x)

            backend: `None` or (sequence of) `str`, optional
                Which library or libraries to use for reading a
                dataset. An attempt to open a dataset is made by the
                given backends in the order in which they are
                provided, stopping after the first successful read.

                The available backends are:

                =================  ======================
                Backend            Library
                =================  ======================
                ``'pyfive'``       `pyfive`
                ``'zarr'``         `zarr`
                ``'netCDF4'``      `netCDF4`
                ``'netcdf_file'``  `scipy.io.netcdf_file`
                ``'xarray'``       `xarray`
                ``'h5py'``         `h5py`
                ``'ppfive'``       `ppfive`
                =================  ======================

                By default *backend* is `None`, which is equivalent to
                providing the ordered sequence of backends:

                ``('pyfive', 'zarr', 'netCDF4', 'netcdf_file',
                'xarray', 'h5py', 'ppfive')``

                *Example:*
                  To only attempt ``'netCDF4'``: ``'netCDF4'`` or
                  ``['netCDF4']``

                *Example:*
                  To only attempt ``'netCDF4'`` or ``'pyfive'``, in
                  that order: ``('netCDF4', 'pyfive')``

            metadata_strategy: `str`, optional
                The strategy used for retrieving, via the backend
                library, metadata from the dataset during the initial
                parsing of the dataset, and caching it. Must be one
                of:

                * ``'minimal'``

                  This is the default. Only the minimum amount of
                  metadata required to parse the dataset is retrieved
                  from the dataset and cached. For instance, this
                  includes all variable and group attributes, but may
                  exclude (depending on the backend library) the
                  variable shapes.

                * ``'maximal'``

                  All required metadata is retrieved from the dataset
                  and cached. The dataset then does not need to
                  revisited except to access the variable data arrays.

                Dataset metadata caching can also be applied to an
                existing `File` instance with the `cache_metadata`
                method.

            pyfive_options: `dict` or `None`, optional
                Keyword arguments that are passed to `pyfive.File` to
                be used when opening a netCDF-4 dataset with the
                ``'pyfive'`` backend. Setting to `None` (the default)
                is equivalent to providing an empty
                dictionary. Ignored if *dataset* is already a
                `pyfive.File`-like object.

            h5py_options: `dict` or `None`, optional
                Keyword arguments that are passed to `h5py.File` to be
                used when opening a netCDF-4 dataset with the
                ``'h5py'`` backend. Setting to `None` (the default) is
                equivalent to providing an empty dictionary.

            xarray_options: `dict` or `None`, optional
                Keyword arguments that are passed to
                `xarray.open_datatree` to be used when opening a
                dataset with the ``'xarray'`` backend. Setting to
                `None` (the default) is equivalent to providing an
                empty dictionary. Ignored if *dataset* is already an
                `xarray.Dataset`-like or `xarray.DataTree`-like
                object.

            zarr_options: `dict` or `None`, optional
                Keyword arguments that are passed to `zarr.open` to be
                used when opening a Zarr or Kerchunk dataset with the
                ``'zarr'`` backend. Setting to `None` (the default) is
                equivalent to providing an empty dictionary.

            zarr_dimension_search: `str`, optional
                How to interpret a Zarr or Kerchunk dataset dimension
                name that contains no group-separator characters, such
                as ``dim`` (as opposed to ``group/dim``,
                ``/group/dim``, ``../dim``, etc.). Ignored for other
                dataset types.

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
                 Set the verbosity. If *verbose* is ``0`` there is no
                 verbose output, and more output is produced for
                 progressively larger values of *verbose*. Values of
                 ``5`` and higher (or the value ``-1``) produce the
                 same maximally verbose output.

        """
        try:
            import pyfive
        except ModuleNotFoundError:
            pyfive = None

        try:
            import xarray
        except ModuleNotFoundError:
            xarray = None

        # Options for the open functions
        read_options = {}
        if h5py_options:
            read_options["h5py"] = h5py_options

        if pyfive_options:
            read_options["pyfive"] = pyfive_options

        if zarr_options:
            read_options["zarr"] = zarr_options

        if xarray_options:
            read_options["xarray"] = xarray_options

        self._zarr_dimension_search = zarr_dimension_search

        # The name of the dataset
        dataset_name = ""
        # The file system procotol of the dataset. -1 is a non-string
        # and non-None code for an unknown file system protocol.
        protocol = -1
        # The log of how the dataset is read
        self._log_read = []

        if pyfive is not None and isinstance(dataset, pyfive.File):
            # --------------------------------------------------------
            # Input is `pyfive.File`-like
            # --------------------------------------------------------
            nc = dataset
            attrs = nc.attrs
            library = get_library(nc)
            self._owns_nc = False

            # Use the 'pyfive' logic to parse the dataset
            backend = "pyfive"

            # Attempt to get the dataset name and file system protocol
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

        elif xarray is not None and isinstance(
            dataset, (xarray.Dataset, xarray.DataTree)
        ):
            # --------------------------------------------------------
            # Input is `xarray.Dataset`-like or `xarray.DataTree`-like
            # --------------------------------------------------------
            if isinstance(dataset, xarray.Dataset):
                # Convert a Dataset to a DataTree
                dataset = xarray.DataTree(dataset=dataset)

            nc = dataset
            attrs = nc.attrs
            library = get_library(nc)
            self._owns_nc = False

            # Use the 'xarray' logic to parse the dataset
            backend = "xarray"

            # Attempt to get the dataset name and file system protocol
            try:
                dataset_name = dataset.encoding.get("source")
            except AttributeError:
                pass

        else:
            # --------------------------------------------------------
            # Input is string-like, file-like, or directory-like
            # --------------------------------------------------------
            # Attempt to get the dataset name and file system protocol
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

            # Map backend names to dataset-read functions
            #
            # Note to developers: If you change the order of this
            #                     dictionary, you must update the
            #                     `__init__` docstring, and consider
            #                     updating {{read netcdf_backend:
            #                     ...}} and {{read cfa_backend: ...}}
            #                     in docstring/docstring.py
            read_functions = {
                "pyfive": pyfive_open,
                "zarr": zarr_open,
                "netCDF4": netCDF4_open,
                "netcdf_file": netcdf_file_open,
                "xarray": xarray_open,
                "h5py": h5py_open,
                "ppfive": ppfive_open,
            }
            if backend is not None:
                # Restrict to selected backends
                if isinstance(backend, str):
                    backend = (backend,)

                try:
                    read_functions = {b: read_functions[b] for b in backend}
                except KeyError as error:
                    raise ValueError(
                        f"Invalid value for backend. Got {error}, "
                        f"expected one of {tuple(read_functions)}"
                    )

            nc = None
            for backend, func in read_functions.items():
                options = read_options.get(backend, {})
                try:
                    nc, attrs, library = func(dataset, options)
                except Exception as error:
                    self._log_read.append(
                        f"{backend}: {error.__class__.__name__}: {error}"
                    )
                else:
                    self._log_read.append(f"{backend}: Successfully opened")
                    break

            if nc is None:
                try:
                    # Rewind file-like
                    dataset.seek(0)
                except Exception:
                    pass

                raise NetCDFError(
                    f"Can't open {dataset} as a netCDF dataset with any of "
                    f"the backends {tuple(read_functions)}:\n\n"
                    f"{self.log(display=False)}"
                )

            # The opened dataset is owned internally
            self._owns_nc = True

        # Cache the backend, library, dataset, and dataset name
        if not isinstance(dataset_name, str):
            dataset_name = ""

        self._backend = backend
        self._library = library
        self._dataset = dataset
        self._dataset_name = dataset_name

        # Cache the file system protocol, but only if we've found out
        # what it is, and whether or not the dataset exists in the
        # local file system.
        if protocol == -1:
            is_local = None
        else:
            if isinstance(protocol, tuple):
                protocol = protocol[0]

            if protocol in ("", "file", "local", None):
                protocol = "file"
                is_local = True
            else:
                is_local = False

            self._protocol = protocol

        self._is_local = is_local

        # ------------------------------------------------------------
        # Parse the group structure
        # ------------------------------------------------------------
        super().__init__(
            name="", parent=None, root=self, grp=nc, grp_attrs=attrs
        )

        # Cache any extra dataset metadata (after the group structure
        # has been parsed)
        self.cache_metadata(metadata_strategy)

        # Verbose output
        if verbose == -1:
            verbose = 5

        if verbose >= 1:
            self.log()
            if verbose >= 2:
                print()

            if verbose >= 5:
                self.dump(data=True)
            elif verbose >= 4:
                self.dump()
            elif verbose >= 3:
                self.structure()
            elif verbose >= 2:
                print(self)

    def __enter__(self):
        """Enter the runtime context related to this object.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context and close the file.

        .. versionadded:: (cfdm) NEXTVERSION

        """
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

        Closes the underlying netCDF dataset, but only if owned by
        this `File` instance.

        .. versionadded:: (cfdm) NEXTVERSION

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
        data=False,
        depth=None,
        _prefix=None,
        _level=0,
        _structure=False,
    ):
        """A full description of the dataset.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `structure`, `ncdump`

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

            data: `bool`, optional
                If True then include a summary of each variable's data
                array. If False (the default) then don't include these
                data summaries.

            depth: `int` or `None`, optional
                TODOP5

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
                self.dataset_name,
                super().dump(
                    display=False,
                    data=data,
                    depth=depth,
                    _prefix=_prefix,
                    _level=_level,
                    _structure=_structure,
                ),
            )
        )

        if not display:
            return out

        print(out)

    def log(self, display=True):
        """The dataset-read log.

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
        log = "\n\n".join(self._log_read)
        if not display:
            return log

        print(log)

    def ncdump(self, display=True):
        """A text CDL description of the dataset.

        The text representation is in CDL (network Common Data form
        Language) form, and emulates the output of `ncdump -h`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `dump`, `structure`

        :Parameters:

            display: `bool`, optional
                If False then return the CDL description as a
                string. By default the description is printed.

        :Returns:

            `None` or `str`
                The CDL description. If *display* is True then the
                description is printed and `None` is
                returned. Otherwise the description is returned as a
                string.

        """
        dataset_name = self.dataset_name
        if dataset_name:
            dataset_name += " "

        lines = []
        lines.append(f"netcdf {dataset_name}{{")
        cdl_format(self, lines)
        lines.append("}")

        out = "\n".join(lines)
        if not display:
            return out

        print(out)

    def getncattr(self, name):
        """Get a global attribute value by name.

        This method has the same API as `netCDF4.Dataset.getncattr`.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            name: `str`
                The attribute name.

        :Returns:

                The attribute value.

        """
        try:
            return self.attrs[name]
        except KeyError:
            raise AttributeError(
                f"{self.__class__.__name__!r} object has no attribute "
                f"{name!r}"
            )

    def cache_metadata(self, strategy="maximal"):
        """Cache all relevant metadata from the dataset.

        Any metadata that is already cached is not re-retrieved from
        the dataset.

        Metadata may have already been cached within the backend
        library, in which case retrieving and caching it in the `File`
        instance it may by fast.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            strategy: `str`
                The strategy used for caching, via the backend
                library, metadata from the dataset. Must be one of:

                * ``'maximal'``

                  This is the default. All required metadata is
                  retrieved from the dataset and cached. The dataset
                  then does not ever need to revisited except to
                  access the variable data arrays.

                * ``'minimal'``

                  Only the minimum amount of metadata required to
                  parse the dataset is retrieved from the dataset and
                  cached. For instance, this includes all variable and
                  group attributes, but may exclude (depending on the
                  backend library) the variable shapes. Minimal
                  metdata caching is always applied during `File`
                  instantiation, so there is no benefit in using this
                  option.

        :Returns:

            `None`

        """
        if strategy == "maximal":
            # Execute `Variable` methods that might access the dataset and
            # which have not already been run via `Variable.__init__`.
            for variable in self.all_variables.values():
                variable.__orthogonal_indexing__
                variable.dtype
                variable.shape
                variable.shards
                variable.get_dims()
                variable.chunking()

        elif strategy == "minimal":
            # Minimal caching is always already done in
            # `File.__init__`
            pass

        else:
            raise ValueError(
                f"Invalid value for metadata_strategy. Got {strategy!r}, "
                "expected one of 'minimal', 'maximal'"
            )
