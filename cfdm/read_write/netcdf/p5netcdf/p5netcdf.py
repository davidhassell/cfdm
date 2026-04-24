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
from .utils_zarr import zarr_open, zarr_parse_group_structure

np.set_printoptions(floatmode="maxprec")


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
                The absolute netCDF path, e.g. ``'/lat'`` or
                ``'/group/time'``.

        """
        path = getattr(self, "_path", None)
        if path is None:
            parent = self.parent
            if parent.isroot:
                path = f"/{self.name}"
            else:
                path = f"{parent.path}/{self.name}"
            # parent = self.parent
            # if parent.isroot:
            #    path = ""
            # else:
            #    path = parent.path
            #
            # path += f"/{self.name}"
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
        if self.backend == "netcdf_file":
            # Need to copy the numpy array returned by
            # scipy.io.netcdf_file with mmap=True. See `File.close`
            # and the scipy.io.netcdf_file docs for details.
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
            f"<netcdf.{self.__class__.__name__}: "
            f"{self.path}, shape={self.shape}, dimensions={dims}>"
        )

    @property
    def attrs(self):
        """The variable attributes.

        :Returns:

            `dict`
                The attribute values, keyed by their names.

        """
        return self._attrs

    @property
    def backend(self):
        """The name of the library package that provides the backend.

        .. seealso:: `lib`

        :Returns:

            `str`
                The name of the library package that provides the
                backend.

        """
        return self._backend

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
                    dtype = (
                        self._var[(slice(0, 1),) * len(self.shape)]
                        .flat[0]
                        .dtype
                    )

            if dtype is not str and dtype != np.dtypes.StringDType():
                dtype = np.dtype(f"{dtype.kind}{dtype.itemsize}")

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
        """The library package that provides the backend.

        .. seealso:: `backend`

        :Returns:

                The library package that provides the backend.

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
            parent = self.parent
            if parent.isroot:
                path = f"/{self.name}"
            else:
                path = f"{parent.path}/{self.name}"

            #            match self.backend:
            #                case "pyfive" | "zarr" | "h5py":
            #                    path = self._var.name
            #                case "netCDF4":
            #                    parent = self.parent
            #                    if parent.isroot:
            #                        path = ""
            #                    else:
            #                        path = parent.path
            #
            #                    path += f"/{self.name}"
            #                    if not path.startswith('/'):
            #                        path = f"/{path}"
            #
            #                case "netcdf_file":
            #                    path = f"/{self.name}"

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
    def shards(self):
        """The TODO dimension lengths of the variable.

        :Returns:

            `tuple` of `int`
                The dimension lengths (e.g. ``(12, 96, 73)``).

        """
        shards = getattr(self, "_shards", None)
        if shards is None:
            shards = getattr(self._var, "shards", None)
            self._shards = shards

        return shards

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
                        if self._var.data_model == "NETCDF3_CLASSIC":
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
                    "than None by the zarr_parse_group_structure function. "
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
        return self.dump(display, _prefix, _level, _structure=True)


class Group(Mapping):
    """A netCDF group.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    __hash__ = None

    # Store classes for creating dimensions, variables and sub-groups.
    __Dimension = Dimension
    __Variable = Variable
    # Note: __Group will be re-set to `Group` after the `Group` class
    #       has been finished defining itself.
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
        self._backend = root.backend
        self._lib = root.lib
        self._grp = grp
        self._isroot = parent is None

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
            f"<netcdf.{self.__class__.__name__}: "
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
        """TODO."""
        root = self.root

        if self.isroot:
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
                hdf5_parse_group_structure(self)

            case "netCDF4":
                # ----------------------------------------------------
                # netCDF4
                # ----------------------------------------------------
                # Create dimensions in this group
                for name, dim in self._grp.dimensions.items():
                    self._create_dimension(name, dim.size, dim.isunlimited())

                # Create variables in this group
                for name, var in self._grp.variables.items():
                    attrs = {
                        attr: var.getncattr(attr) for attr in var.ncattrs()
                    }
                    self._create_variable(name, var, attrs)

                # Create subgroups
                for name, grp in self._grp.groups.items():
                    attrs = {
                        attr: grp.getncattr(attr) for attr in grp.ncattrs()
                    }
                    self._create_group(name, grp, attrs)

            case "netcdf_file":
                # ----------------------------------------------------
                # netcdf_file
                # ----------------------------------------------------
                # Create dimensions in this group
                for name, size in self._grp.dimensions.items():
                    self._create_dimension(name, size, isunlimited=False)

                # Create variables in this group
                for name, var in self._grp.variables.items():
                    self._create_variable(name, var, var._attributes)

            case "zarr":
                # ----------------------------------------------------
                # zarr
                # ----------------------------------------------------
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
    def backend(self):
        """The name of the library package that provides the backend.

        .. seealso:: `lib`

        :Returns:

            `str`
                The name of the library package that provides the
                backend.

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
    def isroot(self):
        """Whether or not this is the root group.

        :Returns:

            `bool`
                `True` if this is the root group, otherwise `False`.

        """
        return self._isroot

    @property
    def lib(self):
        """The library package that provides the backend.

        .. seealso:: `backend`

        :Returns:

                The library package that provides the backend.

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
            print(self.backend)
            match self.backend:
                case "pyfive" | "zarr" | "h5py":
                    path = self._grp.name
                    print('_______________', path)
                case "netCDF4":
                    path = self._grp.path
                case "netcdf_file":
                    path = '/'

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

    def is_sub_group(self, other):
        """Return True if the group is a subgroup of, or is, 'other'.

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
        pyfive_options=None,
        h5py_options=None,
        zarr_dimension_search="closest_ancestor",
    ):
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
                dataset in the event that its dimensions are named in
                a manner that is inconsistent with CF rules defined by
                the CF conventions (section 2.7 Groups).

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

        """
        import pyfive

        if mode != "r":
            raise ValueError("mode must be 'r'. Got: mode={mode!r}")

        self._open_options = {}
        if h5py_options:
            self._open_options["h5py"] = h5py_options

        if pyfive_options:
            self._open_options["pyfive"] = pyfive_options

        self._zarr_dimension_search = zarr_dimension_search

        open_log = []

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
                # Likely a file-like or directory-like object
                pass

            nc = None
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

            if nc is None:
                try:
                    # Rewind file-like
                    dataset.seek(0)
                except AttributeError:
                    pass

                error = "\n\n".join(open_log)
                raise NetCDFError(
                    f"Can't interpret {dataset} as a netCDF dataset "
                    f"with any of the backends {tuple(open_functions)}:\n\n"
                    f"{error}"
                )

            # The opened dataset is owned internally
            self._owns_nc = True

        self._open_log = open_log

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
                    filename = self._grp.store_path

            self._filename = filename

        return filename

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
            # We can't close a scipy.io.netcdf_file instance opened
            # with mmap=True when any variable still exists, or when
            # an array referring to a variable's data still exists
            # (see scipy.io.netcdf_file docs for details). So, rather
            # than attempting to hunt down all such references
            # (messy!), the hack of setting the '_mm_buf' attribute to
            # `None` allows the file to be closed. We get away with
            # this because we know that we've copied all memory mapped
            # data into memory inside `Variable.__getitem__`.
            self._grp._mm_buf = None

        try:
            self._grp.close()
        except AttributeError:
            pass

    @property
    def all_dimensions(self):
        """TODO."""
        if getattr(self, "_all_dimensions", None) is None:
            self._populate_all()

        return self._all_dimensions

    @property
    def all_groups(self):
        """TODO."""
        if getattr(self, "_all_groups", None) is None:
            self._populate_all()

        return self._all_groups

    @property
    def all_variables(self):
        """TODO."""
        if getattr(self, "_all_variables", None) is None:
            self._populate_all()

        return self._all_variables

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
                str(self.filename),
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
        self._backend = "zarr"
        return zarr_open(self, dataset)

    def _open_netCDF4(self, filename):
        """Return an open `netCDF4.Dataset`.

        :Parameters:

            filename: `str`
                The file to open.

        :Returns:

            `netCDF4.Dataset`

        """
        import netCDF4

        self._backend = "netCDF4"
        self._lib = netCDF4
        nc = netCDF4.Dataset(filename, mode="r")
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

        self._backend = "netcdf_file"
        self._lib = netcdf_file
        nc = netcdf_file(filename, mode="r", mmap=True)
        return nc, nc._attributes

    def _open_pyfive(self, dataset):
        """Open a dataset with `pyfive`.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            dataset:
                May be a `str` or `pathlib.Path` path, a file-like object
                (such as `io.BufferedReader` or the result of an `fsspec`
                file system open).

        :Returns:

            `pyfive.File`

        """
        self._backend = "pyfive"
        return pyfive_open(self, dataset)

    def _open_h5py(self, dataset):
        """Open a dataset with `h5py`.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            root: `p5netcdf.File`
                The root group.

            dataset:
                May be a `str` or `pathlib.Path` path, a file-like object
                (such as `io.BufferedReader` or the result of an `fsspec`
                file system open).

        :Returns:

            `h5py.File`

        """
        self._backend = "h5py"
        return h5py_open(self, dataset)
