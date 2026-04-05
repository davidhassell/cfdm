from collections.abc import Mapping

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


def _format_attr(value):
    """Format an attribute according to netCDF.

    * Strings return as pure Python strings.
    * Single numeric values return as true numpy scalars (preserving
      bit-width).
    * Multi-element numeric values return as numpy arrays.
    * String sequences return as Python lists of strings.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        value:
            The raw attribute value from the HDF5 file.

    :Returns:

            The formatted attribute value adhering to netCDF
            conventions.

    """
    # String-like values
    if isinstance(value, (bytes, np.bytes_)):
        return value.decode("utf-8")

    # Sequence (numpy, list, tuple)
    if hasattr(value, "__len__") or hasattr(value, "shape"):
        size = getattr(value, "size", len(value))

        # Empty or string-like primitives
        if size == 0:
            if isinstance(value, (bytes, str)):
                return ""

            return value

        # 1-element sequence (scalar/string)
        if size == 1:
            item = value.flat[0] if hasattr(value, "flat") else value[0]

            if isinstance(item, (bytes, np.bytes_)):
                return item.decode("utf-8")
            if isinstance(item, str):
                return item

            # Numeric scalar: preserve numpy bit-width
            dtype = getattr(value, "dtype", None)
            if dtype:
                return dtype.type(item)
            return np.array(item).dtype.type(item)

        # Multi-element sequence
        first_item = value.flat[0] if hasattr(value, "flat") else value[0]

        # If string/bytes, return as a Python list
        if isinstance(first_item, (str, bytes, np.bytes_)):
            return [
                v.decode("utf-8") if isinstance(v, (bytes, np.bytes_)) else v
                for v in value
            ]

        # Convert a numeric sequence to a numpy array
        if not hasattr(value, "dtype"):
            return np.array(value)

    return value


def _parse_attributes(raw_attributes):
    """Format raw attributes attributes according to netCDF .

    * Strings return as pure Python strings.
    * Single numeric values return as true numpy scalars (preserving
      bit-width).
    * Multi-element numeric values return as numpy arrays.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        raw_attributes: `dict`
            The raw attributes value from the HDF5 file.

    :Returns:

        `dict`
            The formatted attributes

    """
    return {
        k: _format_attr(v)
        for k, v in raw_attributes.items()
        if k not in _IGNORED_ATTRS and not k.startswith(_IGNORED_PREFIXES)
    }


def normalize_path_segments(segments):
    """Resolve '.' and '..' in a list of path segments."""
    segments = segments.split("/")
    resolved = []
    for segment in segments:
        if segment == "." or segment == "":
            continue

        if segment == "..":
            if resolved:
                resolved.pop()
            else:
                # If we try to go above the requested path's base,
                # we just ignore it or let it fail naturally.
                pass
        else:
            resolved.append(segment)

    return resolved


class Dimension:
    """A netCDF dimension.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __init__(self, name, size, isunlimited, parent_group):
        """**Initialisation**

        :Parameters:

            name: `str`
                The name of the dimension in its parent group.

            size: `int`
                The size of the dimension.

            isunlimited: `bool`
                True if the dimension is unlimited.

            parent_group: `Group`
                The group in which this dimension is defined.

        """
        self._name = name
        self._size = size
        self._isunlimited = isunlimited
        self._parent = parent_group

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
        unlimited = " (unlimited)" if self.isunlimited() else ""
        return (
            f"<p5netcdf.{self.__class__.__name__}: "
            f"{self.path}, size={self.size}{unlimited}>"
        )

    @property
    def name(self):
        """The name of the dimension in its parent group.

        :Returns:

            `str`
                The relative netCDF name (e.g. ``'time'``).

        """
        return self._name

    @property
    def path(self):
        """The full absolute path of the dimension.

        :Returns:

            `str`
                The absolute netCDF path (e.g. ``'/subgroup/time'``).

        """
        path = getattr(self, "_path", None)
        if path is None:
            # Get the parent group's path
            group_path = getattr(self._parent, "path", "")

            # Avoid returning '//dimname' if in the root group
            if group_path == "/":
                path = f"/{self.name}"
            else:
                path = f"{group_path}/{self.name}"

        return path

    @property
    def size(self):
        """The size of the dimension.

        :Returns:

            `int`
                The size.

        """
        return self._size

    def group(self):
        """The parent group that defines this dimension.

        :Returns:

            `Group`
                The group.

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

    def __init__(self, name, h5ds, parent_group, _h5ds_attrs=None):
        """**Initialisation**

        :Parameters:

            name: `str`
                The name of the variable in its parent group.

            h5ds: (subclass of) `pyfive.Dataset`
                The underlying pyfive dataset object.

            parent_group: `Group` or `File`
                The parent group containing this variable.

            _h5ds_attrs: `dict` or `None`, optional
                The raw attributes of *h5ds*. If `None` (the default)
                then the attributes are retrieved from *h5ds*.

        """
        self._name = name
        self._h5ds = h5ds
        self._parent = parent_group

        if _h5ds_attrs is None:
            _h5ds_attrs = self._h5ds.attrs

        self.dimensions = self._get_dimensions(_h5ds_attrs)
        self.attrs = _parse_attributes(_h5ds_attrs)

    def __getitem__(self, key):
        """Return a subspace of the data array defined by indices."""
        return self._h5ds[key]

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
            f"<p5netcdf.{self.__class__.__name__}: "
            f"{self.path}, shape={self.shape}, dimensions={dims}>"
        )

        return (
            f"<p5netcdf.{self.__class__.__name__}: "
            f"{self.path}, shape={self.shape}, dims={self.dimensions}>"
        )

    def _get_dimensions(self, h5ds_attrs):
        """Get the variable dimension names.

        Resolves dimension names, handling both standard variables and
        Dimension Scales.

        """
        # Case 1: It's a Dimension Scale itself (no DIMENSION_LIST attribute)
        if h5ds_attrs.get("CLASS") == b"DIMENSION_SCALE":
            #            return (self.name.split("/")[-1],)
            return (self.name,)

        # Case 2: Standard variable with linked dimensions
        if "DIMENSION_LIST" not in h5ds_attrs:
            return ()

        dim_names = []
        for ref in h5ds_attrs["DIMENSION_LIST"]:
            try:
                if hasattr(ref, "item"):
                    ref = ref.item()
                elif isinstance(ref, (list, tuple)) and len(ref) > 0:
                    ref = ref[0]

                root_file = self._h5ds.file
                dim_dataset = root_file[ref]
                dim_names.append(dim_dataset.name.split("/")[-1])
            except (KeyError, ValueError, TypeError):
                continue

        return tuple(dim_names)

    @property
    def chunks(self):
        """Returns the chunk size `tuple`, or None if contiguous."""
        chunks = getattr(self, "_chunks", None)
        if chunks is None:
            chunks = self._h5ds.chunks
            self._chunks = chunks

        return chunks

    @property
    def dtype(self):
        """The numpy data type of the variable's dataset."""
        dtype = getattr(self, "_dtype", None)
        if dtype is None:
            dtype = self._h5ds.dtype
            self._dtype = dtype

        return dtype

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
            maxshape = self._h5ds.maxshape
            self._maxshape = maxshape

        return maxshape

    @property
    def name(self):
        """The name of the variable in its parent group.

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

        :Returns:

            `str`
                The absolute netCDF path (e.g. ``'/group/latitude'``).

        """
        path = getattr(self, "_path", None)
        if path is None:
            path = self._h5ds.name
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
            shape = self._h5ds.shape
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
            size = self._h5ds.size
            self._size = size

        return size

    def chunking(self):
        """Returns the data chunk shape.

        :Returns:

            `list` or 'str`
                The chunk shape (e.g. ``[5, 6, 7]``). If the data is
                contiguous then ``'contiguous` is returned.

        """
        chunks = self.chunks
        if chunks is None:
            return "contiguous"

        return list(chunks)

    def get_dims(self):
        """Return the dimensions of the variable.

        :Returns:

            `tuple` of `Dimension`
                The dimensions for the variable.

        """
        dims = getattr(self, "_dims", None)
        if dims is not None:
            return dims

        dims = []

        for dim_name in self.dimensions:
            current_group = self._parent
            found = False

            # Walk up the tree to find where the dimension is defined
            while current_group is not None:
                if dim_name in current_group.dimensions:
                    dims.append(current_group.dimensions[dim_name])
                    found = True
                    break

                current_group = current_group.parent

            # Fallback if we somehow can't find it at all (shouldn't
            # happen in valid files)
            if not found:
                raise KeyError(
                    f"Dimension {dim_name!r} not found in the group hierarchy."
                )

        dims = tuple(dims)
        self._dims = dims
        return dims

    def group(self):
        """The group that contains this variable.

        :Returns:

            `Group`
                The group object containing this variable.

        """
        return self._parent


class Group(Mapping):
    """A netCDF group.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __init__(self, h5, parent=None):
        """**Initialisation**

        :Parameters:

            h5: (subclass of) `pyfive.Group` or (subclass of)` pyfive.File`
                The underlying pyfive object.

            parent: `Group` or `None`, optional
                The parent group. Set to `None` (the default) for the
                root group.

        """
        self._h5 = h5
        self.parent = parent

        self.dimensions = {}
        self.variables = {}
        self.groups = {}

        self.attrs = _parse_attributes(self._h5.attrs)

        self._parse_structure()

    def __getitem__(self, key):
        """Get a variable or group.

        Absolute and relative nested paths are allowed, which may
        include ``.`` (current group) and ``..`` (parent group)
        elements.

        """
        if key == "":
            return self

        # Determine the starting point
        current = self
        if key.startswith("/"):
            while current.parent is not None:
                current = current.parent

        # Split the path into parts (ignoring empty strings from
        # double-slashes)
        segments = [s for s in key.split("/") if s]

        # Handle a request of just "/"
        if not segments and key.startswith("/"):
            return current

        # Loop through the segments
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
                if i != len(segments) - 1:
                    if key.startswith("/"):
                        start = ""
                    else:
                        start = f" from group {self.path}"

                    raise KeyError(
                        f"Invalid path {key!r}{start}: "
                        f"{current.variables[part].path} is a variable "
                        "and cannot have children"
                    )

                return current.variables[part]

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
        return iter(tuple(self.variables) + tuple(self.groups))

    def __len__(self):
        """The number of variables and sub-groups."""
        return len(self.variables) + len(self.groups)

    def __repr__(self):
        """Called by the `repr` built-in function."""
        pv = "" if len(self.variables) == 1 else "s"
        pg = "" if len(self.groups) == 1 else "s"

        return (
            f"<p5netcdf.{self.__class__.__name__}: "
            f"{self.path}, {len(self.variables)} variable{pv}, "
            f"{len(self.groups)} sub-group{pg}>"
        )

    def _parse_structure(self):
        """Parse the group structure.

        Parses variables, dimensions, and subgroups in a single
        optimised pass.

        :Returns:

            `None`

        """
        import pyfive

        raw_dims = {}
        subgroups_to_process = []
        datasets_to_process = []
        dataset_attrs = {}

        # Categorise objects without double-reading items from the
        # dataset
        for name, h5 in self._h5.items():
            if isinstance(h5, pyfive.Group):
                subgroups_to_process.append((name, h5))
            elif isinstance(h5, pyfive.Dataset):
                datasets_to_process.append((name, h5))

        # Extract dimension scales (strictly ignoring scalars)
        for name, h5ds in datasets_to_process:
            shape = h5ds.shape
            attrs = h5ds.attrs
            dataset_attrs[name] = attrs

            if shape and attrs.get("CLASS") == b"DIMENSION_SCALE":
                # Get ID: Use None if missing to push to end of sort
                dim_id = attrs.get("_Netcdf4Dimid")
                if dim_id is not None:
                    dim_id = int(dim_id)

                dim_name = name.split("/")[-1]

                is_unlimited = False
                maxshape = h5ds.maxshape
                if maxshape and len(maxshape) > 0:
                    is_unlimited = maxshape[0] is None

                raw_dims[dim_name] = {
                    "id": dim_id,
                    "size": shape[0],
                    "is_unlimited": is_unlimited,
                    "is_stub": (
                        b"not a netCDF variable" in attrs.get("NAME", b"")
                    ),
                }

        # Sort and create Dimension objects
        #
        # Sorting ensures consistency with netCDF4-python, which
        # preserves the creation order of its dimensions in an ordered
        # dictionary.
        #
        # We sort by (ID, Name). If ID is `None` (pure HDF5), it's
        # treated as infinity to ensure it appears after the
        # netCDF-indexed dimensions.
        sorted_items = sorted(
            raw_dims.items(),
            key=lambda x: (
                x[1]["id"] if x[1]["id"] is not None else float("inf"),
                x[0],
            ),
        )

        for d_name, d_info in sorted_items:
            self.dimensions[d_name] = Dimension(
                d_name,
                d_info["size"],
                d_info["is_unlimited"],
                parent_group=self,
            )

        # Create variables (skipping internal netCDF stubs)
        for name, h5ds in datasets_to_process:
            dim_name = name.split("/")[-1]

            # If it's in 'raw_dims' and flagged as a stub then skip
            # it. Otherwise - whether it's a coordinate variable,
            # normal data, or a scalar pretending to be a scale - it
            # becomes a Variable.
            is_stub = raw_dims.get(dim_name, {}).get("is_stub", False)

            if not is_stub:
                self.variables[name] = Variable(
                    name,
                    h5ds,
                    parent_group=self,
                    _h5ds_attrs=dataset_attrs[name],
                )

        # Create subgroups
        for name, group in subgroups_to_process:
            self.groups[name] = Group(group, parent=self)

    @property
    def name(self):
        """The name of the group in its parent group.

        :Returns:

            `str`
                The relative netCDF name (e.g. ``'subgroup'``).

        """
        name = getattr(self, "_name", None)
        if name is None:
            name = self.path.split("/")[-1]
            self._name = name

        return name

    @property
    def path(self):
        """The full absolute path of the group.

        :Returns:

            `str`
                The absolute netCDF path (e.g. ``'/model/subgroup'``).

        """
        path = getattr(self, "_path", None)
        if path is None:
            path = self._h5.name
            self._path = path

        return path


class File(Group):
    """A netCDF dataset.

    A `File` is a collection of dimensions, groups, variables and
    attributes which describe the meaning of the data and metadata
    stored in a netCDF dataset.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    _p5netcdf = True

    def __init__(self, dataset):
        """**Initialisation**

        :Parameters:

            dataset:
                The netCDF dataset to be read.

                May be a `str` or `pathlib.Path` path, a file-like
                object (such as `io.BufferedReader` or the result of
                an `fsspec` file system file open), or a (subclass of
                a) `pyfive.File` object.

        """
        import pyfive

        if not isinstance(dataset, pyfive.File):
            dataset = pyfive.File(dataset, mode="r")

        self._h5_file = dataset

        super().__init__(dataset)

    def close(self):
        """Close the file.

        Closes the underlying (subclass of a) `pyfive.File` object.

        :Returns:

            `None`

        """
        self._h5_file.close()

    @property
    def filename(self):
        """The name of the file on disk.

        :Returns:

            `str`
                The filename of the dataset.

        """
        filename = getattr(self, "_filename", None)
        if filename is None:
            filename = self._h5_file.filename
            self._filename = filename

        return filename
