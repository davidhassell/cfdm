from collections.abc import Mapping

import numpy as np

# Strip out the ones that don't start with _nc4 or _Netcdf4
_IGNORED_ATTRS = {
    "CLASS",
    "NAME",
    "REFERENCE_LIST",
    "DIMENSION_LIST",
    "DIMENSION_LABELS",
    "_NCProperties",
    "_nc3_strict",
}


def _format_attr(value):
    """Format an attribute according to netCDF.

    - Strings return as pure Python strings.
    - Single numeric values return as true NumPy scalars (preserving
      bit-width).
    - Multi-element numeric values return as NumPy arrays.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        value:
            The raw attribute value from the HDF5 file.

    :Returns:

            The formatted attribute value adhering to netCDF
            conventions.

    """
    # 1. Handle bytes or 1-element bytes directly (force to string)
    if isinstance(value, bytes):
        return value.decode("utf-8")

    # 2. Handle NumPy string/bytes scalars
    if isinstance(value, np.bytes_):
        return value.decode("utf-8")

    # 3. Handle sequences (lists/tuples)
    if isinstance(value, (list, tuple)):
        # If it's a list of bytes, decode them or unpack if single
        if len(value) == 1 and isinstance(value[0], bytes):
            return value[0].decode("utf-8")

        # If it's a multi-element numeric list, make it an array
        return np.array(value)

    # 4. Handle NumPy arrays and scalars
    if hasattr(value, "shape"):
        # If it's a 1-element array or a scalar, extract the true
        # NumPy scalar
        if value.size == 1:
            # .flat[0] gets the actual scalar element out of the array
            return value.dtype.type(value.flat[0])

        # If it's a multi-element array, leave it as an array
        return value

    # 5. Fallback for raw Python ints/floats
    return value


def _parse_attributes(raw_attributes):
    """Format raw attributes attributes according to netCDF .

    - Strings return as pure Python strings.
    - Single numeric values return as true NumPy scalars (preserving
      bit-width).
    - Multi-element numeric values return as NumPy arrays.

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
        if k not in _IGNORED_ATTRS and not k.startswith(("_Netcdf4", "_nc4"))
    }


class Dimension:
    """Represents a netCDF dimension.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __init__(self, name, size, is_unlimited, parent_group):
        """**Initialisation**

        :Parameters:

            name: `str`
                The name of the dimension.

            size: `int`
                The size of the dimension.

            is_unlimited: `bool`
                True if the dimension is unlimited.

            parent_group: `Group`
                The group in which this dimension is defined.

        """
        self.name = name
        self.size = size
        self._is_unlimited = is_unlimited
        self._parent_group = parent_group

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
        unlimited = ", unlimited" if self._is_unlimited else ""
        return (
            f"<p5netcdf.Dimension {self.name!r}: size {self.size}{unlimited}>"
        )

    def group(self):
        """The group that defines this dimension.

        :Returns:

            `Group`
                The group object containing this dimension.

        """
        return self._parent_group

    def isunlimited(self):
        """Whether the dimension is unlimited.

        :Returns:

            `bool`
                True if the dimension is unlimited, False otherwise.

        """
        return self._is_unlimited


class Variable:
    """Represents a netCDF variable.

    This class wraps a `pyfive.Dataset`, mapping internal HDF5
    dimensions and attributes to standard netCDF structures.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __init__(self, name, h5_dataset, parent_group=None):
        """**Initialisation**

        :Parameters:

            name: `str`
                The name of the variable.

            h5_dataset: `pyfive.Dataset`
                The underlying pyfive dataset object.

            parent_group: `Group`, optional
                The parent group containing this variable.

        """
        self.name = name
        self._h5ds = h5_dataset
        self._parent = parent_group

        h5ds_attrs = self._h5ds.attrs
        self.dimensions = self._get_dimensions(h5ds_attrs)
        self.attrs = _parse_attributes(h5ds_attrs)

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
        return (
            f"<p5netcdf.{self.__class__.__name__} "
            f"{self.name!r}: shape {self.shape}, dims {self.dimensions}>"
        )

    def _get_dimensions(self, h5ds_attrs):
        """Get the variable dimension names.

        Resolves dimension names, handling both standard variables and
        Dimension Scales.

        """
        # Case 1: It's a Dimension Scale itself (no DIMENSION_LIST attribute)
        if h5ds_attrs.get("CLASS") == b"DIMENSION_SCALE":
            return (self.name.split("/")[-1],)

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
        """Returns the chunk size tuple, or None if contiguous."""
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

        Unlimited dimensions are represneted by `None`.

        """
        maxshape = getattr(self, "_maxshape", None)
        if maxshape is None:
            maxshape = self._h5ds.maxshape
            self._maxshape = maxshape

        return maxshape

    @property
    def ndim(self):
        """The number of dimensions for the variable."""
        return len(self.shape)

    @property
    def shape(self):
        """The dimensions' lengths of the variable's dataset."""
        shape = getattr(self, "_shape", None)
        if shape is None:
            shape = self._h5ds.shape
            self._shape = shape

        return shape

    @property
    def size(self):
        """The total number of elements in the variable's dataset."""
        size = getattr(self, "_size", None)
        if size is None:
            size = self._h5ds.size
            self._size = size

        return size

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
    """Represents a netCDF group.

    This class wraps a (sublcass of) `pyfive.Group`, mapping internal
    HDF5 dimensions, attributes, variables, and subgroups to standard
    netCDF structures.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __init__(self, h5, parent=None):
        """**Initialisation**

        :Parameters:

            h5: `pyfive.Group` or `pyfive.File`
                The underlying pyfive object.

            parent: `Group` or `None`, optional
                The parent group. Set to `None` (the default) for the
                root group.

        """
        self._h5 = h5
        self.name = h5.name
        self.parent = parent

        self.dimensions = {}
        self.variables = {}
        self.groups = {}

        self.attrs = _parse_attributes(self._h5.attrs)

        self._parse_structure()

    def __getitem__(self, key):
        """TOOT."""
        # 1. Strip leading slash for absolute paths
        if key.startswith("/"):
            key = key.lstrip("/")

        if not key:
            return self

        # 2. Handle nested paths like 'group/subgroup/variable'
        if "/" in key:
            parts = key.split("/", 1)
            current_part = parts[0]
            remaining_path = parts[1]

            if current_part in self.groups:
                return self.groups[current_part][remaining_path]

            raise KeyError(
                f"{current_part!r} not found in groups of {self.name}"
            )

        # 3. Standard local lookup
        if key in self.variables:
            return self.variables[key]

        if key in self.groups:
            return self.groups[key]

        raise KeyError(
            f"'{key}' not found in variables or groups of {self.name}"
        )

    def __iter__(self):
        """The variables and sub-groups."""
        return iter(tuple(self.variables) + tuple(self.groups))

    def __len__(self):
        """The number of variables and sub-groups."""
        return len(self.variables) + len(self.groups)

    def __repr__(self):
        """Called by the `repr` built-in function."""
        return (
            f"<p5netcdf.{self.__class__.__name__} "
            f"{self.name!r} ({len(self.variables)} variables, "
            f"{len(self.groups)} groups)>"
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

        # Pass 1: Categorize objects without double-reading items from HDF5
        for name, h5 in self._h5.items():
            if isinstance(h5, pyfive.Group):
                subgroups_to_process.append((name, h5))
            elif isinstance(h5, pyfive.Dataset):
                datasets_to_process.append((name, h5))

        # Pass 2: Extract Dimension Scales (strictly ignoring scalars)
        for name, h5ds in datasets_to_process:
            attrs = h5ds.attrs
            shape = h5ds.shape

            if shape and attrs.get("CLASS") == b"DIMENSION_SCALE":
                dim_id = int(attrs.get("_Netcdf4Dimid", -1))
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

        # Pass 3: Sort and create Dimension objects
        sorted_items = sorted(raw_dims.items(), key=lambda x: x[1]["id"])
        for d_name, d_info in sorted_items:
            self.dimensions[d_name] = Dimension(
                d_name,
                d_info["size"],
                d_info["is_unlimited"],
                parent_group=self,
            )

        # Pass 4: Create variables (skipping only dummy stubs)
        for name, h5ds in datasets_to_process:
            dim_name = name.split("/")[-1]

            # If it's in raw_dims and flagged as a stub, we skip it.
            # Otherwise, whether it's a coordinate, normal data, or a
            # scalar pretending to be a scale, it becomes a Variable!
            is_stub = raw_dims.get(dim_name, {}).get("is_stub", False)

            if not is_stub:
                self.variables[name] = Variable(name, h5ds, parent_group=self)

        # Pass 5: Build subgroups
        for name, group in subgroups_to_process:
            self.groups[name] = Group(group, parent=self)

    @property
    def path(self):
        """The full absolute path of the group.

        :Returns:

            `str`
                The absolute HDF5 path (e.g. '/model/subgroup').

        """
        return self.name


class File(Group):
    """The root netCDF file accessor.

    This class wraps a (subclass of) `pyfive.File`, mapping internal
    HDF5 dimensions, attributes, variables, and subgroups to standard
    netCDF structures.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    _p5netcdf = True

    def __init__(self, dataset):
        """**Initialisation**

        :Parameters:

            dataset:
                The dataset to be read. May be a `str` or
                `pathlib.Path` path, a file-like object (such as
                `io.BufferedReader` or the result of an `fsspec` file
                system file open), or a (subclass of a) `pyfive.File`
                object.

        """
        import pyfive

        if not isinstance(dataset, pyfive.File):
            dataset = pyfive.File(dataset)

        self._h5_file = dataset

        super().__init__(dataset)

    def close(self):
        """Close the file.

        Closes the underlying `pyfive.File` object.

        :Returns:

            `None`

        """
        self._h5_file.close()

    @property
    def filename(self):
        """The name of the file on disk.

        :Returns:

            `str`
                The filename associated with the file handle.

        """
        return self._h5_file.filename
