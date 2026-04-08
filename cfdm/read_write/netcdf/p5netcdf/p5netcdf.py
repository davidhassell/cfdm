from collections.abc import Mapping
from itertools import chain
from math import prod

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

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        value:
            The raw attribute value from the netCDF-4 file.

    :Returns:

            The formatted attribute value adhering to netCDF.

    """
    # Handle strings/bytes immediately
    if isinstance(value, (bytes, np.bytes_)):
        return value.decode("utf-8")

    import pyfive

    if isinstance(value, pyfive.Empty):
        dtype = value.dtype
        if dtype.kind in "SUT":
            return ""

        return np.array([], dtype=value)

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
            isinstance(value, np.array) and value.dtype.kind in "SUT"
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
            f"<p5netcdf.{self.__class__.__name__}: "
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
            group_path = getattr(self.parent, "path", "/")
            if group_path == "/":
                path = f"/{self.name}"
            else:
                path = f"{group_path}/{self.name}"

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

    def __init__(self, name, parent, h5ds, h5ds_attrs):
        """**Initialisation**

        :Parameters:

            name: `str`
                The name of the variable in its parent group.

            parent: `Group` or `File`
                The parent group containing this variable.

            h5ds: (subclass of) `pyfive.Dataset`
                The underlying pyfive dataset object.

            h5ds_attrs: `dict`
                The raw attributes of *h5ds*.

        """
        self._name = name
        self._h5ds = h5ds
        self._parent = parent

        self._dimensions = self._get_dimensions(h5ds_attrs)
        self._attrs = _parse_attributes(h5ds_attrs)

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

    def _get_dimensions(self, h5ds_attrs):
        """Get the variable dimension names.

        Resolves dimension names, handling both standard variables and
        Dimension Scales.

        """
        # Case 1: It's a Dimension Scale itself (no DIMENSION_LIST attribute)
        if h5ds_attrs.get("CLASS") == b"DIMENSION_SCALE":
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

                h5_root = self.parent.root._h5
                dim_dataset = h5_root[ref]
                dim_names.append(dim_dataset.name.split("/")[-1])
            except (KeyError, ValueError, TypeError):
                continue

        return tuple(dim_names)

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
            chunks = self._h5ds.chunks
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
        return self._dimensions

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
            size = prod(self.shape)
            self._size = size

        return size

    def chunking(self):
        """Returns the data chunk shape.

        .. seealso:: `chunks`, `netCDF4.Variable.chunking`

        :Returns:

            `list` or 'str`
                The chunk shape, e.g. ``[5, 6, 7]``. If the data is
                contiguous then ``'contiguous` is returned.

        """
        chunks = self.chunks
        if chunks is None:
            return "contiguous"

        return list(chunks)

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
                group_dims = current_group.dimensions
                if dim_name in group_dims:
                    dims.append(group_dims[dim_name])
                    found = True
                    break

                current_group = current_group.parent

            if not found:
                # Fallback if somehow the dimension could not be found
                # (shouldn't happen in valid files!)
                raise KeyError(
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


class Group(Mapping):
    """A netCDF group.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    __hash__ = None

    def __init__(self, name, parent, root, h5, h5_attrs):
        """**Initialisation**

        :Parameters:

            name: `str`
                The name of the group in its parent group. The root
                group has the name ``''``.

            parent: `Group` or `None`
                The parent group. Set to `None` if there is no parent.

            root: `Group` or `File`
                The root group.

            h5: (subclass of) `pyfive.Group` or (subclass of) `pyfive.File`
                The underlying pyfive object.

        """
        self._name = name
        self._parent = parent
        self._root = root
        self._h5 = h5

        self._attrs = _parse_attributes(h5_attrs)

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
        pv = "" if len(self.variables) == 1 else "s"
        pg = "" if len(self.groups) == 1 else "s"

        path = self.path
        if path == "/":
            path = ""
        else:
            path += ", "

        return (
            f"<p5netcdf.{self.__class__.__name__}: "
            f"{path}{len(self.variables)} variable{pv}, "
            f"{len(self.groups)} sub-group{pg}>"
        )

    def __str__(self):
        """Called by the `str` built-in function."""
        lines = [repr(self)]

        # Attributes
        if self.attrs:
            lines.append("Attributes:")
            lines.extend(
                f"    {name}: {value!r}" for name, value in self.attrs.items()
            )

        # Groups
        if self.groups:
            lines.append("Groups:")
            lines.extend(f"    {name}" for name in self.groups)

        # Dimensions
        if self.dimensions:
            lines.append("Dimensions:")
            lines.extend(
                f"    {name}: {dim}" for name, dim in self.dimensions.items()
            )

        # Variables
        if self.variables:
            lines.append("Variables:")
            lines.extend(
                f"    {name}: {var!r}" for name, var in self.variables.items()
            )

        return "\n".join(lines)

    def _parse_group_structure(self, root):
        """Parse the group structure.

        Parses variables, dimensions, and subgroups, recursively.

        :Parameters:

            root: `Group` or `File`
                The root group.

        :Returns:

            `None`

        """
        import pyfive

        raw_dims = {}
        subgroups = []
        datasets = []
        dataset_attrs = {}

        # Categorise objects without double-reading items from the
        # dataset
        for name, h5 in self._h5.items():
            if isinstance(h5, pyfive.Group):
                subgroups.append((name, h5))
            elif isinstance(h5, pyfive.Dataset):
                datasets.append((name, h5))

        # Extract dimension scales (strictly ignoring scalars)
        for name, h5ds in datasets:
            shape = h5ds.shape
            attrs = h5ds.attrs
            dataset_attrs[name] = attrs

            if shape and attrs.get("CLASS") == b"DIMENSION_SCALE":
                # Get ID: Use `None` if missing to push to end of sort
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
            self._dimensions[d_name] = Dimension(
                name=d_name,
                size=d_info["size"],
                isunlimited=d_info["is_unlimited"],
                parent=self,
            )

        # Create variables (skipping internal netCDF stubs)
        for name, h5ds in datasets:
            dim_name = name.split("/")[-1]

            # If it's in 'raw_dims' and flagged as a stub then skip
            # it. Otherwise - whether it's a coordinate variable,
            # normal data, or a scalar pretending to be a scale - it
            # becomes a Variable.
            is_stub = raw_dims.get(dim_name, {}).get("is_stub", False)

            if not is_stub:
                self._variables[name] = Variable(
                    name=name,
                    parent=self,
                    h5ds=h5ds,
                    h5ds_attrs=dataset_attrs[name],
                )

        # Create subgroups
        for name, group in subgroups:
            self._groups[name] = Group(
                name=name,
                parent=self,
                root=root,
                h5=group,
                h5_attrs=group.attrs,
            )

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
                (i.e. this is the root group).

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
            path = self._h5.name
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


class File(Group):
    """A netCDF dataset.

    A `File` is a collection of dimensions, groups, variables and
    attributes which describe the meaning of the data and metadata
    stored in a netCDF dataset.

    **Performance**

    `p5netcdf` is "structure- and attribute-eager", meaning that
    during `File` instantiation, the entire netCDF-4 group, variable,
    and dimension structure is parsed; along with all group and
    variable attributes. Variable data access is always via access to
    an underlying (subclass of a) `pyfive.Dataset` object. Some
    `Variable` and `Group` properties and methods also access an
    underlying (subclass of a) `pyfive.Variable` or `pyfive.Group`
    object, but only for the first request, after which the result is
    cached (e.g. the first time `Variable.shape` is it requested it is
    retrieved from the underlying (subclass of a) `pyfive.Variable`,
    and subsequent requests access the shape cached inside the
    `Variable` object).

    **Instantiation from a `pyfive`, or an implementation of the
      `pyfive` API**

    A `File` can be instantiated from pre-existing `pyfive.File`
    object, or a subclass of a `pyfive.File` object defined by an
    implementation of the `pyfive` API.

    An implementation of the `pyfive` API (e.g. `my_pyfive`) must
    provide `my_pyfive.File`, `my_pyfive.Group`, and
    `my_pyfive.Dataset` classes that inherit from their respective
    `pyfive` base classes; and must expose following attributes and
    methods:

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

    .. versionadded:: (cfdm) NEXTVERSION

    """

    _p5netcdf = True

    def __init__(self, dataset, mode="r", pyfive_options=None):
        """**Initialisation**

        :Parameters:

            dataset:
                The netCDF dataset to be read.

                May be a `str` or `pathlib.Path` path, a file-like
                object (such as `io.BufferedReader` or the result of
                an `fsspec` file system open), a `pyfive.File` object,
                or a subclass of a `pyfive.File` object.

                If *dataset* is path or file-like obect, then a
                `pyfive.File` object is automatically created
                internally. E.g ``p5netcdf.File('file.nc')`` is
                equivalent to ``py5 = pyfive.File('file.nc');
                p5netcdf.File(py5)`` (see `close`).

            mode: `str`, optional
                The access mode used when using `pyfive.File` to open
                the *dataset*. The only allowed value is ``'r'``
                (read-only), and this is the default. Ignored if
                *dataset* is already a (subclass of a) `pyfive.File`
                object.

            pyfive_options: `dict` or `None`, optional
                Keyword arguments that are passed to `pyfive.File` to
                be used when opening the *dataset*. Setting to `None`
                (the default) is equivalent to providing an empty
                dictionary. Ignored if *dataset* is already a
                (subclass of a) `pyfive.File` object.

        """
        import pyfive

        if isinstance(dataset, pyfive.File):
            h5 = dataset
            self._is_owner = False
        else:
            if pyfive_options is None:
                pyfive_options = {}

            h5 = pyfive.File(dataset, mode="r", **pyfive_options)
            self._is_owner = True

        attrs = h5.attrs
        if "_NCProperties" not in attrs:
            self.close()
            raise ValueError(
                f"{dataset!r} is not a valid netCDF4 file "
                "(missing _NCProperties attribute)."
            )

        super().__init__(
            name="", parent=None, root=self, h5=h5, h5_attrs=attrs
        )

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context and ensure the file is closed."""
        self.close()

    def close(self):
        """Close the dataset.

        Closes the underlying netCDF dataset, but not if the dataset
        was originally defined by a (subclass of a) `pyfive.File`
        object.

        :Returns:

            `None`

        """
        if self._is_owner:
            self._h5.close()

    @property
    def filename(self):
        """The name of the file on disk.

        :Returns:

            `str`
                The filename of the dataset.

        """
        filename = getattr(self, "_filename", None)
        if filename is None:
            filename = self._h5.filename
            self._filename = filename

        return filename
