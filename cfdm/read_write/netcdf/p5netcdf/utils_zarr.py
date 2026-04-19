from .utils import NetCDFError


def zarr_dtype(variable):
    """The numpy data type of the variable."""
    dtype = variable._var.dtype
    return dtype  # TODO


def zarr_dimension_maps(group):
    """Populate the root dimension map dictionaries.

    Stores the dictionary of the dimensions defined in *group*, and
    for all sub-groups. For instance::

       {'/': {'bounds2': <netcdf.Dimension: /bounds2, size=2>},
        '/forecast': {'lon': <netcdf.Dimension: /forecast/lon, size=8, unlimited>},
        '/forecast/model': {'lat': <netcdf.Dimension: /forecast/model/lat, size=5>}}
        '/forecast/model2': {}}

    Stores the tuple of the dimensions for all variables in *group*,
    and all variables in all sub-groups. For instance::

       {'/forecast/lon': (<netcdf.Dimension: /forecast/lon, size=8, unlimited>,),
        '/forecast/lon_bnds': (<netcdf.Dimension: /forecast/lon, size=8, unlimited>,
                               <netcdf.Dimension: /bounds2, size=2>),
        '/forecast/model/lat': (<netcdf.Dimension: /forecast/model/lat, size=5>,),
        '/forecast/model/lat_bnds': (<netcdf.Dimension: /forecast/model/lat, size=5>,
                                     <netcdf.Dimension: /bounds2, size=2>),
        '/forecast/model/q': (<netcdf.Dimension: /forecast/model/lat, size=5>,
                              <netcdf.Dimension: /forecast/lon, size=8, unlimited>),
        '/forecast/model2/tas': (<netcdf.Dimension: /forecast/model/lat, size=5>,
                                 <netcdf.Dimension: /forecast/lon, size=8, unlimited>),
        '/time': ()}

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        group: `Group`
            The group object.

    :Returns:

        `None`

    """
    group_path = group.path
    root = group.root
    group_to_dims = root._group_to_dims
    variable_to_dims = root._variable_to_dims
    group_dimension_search = (  # root._group_dimension_search
        "closest_ancestor"  # root._group_dimension_search
    )

    # Initialise the mapping from this group to its `Dimension`
    # objects. Use 'setdefault' because a previous call to
    # `zarr_dimension_maps` might already have done this.
    group_to_dims.setdefault(group_path, {})

    # Loop over variables in this group, sorted by variable name.

    # for v in dict(sorted(group.arrays())).values():
    for v in group.variables.values():
        # Initialise mapping from the variable to its Dimension
        # objects
        var_path = v.path
        variable_to_dims[var_path] = ()

        raw_dimension_names = zarr_raw_dimension_names(v)
        if not raw_dimension_names:
            # A scalar variable has no dimensions
            continue

        # Loop over this variable's dimension names
        for name, size in zip(raw_dimension_names, v.shape):
            name_split = name.split("/")
            basename = name_split[-1]

            # --------------------------------------------------------
            # Define 'g' as the absolute path name of the group in
            # which to register the logical dimension object for this
            # dimension.
            #
            # Which group is defined will depend on the nature of the
            # dimension's 'name'.
            # --------------------------------------------------------
            if "/" not in name:
                # ----------------------------------------------------
                # Raw dimension name which contains no '/' characters
                #
                # The behaviour depends on the search algorithm
                # defined by 'group_dimension_search'.
                #
                # E.g. "dim"
                # ----------------------------------------------------
                if group_dimension_search in (
                    "closest_ancestor",
                    "furthest_ancestor",
                ):
                    # Find the names of all ancestor groups, in the
                    # appropriate order for searching.
                    group_split = group_path.split("/")
                    ancestor_names = [
                        "/".join(group_split[:n])
                        for n in range(1, len(group_split))
                    ]
                    ancestor_names[0] = "/"
                    # E.g. if the current group is /g1/g2/g3 then the
                    #      ancestor group names are [/, /g1, /g1/g2]

                    if group_dimension_search == "closest_ancestor":
                        # "closest_ancestor" searching requires the
                        # ancestor group order to be reversed,
                        # e.g. [/g1/g2, /g1, /]
                        ancestor_names = ancestor_names[::-1]

                    # Search through the ancestors in order, stopping
                    # if we find a matching dimension.
                    found_dim_in_ancestor = False
                    for g in ancestor_names:
                        zarr_dim = group_to_dims[g].get(basename)
                        if zarr_dim is not None and zarr_dim.size == size:
                            # Found a dimension in this ancestor group
                            # 'g' with the right name and size
                            found_dim_in_ancestor = True
                            break

                    if not found_dim_in_ancestor:
                        # Dimension 'basename' could not be matched to
                        # any ancestor group dimensions, so define it
                        # in the current group.
                        g = group_path

                elif group_dimension_search == "local":
                    # Assume that the dimension is different to any
                    # with same name and size defined in any ancestor
                    # group.
                    g = group_path

                else:
                    raise NetCDFError(
                        "Bad 'group_dimension_search' value: "
                        f"{group_dimension_search!r}"
                    )
            else:
                # ----------------------------------------------------
                # Raw dimension name contains '/' characters
                # ----------------------------------------------------
                if name.endswith("/"):
                    raise NetCDFError(
                        "Dimension names can't end with '/': "
                        f"dataset={group.dataset_name()} "
                        f"variable={var_path} "
                        f"dimension_name={name}"
                    )

                g = "/".join(name_split[:-1])
                try:
                    g = group[g].path
                except KeyError:
                    raise NetCDFError("Bad dimension name TODO")

            # TODO
            zarr_dim = None
            if g in group_to_dims:
                # Group 'g' is already registered in the mapping
                zarr_dim = group_to_dims[g].get(basename)
                if zarr_dim is not None:
                    # Dimension 'basename' is already registered in
                    # group 'g'
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

                zarr_dim = parent._create_dimension(basename, size, False)
                group_to_dims[g][basename] = zarr_dim

            # Map the variable to the `Dimension` object
            variable_to_dims[var_path] += (zarr_dim,)

    # ----------------------------------------------------------------
    # Recursively scan all sub-groups
    # ----------------------------------------------------------------
    for g in group.groups.values():
        zarr_dimension_maps(g)


def zarr_raw_dimension_names(variable):
    """Return the raw dimension names for a variable.

    :Parameters:

        var: `Variable`
            The variable object.

    :Returns:

        `list` of `str`
            The raw dimension names stored in the embdedded
            `zarr.Variable`. A scalar variable will have an empty
            list.

    """
    metadata = variable._var.metadata
    zarr_format = metadata.zarr_format
    match zarr_format:
        case 3:
            dimensions = metadata.dimension_names
        case 2:
            dimensions = metadata.attributes.get("_ARRAY_DIMENSIONS")
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


def zarr_open(root, dataset):
    """Open a dataset with `zarr`.

    :Parameters:

        root: `p5netcdf.File`
            The root group.

        dataset:
            The dataset. May be a string-valued path, a file-like
            object, or a directory-like object.

    :Returns:

        `zarr.Group`

    """
    import zarr

    nc = zarr.open(dataset, mode="r")
    root._lib = zarr
    return nc, nc.attrs


def zarr_parse_group_structure(group):
    """TODO.

    group: `Group` or `File`

    """
    # Create variables in this group
    for name, var in dict(group._grp.arrays()).items():
        group._create_variable(name, var, var.attrs)

    # Create subgroups
    for name, grp in dict(group._grp.groups()).items():
        group._create_group(name, grp, grp.attrs)

    # Starting from the root group, i) create dimensions in all
    # groups, and ii) attach dimensions to each variable.
    if group.isroot:
        root = group
        root._group_to_dims = {}
        root._variable_to_dims = {}
        zarr_dimension_maps(root)

        for path, dims in root._group_to_dims.items():
            group = root[path]
            for name, dim in dims.items():
                group._dimensions[name] = dim

            for name, var in group.variables.items():
                var._dims = root._variable_to_dims[var.path]

        del root._group_to_dims, root._variable_to_dims
