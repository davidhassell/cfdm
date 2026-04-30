# --------------------------------------------------------------------
# netCDF4
# --------------------------------------------------------------------
def netCDF4_parse_group_structure(group):
    """Parse the group structure for the `netCDF4` backend.

    Parses variables, dimensions, and sub-groups, recursively.

    :Parameters:

        group: `Group` or `File`
            The group to be parsed.

    :Returns:

        `None`

    """
    # Create dimensions in this group
    for name, dim in group._grp.dimensions.items():
        group._create_dimension(name, dim.size, dim.isunlimited())

    # Create variables in this group
    for name, var in group._grp.variables.items():
        attrs = {attr: var.getncattr(attr) for attr in var.ncattrs()}
        group._create_variable(name, var, attrs)

    # Recursively create subgroups
    for name, grp in group._grp.groups.items():
        attrs = {attr: grp.getncattr(attr) for attr in grp.ncattrs()}
        group._create_group(name, grp, attrs)


def netCDF4_open(dataset, options):
    """Open a dataset with `netCDF4` library.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        dataset:
            The definition of the netCDF dataset to be read. One of:

            * string-like (such as `str` or `pathlib.Path`)
            * file-like (such as `io.BufferedReader` or the result
                         of an `fsspec` file system open)
            * directory-like (such as `fsspec.mapping.FSMap`)

             An exception is raised if the library can't interpret the
             dataset definition.
    
        options: `dict`
            Additional keyword parameters to pass to
            `netCDF4.Dataset`.

    :Returns:

        (`netCDF4.Dataset`, `dict`, library)
            The opened dataset, the dataset's global attributes, and
            the `netCDF4` library itself.

    """
    import netCDF4

    nc = netCDF4.Dataset(dataset, mode="r", **options)
    nc.set_auto_maskandscale(False)
    attrs = {attr: nc.getncattr(attr) for attr in nc.ncattrs()}
    return nc, attrs, netCDF4


# --------------------------------------------------------------------
# netcdf_file
# --------------------------------------------------------------------
def netcdf_file_parse_group_structure(group):
    """Parse the group structure for the `netcdf_file` backend.

    Parses variables, dimensions, and sub-groups, recursively.

    :Parameters:

        group: `Group` or `File`
            The group to be parsed.

    :Returns:

        `None`

    """
    # Create dimensions in this group
    for name, size in group._grp.dimensions.items():
        group._create_dimension(name, size, isunlimited=False)

    # Create variables in this group
    for name, var in group._grp.variables.items():
        group._create_variable(name, var, var._attributes)


def netcdf_file_open(dataset, options):
    """Open a dataset with `netcdf_file` library.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        dataset:
            The definition of the netCDF dataset to be read. One of:

            * string-like (such as `str` or `pathlib.Path`)
            * file-like (such as `io.BufferedReader` or the result
                         of an `fsspec` file system open)
            * directory-like (such as `fsspec.mapping.FSMap`)

             An exception is raised if the library can't interpret the
             dataset definition.

        options: `dict`
            Additional keyword parameters to pass to `netcdf_file`.

    :Returns:

        (`scipy.io.netcdf_file`, `dict`, library)
            The opened dataset, the dataset's global attributes, and
            the `netcdf_file` library itself.

    """
    from scipy.io import netcdf_file

    nc = netcdf_file(dataset, mode="r", mmap=True, **options)
    attrs = nc._attributes
    return nc, attrs, netcdf_file


def netcdf_file_close(root):
    """Close the dataset opened with `netcdf_file`.

    It is assumed, but not checked, that the backend is indeed
    `netcdf_file`.
    
    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        root: `p5netcdf.File`
            The root group.

    :Returns:

        `None`

    """
    # We can't close a scipy.io.netcdf_file instance opened with
    # mmap=True when any variable still exists, or when an array
    # referring to a variable's data still exists (see
    # scipy.io.netcdf_file docs for details). So, rather than
    # attempting to hunt down all such references (messy!), the hack
    # of setting the '_mm_buf' attribute to `None` allows the file to
    # be closed. We get away with this because we know that we've
    # copied all memory mapped data into memory inside
    # `Variable.__getitem__`.
    root._grp._mm_buf = None

    root._grp.close()


def netcdf_file_dtype(variable):
    """The numpy data type of variable opened with `netcdf_file`.

    It is assumed, but not checked, that the backend is indeed
    `netcdf_file`.
    
    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        variable: `p5netcdf.Variable`
            The variable.

    :Returns:

        `numpy.dtype`

    """
    return variable._var[(slice(0, 1),) * len(variable.shape)].flat[0].dtype
