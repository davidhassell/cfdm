"""Utilities for the backend `xarray` backend `p5netcdf`."""


# --------------------------------------------------------------------
# xarray
# --------------------------------------------------------------------
def xarray_parse_group_structure(group):
    """Parse the group structure for the `xarray` backend.

    Parses variables, dimensions, and sub-groups, recursively.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        group: `Group` or `File`
            The group to be parsed.

    :Returns:

        `None`

    """
    # 1. Create dimensions in this group
    grp = group._grp

    # Find which dimensions are actually defined in this group, as
    # opposed to being copied from an ancestor group.
    maps = grp.sizes.mapping.maps
    local_map = maps[0]
    ancestor_maps = maps[1:]
    defined_dims = {
        name: size
        for name, size in local_map.items()
        if not any(name in m for m in ancestor_maps)
    }

    grp = grp.to_dataset(inherit=False)
    for name, size in defined_dims.items():
        unlimited = name in grp.encoding.get("unlimited_dims", ())
        group._create_dimension(name, size, unlimited)

    # 2. Create variables in this group
    for name, var in grp.variables.items():
        group._create_variable(name, var, var.attrs)

    # 3. Recursively create subgroups
    for name, grp in group._grp.children.items():
        group._create_group(name, grp, grp.attrs)


def xarray_open(dataset, options):
    """Open a dataset with `xarray`.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        dataset:
            The definition of the netCDF dataset to be read. One of:

            * string-like (such as `str` or `pathlib.Path`)
            * file-like (such as `io.BufferedReader` or the result
                         of an `fsspec` file system open)
            * directory-like (such as `fsspec.mapping.FSMap`)

             An exception is raised if the *dataset* can't be
             interpreted.

        options: `dict`
            Additional keyword parameters to pass to
            `xarray.open_datatree`.

    :Returns:

        (`xarray.DataTree`, `dict`, library)
            The opened dataset, the dataset's global attributes, and
            the `xarray` library itself.

    """
    import xarray

    nc = xarray.open_datatree(
        dataset, mask_and_scale=False, decode_cf=False, **options
    )
    attrs = nc.attrs
    return nc, attrs, xarray
