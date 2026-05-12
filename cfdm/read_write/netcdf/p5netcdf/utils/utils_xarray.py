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
    # Create dimensions in this group
    grp = group._grp
    for name in get_local_dims(grp):
        unlimited = name in grp.encoding.get('unlimited_dims', ())
        group._create_dimension(name,  grp.sizes[name], unlimited)
        
    # Create variables in this group
    grp = group._grp.to_dataset(inherit=False)   
    for name, var in grp.variables.items():
        group._create_variable(name, var, var.attrs)

    # Recursively create subgroups
    for name, grp in group._grp.children.items():
        group._create_group(name, grp,  grp.attrs)

def get_local_dims(grp):
    """
    Returns dimensions that are defined in this node 
    and have NOT appeared in any parent node's .dims.
    """
    local_dims = set(grp.sizes)
    
    # Walk up to the root
    current = grp.parent
    while current is not None:
        # Subtract any dimensions already 'known' by ancestors
        local_dims -= set(current.sizes)
        current = current.parent
        
    return local_dims

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

    nc = xarray.open_datatree(dataset, mask_and_scale=False, **options)
    attrs = nc.attrs
    return nc, attrs, xarray

