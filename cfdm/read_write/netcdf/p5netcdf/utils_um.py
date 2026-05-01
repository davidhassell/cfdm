# --------------------------------------------------------------------
# PP/UM
# --------------------------------------------------------------------
def ppfive_open(dataset, options):
    """Open a dataset with `ppfive` library.

    .. versionadded:: (cfdm) NEXTVERSION

    :Parameters:

        dataset:
            The definition of the netCDF dataset to be read. One of:

            * string-like (such as `str` or `pathlib.Path`)
            * file-like (such as `io.BufferedReader` or the result
                         of an `fsspec` file system open)
            * directory-like (such as `fsspec.mapping.FSMap`)

             An exception is raised if the library can't interpret the
             *dataset*.

        options: `dict`
            Additional keyword parameters to pass to `ppfive.File`.

    :Returns:

        (`ppfive.File`, `dict`, library)
            The opened dataset, the dataset's global attributes, and
            the `ppfive` library itself.

    """
    import ppfive

    nc = ppfive.File(dataset, mode="r", **options)
    return nc, nc.attrs, ppfive
