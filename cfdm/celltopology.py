from . import core, mixin


class CellTopology(
    mixin.NetCDFVariable,
    mixin.Topology,
    mixin.Files,
    core.CellTopology,
):
    """A cell  topology construct of the CF data model.

    TODOUGRID

    See CF Appendix I "The CF Data Model".

    **NetCDF interface**

    {{netCDF variable}}

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self,
        topology=None,
        properties=None,
        data=None,
        source=None,
        copy=True,
        _use_data=True,
    ):
        """**Initialisation**

        :Parameters:

            {{topology: `str`, optional}}

            {{init properties: `dict`, optional}}

                *Parameter example:*
                  ``{'long_name': 'Maps faces to faces'}``

            {{init data: data_like, optional}}

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            topology=topology,
            properties=properties,
            data=data,
            source=source,
            copy=copy,
            _use_data=_use_data,
        )

        self._initialise_netcdf(source)
        self._initialise_original_filenames(source)

    def __getitem__(self, indices):
        """Return a subspace defined by indices.

        f.__getitem__(indices) <==> f[indices]

        Indexing follows rules that are very similar to the `numpy`
        indexing rules, the only difference being:

        * An integer index i takes the i-th element but does not
          reduce the rank by one.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Returns:

            `{{class}}`
                The subspaced construct.

        """
        if isinstance(indices, tuple):
            indices = indices * 2
        else:
            indices = (indices, indices)

        return super().__getitem__(indices)

    @property
    def sparse_array(self):
        """Return an independent `scipy` sparse array of the data.

        An `AttributeError` is raised if a sparse array representation
        is not available.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `array`

        :Returns:

                An independent `scipy` sparse array of the data.

        **Examples**

        >>> from scipy.sparse import issparse
        >>> issparse(c.sparse_array)
        True

        """
        data = self.get_data(None, _units=None, _fill_value=None)
        if data is None:
            raise AttributeError(
                "Can't get sparse array representation when there is no data"
            )

        return data.sparse_array

    def dump(
        self,
        display=True,
        _omit_properties=None,
        _key=None,
        _level=0,
        _title=None,
        _axes=None,
        _axis_names=None,
    ):
        """A full description of the cell topology construct.

        Returns a description of all properties, including those of
        components, and provides selected values of all data arrays.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            {{returns dump}}

        """
        if _title is None:
            _title = "Cell Topology: " + self.identity(default="")

        return super().dump(
            display=display,
            _key=_key,
            _omit_properties=_omit_properties,
            _level=_level,
            _title=_title,
            _axes=_axes,
            _axis_names=_axis_names,
        )
