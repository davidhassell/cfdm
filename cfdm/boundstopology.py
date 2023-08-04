from . import core, mixin
from .decorators import _inplace_enabled, _inplace_enabled_define_and_cleanup


class BoundsTopology(
    mixin.NetCDFVariable,
    mixin.Topology,
    mixin.Files,
    core.BoundsTopology,
):
    """A bounds topology construct of the CF data model.

    TODOUGRID

    See CF Appendix I "The CF Data Model".

    **NetCDF interface**

    {{netCDF variable}}

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self,
        cell_type=None,
        properties=None,
        data=None,
        source=None,
        copy=True,
        _use_data=True,
    ):
        """**Initialisation**

        :Parameters:

            {{cell_type: `str`, optional}}

            {{init properties: `dict`, optional}}

                *Parameter example:*
                  ``{'long_name': 'Maps faces to faces'}``

            {{init data: data_like, optional}}

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            cell_type=cell_type,
            properties=properties,
            data=data,
            source=source,
            copy=copy,
            _use_data=_use_data,
        )

        self._initialise_netcdf(source)
        self._initialise_original_filenames(source)

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
        """A full description of the bounds topology construct.

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
            _title = "Bounds Topology: " + self.identity(default="")

        return super().dump(
            display=display,
            _key=_key,
            _omit_properties=_omit_properties,
            _level=_level,
            _title=_title,
            _axes=_axes,
            _axis_names=_axis_names,
        )

    @_inplace_enabled(default=False)
    def normalise(self, start_index=0, inplace=False):
        """TODOUGRID.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            start_index: `int`, optional
                The start index for bounds identification in the
                normalised data. Must be ``0`` (the default) or
                ``1``.

                If *start_index* is ``0`` then the range of values in
                the normalised data will ``[0, N-1]``, where ``N`` is
                the number of unique values in the data (i.e. the
                number of distinct bounds).

                If *start_index* is ``1`` then the range of values in
                the normalised data will ``[1, N]``.

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The normailised bounds topology construct, or `None`
                if the operation was in-place.

        **Examples*

        >>> data = {{package}}.Data(
        ...   [[1, 4, 5, 2], [4, 10, 1, -99], [122, 123, 106, 105]]
        ... )
        >>> data[1, 3] = {{package}}.masked
        >>> b = {{package}}.{{class}}(data=data)
        >>> print(b.array)
        [[1 4 5 2]
         [4 10 1 --]
         [122 123 106 105]]
        >>> c = b.normalise()
        >>> print(c.array)
        [[0 2 3 1]
         [2 4 0 --]
         [7 8 6 5]]
        >>> (c.array == b.normalise().array).all()
        True

        >>> d = b.normalise(start_index=1)
        >>> print(d.array)
        [[1 3 4 2]
         [3 5 1 --]
         [8 9 7 6]]
        >>> (d.array == c.array + 1).all()
        True

        """
        import numpy as np

        d = _inplace_enabled_define_and_cleanup(self)

        data = d.array
        n, b = np.where(~np.ma.getmaskarray(data))
        data[n, b] = np.unique(data[n, b], return_inverse=True)[1]

        if start_index:
            if start_index != 1:
                raise ValueError("TODOUGRID")

            data += start_index

        d.set_data(data, copy=False)
        return d
