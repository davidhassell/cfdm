from . import mixin
from . import core


class InterpolationCoefficient(
    mixin.NetCDFVariable, mixin.PropertiesData, core.abstract.PropertiesData
):
    """An interpolation coefficient variable required to uncompress a sampled array TODO.

    TODO

    **NetCDF interface**

    The netCDF variable name of the list variable may be accessed with
    the `nc_set_variable`, `nc_get_variable`, `nc_del_variable` and
    `nc_has_variable` methods.

    The netCDF variable group structure may be accessed with the
    `nc_set_variable`, `nc_get_variable`, `nc_variable_groups`,
    `nc_clear_variable_groups` and `nc_set_variable_groups` methods.

    .. versionadded:: (cfdm) TODO

    """

    def __init__(
        self,
        properties=None,
        data=None,
        dimensions={},
        source=None,
        copy=True,
        _use_data=True,
    ):
        """**Initialization**

        :Parameters:

            {{init properties: `dict`, optional}}

                *Parameter example:*
                  ``properties={"long_name": "interpolation coefficients"}``

            {{init data: data_like, optional}}

            dimensions: `dict`, optional
                Mapping of interpolation dimensions in the parent tie
                point variable to dimensions of *data*.

                *Parameter example:*
                  If the interpolation dimensions in the parent tie point
                  variable have positions 1 and 2, corresponding
                  repectively to interpolation coefficient dimensions 1
                  and 0, then use ``dimensions={1: 1, 2: 0}``.

            source: optional
                Initialize the properties and data from those of *source*.

                {{init source}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            properties=properties,
            data=data,
            dimensions=dimensions,
            source=source,
            copy=copy,
            _use_data=_use_data,
        )

        self._initialise_netcdf(source)

    def dump(
        self,
        display=True,
        _key=None,
        _title=None,
        _create_title=True,
        _prefix="",
        _level=0,
        _omit_properties=None,
        _axes=None,
        _axis_names=None,
    ):
        """A full description of the interpolation coefficient variable.

        Returns a description of all properties, including those of
        components, and provides selected values of all data arrays.

        .. versionadded:: (cfdm) TODO

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            {{returns dump}}

        """
        if _create_title and _title is None:
            _title = "InterpolationCoefficient: " + self.identity(default="")

        return super().dump(
            display=display,
            _key=_key,
            _omit_properties=_omit_properties,
            _prefix=_prefix,
            _level=_level,
            _title=_title,
            _create_title=_create_title,
            _axes=_axes,
            _axis_names=_axis_names,
        )


# --- End: class
