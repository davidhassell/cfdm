from . import core, mixin


class ErrorCorrelationParameter(
    mixin.NetCDFVariable,
    mixin.PropertiesData,
    mixin.Files,
    core.ErrorCorrelationParameter,
):
    """TODOU A cell bounds component.

    Specifically, a cell bounds component of a coordinate or domain
    ancillary construct of the CF data model.

    An array of cell bounds spans the same domain axes as its
    coordinate array, with the addition of an extra dimension whose
    size is that of the number of vertices of each cell. This extra
    dimension does not correspond to a domain axis construct since it
    does not relate to an independent axis of the domain. Note that,
    for climatological time axes, the bounds are interpreted in a
    special way indicated by the cell method constructs.

    In the CF data model, a bounds component does not have its own
    properties because they can not logically be different to those of
    the coordinate construct itself. However, it is sometimes desired
    to store attributes on a CF-netCDF bounds variable, so it is also
    allowed to provide properties to a bounds component.

    **NetCDF interface**

    {{netCDF variable}}

    The name of the trailing netCDF dimension spanned by bounds (which
    does not correspond to a domain axis construct) may be accessed
    with the `nc_set_dimension`, `nc_get_dimension`,
    `nc_del_dimension`, and `nc_has_dimension` methods.

    {{netCDF variable group}}

    {{netCDF dataset chunks}}

    .. versionadded:: (cfdm) 1.7.0

    """

    def dump(
        self,
        data=None,
        display=True,
        _title=None,
        _prefix="",
        _level=0,
        _create_title=True,
        _omit_properties=None,
    ):
        """A full description of the bounds component.

        Returns a description of all properties and provides selected
        values of all data arrays.

        .. versionadded:: (cfdm) 1.7.0

        :Parameters:

            {{data: `bool` or `None`, optional}}

                .. versionadded:: (cfdm) 1.13.0.0

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            {{returns dump}}

        """
        if _create_title and _title is None:
            _title = "Error-correlation parameter: " + self.identity(
                default=""
            )

        return super().dump(
            data=data,
            display=display,
            _omit_properties=_omit_properties,
            _prefix=_prefix,
            _level=_level,
            _title=_title,
            _create_title=_create_title,
        )
