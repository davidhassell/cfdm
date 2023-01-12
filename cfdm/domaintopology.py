from . import core, mixin


class DomainTopology(
    mixin.NetCDFVariable, mixin.PropertiesData, core.DomainTopology
):
    """A domain topology construct of the CF data model.

    A domain topology construct describes explicitly the connectivity
    of domain cells indexed by a single domain axis construct. When
    two cells are connected, operations on the data stored on them may
    be assumed to be continuous across their common boundary.

    The domain topology array must be a symmetric matrix (i.e. a
    square matrix that is equal to its transpose), and is interpreted
    in a boolean context. The diagonal elements of this array must be
    False.

    See CF Appendix I "The CF Data Model".

    **NetCDF interface**

    {{netCDF variable}}

    .. versionadded:: (cfdm) 1.11.0.0

    """

    def __init__(
        self,
        properties=None,
        data=None,
        source=None,
        copy=True,
        _use_data=True,
    ):
        """**Initialisation**

        :Parameters:

            {{init properties: `dict`, optional}}

                *Parameter example:*
                  ``{'long_name': 'Maps faces to faces'}``

            {{init data: data_like, optional}}

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            properties=properties,
            data=data,
            source=source,
            copy=copy,
            _use_data=_use_data,
        )

        self._initialise_netcdf(source)

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
        """A full description of the domain topology construct.

        Returns a description of all properties, including those of
        components, and provides selected values of all data arrays.

        .. versionadded:: (cfdm) 1.11.0.0

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            {{returns dump}}

        """
        if _title is None:
            _title = "Domain Topology: " + self.identity(default="")

        return super().dump(
            display=display,
            _key=_key,
            _omit_properties=_omit_properties,
            _level=_level,
            _title=_title,
            _axes=_axes,
            _axis_names=_axis_names,
        )
