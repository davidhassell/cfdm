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

    A domain topology construct describes logically and explicitly the
    contiguity of domain cells indexed by a single domain axis
    construct, where two cells are described as contiguous if and only
    if they share at least one common boundary vertex. A domain
    construct allows contiguity to be ascertained without comparison
    of boundary vertices, which may be co-located for non-contiguous
    cells.

    A domain topology construct contains an array that spans a single
    domain axis construct with the addition of an extra dimension that
    indexes the cell bounds for the corresponding coordinates.
    Identical array values indicate that the corresponding cell
    vertices map to the same node of the domain, but otherwise the
    array values are arbitrary.

    In CF-netCDF a domain topology can only be provided for a domain
    defined by a UGRID mesh topology variable, supplied by a node
    connectivity variable, such as is named by a
    "face_node_connectivity" attribute. The indices contained in a
    node connectivity variable may be used directly to create a domain
    topology construct but the CF data model attaches no significance
    to the values, other than the fact that not all indices are the
    same.

    See CF Appendix I "The CF Data Model".

    **NetCDF interface**

    {{netCDF variable}}

    .. versionadded:: (cfdm) TODOUGRIDVER

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
        """A full description of the domain topology construct.

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

    @_inplace_enabled(default=False)
    def normalise_bounds_connectivity(self, start_index=0, inplace=False):
        """TODOUGRID
        
        .. versionadded:: (cfdm) TODOUGRIDVER

        :Returns:

            TODOUGRID

        """
        if self.get_connectivity_type(None) != 'bounds':
            raise ValueError("TODOUGRID")
        
        d = _inplace_enabled_define_and_cleanup(self)

        data = d.array
        n, b = np.where(~np.ma.getmaskarray(data))
        data[n, b] = np.unique(data[n, b], return_inverse=True)[1]

        if start_index:
            data += start_index
            
        d.set_data(data)

        return d
