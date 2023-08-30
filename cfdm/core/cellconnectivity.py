from . import abstract


class CellConnectivity(abstract.Topology):
    """A cell connectivity construct of the CF data model.

    A cell connectivity construct defines explicitly how cells
    arranged in two or three dimensions in real space but indexed by a
    single domain (discrete) axis are connected. Connectivity can only
    be provided when the domain axis construct also has a domain
    topology construct, and two cells can only be connected if they
    also have a topological relationship. For instance, the
    connectivity of two-dimensional face cells could be characterised
    by whether or not they have shared edges, where the edges are
    defined by connected nodes of the domain topology construct.

    The cell connectivity construct consists of an array recording the
    connectivity, and properties to describe the data. There must be a
    property indicating the condition by which the connectivity is
    derived from the domain topology. The array spans the domain axis
    construct with the addition of a ragged dimension that indexes a
    unique identity for the cell as well as the identities of all
    other cells to which it is connected. The cell identity must be
    recorded in the first element of the ragged dimension, otherwise
    the order is arbitrary. Note that the connectivity array for point
    cells is, by definition, equivalent to the array of the domain
    topology construct.

    When cell connectivity constructs are present they are considered
    to be definitive and must be used in preference to the
    connectivities implied by inspection of any other constructs,
    apart from the domain topology construct, which are not guaranteed
    to be the same.

    In CF-netCDF a cell topology construct can only be provided by a
    UGRID mesh topology variable. The construct array is supplied
    either indirectly by any of the UGRID variables that are used to
    define a domain topology construct, or directly by the UGRID
    "edge_edge_connectivity" variable (for edge cells) or
    "face_face_connectivity" variable (for face cells). In the direct
    case, the integer indices contained in the UGRID variable may be
    used as the cell identities, although the CF data model attaches
    no significance to the values other than the fact that some values
    are the same as others.
    
    Restricting the types of connectivity to those implied by the
    geospatial topology of the cells precludes connectivity derived
    from any other sources, but is consistent with UGRID encoding
    within CF-netCDF.

    See CF Appendix I "The CF Data Model".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self,
        connectivity=None,
        properties=None,
        data=None,
        source=None,
        copy=True,
        _use_data=True,
    ):
        """**Initialisation**

        :Parameters:

            {{init connectivty: `str`, optional}}

            {{init properties: `dict`, optional}}

                *Parameter example:*
                  ``properties={'long_name': 'face-face connectivity'}``

            {{init data: data_like, optional}}

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            properties=properties,
            source=source,
            data=data,
            copy=copy,
            _use_data=_use_data,
        )

        if source is not None:
            try:
                connectivity = source.get_connectivity(None)
            except AttributeError:
                connectivity = None

        if connectivity is not None:
            self.set_connectivity(connectivity)

    @property
    def construct_type(self):
        """Return a description of the construct type.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Returns:

            `str`
                The construct type.

        **Examples:**

        >>> c = {{package}}.{{class}}()
        >>> c.construct_type
        'cell_connectivity'

        """
        return "cell_connectivity"

    def del_connectivity(self, default=ValueError()):
        """Remove the connectivity.

        {{{cell connectivity type}}

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `get_connectivity`, `has_connectivity`,
                     `set_connectivity`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                connectivity has not been set.

                {{default Exception}}

        :Returns:

                The removed connectivity.

        **Examples**
        
        >>> d = {{package}}.{{class}}()
        >>> d.has_connectivity()
        False
        >>> d.set_connectivity('face')
        >>> d.has_connectivity()
        True
        >>> d.get_connectivity()
        'face'
        >>> d.del_connectivity()
        'face'
        >>> d.get_connectivity()
        Traceback (most recent call last):
            ...
        ValueError: {{class}} has no 'connectivity' component
        >>> print(d.get_connectivity(None))
        None 
    
        """
        return self._del_component("connectivity", default=default)

    def has_connectivity(self):
        """Whether the connectivity type has been set.

        {{{cell connectivity type}}

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_connectivity`, `get_connectivity`,
                     `set_connectivity`

        :Returns:

             `bool`
                `True` if the connectivity has been set, otherwise
                `False`.

        **Examples**
        
        >>> d = {{package}}.{{class}}()
        >>> d.has_connectivity()
        False
        >>> d.set_connectivity('face')
        >>> d.has_connectivity()
        True
        >>> d.get_connectivity()
        'face'
        >>> d.del_connectivity()
        'face'
        >>> d.get_connectivity()
        Traceback (most recent call last):
            ...
        ValueError: {{class}} has no 'connectivity' component
        >>> print(d.get_connectivity(None))
        None

        """
        return self._has_component("connectivity")

    def get_connectivity(self, default=ValueError()):
        """Return the connectivity type.

        {{cell connectivity type}}

        See `set_connectivity` for the connectivity type definitions.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_connectivity`, `has_connectivity`,
                     `set_connectivity`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                connectivity has not been set.

                {{default Exception}}

        :Returns:

                The value of the connectivity.

        **Examples**
        
        >>> d = {{package}}.{{class}}()
        >>> d.has_connectivity()
        False
        >>> d.set_connectivity('face')
        >>> d.has_connectivity()
        True
        >>> d.get_connectivity()
        'face'
        >>> d.del_connectivity()
        'face'
        >>> d.get_connectivity()
        Traceback (most recent call last):
            ...
        ValueError: {{class}} has no 'connectivity' component
        >>> print(d.get_connectivity(None))
        None 
    
        """
        return self._get_component("connectivity", default=default)

    def set_connectivity(self, connectivity):
        """Set the connectivity type.

        {{{cell connectivity type}}

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_connectivity`, `get_connectivity`,
                     `has_connectivity`

        :Parameters:

            connectivity: `str`
                The value for the connectivity.

        :Returns:

             `None`

        **Examples**
        
        >>> d = {{package}}.{{class}}()
        >>> d.has_connectivity()
        False
        >>> d.set_connectivity('face')
        >>> d.has_connectivity()
        True
        >>> d.get_connectivity()
        'face'
        >>> d.del_connectivity()
        'face'
        >>> d.get_connectivity()
        Traceback (most recent call last):
            ...
        ValueError: {{class}} has no 'connectivity' component
        >>> print(d.get_connectivity(None))
        None 
    
        """
        self._set_component("connectivity", connectivity, copy=False)
