from . import abstract


class DomainTopology(abstract.PropertiesData):
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

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self,
        properties=None,
        bounds_connectivity=None,
        cell_connectivity=None,
        source=None,
        copy=True,
        _use_data=True,
    ):
        """**Initialisation**

        :Parameters:

            {{init properties: `dict`, optional}}

                *Parameter example:*
                  ``properties={'standard_name': 'altitude'}``

            bounds_connectivity: data_like

            cell_connectivity: data_like

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(properties=properties, source=source, copy=copy)

        if source is not None and _use_data:
            try:
                bounds_connectivity = source.get_bounds_connecvity(None)
            except AttributeError:
                bounds_connectivity = None
                
            try:
                cell_connectivity = source.get_cell_connecvity(None)
            except AttributeError:
                cell_connectivity = None

        if _use_data:
            if bounds_connectivity is not None:
                self.set_bounds_connectivity(data, copy=copy)

            if cell_connectivity is not None:
                if bounds_connectivity is not None:
                    raise ValueError("TODOUGRID")

                self.set_cell_connectivity(data, copy=copy)

    @property
    def construct_type(self):
        """Return a description of the construct type.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Returns:

            `str`
                The construct type.

        **Examples:**

        >>> d = {{package}}.{{class}}()
        >>> d.construct_type
        'domain_topology'

        """
        return "domain_topology"

   def get_bounds_connectivity(self, default=ValueError(), _units=True, _fill_value=True):
        """TODOUGRID

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `get_data`, `set_bounds_connectivity`, 
        """
        if self.get_connectivity_type(None) != 'bounds':
            if default is None:
                return

            return self._default(
                default, message=(f"{self.__class__.__name__} has no "
                                  "bounds connectivity data"
                                  )
        
        return self.get_data(default=default, _units=_units, _fill_value=_fill_value)
              
   def get_cell_connectivity(self, default=ValueError(), _units=True, _fill_value=True):
        """TODOUGRID

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `get_data`, `set_cell_connectivity`, 
        """
        if self.get_connectivity_type(None) != 'cell':
            if default is None:
                return

            return self._default(
                default, message=(f"{self.__class__.__name__} has no "
                                  "cell connectivity data"
                                  )
        
        return self.get_data(default=default, _units=_units, _fill_value=_fill_value)
              
   def get_connecticity_type(self, default=ValueError()):
        """TODOUGRID

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `set_bounds_connectivity`,
                     `set_cell_connectivity`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                connectivty type data has been set.

                {{default Exception}}

        :Returns:

            `str`
                The connectivity type.

        **Examples**

        >>> d.get_connectivity_type()
        'bounds'

        >>> d.get_connectivity_type()
        'cell'

        """
        return self._get_component("connectivity_type", default)
    
    def set_bounds_connectivity(self, data, copy=True, inplace=True):
        """Set the data.

        The units, calendar and fill value of the incoming `Data` instance
        are removed prior to insertion.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `data`, `del_data`, `get_data`, `has_data`

        :Parameters:

            data: data_like
                The data to be inserted.

                {{data_like}}

            copy: `bool`, optional
                If True (the default) then copy the data prior to
                insertion, else the data is not copied.

            {{inplace: `bool`, optional (default True)}}

        :Returns:

            `None` or `{{class}}`
                If the operation was in-place then `None` is returned,
                otherwise return a new `{{class}}` instance containing the
                new data.

        TODOUGRID

        **Examples**

        >>> f = {{package}}.{{class}}()
        >>> f.set_data([1, 2, 3])
        >>> f.has_data()
        True
        >>> f.get_data()
        <{{repr}}Data(3): [1, 2, 3]>
        >>> f.data
        <{{repr}}Data(3): [1, 2, 3]>
        >>> f.del_data()
        <{{repr}}Data(3): [1, 2, 3]>
        >>> g = f.set_data([4, 5, 6], inplace=False)
        >>> g.data
        <{{repr}}Data(3): [4, 5, 6]>
        >>> f.has_data()
        False
        >>> print(f.get_data(None))
        None
        >>> print(f.del_data(None))
        None

        """
        if self.get_connectivity_type() == 'cell':
            raise ValueError("TODOUGRID remove cell connectivity with 'del_data' first")
        
        d = super().set_data(data, copy=copy, inplace=inplace)
        if d is None:
            e = self
        else:
            e = d
            
        e._set_component("connectivity_type", "bounds", copy=False)
        return d
        
    def set_cell_connectivity(self, data, copy=True, inplace=True):
        """Set the data. TODOUGRID

        The units, calendar and fill value of the incoming `Data` instance
        are removed prior to insertion.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `data`, `del_data`, `get_data`, `has_data`

        :Parameters:

            data: data_like
                The data to be inserted.

                {{data_like}}

            copy: `bool`, optional
                If True (the default) then copy the data prior to
                insertion, else the data is not copied.

            {{inplace: `bool`, optional (default True)}}

        :Returns:

            `None` or `{{class}}`
                If the operation was in-place then `None` is returned,
                otherwise return a new `{{class}}` instance containing the
                new data.

        **Examples**

        >>> f = {{package}}.{{class}}()
        >>> f.set_data([1, 2, 3])
        >>> f.has_data()
        True
        >>> f.get_data()
        <{{repr}}Data(3): [1, 2, 3]>
        >>> f.data
        <{{repr}}Data(3): [1, 2, 3]>
        >>> f.del_data()
        <{{repr}}Data(3): [1, 2, 3]>
        >>> g = f.set_data([4, 5, 6], inplace=False)
        >>> g.data
        <{{repr}}Data(3): [4, 5, 6]>
        >>> f.has_data()
        False
        >>> print(f.get_data(None))
        None
        >>> print(f.del_data(None))
        None

        """
        if self.get_connectivity_type() == 'bounds':
            raise ValueError("TODOUGRID remove bounds connectivity with 'del_data' first")
        
        d = super().set_data(data, copy=copy, inplace=inplace)
        if d is None:
            e = self
        else:
            e = d
            
        e._set_component("connectivity_type", "cell", copy=False)
        return d
    
    def set_data(self, data, copy=True, inplace=True):
        """TODOUGRIDSet the data.

        The units, calendar and fill value of the incoming `Data` instance
        are removed prior to insertion.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `data`, `del_data`, `get_data`, `has_data`

        :Parameters:

            data: data_like
                The data to be inserted.

                {{data_like}}

            copy: `bool`, optional
                If True (the default) then copy the data prior to
                insertion, else the data is not copied.

            {{inplace: `bool`, optional (default True)}}

        :Returns:

            `None` or `{{class}}`
                If the operation was in-place then `None` is returned,
                otherwise return a new `{{class}}` instance containing the
                new data.

        **Examples**

        >>> f = {{package}}.{{class}}()
        >>> f.set_data([1, 2, 3])
        >>> f.has_data()
        True
        >>> f.get_data()
        <{{repr}}Data(3): [1, 2, 3]>
        >>> f.data
        <{{repr}}Data(3): [1, 2, 3]>
        >>> f.del_data()
        <{{repr}}Data(3): [1, 2, 3]>
        >>> g = f.set_data([4, 5, 6], inplace=False)
        >>> g.data
        <{{repr}}Data(3): [4, 5, 6]>
        >>> f.has_data()
        False
        >>> print(f.get_data(None))
        None
        >>> print(f.del_data(None))
        None

        """
        raise ValueError("TODOUGIRD use set_bounds_connectivty or set_cell_connectiiy")
