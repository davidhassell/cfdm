from . import abstract


class DomainTopology(abstract.PropertiesData):
    """A domain topology construct of the CF data model.

    TODOUGRID

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
                TODOUGRID

            cell_connectivity: data_like
                TODOUGRID

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        if source is not None:
            try:
                properties = source.properties()
            except AttributeError:
                properties = None

            if _use_data:
                try:
                    bounds_connectivity = source.get_bounds_connectivity(None)
                except AttributeError:
                    bounds_connectivity = None

                try:
                    cell_connectivity = source.get_cell_connectivity(None)
                except AttributeError:
                    cell_connectivity = None

        super().__init__(properties=properties, copy=copy)

        if _use_data:
            if bounds_connectivity is not None:
                self.set_bounds_connectivity(bounds_connectivity, copy=copy)

            if cell_connectivity is not None:
                if bounds_connectivity is not None:
                    raise ValueError("TODOUGRID")

                self.set_cell_connectivity(cell_connectivity, copy=copy)

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

    def get_bounds_connectivity(
        self, default=ValueError(), _units=True, _fill_value=True
    ):
        """TODOUGRID.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `get_data`, `set_bounds_connectivity`,

        """
        data = None
        if self.get_connectivity_type(None) == "bounds":
            data = self.get_data(
                default=None, _units=_units, _fill_value=_fill_value
            )

        if data is not None:
            return data

        if default is None:
            return

        return self._default(
            default,
            message=(
                f"{self.__class__.__name__} has no " "bounds connectivity data"
            ),
        )

    def get_cell_connectivity(
        self, default=ValueError(), _units=True, _fill_value=True
    ):
        """TODOUGRID.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `get_data`, `set_cell_connectivity`,

        """
        data = None
        if self.get_connectivity_type(None) == "cell":
            data = self.get_data(
                default=None, _units=_units, _fill_value=_fill_value
            )

        if data is not None:
            return data

        if default is None:
            return

        return self._default(
            default,
            message=(
                f"{self.__class__.__name__} has no " "cell connectivity data"
            ),
        )

    def get_connectivity_type(self, default=ValueError()):
        """TODOUGRID.

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
        if self.get_connectivity_type(None) == "cell":
            raise ValueError(
                "TODOUGRID remove cell connectivity with 'del_data' first"
            )

        d = super().set_data(data, copy=copy, inplace=inplace)
        if d is None:
            e = self
        else:
            e = d

        e._set_component("connectivity_type", "bounds", copy=False)
        return d

    def set_cell_connectivity(self, data, copy=True, inplace=True):
        """Set the data. TODOUGRID.

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
        if self.get_connectivity_type(None) == "bounds":
            raise ValueError(
                "TODOUGRID remove bounds connectivity with 'del_data' first"
            )

        d = super().set_data(data, copy=copy, inplace=inplace)
        if d is None:
            e = self
        else:
            e = d

        e._set_component("connectivity_type", "cell", copy=False)
        return d

    def set_data(self, data, copy=True, inplace=True):
        """Hmm TODOUGRID Set the data.

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
        raise ValueError(
            "TODOUGRID use set_bounds_connectivty or set_cell_connectiiy"
        )
