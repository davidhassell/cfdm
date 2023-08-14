from . import abstract


class CellConnectivity(abstract.Topology):
    """A cell connectivity construct of the CF data model.

    TODOUGRID

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

            TODOUGRID

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
                True if the connectivity has been set, otherwise False.

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

        """
        self._set_component("connectivity", connectivity, copy=False)
