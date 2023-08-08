from .propertiesdata import PropertiesData


class Topology(PropertiesData):
    """Abstract base class for bounds and cell topology constructs.

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

            topology: `str`, optional
                TODOUGRID Set the measure that indicates the metric given by
                the data array.

                The topology may also be set after initialisation with
                the `set_topology` method.

                *Parameter example:*
                  ``topology='face'``

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
                topology = source.get_topology(None)
            except AttributeError:
                topology = None

        if topology is not None:
            self.set_topology(topology)

    @property
    def ndim(self):
        """The number of data dimensions.

        Only dimensions that correspond to domain axis constructs are
        included.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `data`, `has_data`, `shape`, `size`

        **Examples**

        >>> d.shape
        (1324,)
        >>> d.ndim
        1
        >>> f.size
        1324

        """
        try:
            return len(self.shape)
        except AttributeError:
            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute 'ndim'"
            )

    @property
    def shape(self):
        """A tuple of the data array's dimension sizes.

        Only dimensions that correspond to domain axis constructs are
        included.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `data`, `has_data`, `ndim`, `size`

        **Examples**

        >>> d.shape
        (1324,)
        >>> d.ndim
        1
        >>> d.size
        1324

        """
        data = self.get_data(None, _units=False, _fill_value=False)
        if data is not None:
            return data.shape[:1]

        raise AttributeError(
            f"{self.__class__.__name__} object has no attribute 'shape'"
        )

    @property
    def size(self):
        """The number elements in the data.

        `size` is equal to the product of `shape`, that only includes
        the sizes of dimensions that correspond to domain axis
        constructs.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `data`, `has_data`, `ndim`, `shape`

        **Examples**

        >>> d.shape
        (1324,)
        >>> d.ndim
        1
        >>> d.size
        1324

        """
        try:
            return self.shape[0]
        except AttributeError:
            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute 'size'"
            )

    def del_topology(self, default=ValueError()):
        """Remove the topology.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `get_topology`, `has_topology`, `set_topology`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                topology has not been set.

                {{default Exception}}

        :Returns:

                The removed topology.

        """
        return self._del_component("topology", default=default)

    def has_topology(self):
        """Whether the topology type has been set.

        The topology type specifies which aspect of the mesh topology
        is represented by the bounds topology construct.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_topology`, `get_topology`, `set_topology`

        :Returns:

             `bool`
                True if the topology has been set, otherwise False.

        """
        return self._has_component("topology")

    def get_topology(self, default=ValueError()):
        """Return the topology type.

        The topology type specifies which aspect of the mesh topology
        is represented by the bounds topology construct.

        See `set_topology` for the topology type definitions.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_topology`, `has_topology`, `set_topology`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                topology has not been set.

                {{default Exception}}

        :Returns:

                The value of the topology.

        """
        return self._get_component("topology", default=default)

    def set_topology(self, topology):
        """Set the topology type.

        The topology type specifies which aspect of the mesh topology
        is represented by the bounds topology construct.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_topology`, `get_topology`, `has_topology`

        :Parameters:

            topology: `str`
                The value for the topology.

        :Returns:

             `None`

        """
        self._set_component("topology", topology, copy=False)
