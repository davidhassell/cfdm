from .propertiesdata import PropertiesData


class Topology(PropertiesData):
    """Abstract base class for bounds and cell topology constructs.

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

            cell_type: `str`, optional
                TODOUGRID Set the measure that indicates the metric given by
                the data array.

                The cell type may also be set after initialisation
                with the `set_cell_type` method.

                *Parameter example:*
                  ``cell_type='face'``

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
                cell_type = source.get_cell_type(None)
            except AttributeError:
                cell_type = None

        if cell_type is not None:
            self.set_cell_type(cell_type)

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
        data = self.get_data(None, _units=False, _fill_value=False)
        if data is not None:
            return 1

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

    def del_cell_type(self, default=ValueError()):
        """Remove the cell type.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `get_cell_type`, `has_cell_type`, `set_cell_type`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                cell type has not been set.

                {{default Exception}}

        :Returns:

                The removed cell_type.

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_cell_type('face')
        >>> c.has_cell_type()
        True
        >>> c.get_cell_type()
        'face'
        >>> c.del_cell_type()
        'face'
        >>> c.has_cell_type()
        False
        >>> print(c.del_cell_type(None))
        None
        >>> print(c.get_cell_type(None))
        None

        """
        return self._del_component("cell_type", default=default)

    def has_cell_type(self):
        """Whether the cell type has been set.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_cell_type`, `get_cell_type`, `set_cell_type`

        :Returns:

             `bool`
                True if the cell type has been set, otherwise False.

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_cell_type('face')
        >>> c.has_cell_type()
        True
        >>> c.get_cell_type()
        'face'
        >>> c.del_cell_type()
        'face'
        >>> c.has_cell_type()
        False
        >>> print(c.del_cell_type(None))
        None
        >>> print(c.get_cell_type(None))
        None

        """
        return self._has_component("cell_type")

    def get_cell_type(self, default=ValueError()):
        """Return the cell type.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_cell_type`, `has_cell_type`, `set_cell_type`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                cell type has not been set.

                {{default Exception}}

        :Returns:

                The value of the cell type.

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_cell_type('face')
        >>> c.has_cell_type()
        True
        >>> c.get_cell_type()
        'face'
        >>> c.del_cell_type()
        'face'
        >>> c.has_cell_type()
        False
        >>> print(c.del_cell_type(None))
        None
        >>> print(c.get_cell_type(None))
        None

        """
        return self._get_component("cell_type", default=default)

    def set_cell_type(self, cell_type):
        """Set the cell type.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_cell_type`, `get_cell_type`, `has_cell_type`

        :Parameters:

            cell_type: `str`
                The value for the cell type.

        :Returns:

             `None`

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_cell_type('face')
        >>> c.has_cell_type()
        True
        >>> c.get_cell_type()
        'face'
        >>> c.del_cell_type()
        'face'
        >>> c.has_cell_type()
        False
        >>> print(c.del_cell_type(None))
        None
        >>> print(c.get_cell_type(None))
        None

        """
        return self._set_component("cell_type", cell_type, copy=False)
