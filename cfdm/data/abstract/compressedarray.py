import numpy as np

from .array import Array


class DeprecationError(Exception):
    """Deprecation error."""

    pass


class CompressedArray(Array):
    """Mixin class for a container of an underlying compressed array.

    .. versionadded:: (cfdm) 1.7.0

    """

    def __new__(cls, *args, **kwargs):
        """Store subarray classes.

        A child class must define its subarray classes in the
        `_Subarray` dictionary.

        .. versionadded:: (cfdm) 1.9.TODO.0

        """
        instance = super().__new__(cls)
        instance._Subarray = {}
        return instance

    def __init__(
        self,
        compressed_array=None,
        shape=None,
        size=None,
        ndim=None,
        compressed_dimension=None,
        compressed_dimensions={},
        compression_type=None,
        source=None,
        copy=True,
    ):
        """**Initialisation**

        :Parameters:

            compressed_array: array_like
                The compressed array.

            shape: `tuple`
                The uncompressed array dimension sizes.

            compressed_dimensions: `dict`
                Mapping of dimensions of the compressed array to their
                corresponding dimensions in the uncompressed
                array. Compressed array dimensions that are not
                compressed must be omitted from the mapping.

                *Parameter example:*
                  ``{0: (0, 1)}``

                *Parameter example:*
                  ``{0: (0, 1, 2)}``

                *Parameter example:*
                  ``{2: (2, 3)}``

                *Parameter example:*
                  ``{1: (1,)}``

                *Parameter example:*
                  ``{0: (0,), 2: (2,)}``

                .. versionadded:: (cfdm) 1.9.TODO.0

            compression_type: `str`
                The type of compression.

            kwargs: optional
                Further named parameters and their values needed to define
                the compressed array.

            compressed_dimension: Deprecated at version 1.9.TODO.0
                Use the *compressed_dimensions* parameter instead.

            source: optional
                Initialise the array from the given object.

                {{init source}}

                .. versionadded:: (cfdm) 1.9.TODO.0

            copy: `bool`, optional
                If False then do not deep copy input parameters prior
                to initialisation. By default arguments are deep
                copied.

                .. versionadded:: (cfdm) 1.9.TODO.0

            size: `int`
                Deprecated at version 1.9.TODO.0. If set will be
                ignored.

                Number of elements in the uncompressed array.

            ndim: `int`
                Deprecated at version 1.9.TODO.0. If set will be
                ignored.

                The number of uncompressed array dimensions

        """
        if compressed_dimension is not None:
            raise DeprecationError(
                "The 'compressed_dimension' keyword was deprecated at "
                "version 1.9.TODO.0. "
                "Use the 'compressed_dimensions' keyword instead."
            )  # pragma: no cover

        super().__init__(source=source, copy=copy)

        if source is not None:
            try:
                compressed_array = source._get_compressed_Array(None)
            except AttributeError:
                compressed_array = None

            try:
                compression_type = source.get_compression_type()
            except AttributeError:
                compression_type = None

            try:
                compressed_dimensions = source.compressed_dimensions()
            except AttributeError:
                compressed_dimensions = {}

            try:
                shape = source.shape
            except AttributeError:
                shape = None
        else:
            compressed_dimensions = compressed_dimensions.copy()

        if compressed_array is not None:
            self._set_compressed_Array(compressed_array, copy=copy)

        if shape is not None:
            self._set_component("shape", shape, copy=False)

        if compression_type:
            self._set_component(
                "compression_type", compression_type, copy=False
            )

        self._set_component(
            "compressed_dimensions", compressed_dimensions, copy=False
        )

    def _first_or_last_element(self, indices):
        """Return the first or last element of the compressed array.

        This method will return the first or last element of the
        compressed array without performing any decompression.

        First and last elements are only recognised by exact *indices*
        matches to:

        * ``(slice(0, 1, 1),) * self.ndim``
        * ``(slice(-1, None, 1),) * self.ndim``

        Any other value of indices will raise an `IndexError`. For
        instance, note that ``slice(0, 1, 1)`` is not an exact match
        to ``slice(0, 1)``.

        .. versionadded:: (cfdm) 1.9.TODO.0

        .. seealso:: `__getitem__`

        :Parameters:

            indices:
                Indices to the uncompressed array.

        :Returns:

            `numpy.ndarray`
                The first or last element. If the *indices* do not
                acceptably select the first or last element then an
                `IndexError` is raised.

        """
        ndim = self.ndim
        for index in (slice(0, 1, 1), slice(-1, None, 1)):
            if indices == (index,) * ndim:
                data = self.source()
                return np.asanyarray(data[(index,) * data.ndim])

        # Indices do not acceptably select the first nor last element
        raise IndexError()

    def _get_compressed_Array(self, default=ValueError()):
        """Return the compressed array.

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the array
                has not been set. If set to an `Exception` instance then
                it will be raised instead.

        :Returns:

                The compressed Array instance.

        **Examples**

        >>> c = d._get_compressed_Array()

        """
        return self._get_component("compressed_Array", default)

    def _set_compressed_Array(self, array, copy=True):
        """Set the compressed array.

        .. versionadded:: (cfdm) 1.7.0

        :Parameters:

            array: subclass of `cfdm.data.Array`
                The compressed data to be inserted.

        :Returns:

            `None`

        **Examples**

        >>> d._set_compressed_Array(a)

        """
        if copy:
            array = array.copy()

        self._set_component("compressed_Array", array, copy=False)

    @property
    def array(self):
        """Returns a numpy array containing the uncompressed data.

        :Returns:

            `numpy.ndarray`
                The uncompressed array.

        **Examples**

        >>> n = a.array
        >>> isinstance(n, numpy.ndarray)
        True

        """
        return self[...]

    @property
    def dtype(self):
        """Data-type of the uncompressed data."""
        raise NotImplementedError(
            f"Must implement {self.__class__.__name__}.dtype"
        )  # pragma: no cover

    @property
    def shape(self):
        """Shape of the uncompressed data."""
        return self._get_component("shape")

    @property
    def compressed_array(self):
        """Returns an independent numpy array with the compressed data.

        :Returns:

            `numpy.ndarray`
                The compressed array.

        **Examples**

        >>> n = a.compressed_array

        >>> isinstance(n, numpy.ndarray)
        True

        """
        ca = self._get_compressed_Array(None)
        if ca is None:
            raise ValueError("There is no underlying compressed array")

        return ca.array

    def get_compressed_axes(self):
        """Return axes that are compressed in the underlying array.

        :Returns:

            `list`
                The compressed axes described by their integer
                positions in the uncompressed array.

        **Examples**

        >>> c.ndim
        4
        >>> c.compressed_array.ndim
        3
        >>> c.compressed_axes()
        [1, 2]

        """
        return sorted(
            {x for y in self.compressed_dimensions().values() for x in y}
        )

    def get_compressed_dimension(self, default=ValueError()):
        """Returns the compressed dimension's position in the array.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `get_compressed_axes`, `get_compression_type`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                underlying array is not compressed. If set to an
                `Exception` instance then it will be raised instead.

        :Returns:

            `int`
                The position of the compressed dimension in the compressed
                array. If the underlying is not compressed then *default*
                is returned, if provided.

        **Examples**

        >>> i = d.get_compressed_dimension()

        """
        compressed_dimensions = self.compressed_dimensions()
        if len(compressed_dimensions) > 1:
            raise ValueError(
                "Can't get unique compressed dimension when there "
                f"more than one: {self.compressed_dimensions()}"
            )

        return tuple(compressed_dimensions)[0]

    def compressed_dimensions(self):
        """Mapping of compressed to uncompressed dimensions.

        A dictionary key is a position of a dimension in the
        compressed data, with a value of the positions of the
        corresponding dimensions in the uncompressed data. Compressed
        array dimensions that are not compressed are omitted from the
        mapping.

        .. versionadded:: (cfdm) 1.9.TODO.0

        .. seealso:: `get_compressed_axes`, `get_compression_type`

        :Returns:

            `dict`
                The mapping of dimensions of the compressed array to
                their corresponding dimensions in the uncompressed
                array. Compressed array dimensions that are not
                compressed are omitted from the mapping.

        """
        return self._get_component("compressed_dimensions").copy()

    def conformed_data(self):
        """The data as required by the decompression algorthm.

        .. versionadded:: (cfdm) 1.9.TODO.0

        :Returns:

            `dict`
                The compressed data, with the key ``'data'``.

        """
        return {"data": self.source().copy()}

    def get_Subarray(self):
        """Return the Subarray class.

        .. versionadded:: (cfdm) 1.9.TODO.0

        :Returns:

            `Subarray`
                The class for representing subarrays.

        """
        return self._Subarray[self.get_compression_type()]

    def source(self, default=ValueError()):
        """Return the underlying array object.

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the array
                has not been set. If set to an `Exception` instance then
                it will be raised instead.

        :Returns:

            subclass of `Array`
                The underlying array object.

        **Examples**

        >>> array
        <RaggedContiguousArray(2, 4): >
        >>> array.source()
        <Data(5): [280.0, ..., 279.5]>

        """
        return self._get_compressed_Array(default=default)

    def subarray_shapes(self, shapes):
        """Create the subarray shapes along each uncompressed dimension.

        .. versionadded:: (cfdm) 1.9.TODO.0

        .. seealso:: `subarray`

        :Parameters:

            {{subarray_shapes chunks: `int`, sequence, `dict`, or `str`, optional}}

        :Returns:

            `list`
                 The subarray shapes along each uncompressed
                 dimension.

        """
        raise NotImplementedError(
            f"Must implement {self.__class__.__name__}.subarray_shapes"
        )  # pragma: no cover

    def subarrays(self, shapes=-1):
        """Return descriptors for every subarray.

        Theses descriptors are used during subarray decompression.

        .. versionadded:: (cfdm) 1.9.TODO.0

        :Parameters:

            {{subarrays chunks: ``-1`` or sequence, optional}}

        :Returns:

            sequence of iterables
                Each iterable iterates over a particular descriptor
                from each subarray.

        """
        raise NotImplementedError(
            f"Must implement {self.__class__.__name__}.subarrays"
        )  # pragma: no cover

    def to_memory(self):
        """Bring data on disk into memory.

        There is no change to data that is already in memory.

        .. versionadded:: (cfdm) 1.9.TODO.0

        :Returns:

            `{{class}}`
                A copy of the array with all of its data in memory.

        """
        a = self.copy()
        try:
            a._set_compressed_Array(a.source().to_memory(), copy=False)
        except AttributeError:
            pass

        return a
