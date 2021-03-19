from .array import Array


class CompressedArray(Array):
    """Mixin class for a container of an underlying compressed array.

    .. versionadded:: (cfdm) 1.7.0

    """

    def __init__(
        self,
        compressed_array=None,
        shape=None,
        size=None,
        ndim=None,
        source_compressed_axes=None,
        compression_type=None,
        **kwargs
    ):
        """Initialisation.

        :Parameters:

            compressed_array: subclass of `Array`
                The compressed array.

            shape: `tuple`
                The uncompressed array dimension sizes.

            size: `int`
                Number of elements in the uncompressed array.

            ndim: `int`
                The number of uncompressed array dimensions

            source_compressed_axes: `int` or `tuple`
                The positions of the axes in the underlying compressed
                array that correspond to the axes that have been
                compressed. If two or more axes have been compressed
                to one axis then its integer position must be
                provided. Otherwise a tuple of integer positions that
                coorespond to the compressed axes is required.

            compression_type: `str`
                The type of compression.

            kwargs: optional
                Further named parameters and their values needed to define
                the compressed array.

        """
        super().__init__(
            shape=shape,
            size=size,
            ndim=ndim,
            source_compressed_axes=source_compressed_axes,
            compression_type=compression_type,
            **kwargs
        )

        self._set_compressed_Array(compressed_array, copy=False)

    def _get_compressed_Array(self, default=ValueError()):
        """Return the compressed array.

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the array
                has not been set. If set to an `Exception` instance then
                it will be raised instead.

        :Returns:

                The compressed Array instance.

        **Examples:**

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

        **Examples:**

        >>> d._set_compressed_Array(a)

        """
        if copy:
            array = array.copy()

        self._set_component("compressed_Array", array, copy=False)

    # ----------------------------------------------------------------
    # Attributes
    # ----------------------------------------------------------------
    @property
    def array(self):
        """Returns a numpy array containing the uncompressed data.

        :Returns:

            `numpy.ndarray`
                The uncompressed array.

        **Examples:**

        >>> n = a.array
        >>> isinstance(n, numpy.ndarray)
        True

        """
        return self[...]

    @property
    def dtype(self):
        """Data-type of the data elements.

        **Examples:**

        >>> a.dtype
        dtype('float64')
        >>> print(type(a.dtype))
        <type 'numpy.dtype'>

        """
        return self._get_compressed_Array().dtype

    @property
    def ndim(self):
        """The number of dimensions of the uncompressed data.

        **Examples:**

        >>> d.shape
        (73, 96)
        >>> d.ndim
        2
        >>> d.size
        7008

        >>> d.shape
        (1, 1, 1)
        >>> d.ndim
        3
        >>> d.size
        1

        >>> d.shape
        ()
        >>> d.ndim
        0
        >>> d.size
        1

        """
        return self._get_component("ndim")

    @property
    def shape(self):
        """Shape of the uncompressed data.

        **Examples:**

        >>> d.shape
        (73, 96)
        >>> d.ndim
        2
        >>> d.size
        7008

        >>> d.shape
        (1, 1, 1)
        >>> d.ndim
        3
        >>> d.size
        1

        >>> d.shape
        ()
        >>> d.ndim
        0
        >>> d.size
        1

        """
        return self._get_component("shape")

    @property
    def size(self):
        """Number of elements in the uncompressed data.

        **Examples:**

        >>> d.shape
        (73, 96)
        >>> d.size
        7008
        >>> d.ndim
        2

        >>> d.shape
        (1, 1, 1)
        >>> d.ndim
        3
        >>> d.size
        1

        >>> d.shape
        ()
        >>> d.ndim
        0
        >>> d.size
        1

        """
        return self._get_component("size")

    @property
    def compressed_array(self):
        """Returns an independent numpy array with the compressed data.

        :Returns:

            `numpy.ndarray`
                The compressed array.

        **Examples:**

        >>> n = a.compressed_array
        >>> isinstance(n, numpy.ndarray)
        True

        """
        ca = self._get_compressed_Array(None)
        if ca is None:
            raise ValueError("There is no underlying compressed array")

        return ca.array

    # ----------------------------------------------------------------
    # Methods
    # ----------------------------------------------------------------
    def get_compressed_axes(self):
        """Return the axes that have underlying compression.

        .. seealso:: `get_source_compressed_axes`

        :Returns:

            `tuple` of `int`
                The compressed axes described by their integer positions.

        **Examples:**

        >>> c.ndim
        4
        >>> c.compressed_array.ndim
        3
        >>> c.compressed_axes()
        [1, 2]

        """
        compressed_dimension = self.get_source_compressed_axes()

        if not isinstance(compressed_dimension, int):
            return compressed_dimension

        compressed_ndim = self._get_compressed_Array().ndim

        return tuple(
            range(
                compressed_dimension,
                self.ndim - (compressed_ndim - compressed_dimension - 1),
            )
        )

    def get_source_compressed_axes(self):
        """Return the source array axes that correspond to compressed
        axes.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `get_compressed_array`, `get_compressed_axes`,
                     `get_compressed_type`

        :Returns:

            `int` or `tuple`
                The positions of the axes in the underlying compressed
                array that correspond to the axes that have been
                compressed. If two or more axes have been compressed
                to one axis then its integer position is
                returned. Otherwise a tuple of integer positions that
                coorespond to the compressed axes is returned.

        **Examples:**

        >>> d.get_source_compressed_axes()
        2

        >>> d.get_source_compressed_axes()
        (1,)

        >>> d.get_source_compressed_axes()
        (1, 3)

        """
        return self._get_component("source_compressed_axes")

    def to_memory(self):
        """Bring an array on disk into memory and retain it there.

        There is no change to an array that is already in memory.

        :Returns:

            `{{class}}`
                The array that is stored in memory.

        **Examples:**

        >>> b = a.to_memory()

        """
        self._get_compressed_Array(self._get_compressed_Array().to_memory())
        return self

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

        **Examples:**

        >>> array
        <RaggedContiguousArray(2, 4): >
        >>> array.source()
        <Data(5): [280.0, ..., 279.5]>

        """
        return self._get_compressed_Array(default=default)
