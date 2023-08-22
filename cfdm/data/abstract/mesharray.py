from itertools import accumulate, product

from .compressedarray import CompressedArray


class MeshArray(CompressedArray):
    """Abstract base class for an underlying UGRID connectivity array.

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __new__(cls, *args, **kwargs):
        """Store subarray classes.

        A child class must define its subarray classes in the
        `_Subarray` dictionary.

        .. versionadded:: (cfdm) 1.10.0.0

        """
        instance = super().__new__(cls)
        instance._Subarray = {}
        return instance

    def __init__(
        self,
        connectivity=None,
        shape=None,
        compressed_dimensions=None,
        start_index=None,
        compression_type=None,
        source=None,
        copy=True,
    ):
        """**Initialisation**

        :Parameters:

            connectivity: array_like
                TODOUGRID

            start_index: `int`
                TODOUGRID

            compression_type: `str`, optional
                TODOUGRID

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            compressed_array=connectivity,
            shape=shape,
            compressed_dimensions=compressed_dimensions,  # {0: (0,), 1: (1,)},
            compression_type=compression_type,
            source=source,
            copy=copy,
        )

        if source is not None:
            try:
                start_index = source.get_start_index(None)
            except AttributeError:
                start_index = None

        if start_index is not None:
            self._set_component("start_index", start_index, copy=False)

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the uncompressed data as an independent
        `scipy` Compressed Sparse Row (CSR) array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        # -------------------------------------------------------------
        # Method: Uncompress the entire array and then subspace it
        # ------------------------------------------------------------
        compressed_dimensions = self.compressed_dimensions()

        conformed_data = self.conformed_data()
        compressed_data = conformed_data["data"]

        start_index = self.get_start_index()

        for _, u_shape, c_indices, _ in zip(*self.subarrays()):
            u = self.get_Subarray()(
                data=compressed_data,
                indices=c_indices,
                shape=u_shape,
                compressed_dimensions=compressed_dimensions,
                start_index=start_index,
            )
            u = u[...]

        if indices is Ellipsis:
            return u

        return self.get_subspace(u, indices, copy=True)

    @property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return self._get_compressed_Array().dtype

    def get_start_index(self, default=ValueError()):
        """TODOUGRID.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            default: optional
                Return the value of the *default* parameter if there
                is no start index.

                {{default Exception}}

        :Returns:

            `int`
                The start index.

        """
        return self._get_component("start_index", default)

    def subarray_shapes(self, shapes):
        """Create the subarray shapes along each uncompressed dimension.

        Note that the ouput is indpendent of the *shapes* parameter,
        because each dimension of the compressed data corresponds to a
        unique dimension of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `subarray`

        :Parameters:

            {{subarray_shapes chunks: `int`, sequence, `dict`, or `str`, optional}}

        :Returns:

            `list`
                The subarray sizes along each uncompressed dimension.

        **Examples**

        >>> a.shape
        (4, 4)
        >>> a.compressed_dimensions()
        {0: (0,), 1: (1,)}
        >>> a.subarray_shapes(-1)
        [(4,), (4)]
        >>> a.subarray_shapes("auto")
        [(4,), (4)]
        >>> a.subarray_shapes("60B")
        [(4,), (4)]

        """
        return [(size,) for size in self.shape]

    def subarrays(self, shapes=-1):
        """Return descriptors for every subarray.

        These descriptors are used during subarray decompression.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            {{subarrays chunks: ``-1`` or sequence, optional}}

        :Returns:

             4-`tuple` of iterators
                Each iterable iterates over a particular descriptor
                from each subarray.

                1. The indices of the uncompressed array that
                   correspond to each subarray.

                2. The shape of each uncompressed subarray.

                3. The indices of the compressed array that correspond
                   to each subarray.

                4. The location of each subarray on the uncompressed
                   dimensions.

        **Examples**

        A compressed array with shape (4, 4) that has an original
        UGRID connectivity array with shape (4, 2).

        >>> u_indices, u_shapes, c_indices, locations = x.subarrays()
        >>> for i in u_indices:
        ...    print(i)
        ...
        (slice(0, 4, None), slice(0, 4, None))
        >>> for i in u_shapes
        ...    print(i)
        ...
        (4, 4)
        >>> for i in c_indices:
        ...    print(i)
        ...
        (slice(0, 4, None), slice(0, 2, None))
        >>> for i in locations:
        ...    print(i)
        ...
        (0, 0)

        """
        d1, u_dims = self.compressed_dimensions().popitem()

        shapes = self.subarray_shapes(shapes)

        # The indices of the uncompressed array that correspond to
        # each subarray, the shape of each uncompressed subarray, and
        # the location of each subarray.
        c = shapes[0]
        size1 = self.shape[1]
        locations = [[i for i in range(len(c))], (0,)]
        u_shapes = [c, (size1,)]

        c = tuple(accumulate((0,) + c))
        u_indices = [
            [slice(i, j) for i, j in zip(c[:-1], c[1:])],
            (slice(0, size1),),
        ]

        # The indices of the compressed array that correspond to each
        # subarray
        c_indices = [u_indices[0], (slice(0, self.source().shape[1]),)]

        return (
            product(*u_indices),
            product(*u_shapes),
            product(*c_indices),
            product(*locations),
        )
