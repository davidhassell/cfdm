from itertools import accumulate, product
from numbers import Number

import numpy as np

from .compressedarray import CompressedArray


class MeshArray(CompressedArray):
    """Abstract base class for data based on a UGRID connectivity array.

    .. versionadded:: (cfdm) UGRIDVER

    """

    def __new__(cls, *args, **kwargs):
        """Store subarray classes.

        A child class must define its subarray classes in the
        `_Subarray` dictionary.

        .. versionadded:: (cfdm) UGRIDVER

        """
        instance = super().__new__(cls)
        instance._Subarray = {}
        return instance

    def __init__(
        self,
        connectivity=None,
        shape=None,
        compressed_dimensions=None,
        compression_type=None,
        start_index=None,
        source=None,
        copy=True,
    ):
        """**Initialisation**

        :Parameters:

            connectivity: array_like
                A 2-d integer array of indices that corresponds to a
                UGRID "edge_node_connectivity",
                "face_node_connectivity", "edge_edge_connectivty",
                "face_face_connectivty", or variable.

            shape
                The shape of the CF data model view of the
                connectivity array.

            {{start_index: `int`}}

            compression_type: `str`, optional
                The type of compression.

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            compressed_array=connectivity,
            shape=shape,
            compressed_dimensions=compressed_dimensions,
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

        Returns a subspace of the uncompressed array as an independent
        numpy array.

        .. versionadded:: (cfdm) UGRIDVER

        """
        # ------------------------------------------------------------
        # Method: Uncompress the entire array and then subspace it
        # ------------------------------------------------------------
        # Initialise the un-sliced uncompressed array
        u = np.ma.empty(self.shape, dtype=self.dtype)

        Subarray = self.get_Subarray()

        compressed_dimensions = self.compressed_dimensions()

        conformed_data = self.conformed_data()
        start_index = self.get_start_index()

        for u_indices, u_shape, c_indices, _ in zip(*self.subarrays()):
            subarray = Subarray(
                indices=c_indices,
                shape=u_shape,
                compressed_dimensions=compressed_dimensions,
                start_index=start_index,
                **conformed_data,
            )
            u[u_indices] = subarray[...]

        if indices is Ellipsis:
            return u

        return self.get_subspace(u, indices, copy=False)

    @property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) UGRIDVER

        """
        return self._get_compressed_Array().dtype

    def get_start_index(self, default=ValueError()):
        """Return the start index.

        .. versionadded:: (cfdm) UGRIDVER

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

        .. versionadded:: (cfdm) UGRIDVER

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
        if shapes == -1:
            return [(size,) for size in self.shape]

        u_dims = self.get_compressed_axes()

        if isinstance(shapes, (str, Number)):
            return [
                (size,) if i in u_dims else shapes
                for i, size in enumerate(self.shape)
            ]

        if isinstance(shapes, dict):
            shapes = [
                shapes[i] if i in shapes else None for i in range(self.ndim)
            ]
        elif len(shapes) != self.ndim:
            raise ValueError(
                f"Wrong number of 'shapes' elements in {shapes}: "
                f"Got {len(shapes)}, expected {self.ndim}"
            )

        # chunks is a sequence
        return [
            (size,) if i in u_dims else c
            for i, (size, c) in enumerate(zip(self.shape, shapes))
        ]

    def subarrays(self, shapes=-1):
        """Return descriptors for every subarray.

        These descriptors are used during subarray decompression.

        .. versionadded:: (cfdm) UGRIDVER

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
        dims = self.compressed_dimensions().keys()

        shapes = self.subarray_shapes(shapes)

        # The indices of the uncompressed array that correspond to
        # each subarray, the shape of each uncompressed subarray, and
        # the location of each subarray
        locations = []
        u_shapes = []
        u_indices = []
        for d, (size, c) in enumerate(zip(self.shape, shapes)):
            if d in dims:
                locations.append((0,))
                u_shapes.append((size,))
                u_indices.append((slice(None),))
            else:
                # Note: c can not be (nan,) when d is not in dims
                locations.append([i for i in range(len(c))])
                u_shapes.append(c)

                c = tuple(accumulate((0,) + c))
                u_indices.append([slice(i, j) for i, j in zip(c[:-1], c[1:])])

        # The indices of the compressed array that correspond to each
        # subarray
        if 0 in dims:
            c_indices = [(slice(None),)]
        else:
            c_indices = [u_indices[0]]

        c_indices.append((slice(None),))

        return (
            product(*u_indices),
            product(*u_shapes),
            product(*c_indices),
            product(*locations),
        )
