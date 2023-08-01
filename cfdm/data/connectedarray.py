from itertools import accumulate, product
from numbers import Number

import numpy as np

from ..core.utils import cached_property
from .abstract import CompressedArray
from .subarray import ConnectivitySubarray


class ConnectivityArray(CompressedArray):
    """An underlying UGRID connectivity array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self,
        compressed_array=None,
        connectivty_indices=None,
        source=None,
        copy=True,
    ):
        """**Initialisation**

        :Parameters:

            compressed_array: array_like
                The 2-d compressed array.

            shape: `tuple`
                The shape of the uncompressed array.

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            compressed_array=compressed_array,
            shape=compressed_array.shape,
            compressed_dimensions={
                0: (
                    0,
                    1,
                )
            },
            compression_type="connected",
            start_index=0,
            connectivty_indices=connectivty_indices,
            source=source,
            copy=copy,
        )

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the connectivity array as an independent
        scipy sparse array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        # ------------------------------------------------------------
        # Method: Uncompress the entire array and then subspace it
        # ------------------------------------------------------------
        # Initialise the un-sliced uncompressed array
        u = np.ma.masked_all(self.shape, dtype=self.dtype)

        Subarray = self.get_Subarray()

        compressed_dimensions = self.compressed_dimensions()

        conformed_data = self.conformed_data()
        node_coordinates = conformed_data["data"]
        connectivity_indices = conformed_data["connectivity_indices"]

        for u_indices, u_shape, c_indices, _ in zip(*self.subarrays()):
            subarray = Subarray(
                data=node_coordinates,
                indices=c_indices,
                shape=u_shape,
                compressed_dimensions=compressed_dimensions,
                connectivity_indices=connectivity_indices,
            )
            u[u_indices] = subarray[...]

        if indices is Ellipsis:
            return u

        return self.get_subspace(u, indices, copy=True)

    @property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return self.source().dtype

    def conformed_data(self):
        """The compressed data and list variable.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Returns:

            `dict`
                The conformed gathered data, with the key ``'data'``;
                and the `tuple` of uncompressed indices with the key
                ``'uncompressed_indices'``.

        """
        out = super().conformed_data()
        out["node_coordinates"] = self.get_node_coordinates()
        return out

    def get_node_coordinates(self, default=ValueError()):
        """TODOUGRID Return the list variable for a compressed array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                node coordinates have not been set.

                {{default Exception}}

        :Returns:

            `NodeCoordinates`
                TODOUGRID

        """
        return self._get_component("node_coordinates", default=default)

    def subarray_shapes(self, shapes):
        """Create the subarray shapes along each uncompressed dimension.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `subarray`

        :Parameters:

            {{subarray_shapes chunks: `int`, sequence, `dict`, or `str`, optional}}

        :Returns:

            `list`
                The subarray sizes along each uncompressed dimension.

        **Examples**

        >>> a.shape
        (4, 3)
        >>> a.compressed_dimensions()
        {0: (0, 1,)}
        >>> a.subarray_shapes(-1)
        [(4,), (3)]
        >>> a.subarray_shapes("auto")
        ["auto", (3)]
        >>> a.subarray_shapes(2)
        [2, (3)]
        >>> a.subarray_shapes((2, None))
        [2, (3,)]
        >>> a.subarray_shapes(((1, 3), None))
        [(1, 3), (3,)]
        >>> a.subarray_shapes(("auto", None))
        ["auto", (3,)]
        >>> a.subarray_shapes("60B", None))
        ["60B", (3,)]
        >>> a.subarray_shapes({0: (1, 3)})
        [(1, 3,), (3,)]

        >>> import dask.array as da
        >>> da.core.normalize_chunks(
        ...   a.subarray_shapes("auto"), shape=a.shape, dtype=a.dtype
        ... )
        [(4,), (3,)]
        >>> da.core.normalize_chunks(
        ...   a.subarray_shapes(2), shape=a.shape, dtype=a.dtype
        ... )
        [(2, 2), (3,)]

        """
        if shapes == -1:
            return [(size,) for size in self.shape]

        if isinstance(shapes, (str, Number)):
            return [shapes, (self.shape[1],)]

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
        return [shapes[0], (self.shape[1],)]

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

        A compressed array with shape (4, 3) that has an original
        UGRID connectivity array with shape (4, 3).

        >>> u_indices, u_shapes, c_indices, locations = x.subarrays()
        >>> for i in u_indices:
        ...    print(i)
        ...
        (slice(0, 4, None), slice(0, 3, None))
        >>> for i in u_shapes
        ...    print(i)
        ...
        (4, 3)
        >>> for i in c_indices:
        ...    print(i)
        ...
        (slice(0, 4, None), slice(0, 3, None))
        >>> for i in locations:
        ...    print(i)
        ...
        (0, 0)

        >>> (
        ...  u_indices, u_shapes, c_indices, locations
        ... )= x.subarrays(shapes=((1, 3), None))
        >>> for i in u_indices:
        ...    print(i)
        ...
        (slice(0, 1, None), slice(0, 3, None))
        (slice(1, 4, None), slice(0, 3, None))
        >>> for i in u_shapes
        ...    print(i)
        ...
        (1, 3)
        (3, 3)
        >>> for i in c_indices:
        ...    print(i)
        ...
        (slice(0, 1, None), slice(0, 3, None))
        (slice(1, 4, None), slice(0, 3, None))
        >>> for i in locations:
        ...    print(i)
        ...
        (0, 0)
        (1, 0)

        """
        d1, u_dims = self.compressed_dimensions().popitem()

        shapes = self.subarray_shapes(shapes)

        # The indices of the uncompressed array that correspond to
        # each subarray, the shape of each uncompressed subarray, and
        # the location of each subarray.
        c = shapes[0]
        n_cells, n_bounds = self.shape
        locations = [[i for i in range(len(c))], (0,)]
        #        locations = [(0,), (0,)]
        u_shapes = [c, (n_bounds,)]
        #        u_shapes = [(n_cells,), (n_bounds,)]

        c = tuple(accumulate((0,) + c))
        u_indices = [
            [slice(i, j) for i, j in zip(c[:-1], c[1:])],
            (slice(0, n_bounds),),
        ]
        #        u_indices = [(slice(0, n_cells),), (slice(0, n_bounds),),]

        # The indices of the compressed array that correspond to each
        # subarray
        c_indices = [(slice(0, self.data.size),)]

        return (
            product(*u_indices),
            product(*u_shapes),
            product(*c_indices),
            product(*locations),
        )
