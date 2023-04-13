from itertools import accumulate, product
from numbers import Number

import numpy as np

from ..core.utils import cached_property
from .abstract import CompressedArray
from .subarray import ConnectivitySubarray


class ConnectivityArray(CompressedArray):
    """An underlying UGRID connectivity array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) 1.11.0.0

    """

    def __new__(cls, *args, **kwargs):
        """Store subarray classes.

        If a child class requires different subarray classes than the
        ones defined here, then they must be defined in the __new__
        method of the child class.

        .. versionadded:: (cfdm) 1.11.0.0

        """
        instance = super().__new__(cls)
        instance._Subarray = {"connectivity": ConnectivitySubarray}
        return instance

    def __init__(
        self, compressed_array=None, shape=None, source=None, copy=True
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
            shape=shape,
            compressed_dimensions={1: (1,)},  # ??
            compression_type="connectivity",
            source=source,
            copy=copy,
        )

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the uncompressed array as an independent
        numpy array.

        .. versionadded:: (cfdm) 1.11.0.0

        """
        # ------------------------------------------------------------
        # Method: Uncompress the entire array and then subspace it
        # ------------------------------------------------------------
        # Initialise the un-sliced uncompressed array
        u = np.full(self.shape, False, dtype=self.dtype)

        connectivity = self.get_connectivity().array

        if index is None:
            if cell_dimension == 1:
                connectivity = np.tranpose(connectivity)
        elif cell_dimension == 1:
            connectivity = np.tranpose(connectivity[:, index])
        else:
            connectivity = connectivity[index, :]

        if (
            location == "face"
            and connectivity_type == "face_face_connectivity"
        ):
            for i, x in enumerate(connectivity):
                j = x.compressed()
                u[i, j] = True

        elif location == "node":
            # In this case 'connectivity' is edge_node_connectivity
            for i in range(u.shape[0]):
                j = np.unique(connectivity[np.where(connectivity == i)[0]])
                u[i, j] = True

            u.fill_diagonal(u, False)
        else:
            # In this case 'connectivity' is either
            # edge_node_connectivity or face_node_connectivity
            for i, x in enumerate(connectivity):
                y = connectivity == x[0]
                for k in x[1:]:
                    y |= connectivity == k

                j = np.unique(np.where(y)[0])
                u[i, j] = True

            u.fill_diagonal(u, False)

        if indices is Ellipsis:
            return u

        # TODOUGRID: (indices,) -> (indices, indices)

        return self.get_subspace(u, indices, copy=True)

        Subarray = self.get_Subarray()

        compressed_dimensions = self.compressed_dimensions()

        conformed_data = self.conformed_data()
        compressed_data = conformed_data["data"]

        for u_indices, u_shape, c_indices, _ in zip(*self.subarrays()):
            subarray = Subarray(
                data=compressed_data,
                indices=c_indices,
                shape=u_shape,
                compressed_dimensions=compressed_dimensions,
            )
            u[u_indices] = subarray[...]

        if indices is Ellipsis:
            return u

        return self.get_subspace(u, indices, copy=True)

    @cached_property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) 1.11.0.0

        """
        return np.dtype(bool)

    def get_connectivity(self, default=ValueError()):
        """TODOUGIRD Return the list variable for a compressed array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the list
                variable has not been set.

                {{default Exception}}

        :Returns:

            `List`
                The list variable. TODOUGRID

        """
        return self._get_component("connectivity_variable", default=default)

    def subarray_shapes(self, shapes):
        """Create the subarray shapes along each uncompressed dimension.

        .. versionadded:: (cfdm) 1.11.0.0

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
        {1: (1,)}
        >>> a.subarray_shapes(-1)
        [(4,), (4)]
        >>> a.subarray_shapes("auto")
        ["auto", (4)]
        >>> a.subarray_shapes(2)
        [2, (4)]
        >>> a.subarray_shapes((2, None))
        [2, (4,)]
        >>> a.subarray_shapes(((1, 3), None))
        [(1, 3), (4,)]
        >>> a.subarray_shapes(("auto", None))
        ["auto", (4,)]
        >>> a.subarray_shapes("60B", None))
        ["60B", (4,)]
        >>> a.subarray_shapes({0: (1, 3)})
        [(1, 3,), (4,)]

        >>> import dask.array as da
        >>> da.core.normalize_chunks(
        ...   a.subarray_shapes("auto"), shape=a.shape, dtype=a.dtype
        ... )
        [(4,), (4,)]
        >>> da.core.normalize_chunks(
        ...   a.subarray_shapes(2), shape=a.shape, dtype=a.dtype
        ... )
        [(2, 2), (4,)]

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

        .. versionadded:: (cfdm) 1.11.0.0

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
        UGRID connectivity array with shape (4, 3).

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
        (slice(0, 1, None), slice(0, 4, None))
        (slice(1, 4, None), slice(0, 4, None))
        >>> for i in u_shapes
        ...    print(i)
        ...
        (1, 4)
        (3, 4)
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
