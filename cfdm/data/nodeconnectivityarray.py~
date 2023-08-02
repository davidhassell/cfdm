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

    def __new__(cls, *args, **kwargs):
        """Store subarray classes.

        If a child class requires different subarray classes than the
        ones defined here, then they must be defined in the __new__
        method of the child class.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        instance = super().__new__(cls)
        instance._Subarray = {"connectivity": ConnectivitySubarray}
        return instance

    def __init__(self, compressed_array=None, source=None, copy=True):
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
            shape=(compressed_array.shape[0],) * 2,
            compressed_dimensions={2: (2,)},
            compression_type="connectivity",
            start_index=0,
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
        compressed_dimensions = self.compressed_dimensions()

        conformed_data = self.conformed_data()
        compressed_data = conformed_data["data"]

        (_, u_shape, c_indices, _) = zip(*self.subarrays())
        u = self.get_Subarray()(
            data=compressed_data,
            indices=c_indices,
            shape=u_shape,
            compressed_dimensions=compressed_dimensions,
        )

        if indices is Ellipsis:
            return u

        return self.get_subspace(u, indices, copy=True)

    def array(self):
        """TODOUGRID.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `sparse_array`

        """
        return self[...].toarray()

    @cached_property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return np.dtype(bool)

    def sparse_array(self):
        """TODOUGRID.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `array`

        """
        return self[...]

    def get_connectivity(self, default=ValueError()):
        """TODOUGIRD Return the list variable for a compressed array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the list
                variable has not been set.

                {{default Exception}}

        :Returns:

            `Connectivity`
                TODOUGRID

        """
        return self._get_component("connectivity_variable", default=default)

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
        (4, 4)        
        >>> a.subarray_shapes(-1)
        [(4,), (4)]

        """
        return [(size,) for size in self.shape]
#        if shapes == -1:
#            return [(size,) for size in self.shape]
#
#        if isinstance(shapes, (str, Number)):
#            return [shapes, (self.shape[1],)]
#
#        if isinstance(shapes, dict):
#            shapes = [
#                shapes[i] if i in shapes else None for i in range(self.ndim)
#            ]
#        elif len(shapes) != self.ndim:
#            raise ValueError(
#                f"Wrong number of 'shapes' elements in {shapes}: "
#                f"Got {len(shapes)}, expected {self.ndim}"
#            )
#
#        # chunks is a sequence
#        return [shapes[0], (self.shape[1],)]

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
