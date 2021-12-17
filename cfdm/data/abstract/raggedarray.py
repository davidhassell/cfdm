import numpy as np

from ..subarray import RaggedSubarray
from .compressedarray import CompressedArray


class RaggedArray(CompressedArray):
    """An underlying TODO contiguous ragged array.

    A collection of features stored using a contiguous ragged array
    combines all features along a single dimension (the "sample
    dimension") such that each feature in the collection occupies a
    contiguous block.

    The information needed to uncompress the data is stored in a
    "count variable" that gives the size of each block.

    It is assumed that the compressed dimension is the left-most
    dimension in the compressed array.

    .. versionadded:: (cfdm) 1.7.0

    """

    def __new__(cls, *args, **kwargs):
        """Store subarray classes.

        If a child class requires different subarray classes than the
        ones defined here, then they must be defined in the __new__
        method of the child class.

        .. versionadded:: (cfdm) 1.9.TODO.0

        """
        instance = super().__new__(cls)

        instance._Subarray = {
            "ragged contiguous": RaggedSubarray,
            "ragged indexed": RaggedSubarray,
            "ragged indexed contiguous": RaggedSubarray,
        }

        return instance

    def __init__(
        self,
        compressed_array=None,
        shape=None,
        compressed_dimensions={},
        count=None,
        index=None,
    ):
        """**Initialisation**

        :Parameters:

            compressed_array: `Data`
                The compressed data.

            shape: `tuple`
                The shape of the uncompressed array.

            compressed_dimensions: `dict`
                Mapping of dimensions of the compressed array to their
                corresponding dimensions in the uncompressed
                array. Compressed array dimensions that are not
                compressed must be omitted from the mapping.

                *Parameter example:*
                  ``{0: (0, 1)}``

                *Parameter example:*
                  ``{0: (0, 1, 2)}``

            count: `Count`, optional
                A count variable for uncompressing the data,
                corresponding to a CF-netCDF count variable, if
                required by the decompression method.

            index: `Index`, optional
                An index variable for uncompressing the data,
                corresponding to a CF-netCDF count variable, if
                required by the decompression method.

        """
        if count is not None:
            if index is not None:
                compression_type = "ragged indexed contiguous"
            else:
                compression_type = "ragged contiguous"
        elif index is not None:
            compression_type = "ragged indexed"
        else:
            compression_type = None

        super().__init__(
            compressed_array=compressed_array,
            shape=shape,
            count=count,
            index=index,
            compression_type=compression_type,
            compressed_dimensions=compressed_dimensions.copy(),
        )

    def __getitem__(self, indices):
        """Returns a subspace of the array defined by the given indices.

        That is, returns a subspace of the uncompressed data in an
        independent numpy array. Note that:

        x.__getitem__(indices) <==> x[indices]

        The indices that define the subspace are relative to the
        uncompressed data and must be either `Ellipsis` or a sequence that
        contains an index for each dimension. In the latter case, each
        dimension's index must either be a `slice` object or a sequence of
        two or more integers.

        Indexing is similar to numpy indexing. The only difference to
        numpy indexing (given the restrictions on the type of indices
        allowed) is:

        * When two or more dimension's indices are sequences of integers
          then these indices work independently along each dimension
          (similar to the way vector subscripts work in Fortran).

        .. versionadded:: (cfdm) 1.9.TODO.0

        """
        # ------------------------------------------------------------
        # Method: Uncompress the entire array and then subspace it
        # ------------------------------------------------------------
        Subarray = self._Subarray[self.get_compression_type()]

        compression_type = self.get_compression_type()
        Subarray = self._Subarray.get(compression_type)
        if Subarray is None:
            if not compression_type:
                raise IndexError(
                    "Can't subspace ragged data without a "
                    "standardised ragged compression type"
                )

            raise ValueError(
                "Can't subspace ragged data with unknown "
                f"ragged compression type {compression_type!r}"
            )

        # Initialise the un-sliced uncompressed array
        uarray = np.ma.masked_all(self.shape, dtype=self.dtype)

        compressed_dimensions = self.compressed_dimensions()
        compressed_data = self.conformed_data()["data"]

        for u_indices, r_indices, shape in zip(*self.subarrays()):
            subarray = Subarray(
                data=compressed_data,
                indices=r_indices,
                shape=shape,
                compressed_dimensions=compressed_dimensions,
            )
            uarray[u_indices] = subarray[...]

        return self.get_subspace(uarray, indices, copy=True)

    def get_count(self, default=ValueError()):
        """Return the count variable for the compressed array.

        .. versionadded:: (cfdm) 1.9.TODO.0

        :Returns:

            `Count`

        """
        out = self._get_component("count", None)
        if out is None:
            if default is None:
                return

            return self._default(
                default, f"{self.__class__.__name__!r} has no count variable"
            )

        return out

    def get_index(self, default=ValueError()):
        """Return the index variable for the compressed array.

        .. versionadded:: (cfdm) 1.9.TODO.0

        :Returns:

            `Index`

        """
        out = self._get_component("index", None)
        if out is None:
            if default is None:
                return

            return self._default(
                default, f"{self.__class__.__name__!r} has no index variable"
            )

        return out

    def to_memory(self):
        """Bring an array on disk into memory and retain it there.

        There is no change to an array that is already in memory.

        .. versionadded:: (cfdm) 1.9.TODO.0

        :Returns:

            `{{class}}`
                TODO

        **Examples**

        >>> a.to_memory()

        """
        super().to_memory()

        count = self.get_count(None)
        if count is not None:
            count.data.to_memory()

        index = self.get_index(None)
        if index is not None:
            index.data.to_memory()
