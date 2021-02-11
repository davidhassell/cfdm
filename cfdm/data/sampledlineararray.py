import numpy as np

from .abstract import Sampledrray


class SampledLinearArray(Sampledrray):
    """TODO

    .. versionadded:: (cfdm) TODO

    """

    def __init__(
        self,
        shape=None,
        size=None,
        ndim=None,
        compressed_dimensions=None,
        interpolation=None,
        tie_points=None,
        tie_point_indices={},
        interpolation_parameters={},
    ):
        """**Initialization**

        :Parameters:

            compressed_array: `Data`
                The compressed array.

            shape: `tuple`
                The uncompressed array dimension sizes.

            size: `int`
                Number of elements in the uncompressed array.

            ndim: `int`
                The number of uncompressed array dimensions.

            compressed_dimensions: sequence of `int`
                The positions of the compressed dimensions in the
                compressed array.

            tie_point_indices: `dict`, optional
                TODO

            interpolation_parameters: `dict`
                TODO

        """
        compressed_dimensions = tuple(sorted(compressed_dimensions))

        super().__init__(
            shape=shape,
            size=size,
            ndim=ndim,
            compressed_dimensions=compressed_dimensions,
            interpolation=interpolation,
            tie_points=tie_points,
            tie_point_indices=tie_point_indices.copy(),
            interpolation_parameters=interpolation_parameters.copy(),
            compression_type="sampled",
        )

    def __getitem__(self, indices):
        """x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the array as an independent numpy array.

        The indices that define the subspace must be either `Ellipsis`
        or a sequence that contains an index for each dimension. In
        the latter case, each dimension's index must either be a
        `slice` object or a sequence of two or more integers.

        Indexing is similar to numpy indexing. The only difference to
        numpy indexing (given the restrictions on the type of indices
        allowed) is:

          * When two or more dimension's indices are sequences of
            integers then these indices work independently along each
            dimension (similar to the way vector subscripts work in
            Fortran).

        .. versionadded:: (cfdm) TODO

        """
        # ------------------------------------------------------------
        # Method: Uncompress the entire array and then subspace it
        # ------------------------------------------------------------
        d0 = self.get_compression_dimension()[0]

        tie_points = self.get_tie_points()
         
        # Initialise the un-sliced uncompressed array
        uarray = np.ma.masked_all(self.shape, dtype=float)

        for tp_indices, u_indices in zip(*self._interpolation_zones()):
            u_slice  = u_indices[d0]
            
            tp_index0 = list(tp_indices)
            tp_index1 = tp_index0[:]
            tp_index0[d0] = tp_index0[d0][:1]
            tp_index1[d0] = tp_index1[d0][1:]
            
            uarray[u_indices] = self._linear_interpolation(
                d0, u_slice,
                tie_points[tuple(tp_index0)],
                tie_points[tuple(tp_index1)],
            )
            
        self._calculate_s.cache_clear()
            
        return self.get_subspace(uarray, indices, copy=True)
    
    # ----------------------------------------------------------------
    # Private methods
    # ----------------------------------------------------------------
    def _linear_interpolation(self, d, u_slice, tp0, tp1):
        """TODO

        :Parameters:

            d: `int`

            u_slice: `slice`
                The `slice` object that describes which elements along
                interpolation dimension *d* of the of the uncompressed
                array are in this interpolation zone.

            tie_points: `Data`
        
            tp_indices: `tuple` of indices

        """
        s, one_minus_s = self._calculate_s(
            d, u_slice.stop - u_slice.start
        )
        
        return tp0.array * one_minus_s + tp1.array * s

    @lru_cache(maxsize=32)
    def _calculate_s(self, d, n):
        """TODO

        :Parameters:

            d: `int`
                The interpolation dimension along which linear
                interpolation is being applied.

            n: `int`
                The uncompressed size of the interpolation zone along
                interpolation dimension *d*.

        :Returns:

            2-`tuple`
                The interpolation coefficents ``s`` and ``1 - s`` in
                that order, each of which are numpy arrays containing
                floats in the range [0.0, 1.0]. The numpy arrays are
                broadcastable to the tie points array.

        """

        # Create the interpolation variable, s, and make it
        # broadcastable to the tie points array.
        delta = 1 / n
        s = np.arange(0, 1 + delta / 2, delta)

        s_shape = [1] * self.ndim
        s_shape[d] = s.size
        s.resize(s_shape)

        return s, 1 - s
    
    # ----------------------------------------------------------------
    # Attributes
    # ----------------------------------------------------------------
    @property
    def interpolation(self):
        """TODO"""
        return "linear"

    # ----------------------------------------------------------------
    # Methods
    # ----------------------------------------------------------------
