import numpy as np

from .abstract import Sampledrray


class SampledLinearArray(Sampledrray):
    """TODO

    .. versionadded:: (cfdm) TODO

    """

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

        first_zone = True
        for tp_indices, u_indices, new_area in zip(
            *self._interpolation_zones()
        ):
            u_slice = u_indices[d0]
            n = u_slice.stop - u_slice.start

            tp_index0 = list(tp_indices)
            tp_index1 = tp_index0[:]
            tp_index0[d0] = tp_index0[d0][:1]
            tp_index1[d0] = tp_index1[d0][1:]

            uarray[u_indices] = self._linear_interpolation(
                d0,
                n,
                new_area[d0],
                tie_points[tuple(tp_index0)].array,
                tie_points[tuple(tp_index1)].array,
            )

        self._calculate_s.cache_clear()

        return self.get_subspace(uarray, indices, copy=True)

    # ----------------------------------------------------------------
    # Private methods
    # ----------------------------------------------------------------
    def _linear_interpolation(self, d, n, new_area, a0, a1):
        """TODO

        .. versionadded:: (cfdm) TODO

        :Parameters:

            d: `int`
                The position of the tie point interpolation
                dimension.

            n: sequence of `slice` or `list`
                The number of uncompressed elements in this
                interpolation zone along the interpolation dimension.

            new_area: `bool`
                Set to True if this interpolation is at the start of a
                new interpolation area, otherwise set to False. If
                False then the first element along the interpolation
                dimension of the uncompressed array is removed.

            a0, a1: array_like
               The arrays containing the points for pair-wise
               interpolation along dimension *d*

        """
        s, one_minus_s = self._calculate_s(d, n)

        u = a0 * one_minus_s + a1 * s

        if not new_area:
            indices = [slice(None)] * u.ndim
            indices[d] = slice(1, None)
            u = u[tuple(indices)]

        return u

    @lru_cache(maxsize=32)
    def _calculate_s(self, d, n):
        """Create the interpolation coefficients and 1-s.

        .. versionadded:: (cfdm) TODO

        :Parameters:

            d: `int`
                The position of the tie point interpolation dimension.

            n: `int`
                The number of uncompressed elements in this
                interpolation zone along the interpolation dimension.

        :Returns:

            2-`tuple`
                The interpolation coefficents ``s`` and ``1 - s`` in
                that order, each of which are numpy arrays containing
                floats in the range [0.0, 1.0]. The numpy arrays have
                size 1 dimensions corresponding to all tie point
                dimensions other than *d*.

        **Examples:**

        >>> x.ndim
        >>> 2
        >>> x._calculate_s(1, 6)
        (array([[0. , 0.2, 0.4, 0.6, 0.8, 1. ]]),
         array([[1. , 0.8, 0.6, 0.4, 0.2, 0. ]]))

        """
        delta = 1 / (n - 1)
        s = np.arange(0, 1 + delta / 2, delta)

        one_minus_s = s[::-1]

        new_shape = [1] * self.ndim
        new_shape[d] = s.size

        return (s.reshape(new_shape), one_minus_s.reshape(new_shape))

    # ----------------------------------------------------------------
    # Attributes
    # ----------------------------------------------------------------
    @property
    def interpolation(self):
        """The description of the interpolation method."""
        return "linear"
