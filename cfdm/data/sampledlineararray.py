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
        tie_point_indices=None,
        interpolation_coefficients=(),
        interpolation_coefficients=(),
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

            interpolation_coefficients: `dict`
                TODO

            interpolation_configuration: `dict`
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
            interpolation_coefficients=interpolation_coefficients.copy(),
            interpolation_configurations=interpolation_configuration.copy(),
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
        d = self.get_compression_dimension()[0]
        
        # Initialise the un-sliced uncompressed array
        uarray = np.ma.masked_all(self.shape, dtype=_float)

        s_shape = [1] * self.ndim
        
        for tp_indices, u_indices, n_points in zip(
                *self._define_interpolation_zones()
        ):
            # Create interpolation variable, s
            delta = n_points[0]
            s = np.arange(0, 1 + delta / 2, delta)
            s_shape[d] = s.size
            s.resize(s_shape)

            tp_indices = tp_indices[0]
            
            x = tie_points[tp_indices[:1]].array * (1 - s)
            x += tie_points[tp_indices[1:]].array * s
            
            uarray[u_indices] = x

        return self.get_subspace(uarray, indices, copy=True)

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
