import numpy as np

from .abstract import CompressedArray
from .mixin import SubsampledArray
#from .subsampledgeneralarray import SubsampledGeneralArray


class SubsampledLinearArray(SubsampledArray, CompressedArray): #SubsampledGeneralArray):
    """TODO.

    .. versionadded:: (cfdm) 1.9.TODO.0

    """

    def __init__(
        self,
        compressed_array=None,
        shape=None,
        size=None,
        ndim=None,
        dtype=None,
        compressed_axes=None,
        tie_point_indices=None,
        computational_precision=None,
            bounds=False,
            interpolation_variable=None,
    ):
        """Initialisation.

        :Parameters:

            compressed_array: `Data`
                The tie points array.

            shape: `tuple`
                The uncompressed array dimension sizes.

            size: `int`
                Number of elements in the uncompressed array.

            ndim: `int`
                The number of uncompressed array dimensions.

            dtype: data-type, optional
               The data-type for the uncompressed array. This datatype
               type is also used in all interpolation calculations. By
               default, the data-type is double precision float.

            compressed_axes: sequence of `int`
                The position of the compressed axis in the tie points
                array.

                *Parameter example:*
                  ``compressed_axes=[1]``

            tie_point_indices: `dict`, optional
                The tie point index variable for each subsampled
                dimension. An integer key indentifies a subsampled
                dimensions by its position in the tie points array,
                and the value is a `TiePointIndex` variable.

            computational_precision: `str`, optional
                The floating-point arithmetic precision used during
                the preparation and validation of the compressed
                coordinates.

                *Parameter example:*
                  ``computational_precision='64'``

            bounds: `bool`, optional
                If True then the tie points represent coordinate
                bounds. See CF section 8.3.9 "Interpolation of Cell
                Boundaries".

             interpolation_variable: `Interpolation`, optional
        TODO	
        """
        super().__init__(
            compressed_array=compressed_array,
            shape=shape,
            size=size,
            ndim=ndim,
            compressed_dimension=tuple(compressed_axes),
            compression_type="subsampled",
            interpolation_name="linear",
            computational_precision=computational_precision,
            tie_point_indices=tie_point_indices.copy(),
            bounds=bounds,
            interpolation_variable=interpolation_variable,
        )

        if dtype is None:            
            dtype = self._default_dtype

        self.dtype = dtype

    def __getitem__(self, indices):
        """x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the array as an independent numpy array.

        .. versionadded:: (cfdm) 1.9.TODO.0

        """
        # If the first or last element is requested then we don't need
        # to interpolate
        try:
            return self._first_or_last_index(indices)
        except IndexError:
            pass

        # ------------------------------------------------------------
        # Method: Uncompress the entire array and then subspace it
        # ------------------------------------------------------------
        (d0,) = self.get_compressed_axes()

        tie_points = self.get_tie_points()

        # Initialise the un-sliced uncompressed array
        uarray = np.ma.masked_all(self.shape, dtype=self.dtype)

        # Interpolate the tie points for each interpolation subarea
        for u_indices, tp_indices, subarea_shape, first, _ in zip(
            *self.interpolation_subareas()
        ):
            ua = self._select_tie_points(tie_points, tp_indices, {d0: 0})
            ub = self._select_tie_points(tie_points, tp_indices, {d0: 1})
            u = self._linear_interpolation(ua, ub, (d0,), subarea_shape, first)

            self._set_interpolated_values(uarray, u_indices, (d0,), u)

        self._s.cache_clear()

        return self.get_subspace(uarray, indices, copy=True)

    def _linear_interpolation(self, ua, ub, subsampled_dimensions,
                              subarea_shape, first):
        """Interpolate linearly between pairs of tie points.

        This is the linear interpolation operator ``fl`` defined in CF
        appendix J:

        u = fl(ua, ub, s) = ua + s*(ub-ua)
                          = ua*(1-s) + ub*s

        .. versionadded:: (cfdm) 1.9.TODO.0

        :Parameters:

            ua, ub: array_like
               The arrays containing the points for pair-wise
               interpolation along dimension *d0*.

            subsampled_dimensions: `tuple` of `int`
                The position of a subsampled dimension in the tie
                points array.

            subarea_shape: `tuple` of `int`
                The shape of the uncompressed interpolation subararea,
                including all tie points, but excluding a bounds
                dimension.

            first: `tuple`
                For each tie point array dimension, True if the
                interpolation subarea is the first (in index space) of
                a new continuous area, otherwise False.

        :Returns:

            `numpy.ndarray`

        """
        (d0,) = subsampled_dimensions
        
        # Get the interpolation coefficents
        s, one_minus_s = self._s(d0, subarea_shape)

        # Interpolate
        u = ua * one_minus_s + ub * s

        if not first[d0]:
            # Remove the first point of the interpolation subarea if
            # it is not the first (in index space) of a continuous
            # area. This is beacuse this value in the uncompressed
            # data has already been calculated from the previous (in
            # index space) interpolation subarea.
            indices = [slice(None)] * u.ndim
            indices[d0] = slice(1, None)
            u = u[tuple(indices)]

        return u
