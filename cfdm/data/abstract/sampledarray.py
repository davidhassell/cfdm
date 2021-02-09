from itertools import product

import numpy as np

from . import CompressedArray


_float64 = np.dtype(float)


class SampledArray(CompressedArray):
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
            interpolation_configuation=(),
            compression_type="linear"
    ):
        """**Initialization**

        :Parameters:

            tie_points: `Data`
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

            interpolation: `str`
                TODO The interpolation method

            tie_point_indices: `dict`, optional
                TODO

            interpolation_coefficients: `dict`
                TODO

            interpolation_configuration: `dict`
                TODO

        """
        super().__init__(
            compressed_array=tie_points,
            shape=shape,
            size=size,
            ndim=ndim,
            compressed_dimensions=compressed_dimensions,
            compression_type="sampled",
            interpolation=interpolation,
            tie_point_indices=tuple(tie_point_indices),
            interpolation_coefficients=interpolation_coefficients.copy(),
            interpolation_configurations=interpolation_configuration.copy(),
            
        )

    def _conform_interpolation_formula_terms(self):
        """Make sure the interpolation coefficents/configuration have the
         same relative dimension order as the tie points

        """
        dims = tuple(range(ndim))

        term_dimensions = self.term_dimensions

        interpolation_coefficients = self.interpolation_coefficients
        
        for term, c in (
                tuple(self.interpolation_coefficients.items())
                + tuple(self.get_interpolation_configuration.items())
        ):
            dimensions = term_dimensions[term]
            new_order = [dimensions.index(i) for i in dims
                         if i in dimensions]
            if new_order == list(range(c.ndim)):
                continue
            
            interpolation_ceofficients[term] = c.tranpose(new_order)
            term_dimensions[term] = tuple([dimensions[i] for i in new_order])

    def _define_interpolation_zones(self):
        """TODO

        :Returns:
        
            3-`tuple`
            
               * The indices of the tie point array that correspond to
                 each interpolation zone.

               * The indices of the uncompressed array that correspond
                 to each interpolation zone.

               * The shape of the interpolation zone in the
                 uncompressed array, ignoring any non-interpolation
                 dimensions.

        """
        tie_point_indices = self.get_tie_point_indices()
        
        compressed_dimensions = self.get_compression_dimension()

        slices = [slice(None)] * self.ndim

        u_slice = slices[:]        

        points = []
        
        for d, tp_indices in zip(
                self.get_compression_dimension(),
                self.get_tie_point_indices()
        ):
            tp_slices = []
            d_points = []
            d_zzz = []

            tp_indices = tp_indices.array.tolist()
            for i, (index0, index1) in enumerate(
                    zip(tp_indices[:-1], tp_indices[1:])
            ):
                diff = index1 - index0
                if diff <= 1:
                    continue

                tp_slices.append([i, i + 1])
                d_zzz.append(slice(index0, index1 + 1))
                d_points.append(1 / diff)
                
            slices[d] = tp_slices
            u_slices[d] = d_zzz
            points.append(d_points)
            
        return product(*slices), product(*u_slices), product(*points)
            
    # ----------------------------------------------------------------
    # Attributes
    # ----------------------------------------------------------------
    @property
    def dtype(self):
        """Data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODO

        **Examples:**

        >>> a.dtype
        dtype('float64')
        >>> print(type(a.dtype))
        <type 'numpy.dtype'>

        """
        return _float64

    @property
    def interpolation(self):
        """TODO"""
        raise NotImplementedError("Must implement in subclasses")

    # ----------------------------------------------------------------
    # Methods
    # ----------------------------------------------------------------
    def get_compressed_axes(self):
        """Return axes that are compressed in the underlying array.

        :Returns:

            `list`
                The compressed axes described by their integer positions.

        **Examples:**

        >>> c.ndim
        4
        >>> c.compressed_array.ndim
        3
        >>> c.compressed_axes()
        [1, 2]

        """
        return list(self.get_compressed_dimension())

     def get_interpolation_coefficients(self):
        """Return the interpolation coefficient variables for sampled
        dimensions.

        .. versionadded:: (cfdm) TODO

        :Returns:

        **Examples:**

        >>> c = d.get_interpolation_coefficients)

        """
        try:
            return self._get_component("interpolation_coefficients")
        except ValueError:
            return {}

    def get_interpolation_configuration(self):
        """Return the interpolation TODO

        .. versionadded:: (cfdm) TODO

        :Returns:

        **Examples:**

        """
        try:
            return self._get_component("interpolation_configuration")
        except ValueError:
            return {}

    def get_sampled_dimensions(self):
        """Return the positions of the sampled dimensions in array.

        .. versionadded:: (cfdm) TODO

        .. seealso:: TODO

        :Returns:

            `tuple` of `int`
                The positions of the sampled dimensions in the array.

        **Examples:**

        >>> i = d.get_sampled_dimensions()

        """
        return self._get_component("sampled_dimensions")

    def get_tie_point_indices(self, default=ValueError()):
        """Return the tie point index variables for sampled dimensions.

        .. versionadded:: (cfdm) TODO

        :Parameters:

            default: optional
                Return the value of the *default* parameter if tie point
                index variables have not been set.

                {{default Exception}}

        :Returns:

            `tuple` of `TiePointIndex`
                The tie point index variables.

        **Examples:**

        >>> c = d.get_tie_point_indices()

        """
        try:
            return self._get_component("tie_point_indices")
        except ValueError:
            return self._default(
                default,
                f"{self.__class__.__name__} has no tie point indices"
            )
        
    def get_tie_points(self, default=ValueError()):
        """Return the tie point variables for sampled dimensions.

        .. versionadded:: (cfdm) TODO

        :Parameters:

            default: optional
                Return the value of the *default* parameter if tie point
                variables have not been set.

                {{default Exception}}

        :Returns:

            `TiePoint`
                The tie point variable.

        **Examples:**

        >>> c = d.get_tie_points()

        """
        try:
            return self._get_compressed_Array()
        except ValueError:
            return self._default(
                default, f"{self.__class__.__name__} has no tie points"
            )

    def to_memory(self):
        """Bring an array on disk into memory and retain it there.

        There is no change to an array that is already in memory.

        .. versionadded:: (cfdm) TODO

        :Returns:

            `{{class}}`
                The array that is stored in memory.

        **Examples:**

        >>> b = a.to_memory()

        """
        for v in self.get_tie_points():
            v.data.to_memory()

        for v in (
            self.get_tie_point_indices()
            + self.get_tie_point_offsets()
            + self.get_interpolation_coefficients()
        ):
            if v is None:
                continue

            v.data.to_memory()

        return self
