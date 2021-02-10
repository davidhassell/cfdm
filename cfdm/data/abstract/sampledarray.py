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
            compressed_array=None,
            shape=None,
            size=None,
            ndim=None,
            compressed_dimensions=None,
            interpolation=None,
            tie_points=None,
            tie_point_indices=None,
            interpolation_parameters={},
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

            interpolation_parameters: `dict`
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
            interpolation_parameters=interpolation_parameters.copy(),
            
        )

    def _conform_interpolation_formula_terms(self):
        """Make sure the interpolation coefficents/configuration have the
         same relative dimension order as the tie points

        """
        dims = tuple(range(ndim))

        term_dimensions = self.term_dimensions

        interpolation_parameters = self.interpolation_parameters
        
        for term, c in (
                tuple(self.interpolation_parameters.items())
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
                 each interpolation zone. Each index for the tie point
                 interpolation dimensions is expressed as a list of
                 two integers, rather than a `slice` object, to
                 facilitate retrieval of each tie point individually.

               * The indices of the uncompressed array that correspond
                 to each interpolation zone. Each index in the tuple
                 is expressed as a `slice` object.

               * The indices for the axis of the uncompressed array
                 that corresponds interpolation dimensions of each
                 interpolation zone. Each index is given as `range`
                 object defineing the actual indices of the the
                 uncompressed array. 

        """
        tie_point_indices = self.get_tie_point_indices()
        
        compressed_dimensions = self.get_compression_dimension()

        slices = [slice(None)] * self.ndim

        u_slice = slices[:]        

        points = []

        interpolation_dimension_indices = [1] * self.ndim
        
        for d, tp_indices in zip(
                self.get_compression_dimension(),
                self.get_tie_point_indices()
        ):
            tp_slices = []
            d_points = []
            d_zzz = []

            tp_indices = tp_indices.array.flatten().tolist()
            
            for i, (index0, index1) in enumerate(
                    zip(tp_indices[:-1], tp_indices[1:])
            ):
                index1 = index1 + 1
                zone_indices = range(index0, index1)
                if len(zone_indices) <= 2:
                    # Interpolation area boundary
                    continue

                # The subspace for the axis of the tie points that
                # corresponds to this axis of the interpolation zone
                tp_slices.append([i, i + 1])

                # The subspace for this axis of the uncompressed array
                # that corresponds to the interpolation zone
                d_zzz.append(slice(index0, index1))

                # The indices for the axis of the uncompressed array
                # that corresponds to this axis of the interpolation
                # zone
                d_points.append(zone_indices)
                
            slices[d] = tp_slices
            u_slices[d] = d_zzz
            interpolation_dimension_indices[d] = d_points
            
        return (product(*slices)
                product(*u_slices),
                product(*interpolation_dimension_indices))
            
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

     def get_interpolation_parameters(self):
        """Return the interpolation parameter variables for sampled
        dimensions.

        .. versionadded:: (cfdm) TODO

        :Returns:

        **Examples:**

        >>> c = d.get_interpolation_parameters)

        """
        return self._get_component("interpolation_parameters")

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
            + self.get_interpolation_parameters()
        ):
            if v is None:
                continue

            v.data.to_memory()

        return self

    def tranpose(self, axes):
        """TODO"""
        # Tranpose the compressed array 
        compressed_array = self.source().tranpose(axes=axes)

        # Transpose the shape
        old_shape = self.shape
        shape = tuple([old_shape.index(i) for n in axes])
            i

        # Change the compressed dimensions
        compressed_dimensions = sorted(
            [axes.index(i)
             for i in self._get_component("compressed_dimension")]
        )
        
        # Change the tie point index dimensions
        tie_point_indices = {
            axes.index(i): v for i, v in self.tie_point_indices.items()
        }
        
        # Change the interpolation parameter dimensions
        parameter_dimensions = {
            term: tuple([axes.index(i) for i in v])
            for term, v in self.parameter_dimensions.items()
        }

        return type(self)(
            compressed_array=compressed_array,
            shape=shape, ndim=self.ndim, size=self.size,
            compressed_dimensions=compressed_dimensions,
            interpolation=self.interpolation,
            tie_point_indices=tie_point_indices,
            interpolation_parameters=self.get_interpolation_parameters(),
            parameter_dimensions=parameter_dimensions,
        )
    
