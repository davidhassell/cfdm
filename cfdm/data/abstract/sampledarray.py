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
            tie_point_indices={},
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

    def _conform_interpolation_parameters(self):
        """Make sure the interpolation parameters arrays have the same
        relative dimension order as the tie point array.

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

    def _interpolation_zones(self,
                             non_interpolation_dimension_value=slice(None)):
        """TODO

        :Returns:
        
            iterator, iterator
               * The indices of the tie point array that correspond to
                 each interpolation zone. Each index for the tie point
                 interpolation dimensions is expressed as a list of
                 two integers, rather than a `slice` object, to
                 facilitate retrieval of each tie point individually.

               * The indices of the uncompressed array that correspond
                 to each interpolation zone. Each index in the tuple
                 is expressed as a `slice` object.

        """
        tie_point_indices = self.get_tie_point_indices()
        
        compressed_dimensions = self.get_compression_dimension()

        # Initialise the indices of the tie point array that
        # correspond to each interpolation zone.
        #
        # This list starts off with (slice(None),) for all dimensions,
        # but each tie point interpolation dimension will then be
        # replaced by a sequence of index pairs that define tie point
        # values for each interpolation zone. Finally, the caresian
        # product of the list is returned
        #
        # For example, if the tie point array has three dimensions and
        # dimensions 0 and 2 are tie point interpolation dimensions,
        # then the list could evolve as follows:
        #
        # Initialization:
        #
        # [(slice(None),), (slice(None),), (slice(None),)]
        #
        # Overwrite tie point interpolation dimension entries with
        # indices to tie point pairs:
        #
        # [[[0, 1], [2, 3]], (slice(None),), [[0,1], [1, 2], [2, 3]]]
        #
        # Returned cartesian product (one set of indices per
        # interpolation zone):
        #
        # [[0, 1], slice(None), [0, 1]),
        #  [0, 1], slice(None), [1, 2]),
        #  [0, 1], slice(None), [2, 3]),
        #  [2, 3], slice(None), [0, 1]),
        #  [2, 3], slice(None), [1, 2]),
        #  [2, 3], slice(None), [2, 3])]
        tp_interpolation_zones = (
            [(non_interpolation_dimension_value,)] * self.ndim
        )
        
        # Initialise indices of the uncompressed array that correspond
        # to each interpolation zone.
        #
        # This list starts off with (slice(None),) for all dimensions,
        # but each interpolation dimension will then be replaced by a
        # sequence of slices that define tie the uncompressed array
        # locations for each interpolation zone. Finally, the caresian
        # product of the list is returned
        #
        # For example, if the tie point array has three dimensions and
        # dimensions 0 and 2 are interpolation dimensions, then list
        # could evolve as follows:
        #
        # Initialization:
        #
        # [(slice(None),), (slice(None),), (slice(None),)]
        #
        # Overwrite interpolation dimension entries with indices to
        # tie point pairs:
        #
        # [[slice(0, 10), slice(11, 20)],
        #  (slice(None),),
        #  [slice(0, 5), slice(5, 10), slice(10, 15)]]
        #
        # Returned cartesian product (one set of indices per
        # interpolation zone):
        #
        # [(slice(0, 10), slice(None), slice(0, 5)),
        #  (slice(0, 10), slice(None), slice(5, 10)),
        #  (slice(0, 10), slice(None), slice(10, 15)),
        #  (slice(11, 20), slice(None), slice(0, 5)),
        #  (slice(11, 20), slice(None), slice(5, 10)),
        #  (slice(11, 20), slice(None), slice(10, 15))]
        u_interpolation_zones = tp_indices[:]
        
        for d self.get_compression_dimension():
            tp_index = []
            u_index = []
            
            tie_point_indices = self.get_tie_point_indices()
            tie_point_indices = tie_point_indices[d].array.flatten().tolist()
            
            for i, (index0, index1) in enumerate(
                    zip(tie_point_indices[:-1], tie_point_indices[1:])
            ):
                if index1 - index0 <= 1:
                    # Interpolation area boundary
                    continue

                # The subspace for the axis of the tie points that
                # corresponds to this axis of the interpolation zone
                tp_index.append([i, i + 1])

                # The subspace for this axis of the uncompressed array
                # that corresponds to the interpolation zone
                u_index.append(slice(index0, index1 + 1))

            tp_interpolation_zones[d] = tp_index
            u_interpolation_zones[d] = u_index
            
        return (product(*tp_interpolation_zones),
                product(*u_interpolation_zones))
            
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
        """Return the tie points data.

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
    
