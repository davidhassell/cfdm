from itertools import product

import numpy as np

from . import CompressedArray


_float64 = np.dtype(float)


class SampledArray(CompressedArray):
    """TODO.

    .. versionadded:: (cfdm) TODO

    """

    def __init__(
        self,
        compressed_array=None,
        shape=None,
        size=None,
        ndim=None,
        compressed_axes=None,
        interpolation=None,
        tie_points=None,
        tie_point_indices={},
        interpolation_parameters={},
        parameter_dimensions={},
    ):
        """Initialisation.

        :Parameters:

            tie_points: `Data`
                The compressed array.

            shape: `tuple`
                The uncompressed array dimension sizes.

            size: `int`
                Number of elements in the uncompressed array.

            ndim: `int`
                The number of uncompressed array dimensions.

            compressed_axes: sequence of `int`
                The positions of the compressed axes.

            interpolation: `str`
                TODO The interpolation method

            tie_point_indices: `dict`, optional
                TODO. The set of keys must be the same as the set of
                compressed_axes.

            interpolation_parameters: `dict`
                TODO

            parameter_dimensions: `dict`
                TODO

        """
        super().__init__(
            compressed_array=tie_points,
            shape=shape,
            size=size,
            ndim=ndim,
            compressed_dimension=tuple(sorted(compressed_axes)),
            compression_type="sampled",
            interpolation=interpolation,
            tie_point_indices=tie_point_indices.copy(),
            interpolation_parameters=interpolation_parameters.copy(),
            parameter_dimensions=parameter_dimensions.copy(),
        )

    def __getitem__(self, indices):
        """x.__getitem__(indices) <==> x[indices]

        Runs the `_conform_interpolation_parameters` method and
        returns `None`.

        In subclasses, however, returns a subspace of the array as an
        independent numpy array.

        """
        self._conform_interpolation_parameters()

    # ----------------------------------------------------------------
    # Private methods
    # ----------------------------------------------------------------
    def _conform_interpolation_parameters(self):
        """Conform the interpolation parameters in-place.

        Tranposes the interpolation parameters to have the same relative
        dimension order as the tie point array.

        """
        parameters = self.get_interpolation_parameters()
        if not parameters:
            return

        term_dimensions = self.get_parameter_dimensions()

        dims = tuple(range(self.ndim))
        for term, c in parameters.items():
            dimensions = term_dimensions[term]
            if dimensions == tuple(sorted(dimensions)):
                # Interpoaltion parameters dimension are already in
                # the correct order
                continue

            new_order = [dimensions.index(i) for i in dims if i in dimensions]
            parameters[term] = c.tranpose(new_order)
            term_dimensions[term] = tuple([dimensions[i] for i in new_order])

    # ----------------------------------------------------------------
    # Attributes
    # ----------------------------------------------------------------
    @property
    def dtype(self):
        """Data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODO

        """
        return _float64

    @property
    def interpolation(self):
        """The description of the interpolation method."""
        raise NotImplementedError("Must implement in subclasses")

    # ----------------------------------------------------------------
    # Methods
    # ----------------------------------------------------------------
    def get_interpolation_parameters(self):
        """Return the interpolation parameter variables for sampled
        dimensions.

        .. versionadded:: (cfdm) TODO

        .. seealso:: `get_parameter_dimensions`

        :Returns:

            `dict`

        """
        return self._get_component("interpolation_parameters")

    def get_parameter_dimensions(self):
        """TODO.

        .. versionadded:: (cfdm) TODO

        .. seealso:: ` get_interpolation_parameters`

        :Returns:

            `dict`

        """
        return self._get_component("parameter_dimensions")

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
                default, f"{self.__class__.__name__} has no tie point indices"
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

    def interpolation_zones(
        self, non_interpolation_dimension_value=slice(None)
    ):
        """TODO.

        .. versionadded:: (cfdm) TODO

        :Returns:

            iterator, iterator, iterator
               * The indices of the tie point array that correspond to
                 each interpolation zone. Each index for the tie point
                 interpolation dimensions is expressed as a list of
                 two integers, rather than a `slice` object, to
                 facilitate retrieval of each tie point individually.

               * The indices of the uncompressed array that correspond
                 to each interpolation zone. Each index in the tuple
                 is expressed as a `slice` object.

               * Flags which state, for each interpolation dimension,
                 whether each interplation zone is at the start of an
                 interpolation area.

        """
        tie_point_indices = self.get_tie_point_indices()

        non_interpolation_dimension_value = slice(None)

        # Initialise the indices of the tie point array that
        # correspond to each interpolation zone.
        #
        # This list starts off with (slice(None),) for all dimensions,
        # but each tie point interpolation dimension will then be
        # replaced by a sequence of index pairs that define tie point
        # values for each interpolation zone. Finally, the cartesian
        # product of the list is returned.
        #
        # For example, if the tie point array has three dimensions and
        # dimensions 0 and 2 are tie point interpolation dimensions,
        # and interpolation dimension 0 has two interpolation areas,
        # then the list could evolve as follows:
        #
        # Initialization:
        #
        # [(slice(None),), (slice(None),), (slice(None),)]
        #
        # Overwrite tie point interpolation dimension entries with
        # indices to tie point pairs:
        #
        # [[[0, 1], [2, 3]],
        #  (slice(None),),
        #  [[0, 1], [1, 2], [2, 3]]]
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
        tp_interpolation_zones = [
            (non_interpolation_dimension_value,)
        ] * self.ndim

        # Initialise indices of the uncompressed array that correspond
        # to each interpolation zone.
        #
        # This list starts off with (slice(None),) for all dimensions,
        # but each interpolation dimension will then be replaced by a
        # sequence of slices that define tie the uncompressed array
        # locations for each interpolation zone, omitting the first
        # tie point if the interpolation is not at teh start of a new
        # interpolation area. Finally, the cartesian product of the
        # list is returned.
        #
        # For example, if the tie point array has three dimensions and
        # dimensions 0 and 2 are interpolation dimensions, and
        # interpolation dimension 0 has two interpolation areas, then
        # list could evolve as follows:
        #
        # Initialization:
        #
        # [(slice(None),), (slice(None),), (slice(None),)]
        #
        # Overwrite interpolation dimension entries with indices to
        # tie point pairs:
        #
        # [[slice(0, 10), slice(10, 20)],
        #  (slice(None),),
        #  [slice(0, 6), slice(6, 11), slice(11, 16)]]
        #
        # Returned cartesian product (one set of indices per
        # interpolation zone):
        #
        # [(slice(0, 10), slice(None), slice(0, 6)),
        #  (slice(0, 10), slice(None), slice(6, 11)),
        #  (slice(0, 10), slice(None), slice(11, 16)),
        #  (slice(10, 20), slice(None), slice(0, 6)),
        #  (slice(10, 20), slice(None), slice(6, 11)),
        #  (slice(10, 20), slice(None), slice(11, 16))]
        u_interpolation_zones = tp_indices[:]

        # Initialise the boolean flags which state, for each
        # interpolation dimension, whether each interplation zone is
        # at the start of an interpolation area, moving from left to
        # right in index space. Non-interpolation dimensions are given
        # the flag `None`.
        #
        # These flags allow the indices in u_interpolation_zones to be
        # modified if it is desired, or not, to overwrite uncompressed
        # values that have otherwise already been (or will be)
        # computed.
        #
        # For the example given for tp_interpolation_zones, we would
        # have
        #
        # Initialization:
        #
        # [(None,), (None,), (None,)]
        #
        # Overwrite interpolation dimension entries with flags for
        # each interpolation zone:
        #
        # [[True, True],
        #  (None,)
        #  [True, False, False]]
        #
        # Returned cartesian product (one set of flags per
        # interpolation zone):
        #
        # [(True, None, True),
        #  (True, None, False),
        #  (True, None, False),
        #  (True, None, True),
        #  (True, None, False),
        #  (True, None, False)]
        new_interpolation_area = [(None,)] * self.ndim

        for d in self.get_compressed_axes():
            tp_index = []
            u_index = []
            new_area = []

            tie_point_indices = tie_point_indices[d].array.flatten().tolist()

            new = True

            for i, (index0, index1) in enumerate(
                zip(tie_point_indices[:-1], tie_point_indices[1:])
            ):
                new_area.append(new)

                if index1 - index0 <= 1:
                    new = True
                    new_area.pop()
                    continue

                # The subspace for the axis of the tie points that
                # corresponds to this axis of the interpolation zone
                tp_index.append([i, i + 1])

                # The subspace for this axis of the uncompressed array
                # that corresponds to the interpolation zone, only
                # including the first tie point if this the first
                # interpolation zone in a new interpolation area.
                if not new:
                    index0 += 1

                u_index.append(slice(index0, index1 + 1))

                new = False

            tp_interpolation_zones[d] = tp_index
            u_interpolation_zones[d] = u_index
            new_interpolation_area[d] = new_area

        return (
            product(*tp_interpolation_zones),
            product(*u_interpolation_zones),
            product(*new_interpolation_area),
        )

    def to_memory(self):
        """Bring an array on disk into memory.

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
        """TODO."""
        # Tranpose the compressed array
        compressed_array = self.source().tranpose(axes=axes)

        # Transpose the shape
        old_shape = self.shape
        shape = tuple([old_shape.index(i) for n in axes])

        # Change the compressed dimensions
        compressed_dimensions = sorted(
            [
                axes.index(i)
                for i in self._get_component("compressed_dimension")
            ]
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
            shape=shape,
            ndim=self.ndim,
            size=self.size,
            compressed_dimensions=compressed_dimensions,
            interpolation=self.interpolation,
            tie_point_indices=tie_point_indices,
            interpolation_parameters=self.get_interpolation_parameters(),
            parameter_dimensions=parameter_dimensions,
        )
