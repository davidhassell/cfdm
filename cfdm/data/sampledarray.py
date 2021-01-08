import numpy

from . import abstract


_float64 = numpy.dtype(float)


class SampledArray(abstract.Array):
    '''TODO

    .. versionadded:: (cfdm) TODO

    '''
    # ----------------------------------------------------------------
    # Define the interpolated datatype, depending on the interpolation
    # method.
    # ----------------------------------------------------------------
    _interpolated_datatype = {
        'linear': _float64,
        'bilinear': _float64,
    }

    def __init__(self, shape=None, size=None, ndim=None,
                 compressed_dimensions=None, interpolation=None,
                 tie_points=None, tie_point_indices=None,
                 interpolation_coefficients=(),
                 interpolation_coefficients=()):
        '''**Initialization**

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
            compresed array.

        interpolation: `str`
            TODO The interpolation method

        tie_point_indices: sequence of `TiePointIndex` or `None`
            TODO An elements of `None` indicates that there is a
            unique tie point for every element of the corresponding
            target domain dimension.

        interpolation_coefficients: sequence of `InterpolationCoefficient`, optional
            TODO. May be an empty sequence.

        interpolation_configuration: sequence of `InterpolationConfiguration`, optional
            TODO. May be an empty sequence.

        '''
        compressed_dimensions = tuple(sorted(compressed_dimensions))

        # Copy variables and ensure that they are stored in tuples
        tie_points = tuple(v.copy() for v in tie_points)
        
        tie_point_indices = tuple(
            v if v is None else v.copy() for v in tie_point_indices
        )
        
        interpolation_coefficients = tuple(
            v.copy() for v in interpolation_coefficients
        )
                
        interpolation_configuration = tuple(
            v.copy() for v in interpolation_configuration
        )
        
        super().__init__(
            shape=shape, size=size, ndim=ndim,
            compressed_dimensions=compressed_dimensions,
            interpolation=interpolation, tie_points=tie_points,
            tie_point_indices=tie_point_indices,
            interpolation_coefficients=interpolation_coefficients,
            interpolation_configurations=interpolation_configurations,
            compression_type='sampled'
        )

    def __getitem__(self, indices):
        '''x.__getitem__(indices) <==> x[indices]

    Returns a subspace of the array as an independent numpy array.

    The indices that define the subspace must be either `Ellipsis` or
    a sequence that contains an index for each dimension. In the
    latter case, each dimension's index must either be a `slice`
    object or a sequence of two or more integers.

    Indexing is similar to numpy indexing. The only difference to
    numpy indexing (given the restrictions on the type of indices
    allowed) is:

      * When two or more dimension's indices are sequences of integers
        then these indices work independently along each dimension
        (similar to the way vector subscripts work in Fortran).

    .. versionadded:: (cfdm) TODO

        '''
        raise NotImplementedError("TODO")
    
        # ------------------------------------------------------------
        # Method: Uncompress the entire array and then subspace it
        # ------------------------------------------------------------

        # Initialise the un-sliced uncompressed array
        uarray = numpy.ma.masked_all(self.shape, dtype=self.dtype)

    # ----------------------------------------------------------------
    # Attributes
    # ----------------------------------------------------------------
    @property
    def dtype(self):
        '''Data-type of the uncompressed data.

    .. versionadded:: (cfdm) TODO

    **Examples:**

    >>> a.dtype
    dtype('float64')
    >>> print(type(a.dtype))
    <type 'numpy.dtype'>

        '''
        datatype = self._interpolated_datatype.get(self.get_interpolation())
        if datatype is not None:
            return datatype
        
        return _float64

    @property
    def ndim(self):
        '''The number of dimensions of the uncompressed data.

    **Examples:**

    >>> d.shape
    (73, 96)
    >>> d.ndim
    2
    >>> d.size
    7008

    >>> d.shape
    (1, 1, 1)
    >>> d.ndim
    3
    >>> d.size
    1

    >>> d.shape
    ()
    >>> d.ndim
    0
    >>> d.size
    1

        '''
        return self._get_component('ndim')

    @property
    def shape(self):
        '''Shape of the uncompressed data.

    **Examples:**

    >>> d.shape
    (73, 96)
    >>> d.ndim
    2
    >>> d.size
    7008

    >>> d.shape
    (1, 1, 1)
    >>> d.ndim
    3
    >>> d.size
    1

    >>> d.shape
    ()
    >>> d.ndim
    0
    >>> d.size
    1

        '''
        return self._get_component('shape')

    @property
    def size(self):
        '''Number of elements in the uncompressed data.

    **Examples:**

    >>> d.shape
    (73, 96)
    >>> d.size
    7008
    >>> d.ndim
    2

    >>> d.shape
    (1, 1, 1)
    >>> d.ndim
    3
    >>> d.size
    1

    >>> d.shape
    ()
    >>> d.ndim
    0
    >>> d.size
    1

        '''
        return self._get_component('size')

    @property
    def compressed_array(self):
        '''Return an independent numpy array containing the compressed data.

    Always raises an exception, as there is no underlying compressed
    array for compression by sampling.

    **Examples:**

    >>> n = a.compressed_array
    >>> ValueError: There is no underlying compressed array for compression by sampling

        '''
        raise ValueError(
            "There is no underlying compressed array for "
            "compression by sampling"
        )

    # ----------------------------------------------------------------
    # Methods
    # ----------------------------------------------------------------
    def get_interpolation_coefficients(self):
        '''Return the interpolation coefficient variables for sampled
    dimensions.

    .. versionadded:: (cfdm) TODO

    :Returns:

        `tuple` of `InterpolationCoefficient`
            The tie point index variables. May be an empty tuple.

    **Examples:**

    >>> c = d.get_interpolation_coefficients)

        '''
        try:
            return self._get_component('interpolation_coefficients')
        except ValueError:
            return ()

    def get_sampled_dimensions(self):
        '''Return the positions of the sampled dimensions in array.

    .. versionadded:: (cfdm) TODO

    .. seealso:: TODO

    :Returns:

        `tuple` of `int`
            The positions of the sampled dimensions in the array.

    **Examples:**

    >>> i = d.get_sampled_dimensions()

        '''
        return self._get_component('sampled_dimensions')

    def get_tie_point_indices(self, default=ValueError()):
        '''Return the tie point index variables for sampled dimensions.

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

        '''
        try:
            return self._get_component('tie_point_indices')
        except ValueError:
            return self._default(
                default,
                "{!r} has no tie point index variables".format(
                    self.__class__.__name__)
            )
        
    def get_tie_point_offsets(self, default=ValueError()):
        '''Return the tie point offset variables for sampled dimensions.

    .. versionadded:: (cfdm) TODO

    :Parameters:

        default: optional
            Return the value of the *default* parameter if tie point
            offset variables have not been set.

            {{default Exception}}

    :Returns:

        `tuple` of `TiePointOffset` or `None`
            The tie point offset variables. An element of `None` is
            equivalent to an offset of zero.

    **Examples:**

    >>> c = d.get_tie_point_offsets()

        '''
        try:
            return self._get_component('tie_point_offsets')
        except ValueError:
            return self._default(
                default,
                "{!r} has no tie point offset variables".format(
                    self.__class__.__name__)
            )
        
    def get_tie_points(self, default=ValueError()):
        '''Return the tie point variables for sampled dimensions.

    .. versionadded:: (cfdm) TODO

    :Parameters:

        default: optional
            Return the value of the *default* parameter if tie point
            variables have not been set.

            {{default Exception}}

    :Returns:

        `tuple` of `TiePoint`
            The tie point variables.

    **Examples:**

    >>> c = d.get_tie_points()

        '''
        try:
            return self._get_component('tie_points')
        except ValueError:
            return self._default(
                default,
                "{!r} has no tie point variables".format(
                    self.__class__.__name__)
            )

    def to_memory(self):
        '''Bring an array on disk into memory and retain it there.

    There is no change to an array that is already in memory.

    .. versionadded:: (cfdm) TODO

    :Returns:

        `{{class}}`
            The array that is stored in memory.

    **Examples:**

    >>> b = a.to_memory()

        '''
        for v in self.get_tie_points():
            v.data.to_memory()
    
        for v in (
                self.get_tie_point_indices() +
                self.get_tie_point_offsets() +
                self.get_interpolation_coefficients()
        ):
            if v is None:
                continue
            
            v.data.to_memory()
                   
        return self

# --- End: class
