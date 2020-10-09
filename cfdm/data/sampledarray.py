import numpy

from .mixin import ArrayMixin

from . import mixin
from .. import core


class SampledArray(abstract.Array):
    '''TODO

    .. versionadded:: (cfdm) TODO

    '''
    # ----------------------------------------------------------------
    # Define the interpolated array datatype, depending on the
    # interpolation type.
    # ----------------------------------------------------------------
    _interpolated_datatype = {
        'linear': numpy.dtype(float),
        'bilinear': numpy.dtype(float),
    }

    def __init__(self, shape=None, size=None, ndim=None,
                 sampled_dimensions=None, interpolation=None,
                 tie_points=None, tie_point_indices=None,
                 tie_point_offsets=None, coefficients=None):
        '''**Initialization**

    :Parameters:

        shape: `tuple`
            The uncompressed array dimension sizes.

        size: `int`
            Number of elements in the uncompressed array.

        ndim: `int`
            The number of uncompressed array dimensions

        sampled_dimensions: sequence of `int`
            The positions of the sampled dimensions in array.

        interpolation: `str`
            TODO

        tie_points: sequence of `TiePoint`
            TODO

        tie_point_indices: sequence of `TiePointIndex` or `None`
            TODO An elements of `None` indicates that there is a tie
            point for every element of the corresponding target domain
            dimension.

        tie_point_offsets: sequence of `TiePointOffset` or `None`
            TODO An element of `None` is equivalent to an offset of
            zero.

        interpolation_coefficients: sequence of `InterpolationCoefficient`
            TODO. May be an empty sequence.

        '''
        sampled_dimensions = tuple(sorted(sampled_dimensions))

        # Copy variables and ensure that they are stored in tuples
        tie_points = tuple(v.copy() for v in tie_points)
        tie_point_indices = tuple(v.copy() for v in tie_points_indices
                                  if v is not None)
        tie_point_offsets = tuple(v.copy() for v in tie_points_offsets
                                  if v is not None)
        interpolation_coefficients = tuple(
            v.copy() for v in interpolation_coefficients
            if v is not None
        )
        
        super().__init__(shape=shape, size=size, ndim=ndim,
                         sampled_dimensions=sampled_dimensions,
                         interpolation=interpolation,
                         tie_points=tie_points,
                         tie_points_indices=tie_points_indices,
                         tie_points_offsets=tie_points_offsets,
                         interpolation_coefficients=interpolation_coefficients,
                         compression_type='sampled')

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
        
        return numpy.dtype(float)

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
