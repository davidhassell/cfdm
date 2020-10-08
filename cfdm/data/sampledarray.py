import numpy

from .mixin import ArrayMixin

from . import mixin
from .. import core


class SampledArray(abstract.Array):
    '''TODO

    .. versionadded:: (cfdm) TODO

    '''
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
            TODO

        interpolation: `str`
            TODO

        tie_points: sequence of `TiePoint`
            TODO

        tie_point_indices: sequence of `TiePointIndex` or `None`
            TODO

        tie_point_offsets: sequence of `TiePointOffset` or `None`, optional
            TODO
        coefficients: sequence of `InterpolationCoefficient`, optional
            TODO

        '''
        super().__init__(shape=shape, size=size, ndim=ndim,
                         sampled_dimensions=sampled_dimensions,
                         interpolation=interpolation,
                         tie_points=tie_points,
                         tie_points_indices=tie_points_indices,
                         tie_points_offsets=tie_points_offsets,
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
        '''Data-type of the data elements.

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

    :Returns:

        `numpy.ndarray`
            The compressed array.

    **Examples:**

    >>> n = a.compressed_array
    >>> import numpy
    >>> isinstance(n, numpy.ndarray)
    True

        '''
        ca = self._get_compressed_Array(None)
        if ca is None:
            raise ValueError("There is no underlying compressed array")

        return ca.array

    # ----------------------------------------------------------------
    # Methods
    # ----------------------------------------------------------------
    def get_sampled_axes(self):
        '''Return axes that are sampled in the underlying array.

    .. versionadded:: (cfdm) TODO

    :Returns:

        `list`
            The sampled axes described by their integer positions.

    **Examples:**

    TODO

        '''
        TODO
        
#        compressed_dimension = self.get_compressed_dimension()
#        compressed_ndim = self._get_compressed_Array().ndim
#
#        return list(range(
#            compressed_dimension,
#            self.ndim - (compressed_ndim - compressed_dimension - 1)
#        ))

    def get_sampled_dimensions(self, *default):
        '''Return the positions of the sampled dimension in the compressed
    array.

    .. versionadded:: (cfdm) 1.7.0

    .. seealso:: `get_compressed_axearray`, `get_compressed_axes`,
                 `get_compressed_type`

    :Parameters:

        default: optional
            Return *default* if the underlying array is not
            compressed.

    :Returns:

        `int`
            The position of the compressed dimension in the compressed
            array. If the underlying is not compressed then *default*
            is returned, if provided.

    **Examples:**

    >>> i = d.get_compressed_dimension()

        '''
        return self._get_component('compressed_dimension', *default)

    # ----------------------------------------------------------------
    # Methods
    # ----------------------------------------------------------------
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
        return 'TODO'

# --- End: class
