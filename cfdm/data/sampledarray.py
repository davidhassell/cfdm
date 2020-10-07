import numpy

from .mixin import ArrayMixin

from . import mixin
from .. import core


class SampledArray(abstract.Array):
    '''TODO

    .. versionadded:: (cfdm) TODO

    '''
    def __init__(self, shape=None, size=None, ndim=None,
                 interpolation=None, tie_points=None,
                 tie_point_indices=None, tie_point_offsets=None):
        '''**Initialization**

    :Parameters:

        shape: `tuple`
            The uncompressed array dimension sizes.

        size: `int`
            Number of elements in the uncompressed array.

        ndim: `int`
            The number of uncompressed array dimensions

        interpolation: `str`

        tie_points: sequence of `TiePoint`
            TODO The count variable required to uncompress the data,
            corresponding to a CF-netCDF count variable.

        tie_point_indices: sequence of `TiePointIndex` or `None`
            TODO The count variable required to uncompress the data,
            corresponding to a CF-netCDF count variable.

        tie_point_offsets: sequence of `TiePointOffset` or `None`, optional
            TODO The count variable required to uncompress the data,
            corresponding to a CF-netCDF count variable.

        coefficients: sequence of `InterpolationCoefficient`, optional

        '''
        super().__init__(compressed_array=compressed_array,
                         shape=shape, size=size, ndim=ndim,
                         count_variable=count_variable,
                         index_variable=index_variable,
                         compression_type='ragged indexed contiguous',
                         compressed_dimension=0)

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
        return self.get_subspace(self._get_component('array'), indices,
                                 copy=True)

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
        return numpy.dtype(float)
#        tie_points = self.tie_points
#        datatype = tie_points[0].dtype
#        
#        for tie_point in tie_points[1:]:
#            datatype = numpy.result_type(datatype, tie_point.dtype)
#
#        return datatype

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
        return self

# --- End: class
