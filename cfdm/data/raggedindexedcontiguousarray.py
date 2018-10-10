from builtins import (range, super, zip)

import numpy

from . import abstract


class RaggedIndexedContiguousArray(abstract.CompressedArray):
    '''A container for an indexed contiguous ragged compressed array.

A collection of features, each of which is sequence of (vertical)
profiles, stored using an indexed contiguous ragged array combines all
feature elements along a single dimension (the "sample" dimension)
such that a contiguous ragged array representation is used for each
profile and the indexed ragged array representation to organise the
profiles into timeseries.

The information needed to uncompress the data is stored in a separate
"count" array that gives the size of each profile; and in a separate
"index" array that specifies the feature that each profile belongs to.

    '''
    def __init__(self, compressed_array=None, shape=None, size=None,
                 ndim=None, count_array=None, index_array=None):
        '''**Initialization**

:Parameters:

    compressed_array: subclass of `Array`
        The compressed array.

    shape: `tuple`
        The uncompressed array dimension sizes.

    size: `int`
        Number of elements in the uncompressed array.

    ndim: `int`
        The number of uncompressed array dimensions

    sample_axis: `int`
        The position of the compressed axis in the compressed array.

    count_array: `Data`
        The "count" array required to uncompress the data, identical
        to the data of a CF-netCDF "count" variable.

    index_array: `Data`
        The "index" array required to uncompress the data, identical
        to the data of a CF-netCDF "index" variable.

        '''
        super().__init__(compressed_array=compressed_array,
                         shape=shape, size=size, ndim=ndim,
                         compression_type='ragged indexed contiguous',
                         _count_array=count_array,
                         _index_array=index_array, sample_axis=0)    
    #--- End: def

    def __getitem__(self, indices):
        '''x.__getitem__(indices) <==> x[indices]

Returns an subspace of the uncompressed data an independent numpy
array.

The indices that define the subspace are relative to the uncompressed
data and must be either `Ellipsis` or a sequence that contains an
index for each dimension. In the latter case, each dimension's index
must either be a `slice` object or a sequence of two or more integers.

Indexing is similar to numpy indexing. The only difference to numpy
indexing (given the restrictions on the type of indices allowed) is:

  * When two or more dimension's indices are sequences of integers
    then these indices work independently along each dimension
    (similar to the way vector subscripts work in Fortran).

        '''
        # ------------------------------------------------------------
        # Method: Uncompress the entire array and then subspace it
        # ------------------------------------------------------------
        
        compressed_array = self.compressed_array

        # Initialise the un-sliced uncompressed array
        uarray = numpy.ma.masked_all(self.shape, dtype=self.dtype)

        count_array = self.count_array.get_array()
        index_array = self.index_array.get_array()
        
        # Loop over instances
        for i in range(uarray.shape[0]):
            
            # For all of the profiles in ths instance, find the
            # locations in the count array of the number of elements
            # in the profile
            xprofile_indices = numpy.where(index_array == i)[0]
                
            # Find the number of profiles in this instance
            n_profiles = xprofile_indices.size
            
            # Loop over profiles in this instance
            for j in range(uarray.shape[1]):
                if j >= n_profiles:
                    continue
                
                # Find the location in the count array of the number
                # of elements in this profile
                profile_index = xprofile_indices[j]
                
                if profile_index == 0:
                    start = 0
                else:                    
                    start = int(count_array[:profile_index].sum())
                    
                stop = start + int(count_array[profile_index])
                
                sample_indices = slice(start, stop)
                
                u_indices = (i, #slice(i, i+1),
                             j, #slice(j, j+1), 
                             slice(0, stop-start)) #slice(0, sample_indices.stop - sample_indices.start))
                
                uarray[u_indices] = compressed_array[sample_indices]
            #--- End: for
        #--- End: for

        return self.get_subspace(uarray, indices, copy=True)
    #--- End: def

    @property
    def count_array(self):
        '''
        '''
        return self._count_array

    @property
    def index_array(self):
        '''
        '''
        return self._index_array
#--- End: class