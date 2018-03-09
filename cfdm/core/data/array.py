import abc

import operator
import sys

import numpy


# ====================================================================
#
# FileArray object
# 
# ====================================================================

class Array(object):
    '''An array.
    
    '''
    __metaclass__ = abc.ABCMeta
           
    def __init__(self, **kwargs):
        '''
        
**Initialization**

:Parameters:

    file: `str`
        The netCDF file name in normalized, absolute form.

    dtype: `numpy.dtype`
        The numpy data type of the data array.

    ndim: `int`
        Number of dimensions in the data array.

    shape: `tuple`
        The data array's dimension sizes.

    size: `int`
        Number of elements in the data array.

'''
        self.__dict__ = kwargs
    #--- End: def
               
    def __deepcopy__(self, memo):
        '''

Used if copy.deepcopy is called on the variable.

'''
        return self.copy()
    #--- End: def

    @abc.abstractmethod
    def __getitem__(self, indices):
        '''x.__getitem__(indices) <==> x[indices]

Returns a numpy array.
        '''
        pass
    #--- End: def

    def __repr__(self):
        '''

x.__repr__() <==> repr(x)

'''      
        return "<{0}: {1}>".format(self.__class__.__name__, str(self))
    #--- End: def
        
    def __str__(self):
        '''

x.__str__() <==> str(x)

'''
        return "shape={0}, dtype={1}".format(self.shape, self.dtype)
    #--- End: def

    @abc.abstractproperty
    def ndim(self):
        pass

    @abc.abstractproperty
    def shape(self):
        pass

    @abc.abstractproperty
    def size(self):
        pass

    @abc.abstractproperty
    def dtype(self):
        pass
    
    @abc.abstractproperty
    def isunique(self):
        pass
    
    @abc.abstractmethod
    def close(self):
        '''
        '''
        pass
    #--- End: def

    def copy(self):
        '''Return a deep copy.

``f.copy() is equivalent to ``copy.deepcopy(f)``.

:Returns:

    out :
        A deep copy.

:Examples:

>>> g = f.copy()

        '''
        C = self.__class__
        new = C.__new__(C)
        new.__dict__ = self.__dict__.copy()
        return new
    #--- End: def

    @classmethod
    def get_subspace(cls, array, indices):
        '''
    
:Parameters:

    array : numpy array

    indices : list

Subset the input numpy array with the given indices. Indexing is similar to
that of a numpy array. The differences to numpy array indexing are:

1. An integer index i takes the i-th element but does not reduce the rank of
   the output array by one.

2. When more than one dimension's slice is a 1-d boolean array or 1-d sequence
   of integers then these indices work independently along each dimension
   (similar to the way vector subscripts work in Fortran).

indices must contain an index for each dimension of the input array.
    '''
        if indices is Ellipsis:
            return array
        
        gg = [i for i, x in enumerate(indices) if not isinstance(x, slice)]
        len_gg = len(gg)
    
        if len_gg < 2:
            # ------------------------------------------------------------
            # At most one axis has a list-of-integers index so we can do a
            # normal numpy subspace
            # ------------------------------------------------------------
            return array[tuple(indices)]
    
        else:
            # ------------------------------------------------------------
            # At least two axes have list-of-integers indices so we can't
            # do a normal numpy subspace
            # ------------------------------------------------------------
            if numpy.ma.isMA(array):
                take = numpy.ma.take
            else:
                take = numpy.take
    
            indices = list(indices)
            for axis in gg:
                array = take(array, indices[axis], axis=axis)
                indices[axis] = slice(None)
            #--- End: for
    
            if len_gg < len(indices):
                array = array[tuple(indices)]
    
            return array
        #--- End: if
    #--- End: def

    @abc.abstractmethod
    def open(self):
        '''
        '''
        pass
    #---End: def
    
#--- End: class

class GatheredArray(Array):
    '''
    '''        
    def __getitem__(self, indices):
        '''
        '''

        array = numpy.ma.masked_all(self.shape, dtype=dtype)
        
        compressed_axes = range(self.sample_axis, array.ndim - (self.gathered_array.ndim - self.sample_axis - 1))
        
        zzz = [reduce(operator.mul, [array.shape[i] for i in compressed_axes[i:]], 1)
               for i in range(1, len(compressed_axes))]
        
        xxx = [[0] * self.indices.size for i in compressed_axes]


        for n, b in enumerate(self.indices.varray):
            if not zzz or b < zzz[-1]:
                xxx[-1][n] = b
                continue
            
            for i, z in enumerate(zzz):
                if b >= z:
                    (a, b) = divmod(b, z)
                    xxx[i][n] = a
                    xxx[-1][n] = b
        #--- End: for

        uncompressed_indices = [slice(None)] * array.ndim        
        for i, x in enumerate(xxx):
            uncompressed_indices[sample_axis+i] = x

        array[tuple(uncompressed_indices)] = self.gathered_array[...]

        if indices is Ellpisis:
            return array

#        indices = self.parse_indices(indices)
        array = self.get_subspace(array, indices)
        
        return array