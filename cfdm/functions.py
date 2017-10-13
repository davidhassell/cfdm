import os
import sys

from collections import Iterable

from itertools   import izip, izip_longest, product
from platform    import system, platform, python_version
from urlparse    import urlparse as urlparse_urlparse
from urlparse    import urljoin  as urlparse_urljoin

import netCDF4
import numpy

from .          import __version__, __file__
from .constants import CONSTANTS


def _numpy_allclose(a, b, rtol=None, atol=None):
    '''

Returns True if two broadcastable arrays have equal values to within
numerical tolerance, False otherwise.

The tolerance values are positive, typically very small numbers. The
relative difference (``rtol * abs(b)``) and the absolute difference
``atol`` are added together to compare against the absolute difference
between ``a`` and ``b``.

:Parameters:

    a, b : array_like
        Input arrays to compare.

    atol : float, optional
        The absolute tolerance for all numerical comparisons, By
        default the value returned by the `ATOL` function is used.

    rtol : float, optional
        The relative tolerance for all numerical comparisons, By
        default the value returned by the `RTOL` function is used.

:Returns:

    out: `bool`
        Returns True if the arrays are equal, otherwise False.

:Examples:

>>> cf._numpy_allclose([1, 2], [1, 2])
True
>>> cf._numpy_allclose(numpy.array([1, 2]), numpy.array([1, 2]))
True
>>> cf._numpy_allclose([1, 2], [1, 2, 3])
False
>>> cf._numpy_allclose([1, 2], [1, 4])
False

>>> a = numpy.ma.array([1])
>>> b = numpy.ma.array([2])
>>> a[0] = numpy.ma.masked
>>> b[0] = numpy.ma.masked
>>> cf._numpy_allclose(a, b)
True

'''

    # THIS IS WHERE SOME NUMPY FUTURE WARNINGS ARE COMING FROM
    
    a_is_masked = numpy.ma.isMA(a)
    b_is_masked = numpy.ma.isMA(b)
#    print a_is_masked   , b_is_masked    
    if not (a_is_masked or b_is_masked):
        try:            
            return numpy.allclose(a, b, rtol=rtol, atol=atol)
        except (IndexError, NotImplementedError, TypeError):
            return numpy_all(a == b)
    else:
        if a_is_masked and b_is_masked:
            if (a.mask != b.mask).any():
                return False
        else:
            return False

        try:
            return numpy.ma.allclose(a, b, rtol=rtol, atol=atol)
        except (IndexError, NotImplementedError, TypeError):
            out = numpy.ma.all(a == b)
            if out is numpy.ma.masked:
                return True
            else:
                return out
#--- End: def

def _numpy_isclose(a, b, rtol=None, atol=None):
    '''Returns a boolean array where two broadcastable arrays are
element-wise equal within a tolerance.

The tolerance values are positive, typically very small numbers. The
relative difference (``rtol * abs(b)``) and the absolute difference
``atol`` are added together to compare against the absolute difference
between ``a`` and ``b``.

:Parameters:

    a, b: array_like
        Input arrays to compare.

    atol: `float`, optional
        The absolute tolerance for all numerical comparisons, By
        default the value returned by the `ATOL` function is used.

    rtol: `float`, optional
        The relative tolerance for all numerical comparisons, By
        default the value returned by the `RTOL` function is used.

:Returns:

    out: `numpy.ndarray`

:Examples:

    '''      
    try:
        return numpy.isclose(a, b, rtol=rtol, atol=atol)
    except (IndexError, NotImplementedError, TypeError):
        return a == b
#--- End: def

def parse_indices(shape, indices):
    '''

:Parameters:

    shape: sequence of `ints`

    indices: `tuple` (not a `list`!)

:Returns:

    out: `list`

:Examples:

'''
    parsed_indices = []
    roll           = {}
    flip           = []
    compressed_indices = []

    if not isinstance(indices, tuple):
        indices = (indices,)

    # Initialize the list of parsed indices as the input indices with any
    # Ellipsis objects expanded
    length = len(indices)
    n = len(shape)
    ndim = n
    for index in indices:
        if index is Ellipsis:
            m = n-length+1
            parsed_indices.extend([slice(None)] * m)
            n -= m            
        else:
            parsed_indices.append(index)
            n -= 1

        length -= 1
    #--- End: for
    len_parsed_indices = len(parsed_indices)

    if ndim and len_parsed_indices > ndim:
        raise IndexError("Invalid indices %s for array with shape %s" %
                         (parsed_indices, shape))

    if len_parsed_indices < ndim:
        parsed_indices.extend([slice(None)]*(ndim-len_parsed_indices))

    if not ndim and parsed_indices:
        raise IndexError("Scalar array can only be indexed with () or Ellipsis")

    for i, (index, size) in enumerate(zip(parsed_indices, shape)):
        is_slice = False
        if isinstance(index, slice):            
            is_slice = True
            start = index.start
            stop  = index.stop
            step  = index.step
            if start is None or stop is None:
                step = 0
            elif step is None:
                step = 1

            if step > 0:
                if 0 < start < size and 0 <= stop <= start:
                    # 6:0:1 => -4:0:1
                    # 6:1:1 => -4:1:1
                    # 6:3:1 => -4:3:1
                    # 6:6:1 => -4:6:1
                    start = size-start
                elif -size <= start < 0 and -size <= stop <= start:
                    # -4:-10:1  => -4:1:1
                    # -4:-9:1   => -4:1:1
                    # -4:-7:1   => -4:3:1
                    # -4:-4:1   => -4:6:1 
                    # -10:-10:1 => -10:0:1
                    stop += size
            elif step < 0:
                if -size <= start < 0 and start <= stop < 0:
                    # -4:-1:-1   => 6:-1:-1
                    # -4:-2:-1   => 6:-2:-1
                    # -4:-4:-1   => 6:-4:-1
                    # -10:-2:-1  => 0:-2:-1
                    # -10:-10:-1 => 0:-10:-1
                    start += size
                elif 0 <= start < size and start < stop < size:
                    # 0:6:-1 => 0:-4:-1
                    # 3:6:-1 => 3:-4:-1
                    # 3:9:-1 => 3:-1:-1
                    stop -= size
            #--- End: if            
                        
            if step > 0 and -size <= start < 0 and 0 <= stop <= size+start:
                index = slice(start, stop, step)

            elif step < 0 and 0 <= start < size and start-size <= stop < 0:
                # [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                # 0:-4:-1  => [0, 9, 8, 7]
                # 6:-1:-1  => [6, 5, 4, 3, 2, 1, 0]
                # 6:-2:-1  => [6, 5, 4, 3, 2, 1, 0, 9]
                # 6:-4:-1  => [6, 5, 4, 3, 2, 1, 0, 9, 8, 7]
                # 0:-2:-1  => [0, 9]
                # 0:-10:-1 => [0, 9, 8, 7, 6, 5, 4, 3, 2, 1]
                index = slice(start, stop, step)

            else:
                start, stop, step = index.indices(size)
                if (start == stop or
                    (start < stop and step < 0) or
                    (start > stop and step > 0)):
                    raise IndexError(
"Invalid indices {} for array with shape {}".format(parsed_indices, shape))
                if step < 0 and stop < 0:
                    stop = None
                index = slice(start, stop, step)
         
        elif isinstance(index, (int, long)):
            if index < 0: 
                index += size

            index = slice(index, index+1, 1)
            is_slice = True
        else:
            convert2positve = True
            if getattr(getattr(index, 'dtype', None), 'kind', None) == 'b':
                # Convert booleans to non-negative integers. We're
                # assuming that anything with a dtype attribute also
                # has a size attribute.
                if index.size != size:
                    raise IndexError(
"Invalid indices {} for array with shape {}".format(parsed_indices, shape))
                
                index = numpy.where(index)[0]
                convert2positve = False
            #--- End: if

            if not numpy.ndim(index):
                if index < 0:
                    index += size

                index = slice(index, index+1, 1)
                is_slice = True
            else:
                len_index = len(index)
                if len_index == 1:                
                    index = index[0]
                    if index < 0:
                        index += size
                    
                    index = slice(index, index+1, 1)
                    is_slice = True
                elif len_index:
                    if convert2positve:
                        # Convert to non-negative integer numpy array
                        index = numpy.array(index)
                        index = numpy.where(index < 0, index+size, index)
    
                    steps = index[1:] - index[:-1]
                    step = steps[0]
                    if step and not (steps - step).any():
                        # Replace the numpy array index with a slice
                        if step > 0:
                            start, stop = index[0], index[-1]+1
                        elif step < 0:
                            start, stop = index[0], index[-1]-1
                            
                        if stop < 0:
                            stop = None
                                
                        index = slice(start, stop, step)
                        is_slice = True
                    else:
#                        if ((step > 0 and (steps <= 0).any()) or
#                            (step < 0 and (steps >= 0).any()) or
#                            not step):
#                           raise ValueError("Bad index (not strictly monotonic): %s" % index)
                        
                        if not (steps > 0).all():
                            raise ValueError(
"Bad index (not strictly monotonically increasing sequence): {}".format(index))
                            
                else:
                    raise IndexError(
                        "Invalid indices {} for array with shape {}".format(
                            parsed_indices, shape))                
            #--- End: if
        #--- End: if
        
        parsed_indices[i] = index    
    #--- End: for

    return parsed_indices
#--- End: def

def get_subspace(array, indices):
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

        indices = indices[:]
        for axis in gg:
            array = take(array, indices[axis], axis=axis)
            indices[axis] = slice(None)
        #--- End: for

        if len_gg < len(indices):
            array = array[tuple(indices)]

        return array
    #--- End: if

#--- End: def

def set_subspace(array, indices, value):
    '''
'''
    gg = [i for i, x in enumerate(indices) if not isinstance(x, slice)]

    if len(gg) < 2: 
        # ------------------------------------------------------------
        # At most one axis has a list-of-integers index so we can do a
        # normal numpy assignment
        # ------------------------------------------------------------
        array[tuple(indices)] = value
    else:
        # ------------------------------------------------------------
        # At least two axes have list-of-integers indices so we can't
        # do a normal numpy assignment
        # ------------------------------------------------------------
        indices1 = indices[:]
        for i, x in enumerate(indices):
            if i in gg:
                y = []
                args = [iter(x)] * 2
                for start, stop in izip_longest(*args):
                    if not stop:
                        y.append(slice(start, start+1))
                    else:
                        step = stop - start
                        stop += 1
                        y.append(slice(start, stop, step))
                #--- End: for
                indices1[i] = y
            else:
                indices1[i] = (x,)
        #--- End: for

#        if not numpy.ndim(value) :
        if numpy.size(value) == 1:
            for i in product(*indices1):
                array[i] = value
                
        else:
            indices2 = []
            ndim_difference = array.ndim - numpy.ndim(value)
            for i, n in enumerate(numpy.shape(value)):
                if n == 1:
                    indices2.append((slice(None),))
                elif i + ndim_difference in gg:
                    y = []
                    start = 0
                    while start < n:
                        stop = start + 2
                        y.append(slice(start, stop))
                        start = stop
                    #--- End: while
                    indices2.append(y)
                else:
                    indices2.append((slice(None),))
            #--- End: for

#            print  'C',indices1
#            print  'D', indices2
#            print 'E', value
            for i, j in izip(product(*indices1), product(*indices2)):
#                print i, j
                array[i] = value[j]
#--- End: def

def ATOL(*arg):
    '''The absolute difference when testing for numerically tolerant
equality.

Two numbers ``x`` and ``y`` are considered equal if ``abs(x-y) <= atol
+ rtol*abs(y)``, where the atol and rtol are set by the `ATOL` and
`RTOL functions respectively. ``atol`` is the absolute difference and
``rtol*abs(y)`` is the relative difference.

.. versionadded:: 1.6

.. seealso:: `RTOL`

:Examples 1:

>>> atol = ATOL()

:Parameters:

    arg: `float`, optional
        The new value of absolute tolerance. The default is to not
        change the current value.

:Returns:

    out: `float`
        The value prior to the change, or the current value if no
        new value was specified.

:Examples 2:

>>> cf.ATOL()
1e-08
>>> old = cf.ATOL(1e-10)
>>> cf.ATOL(old)
1e-10
>>> cf.ATOL()
1e-08

    '''
    old = CONSTANTS['ATOL']
    if arg:
        CONSTANTS['ATOL'] = arg[0]

    return old
#--- End: def

def RTOL(*arg):    
    '''The factor used to calculate the relative difference when testing
for numerically tolerant equality.

Two numbers ``x`` and ``y`` are considered equal if ``abs(x-y) <= atol
+ rtol*abs(y)``, where the atol and rtol are set by the `ATOL` and
`RTOL functions respectively. ``atol`` is the absolute difference and
``rtol*abs(y)`` is the relative difference.

.. versionadded:: 1.6

.. seealso:: `ATOL`

:Examples 1:

>>> rtol = RTOL()

:Parameters:

    arg: `float`, optional
        The new value of relative tolerance. The default is to not
        change the current value.

:Returns:

    out: `float`
        The value prior to the change, or the current value if no
        new value was specified.

:Examples 2:

>>> cf.RTOL()
1.0000000000000001e-05
>>> old = cf.RTOL(1e-10)
>>> cf.RTOL(old)
1e-10
>>> cf.RTOL()
1.0000000000000001e-05

    '''
    old = CONSTANTS['RTOL']
    if arg:
        CONSTANTS['RTOL'] = arg[0]

    return old
#--- End: def

def equals(x, y, rtol=None, atol=None, ignore_data_type=False,
           ignore_fill_value=False, traceback=False):
    '''True if and only if two objects are logically equal.

If the first argument, *x*, has an :meth:`equals` method then it is
used, and in this case ``equals(x, y)`` is equivalent to
``x.equals(y)``. Else if the second argument, *y*, has an
:meth:`equals` method then it is used, and in this case ``equals(x,
y)`` is equivalent to ``y.equals(x)``.

:Parameters:

    x, y :
        The objects to compare for equality.

    atol : float, optional
        The absolute tolerance for all numerical comparisons, By
        default the value returned by the `ATOL` function is used.

    rtol : float, optional
        The relative tolerance for all numerical comparisons, By
        default the value returned by the `RTOL` function is used.

    ignore_fill_value : bool, optional
        If True then `Data` arrays with different fill values are
        considered equal. By default they are considered unequal.

    traceback : bool, optional
        If True then print a traceback highlighting where the two
        objects differ.

:Returns:

    out: `bool`
        Whether or not the two objects are equal.

:Examples:

>>> f
<CF Field: rainfall_rate(latitude(10), longitude(20)) kg m2 s-1>
>>> cfdm.equals(f, f)
True

>>> cfdm.equals(1.0, 1.0)
True
>>> cfdm.equals(1.0, 33)
False

>>> cfdm.equals('a', 'a')
True
>>> cfdm.equals('a', 'b')
False

>>> type(x), x.dtype
(<type 'numpy.ndarray'>, dtype('int64'))
>>> y = x.copy()
>>> cfdm.equals(x, y)
True
>>> cfdm.equals(x, x+1)
False

>>> class A(object):
...     pass
...
>>> a = A()
>>> b = A()
>>> cfdm.equals(a, a)
True
>>> cfdm.equals(a, b)
False

    '''
    eq = getattr(x, 'equals', None)
    if callable(eq):
        # x has a callable equals method
        return eq(y, rtol=rtol, atol=atol,
                  ignore_data_type=ignore_data_type,
                  ignore_fill_value=ignore_fill_value,
                  traceback=traceback)

    eq = getattr(y, 'equals', None)
    if callable(eq):
        # y has a callable equals method
        return eq(x, rtol=rtol, atol=atol,
                  ignore_data_type=ignore_data_type,
                  ignore_fill_value=ignore_fill_value,
                  traceback=traceback)

    if isinstance(x, numpy.ndarray):
        if isinstance(y, numpy.ndarray):
            if x.shape != y.shape:
                return False

            if rtol is None:
                rtol = RTOL()
            if atol is None:
                atol = ATOL()
                    
            return _numpy_allclose(x, y, rtol=rtol, atol=atol)
        else:
            return False
    elif isinstance(y, numpy.ndarray):
        return False

    else:
        return x == y
#--- End: def

def flat(x):
    '''Return an iterator over an arbitrarily nested sequence.

:Parameters:

    x : scalar or arbitrarily nested sequence
        The arbitrarily nested sequence to be flattened. Note that a
        If *x* is a string or a scalar then this is equivalent to
        passing a single element sequence containing *x*.

:Returns:

    out: generator
        An iterator over flattened sequence.

:Examples:

>>> print cfdm.flat([1, [2, [3, 4]]])
<generator object flat at 0x3649cd0>

>>> print list(cfdm.flat([1, (2, [3, 4])]))
[1, 2, 3, 4]

>>> import numpy
>>> print list(cfdm.flat((1, [2, numpy.array([[3, 4], [5, 6]])]))
[1, 2, 3, 4, 5, 6]

>>> for a in cfdm.flat([1, [2, [3, 4]]]):
...     print a,
1 2 3 4

>>> for a in cfdm.flat(['a', ['bc', ['def', 'ghij']]]):
...     print a, ' ',
a bc def ghij

>>> for a in cfdm.flat(2004):
...     print a
2004

>>> for a in cfdm.flat('abcdefghij'):
...     print a
abcdefghij

>>> f
<CF Field: eastward_wind(air_pressure(5), latitude(110), longitude(106)) m s-1>
>>> for a in cfdm.flat(f):
...     print repr(a)
<CF Field: eastward_wind(air_pressure(5), latitude(110), longitude(106)) m s-1>

>>> for a in cfdm.flat([f, [f, [f, f]]]):
...     print repr(a)
<CF Field: eastward_wind(air_pressure(5), latitude(110), longitude(106)) m s-1>
<CF Field: eastward_wind(air_pressure(5), latitude(110), longitude(106)) m s-1>
<CF Field: eastward_wind(air_pressure(5), latitude(110), longitude(106)) m s-1>
<CF Field: eastward_wind(air_pressure(5), latitude(110), longitude(106)) m s-1>

>>> fl = cfdm.FieldList(cfdm.flat([f, [f, [f, f]]])
>>> fl
[<CF Field: eastward_wind(air_pressure(5), latitude(110), longitude(106)) m s-1>,
 <CF Field: eastward_wind(air_pressure(5), latitude(110), longitude(106)) m s-1>,
 <CF Field: eastward_wind(air_pressure(5), latitude(110), longitude(106)) m s-1>,
 <CF Field: eastward_wind(air_pressure(5), latitude(110), longitude(106)) m s-1>]

    '''
    if not isinstance(x, Iterable) or isinstance(x, basestring):
        x = (x,)

    for a in x:
        if not isinstance(a, basestring) and isinstance(a, Iterable):
            for sub in flat(a):
                yield sub
        else:
            yield a
#--- End: def

def abspath(filename):
    '''

Return a normalized absolute version of a file name.

If a string containing URL is provided then it is returned unchanged.

.. seealso:: `dirname`, `pathjoin`, `relpath`

:Parameters:

    filename: `str`
        The name of the file.

:Returns:

    out: `str`
        The normalized absolutized version of *filename*.
 
:Examples:

>>> import os
>>> os.getcwd()
'/data/archive'
>>> cfdm.abspath('file.nc')
'/data/archive/file.nc'
>>> cfdm.abspath('..//archive///file.nc')
'/data/archive/file.nc'
>>> cfdm.abspath('http://data/archive/file.nc')
'http://data/archive/file.nc'

'''
    u = urlparse_urlparse(filename)
    if u.scheme != '':
        return filename

    return os.path.abspath(filename)
#--- End: def

def relpath(filename, start=None):
    '''

Return a relative filepath to a file.

The filepath is relative either from the current directory or from an
optional start point.

If a string containing URL is provided then it is returned unchanged.

.. seealso:: `cf.abspath`, `cf.dirname`, `cf.pathjoin`

:Parameters:

    filename: `str`
        The name of the file.

    start: `str`, optional
        The start point for the relative path. By default the current
        directoty is used.

:Returns:

    out: `str`
        The relative path.

:Examples:

>>> cf.relpath('/data/archive/file.nc')
'../file.nc'
>>> cf.relpath('/data/archive///file.nc', start='/data')
'archive/file.nc'
>>> cf.relpath('http://data/archive/file.nc')
'http://data/archive/file.nc'

'''
    u = urlparse_urlparse(filename)
    if u.scheme != '':
        return filename

    if start is not None:
        return os.path.relpath(filename, start)

    return os.path.relpath(filename)
#--- End: def

def dirname(filename):
    '''

Return the directory name of a file.

If a string containing URL is provided then everything up to, but not
including, the last slash (/) is returned.

.. seealso:: `cf.abspath`, `cf.pathjoin`, `cf.relpath`

:Parameters:

    filename: `str`
        The name of the file.

:Returns:

    out: `str`
        The directory name.

:Examples:

>>> cf.dirname('/data/archive/file.nc')
'/data/archive'
>>> cf.dirname('..//file.nc')
'..'
>>> cf.dirname('http://data/archive/file.nc')
'http://data/archive'

'''
    u = urlparse_urlparse(filename)
    if u.scheme != '':
        return filename.rpartition('/')[0]

    return os.path.dirname(filename)
#--- End: def

def pathjoin(path1, path2):
    '''

Join two file path components intelligently.

If either of the paths is a URL then a URL will be returned

.. seealso:: `cf.abspath`, `cf.dirname`, `cf.relpath`

:Parameters:

    path1: `str`
        The first component of the path.

    path2: `str`
        The second component of the path.

:Returns:

    out: `str`
        The joined paths.

:Examples:

>>> cf.pathjoin('/data/archive', '../archive/file.nc')
'/data/archive/../archive/file.nc'
>>> cf.pathjoin('/data/archive', '../archive/file.nc')
'/data/archive/../archive/file.nc'
>>> cf.abspath(cf.pathjoin('/data/', 'archive/')
'/data/archive'
>>> cf.pathjoin('http://data', 'archive/file.nc')
'http://data/archive/file.nc'

'''
    u = urlparse_urlparse(path1)
    if u.scheme != '':
        return urlparse_urljoin(path1, path2)

    return os.path.join(path1, path2)
#--- End: def

def allclose(x, y, rtol=None, atol=None):
    '''

Returns True if two broadcastable arrays have equal values to within
numerical tolerance, False otherwise.

The tolerance values are positive, typically very small numbers. The
relative difference (``rtol * abs(b)``) and the absolute difference
``atol`` are added together to compare against the absolute difference
between ``a`` and ``b``.

:Parameters:

    x, y: array_like
        Input arrays to compare.

    atol: `float`, optional
        The absolute tolerance for all numerical comparisons, By
        default the value returned by the `ATOL` function is used.

    rtol: `float`, optional
        The relative tolerance for all numerical comparisons, By
        default the value returned by the `RTOL` function is used.

:Returns:

    out: `bool`
        Returns True if the arrays are equal, otherwise False.

:Examples:

'''    
    if rtol is None:
        rtol = RTOL()
    if atol is None:
        atol = ATOL()

    allclose = getattr(x, 'allclose', None)
    if callable(allclose):
        # x has a callable allclose method
       return allclose(y, rtol=rtol, atol=atol)

    allclose = getattr(y, 'allclose', None)
    if callable(allclose):
        # y has a callable allclose method
       return allclose(x, rtol=rtol, atol=atol)

    # x nor y has a callable allclose method
    return _numpy_allclose(x, y, rtol=rtol, atol=atol)
#--- End: def

def environment(display=True):
    '''Return the names and versions of cf-python dependencies.

:Parameters:

    display: `bool`, optional
        If False then return the description of the environment as a
        string. By default the description is printed.

:Returns:

    out: `None` or `str`
        If *display* is True then the description of the environment
        is printed and `None` is returned. Otherwise the description
        is returned as a string.

:Examples:

>>> cfdm.environment()
Platform: Linux-4.4.0-53-generic-x86_64-with-debian-stretch-sid
HDF5 library: 1.8.17
netcdf library: 4.4.1
python: 2.7.13 /home/space/anaconda2/bin/python
netCDF4: 1.2.4 /home/space/anaconda2/lib/python2.7/site-packages/netCDF4/__init__.pyc
numpy: 1.11.3 /home/space/anaconda2/lib/python2.7/site-packages/numpy/__init__.pyc
cfdm: 1.6 /home/space/anaconda2/lib/python2.7/site-packages/cfdm/__init__.pyc

    '''
    out = []
    out.append('Platform: ' + str(platform()))

    out.append('HDF5 library: ' + str(netCDF4. __hdf5libversion__))
    out.append('netcdf library: ' + str(netCDF4.__netcdf4libversion__))

    out.append('python: ' + str(python_version() + ' ' + str(sys.executable)))
    out.append('netCDF4: ' + str(netCDF4.__version__) + ' ' + str(os.path.abspath(netCDF4.__file__)))
    out.append('numpy: ' + str(numpy.__version__) + ' ' + str(os.path.abspath(numpy.__file__)))

    out.append('cfdm: ' + str(__version__) + ' ' + str(os.path.abspath(__file__)))
    
    out = '\n'.join(out)

    if display:
        print out
    else:
        return out
#--- End: def
