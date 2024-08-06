import itertools
import logging

import dask.array as da
import netCDF4
import numpy as np
from dask import compute
from dask.array import Array
from scipy.sparse import issparse

from .. import core
from ..constants import masked as cfdm_masked
from ..decorators import (
    _inplace_enabled,
    _inplace_enabled_define_and_cleanup,
    _manage_log_level_via_verbosity,
)
from ..functions import      atol as _atol
from ..functions import      rtol as _rtol
from ..mixin.container import Container
from ..mixin.files import Files
from ..mixin.netcdf import NetCDFHDF5
from .creation import to_dask
from . import NumpyArray, SparseArray, abstract
from .utils import convert_to_datetime

logger = logging.getLogger(__name__)


_DEFAULT_CHUNKS = "auto"
_DEFAULT_HARDMASK = True

# Contstants used to specify which `Data` components should be cleared
# when a new dask array is set. See `Data._clear_after_dask_update`
# for details.
_NONE = 0  # =  0b0000
_ARRAY = 1  # = 0b0001
_CACHE = 2  # = 0b0010
_CFA = 4  # =   0b0100
_ALL = 15  # =  0b1111


class Data(Container, NetCDFHDF5, Files, core.Data):
    """An orthogonal multidimensional array with masking and units.

    .. versionadded:: (cfdm) 1.7.0

    """

    def __init__(
        self,
        array=None,
        units=None,
        calendar=None,
        fill_value=None,
        hardmask=_DEFAULT_HARDMASK,
        chunks=_DEFAULT_CHUNKS,
        dt=False,
        source=None,
        copy=True,
        dtype=None,
        mask=None,
        mask_value=None,
        to_memory=False,
        init_options=None,
        _use_array=True,
        **kwargs,
    ):
        """**Initialisation**

        :Parameters:

            array: data_like, optional
                The array of values.

                {{data_like}}

                *Parameter example:*
                  ``array=[34.6]``

                *Parameter example:*
                  ``array=[[1, 2], [3, 4]]``

                *Parameter example:*
                  ``array=np.ma.arange(10).reshape(2, 1, 5)``

            units: `str`, optional
                The physical units of the data.

                The units may also be set after initialisation with the
                `set_units` method.

                *Parameter example:*
                  ``units='km hr-1'``

                *Parameter example:*
                  ``units='days since 2018-12-01'``

            calendar: `str`, optional
                The calendar for reference time units.

                The calendar may also be set after initialisation with the
                `set_calendar` method.

                *Parameter example:*
                  ``calendar='360_day'``

            fill_value: optional
                The fill value of the data. By default, or if set to
                `None`, the `numpy` fill value appropriate to the
                array's data type will be used (see
                `numpy.ma.default_fill_value`).

                The fill value may also be set after initialisation with
                the `set_fill_value` method.

                *Parameter example:*
                  ``fill_value=-999.``

            dtype: data-type, optional
                The desired data-type for the data. By default the
                data-type will be inferred form the *array* parameter.

                The data-type may also be set after initialisation
                with the `dtype` attribute.

                *Parameter example:*
                    ``dtype=float``

                *Parameter example:*
                    ``dtype='float32'``

                *Parameter example:*
                    ``dtype=np.dtype('i2')``

            mask: data_like, optional
                Apply this mask to the data given by the *array*
                parameter. By default, or if *mask* is `None`, no mask
                is applied. May be any data_like object that
                broadcasts to *array*. Masking will be carried out
                where mask elements evaluate to `True`.

                {{data_like}}

                This mask will applied in addition to any mask already
                defined by the *array* parameter.

            mask_value: scalar array_like
                Mask *array* where it is equal to *mask_value*, using
                numerically tolerant floating point equality.

                .. versionadded:: (cfdm) 1.11.0.0

            {{init source: optional}}

            {{init copy: `bool`, optional}}

            kwargs: ignored
                Not used. Present to facilitate subclassing.

        """
        self._initialise_netcdf(source)
        self._initialise_original_filenames(source)

        if source is None and isinstance(array, self.__class__):
            source = array

        if init_options is None:
            init_options = {}

        if source is not None:
            try:
                array = source._get_Array(None)
            except AttributeError:
                array = None

            super().__init__(
                source=source, _use_array=_use_array and array is not None
            )
            if _use_array:
                # REVIEW: getitem: `__init__`: set 'asanyarray'
                try:
                    array = source.to_dask_array(asanyarray=False)
                except (AttributeError, TypeError):
                    try:
                        array = source.to_dask_array()
                    except (AttributeError, TypeError):
                        pass
                    else:
                        self._set_dask(array, copy=copy, clear=_NONE)
                else:
                    self._set_dask(
                        array, copy=copy, clear=_NONE, asanyarray=None
                    )
            else:
                self._del_dask(None, clear=_NONE)

            self.hardmask = getattr(source, "hardmask", _DEFAULT_HARDMASK)

            return

        super().__init__(
            array=array,
            fill_value=fill_value,
            _use_array=False,
        )

        # Set the units
        units = Units(units, calendar=calendar)
        self._Units = units

        # Set the mask hardness
        self.hardmask = hardmask

        if array is None:
            # No data has been set
            return

        sparse_array = issparse(array)



        








        





        
        if not _use_array:
            return

        # Still here? Then create a dask array and store it.


        # Find out if the input data is compressed by convention
        try:
            compressed = array.get_compression_type()
        except AttributeError:
            compressed = ""

        if compressed and init_options.get("from_array"):
            raise ValueError(
                "Can't define 'from_array' initialisation options "
                "for compressed input arrays"
            )

        # Bring the compressed data into memory without
        # decompressing it
        if to_memory:
            try:
                array = array.to_memory()
            except AttributeError:
                pass

        if self._is_abstract_Array_subclass(array):
            # Save the input array in case it's useful later. For
            # compressed input arrays this will contain extra
            # information, such as a count or index variable.
            self._set_Array(array)

        # Cast the input data as a dask array
        kwargs = init_options.get("from_array", {})
        if "chunks" in kwargs:
            raise TypeError(
                "Can't define 'chunks' in the 'from_array' initialisation "
                "options. Use the 'chunks' parameter instead."
            )


        
        is_dask = is_dask_collection(array)
        

        

        # Set whether or not to call `np.asanyarray` on chunks to
        # convert them to numpy arrays.
        if is_dask:
            # We don't know what's in the dask array, so we should
            # assume that it might need converting to a numpy array.
            self._set_component("__asanyarray__", True)
        else:
            # Use the array's __asanyarray__ value, if it has one.
            self._set_component("__asanyarray__", bool(
                getattr(array, "__asanyarray__", False)
            ))

        dx = to_dask(array, chunks, **kwargs)

        # Find out if we have an array of date-time objects
        if units.isreftime:
            dt = True

        first_value = None
        if not dt and dx.dtype.kind == "O":
            kwargs = init_options.get("first_non_missing_value", {})
            first_value = first_non_missing_value(dx, **kwargs)

            if first_value is not None:
                dt = hasattr(first_value, "timetuple")

        # Convert string or object date-times to floating point
        # reference times
        if dt and dx.dtype.kind in "USO":
            dx, units = convert_to_reftime(dx, units, first_value)
            # Reset the units
            self._Units = units


        # Store the dask array
        self._set_dask(dx, clear=_NONE, asanyarray=None)

        # Override the data type
        if dtype is not None:
            self.dtype = dtype

        # Apply a mask
        if mask is not None:
            if sparse_array:
                raise ValueError("Can't mask sparse array")

            self.where(mask, cf_masked, inplace=True)

        # Apply masked values
        if mask_value is not None:
            if sparse_array:
                raise ValueError("Can't mask sparse array")

            self.masked_values(mask_value, inplace=True)

    def __array__(self, *dtype):
        """The numpy array interface.

        .. versionadded:: (cfdm) 1.7.0

        :Parameters:

            dtype: optional
                Typecode or data-type to which the array is cast.

        :Returns:

            `numpy.ndarray`
                An independent numpy array of the data.

        **Examples**


        >>> d = {{package}}.{{class}}([1, 2, 3])
        >>> a = np.array(d)
        >>> print(type(a))
        <class 'numpy.ndarray'>
        >>> a[0] = -99
        >>> d
        <{{repr}}{{class}}(3): [1, 2, 3]>
        >>> b = np.array(d, float)
        >>> print(b)
        [1. 2. 3.]

        """
        array = self.array
        if not dtype:
            return array
        else:
            return array.astype(dtype[0], copy=False)

    def __bool__(self):
        """Truth value testing and the built-in operation `bool`

        x.__bool__() <==> bool(x)

        **Performance**

        `__bool__` causes all delayed operations to be computed.

        .. versionadded:: (cfdm) NEXTVERSION

        **Examples**

        >>> bool({{package}}.{{class}}(1.5))
        True
        >>> bool({{package}}.{{class}}([[False]]))
        False

        """
        size = self.size
        if size != 1:
            raise ValueError(
                f"The truth value of a {self.__class__.__name__} with {size} "
                "elements is ambiguous. Use d.any() or d.all()"
            )

        return bool(self.to_dask_array())

    def __len__(self):
        """The built-in function `len`

        x.__len__() <==> len(x)

        .. versionadded:: (cfdm) 1.10.0.0

        **Performance**

        If the shape of the data is unknown then it is calculated
        immediately by executing all delayed operations.

        **Examples**

        >>> len({{package}}.{{class}}([1, 2, 3]))
        3
        >>> len({{package}}.{{class}}([[1, 2, 3]]))
        1
        >>> len({{package}}.{{class}}([[1, 2, 3], [4, 5, 6]]))
        2
        >>> len({{package}}.{{class}}(1))
        Traceback (most recent call last):
            ...
        TypeError: len() of unsized object

        """
        dx = self.to_dask_array()
        if math.isnan(dx.size):
            logger.debug("Computing data len: Performance may be degraded")
            dx.compute_chunk_sizes()

        return len(dx)

    def __repr__(self):
        """Called by the `repr` built-in function.

        x.__repr__() <==> repr(x)

        """
        try:
            shape = self.shape
        except AttributeError:
            shape = ""
        else:
            shape = str(shape)
            shape = shape.replace(",)", ")")

        return f"<{self.__class__.__name__}{shape}: {self}>"

    def __format__(self, format_spec):
        """Interpret format specifiers for size 1 arrays.

        **Examples**

        >>> d = {{package}}.{{class}}(9, 'metres')
        >>> f"{d}"
        '9 metres'
        >>> f"{d!s}"
        '9 metres'
        >>> f"{d!r}"
        '<{{repr}}{{class}}(): 9 metres>'
        >>> f"{d:.3f}"
        '9.000'

        >>> d = {{package}}.{{class}}([[9]], 'metres')
        >>> f"{d}"
        '[[9]] metres'
        >>> f"{d!s}"
        '[[9]] metres'
        >>> f"{d!r}"
        '<{{repr}}{{class}}(1, 1): [[9]] metres>'
        >>> f"{d:.3f}"
        '9.000'

        >>> d = {{package}}.{{class}}([9, 10], 'metres')
        >>> f"{d}"
        >>> '[9, 10] metres'
        >>> f"{d!s}"
        >>> '[9, 10] metres'
        >>> f"{d!r}"
        '<{{repr}}{{class}}(2): [9, 10] metres>'
        >>> f"{d:.3f}"
        Traceback (most recent call last):
            ...
        ValueError: Can't format Data array of size 2 with format code .3f

        """
        if not format_spec:
            return super().__format__("")

        n = self.size
        if n == 1:
            return "{x:{f}}".format(x=self.first_element(), f=format_spec)

        raise ValueError(
            f"Can't format Data array of size {n} with "
            f"format code {format_spec}"
        )

    def __getitem__(self, indices):
        """Return a subspace of the data defined by indices.

        d.__getitem__(indices) <==> d[indices]

        Indexing follows rules that are very similar to the numpy indexing
        rules, the only differences being:

        * An integer index i takes the i-th element but does not reduce
          the rank by one.

        * When two or more dimensions' indices are sequences of integers
          then these indices work independently along each dimension
          (similar to the way vector subscripts work in Fortran). This is
          the same behaviour as indexing on a Variable object of the
          netCDF4 package.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `__setitem__`, `_parse_indices`

        :Returns:

            `{{class}}`
                The subspace of the data.

        **Examples**


        >>> d = {{package}}.{{class}}(np.arange(100, 190).reshape(1, 10, 9))
        >>> d.shape
        (1, 10, 9)
        >>> d[:, :, 1].shape
        (1, 10, 1)
        >>> d[:, 0].shape
        (1, 1, 9)
        >>> d[..., 6:3:-1, 3:6].shape
        (1, 3, 3)
        >>> d[0, [2, 9], [4, 8]].shape
        (1, 2, 2)
        >>> d[0, :, -2].shape
        (1, 10, 1)

        """
        indices = self._parse_indices(indices)
        
        new = self.copy(array=False)
        
        dx = self.to_dask_array()
        
        # Apply 'orthogonal indexing': indices that are 1-d arrays
        # or lists subspace along each dimension
        # independently. This behaviour is similar to Fortran, but
        # different to dask.
        axes_with_list_indices = [
            i
            for i, x in enumerate(indices)
            if isinstance(x, list) or getattr(x, "shape", False)
        ]
        n_axes_with_list_indices = len(axes_with_list_indices)
        
        if n_axes_with_list_indices < 2:
            # At most one axis has a list/1-d array index so do a
            # normal dask subspace
            dx = dx[tuple(indices)]
        else:
            # At least two axes have list/1-d array indices so we
            # can't do a normal dask subspace

            # Subspace axes which have list/1-d array indices
            for axis in axes_with_list_indices:
                dx = da.take(dx, indices[axis], axis=axis)

            if n_axes_with_list_indices < len(indices):
                # Subspace axes which don't have list/1-d array
                # indices. (Do this after subspacing axes which do
                # have list/1-d array indices, in case
                # __keepdims_indexing__ is False.)
                slice_indices = [
                    slice(None) if i in axes_with_list_indices else x
                    for i, x in enumerate(indices)
                ]
                dx = dx[tuple(slice_indices)]
                
        new._set_dask(dx)
        
        if new.shape != self.shape:
            # Delete hdf5 chunksizes when the shape has changed.
            new.nc_clear_hdf5_chunksizes()

        return new


    def __and__(self, other):
        """The binary bitwise operation ``&``

        x.__and__(y) <==> x&y

        """
        return self._binary_operation(other, "__and__")

    def __eq__(self, other):
        """The rich comparison operator ``==``

        x.__eq__(y) <==> x==y

        """
        return self._binary_operation(other, "__eq__")

    def __int__(self):
        """Called by the `int` built-in function.

        x.__int__() <==> int(x)

        **Performance**

        `__int__` causes all delayed operations to be executed, unless
        the dask array size is already known to be greater than 1.

        """
        if self.size != 1:
            raise TypeError(
                "only length-1 arrays can be converted to a "
                f"Python integer. Got {self!r}"
            )

        return int(self.array[(0,) * self.ndim])

    def __iter__(self):
        """Called when an iterator is required.

        x.__iter__() <==> iter(x)

        **Performance**

        If the shape of the data is unknown then it is calculated
        immediately by executing all delayed operations.

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3], 'metres')
        >>> for e in d:
        ...     print(repr(e))
        ...
        <{{repr}}Data(1): [1] metres>
        <{{repr}}Data(1): [2] metres>
        <{{repr}}Data(1): [3] metres>

        >>> d = {{package}}.{{class}}([[1, 2], [3, 4]], 'metres')
        >>> for e in d:
        ...     print(repr(e))
        ...
        <{{repr}}Data: [1, 2] metres>
        <{{repr}}Data: [3, 4] metres>

        >>> d = {{package}}.{{class}}(99, 'metres')
        >>> for e in d:
        ...     print(repr(e))
        ...
        Traceback (most recent call last):
            ...
        TypeError: iteration over a 0-d Data

        """
        try:
            n = len(self)
        except TypeError:
            raise TypeError(f"iteration over a 0-d {self.__class__.__name__}")

        if self.__keepdims_indexing__:
            for i in range(n):
                out = self[i]
                out.reshape(out.shape[1:], inplace=True)
                yield out
        else:
            for i in range(n):
                yield self[i]

    # REVIEW: getitem: `__asanyarray__`: new property `__asanyarray__`
    @property
    def __asanyarray__(self):
        """Whether the chunks need conversion to a `numpy` array.

        .. versionadded:: NEXTVERSION

        :Returns:

            `bool`
                If True then at compute time add a final operation
                (not in-place) to the Dask graph that converts a
                chunk's array object to a `numpy` array if the array
                object has an `__asanyarray__` attribute that is
                `True`, or else does nothing. If False then do not add
                this operation.

        """
        return self._get_component("__asanyarray__", True)

    @property
    def __keepdims_indexing__(self):
        """Flag to indicate whether dimensions indexed with integers are
        kept.

        If set to True (the default) then providing a single integer
        as a single-axis index does *not* reduce the number of array
        dimensions by 1. This behaviour is different to `numpy`.

        If set to False then providing a single integer as a
        single-axis index reduces the number of array dimensions by
        1. This behaviour is the same as `numpy`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `__orthogonal_indexing__`, `__getitem__`,
                     `__setitem__`

        **Examples**

        >>> d = {{package}}.{{class}}([[1, 2, 3], [4, 5, 6]])
        >>> d.__keepdims_indexing__
        True
        >>> e = d[0]
        >>> e.shape
        (1, 3)
        >>> print(e.array)
        [[1 2 3]]

        >>> d.__keepdims_indexing__
        True
        >>> e = d[:, 1]
        >>> e.shape
        (2, 1)
        >>> print(e.array)
        [[2]
         [5]]

        >>> d.__keepdims_indexing__
        True
        >>> e = d[0, 1]
        >>> e.shape
        (1, 1)
        >>> print(e.array)
        [[2]]

        >>> d.__keepdims_indexing__ = False
        >>> e = d[0]
        >>> e.shape
        (3,)
        >>> print(e.array)
        [1 2 3]

        >>> d.__keepdims_indexing__
        False
        >>> e = d[:, 1]
        >>> e.shape
        (2,)
        >>> print(e.array)
        [2 5]

        >>> d.__keepdims_indexing__
        False
        >>> e = d[0, 1]
        >>> e.shape
        ()
        >>> print(e.array)
        2

        """
        return self._get_component("__keepdims_indexing__", True)

    def __setitem__(self, indices, value):
        """Assign to data elements defined by indices.

        d.__setitem__(indices, x) <==> d[indices]=x

        Indexing follows rules that are very similar to the numpy indexing
        rules, the only differences being:

        * An integer index i takes the i-th element but does not reduce
          the rank by one.

        * When two or more dimensions' indices are sequences of integers
          then these indices work independently along each dimension
          (similar to the way vector subscripts work in Fortran). This is
          the same behaviour as indexing on a Variable object of the
          netCDF4 package.

        **Broadcasting**

        The value, or values, being assigned must be broadcastable to the
        shape defined by the indices, using the numpy broadcasting rules.

        **Missing data**

        Data array elements may be set to missing values by assigning them
        to `masked`. Missing values may be unmasked by assigning them to
        any other value.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `__getitem__`, `_parse_indices`

        :Returns:

            `None`

        **Examples**

        >>> d = {{package}}.{{class}}(np.arange(100, 190).reshape(1, 10, 9))
        >>> d.shape
        (1, 10, 9)
        >>> d[:, :, 1] = -10
        >>> d[:, 0] = range(9)
        >>> d[..., 6:3:-1, 3:6] = np.arange(-18, -9).reshape(3, 3)
        >>> d[0, [2, 9], [4, 8]] = {{package}}.{{class}}([[-2, -3]])
        >>> d[0, :, -2] = {{package}}.masked

        """
        indices = self._parse_indices(indices)

        # Missing values could be affected, so make sure that the mask
        # hardness has been applied.
        dx = self.to_dask_array(apply_mask_hardness=True)

        # Do the assignment
        self._set_subspace(dx, indices, value)

        # Remove elements made invalid by updating the `dask` array
        # in-place
        self._clear_after_dask_update(_ALL)
        return

    def __str__(self):
        """Called by the `str` built-in function.

        x.__str__() <==> str(x)

        """
        units = self.get_units(None)
        calendar = self.get_calendar(None)

        isreftime = False
        if units is not None:
            if isinstance(units, str):
                isreftime = "since" in units
            else:
                units = "??"

        try:
            first = self.first_element()
        except Exception:
            out = ""
            if units and not isreftime:
                out += f" {units}"
            if calendar:
                out += f" {calendar}"

            return out

        size = self.size
        shape = self.shape
        ndim = self.ndim
        open_brackets = "[" * ndim
        close_brackets = "]" * ndim

        mask = [False, False, False]

        if isreftime and first is np.ma.masked:
            first = 0
            mask[0] = True

        if size == 1:
            if isreftime:
                # Convert reference time to date-time
                try:
                    first = type(self)(
                        np.ma.array(first, mask=mask[0]), units, calendar
                    ).datetime_array
                except (ValueError, OverflowError):
                    first = "??"

            out = f"{open_brackets}{first}{close_brackets}"
        else:
            last = self.last_element()
            if isreftime:
                if last is np.ma.masked:
                    last = 0
                    mask[-1] = True

                # Convert reference times to date-times
                try:
                    first, last = type(self)(
                        np.ma.array([first, last], mask=(mask[0], mask[-1])),
                        units,
                        calendar,
                    ).datetime_array
                except (ValueError, OverflowError):
                    first, last = ("??", "??")

            if size > 3:
                out = f"{open_brackets}{first}, ..., {last}{close_brackets}"
            elif shape[-1:] == (3,):
                middle = self.second_element()
                if isreftime:
                    # Convert reference time to date-time
                    if middle is np.ma.masked:
                        middle = 0
                        mask[1] = True

                    try:
                        middle = type(self)(
                            np.ma.array(middle, mask=mask[1]),
                            units,
                            calendar,
                        ).datetime_array
                    except (ValueError, OverflowError):
                        middle = "??"

                out = (
                    f"{open_brackets}{first}, {middle}, {last}{close_brackets}"
                )
            elif size == 3:
                out = f"{open_brackets}{first}, ..., {last}{close_brackets}"
            else:
                out = f"{open_brackets}{first}, {last}{close_brackets}"

        if isreftime:
            if calendar:
                out += f" {calendar}"
        elif units:
            out += f" {units}"

        return out

    def _binary_operation(self, other, method):
        """Implement binary arithmetic and comparison operations.

        Implements binary arithmetic and comparison operations with
        the numpy broadcasting rules.

        It is called by the binary arithmetic and comparison methods,
        such as `__sub__`, `__imul__`, `__rdiv__`, `__lt__`, etc.

        .. seealso:: `_unary_operation`

        :Parameters:

            other:
                The object on the right hand side of the operator.

            method: `str`
                The binary arithmetic or comparison method name (such as
                ``'__imul__'`` or ``'__ge__'``).

        :Returns:

            `Data`
                A new data object, or if the operation was in place, the
                same data object.

        **Examples**

        >>> d = {{package}}.{{class}}([0, 1, 2, 3])
        >>> e = {{package}}.{{class}}([1, 1, 3, 4])
        >>> f = d._binary_operation(e, '__add__')
        >>> print(f.array)
        [1 2 5 7]
        >>> e = d._binary_operation(e, '__lt__')
        >>> print(e.array)
        [ True False  True  True]
        >>> d._binary_operation(2, '__imul__')
        >>> print(d.array)
        [0 2 4 6]

        """
        inplace = method[2] == "i"
        if inplace:
            d = self
        else:
            d = self.copy()

        # Ensure other is an independent Data object
        if not isinstance(other, self.__class__):
           if other is None:
                # Can't sensibly initialise a Data object from a bare
                # `None` (issue #281)
                other = np.array(None, dtype=object)

            other = self.asdata(other)

        dx0 = d.to_dask_array()
        dx1 = other.to_dask_array()
        result = getattr(dx0, method)(dx1)
        d._set_dask(result)
        return d

    def _item(self, index):
        """Return an element of the data as a scalar.

        It is assumed, but not checked, that the given index selects
        exactly one element.

        :Parameters:

            index:

        :Returns:

                The selected element of the data.

        **Examples**

        >>> d = {{package}}.{{class}}([[1, 2, 3]], 'km')
        >>> x = d._item((0, -1))
        >>> print(x, type(x))
        3 <class 'int'>
        >>> x = d._item((0, 1))
        >>> print(x, type(x))
        2 <class 'int'>
        >>> d[0, 1] = {{package}}.masked
        >>> d._item((slice(None), slice(1, 2)))
        masked

        """
        array = self[index].array

        if not np.ma.isMA(array):
            return array.item()

        mask = array.mask
        if mask is np.ma.nomask or not mask.item():
            return array.item()

        return np.ma.masked

    def _original_filenames(self, define=None, update=None, clear=False):
        """Return the names of files that contain the original data.

        {{original filenames}}

        .. note:: The original filenames are **not** inherited by
                  parent constructs that contain the data.

        .. versionadded:: (cfdm) 1.10.0.1

        :Parameters:

            {{define: (sequence of) `str`, optional}}

            {{update: (sequence of) `str`, optional}}

            {{clear: `bool` optional}}

        :Returns:

            `set` or `None`
                {{Returns original filenames}}

                If the *define* or *update* parameter is set then
                `None` is returned.

        **Examples**

        >>> d = {{package}}.{{class}}(9)
        >>> d._original_filenames()
        ()
        >>> d._original_filenames(define="file1.nc")
        >>> d._original_filenames()
        ('/data/user/file1.nc',)
        >>> d._original_filenames(update=["file1.nc"])
        >>> d._original_filenames()
        ('/data/user/file1.nc',)
        >>> d._original_filenames(update="file2.nc")
        >>> d._original_filenames()
        ('/data/user/file1.nc', '/data/user/file2.nc')
        >>> d._original_filenames(define="file3.nc")
        >>> d._original_filenames()
        ('/data/user/file3.nc',)
        >>> d._original_filenames(clear=True)
        >>> d._original_filenames()
        ()

        >>> d = {{package}}.{{class}}(9, _filenames=["file1.nc", "file2.nc"])
        >>> d._original_filenames()
        ('/data/user/file1.nc', '/data/user/file2.nc',)

        """
        old = super()._original_filenames(
            define=define, update=update, clear=clear
        )

        if old is None:
            return

        # Find any compression ancillary data variables
        ancils = []
        compression = self.get_compression_type()
        if compression:
            if compression == "gathered":
                i = self.get_list(None)
                if i is not None:
                    ancils.append(i)
            elif compression == "subsampled":
                ancils.extend(self.get_tie_point_indices({}).values())
                ancils.extend(self.get_interpolation_parameters({}).values())
                ancils.extend(self.get_dependent_tie_points({}).values())
            else:
                if compression in (
                    "ragged contiguous",
                    "ragged indexed contiguous",
                ):
                    i = self.get_count(None)
                    if i is not None:
                        ancils.append(i)

                if compression in (
                    "ragged indexed",
                    "ragged indexed contiguous",
                ):
                    i = self.get_index(None)
                    if i is not None:
                        ancils.append(i)

            if ancils:
                # Include original file names from ancillary variables
                for a in ancils:
                    old.update(a._original_filenames(clear=clear))

        # Return the old file names
        return old

    def _parse_axes(self, axes):
        """Parses the data axes and returns valid non-duplicate axes.

        :Parameters:

            axes: (sequence of) `int`
                The axes of the data.

                {{axes int examples}}

        :Returns:

            `tuple`

        **Examples**

        >>> d._parse_axes(1)
        (1,)

        >>> e._parse_axes([0, 2])
        (0, 2)

        """
        if axes is None:
            return axes

        ndim = self.ndim

        if isinstance(axes, int):
            axes = (axes,)

        axes2 = []
        for axis in axes:
            if 0 <= axis < ndim:
                axes2.append(axis)
            elif -ndim <= axis < 0:
                axes2.append(axis + ndim)
            else:
                raise ValueError(f"Invalid axis: {axis!r}")

        # Check for duplicate axes
        n = len(axes2)
        if n > len(set(axes2)) >= 1:
            raise ValueError(f"Duplicate axis: {axes2}")

        return tuple(axes2)

    def _set_Array(self, array, copy=True):
        """Set the array.

        .. seealso:: `_set_CompressedArray`

        :Parameters:

            array: `numpy` array_like or `Array`, optional
                The array to be inserted.

        :Returns:

            `None`

        **Examples**

        >>> d._set_Array(a)

        """
        if not isinstance(array, abstract.Array):
            if not isinstance(array, np.ndarray):
                if issparse(array):
                    array = SparseArray(array)
                else:
                    array = NumpyArray(np.asanyarray(array))
            else:
                array = NumpyArray(array)

        super()._set_Array(array, copy=copy)

    def _set_CompressedArray(self, array, copy=True):
        """Set the compressed array.

        .. versionadded:: (cfdm) 1.7.11

        .. seealso:: `_set_Array`

        :Parameters:

            array: subclass of `CompressedArray`
                The compressed array to be inserted.

        :Returns:

            `None`

        **Examples**

        >>> d._set_CompressedArray(a)

        """
        self._set_Array(array, copy=copy)
        
    def _cfa_del_write(self):
        """Set the CFA write status of the data to `False`.

        .. versionadded:: 3.15.0

        .. seealso:: `cfa_get_write`, `_cfa_set_write`

        :Returns:

            `bool`
                The CFA status prior to deletion.

        """
        return self._del_component("cfa_write", False)

    def _cfa_set_term(self, value):
        """Set the CFA aggregation instruction term status.

        .. versionadded:: 3.15.0

        .. seealso:: `cfa_get_term`, `cfa_set_term`

        :Parameters:

            status: `bool`
                The new CFA aggregation instruction term status.

        :Returns:

            `None`

        """
        if not value:
            return self._del_component("cfa_term", None

        self._set_component("cfa_term", bool(value))

    def _clear_after_dask_update(self, clear=_ALL):
        """Remove components invalidated by updating the `dask` array.

        Removes or modifies components that can't be guaranteed to be
        consistent with an updated `dask` array. See the *clear*
        parameter for details.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `_del_Array`, `_del_cached_elements`,
                     `_cfa_del_write`, `_set_dask`

        :Parameters:

            clear: `int`, optional
                Specify which components should be removed. Which
                components are removed is determined by sequentially
                combining *clear* with the ``_ARRAY``, ``_CACHE`` and
                ``_CFA`` integer-valued contants, using the bitwise
                AND operator:

                * If ``clear & _ARRAY`` is non-zero then a source
                  array is deleted.

                * If ``clear & _CACHE`` is non-zero then cached
                  element values are deleted.

                * If ``clear & _CFA`` is non-zero then the CFA write
                  status is set to `False`.

                By default *clear* is the ``_ALL`` integer-valued
                constant, which results in all components being
                removed.

                If *clear* is the ``_NONE`` integer-valued constant
                then no components are removed.

                To retain a component and remove all others, use
                ``_ALL`` with the bitwise OR operator. For instance,
                if *clear* is ``_ALL ^ _CACHE`` then the cached
                element values will be kept but all other components
                will be removed.

                .. versionadded:: 3.15.0

        :Returns:

            `None`

        """
        if not clear:
            return

        if clear & _ARRAY:
            # Delete a source array
            self._del_Array(None)

        if clear & _CACHE:
            # Delete cached element values
            self._del_cached_elements()

        if clear & _CFA:
            # Set the CFA write status to False
            self._cfa_del_write()

    def _set_dask(self, dx, copy=False, clear=_ALL, asanyarray=False):
        """Set the dask array.

        .. versionadded:: 3.14.0

        .. seealso:: `to_dask_array`, `_clear_after_dask_update`,
                     `_del_dask`

        :Parameters:

            dx: `dask.array.Array`
                The array to be inserted.

            copy: `bool`, optional
                If True then copy *array* before setting it. By
                default it is not copied.

            clear: `int`, optional
                Specify which components should be removed. By default
                *clear* is the ``_ALL`` integer-valued constant, which
                results in all components being removed. See
                `_clear_after_dask_update` for details.

            asanyarray: `bool` or `None`, optional
                If `None` then do nothing. Otherwise set
                `__asanyarray__` to the Boolean value of *asanyarray*.

                .. versionadded:: NEXTVERSION

        :Returns:

            `None`

        """
        if array is NotImplemented:
            logger.warning(
                "WARNING: NotImplemented has been set in the place of a "
                "dask array."
                "\n\n"
                "This could occur if any sort of exception is raised "
                "by a function that is run on chunks (via, for "
                "instance, da.map_blocks or "
                "dask.array.core.elemwise). Such a function could get "
                "run at definition time in order to ascertain "
                "suitability (such as data type casting, "
                "broadcasting, etc.). Note that the exception may be "
                "difficult to diagnose, as dask will have silently "
                "trapped it and returned NotImplemented (for "
                "instance, see dask.array.core.elemwise). Print "
                "statements in a local copy of dask are possibly the "
                "way to go if the cause of the error is not obvious."
            )

        if copy:
            dx = dx.copy()

            
        self._set_component("dask", dx, copy=False)

        if asanyarray is not None:
            self._set_component("__asanyarray__", bool(asanyarray))

        self._clear_after_dask_update(clear)

    def _del_dask(self, default=ValueError(), clear=_ALL):
        """Remove the dask array.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `to_dask_array`, `_clear_after_dask_update`,
                     `_set_dask`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                dask array axes has not been set. If set to an
                `Exception` instance then it will be raised instead.

            clear: `int`, optional
                Specify which components should be removed. By default
                *clear* is the ``_ALL`` integer-valued constant, which
                results in all components being removed. See
                `_clear_after_dask_update` for details. If there is
                no dask array then no components are removed,
                regardless of the value of *clear*.

        :Returns:

            `dask.array.Array`
                The removed dask array.

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3])
        >>> dx = d._del_dask()
        >>> d._del_dask("No dask array")
        'No dask array'
        >>> d._del_dask()
        Traceback (most recent call last):
            ...
        ValueError: 'Data' has no dask array
        >>> d._del_dask(RuntimeError('No dask array'))
        Traceback (most recent call last):
            ...
        RuntimeError: No dask array

        """
        out = self.del_component("dask", None)
        if out is None:            
            return self._default(
                default, f"{self.__class__.__name__!r} has no dask array"
            )

        self._clear_after_dask_update(clear)
        return out

    @classmethod
    def _set_subspace(cls, array, indices, value, orthogonal_indexing=True):
        """Assign to a subspace of an array.

        :Parameters:

            array: array_like
                The array to be assigned to. Must support
                `numpy`-style indexing. The array is changed in-place.

            indices: sequence
                The indices to be applied.

            value: array_like
                The value being assigned. Must support fancy indexing.

            orthogonal_indexing: `bool`, optional
                If True then apply 'orthogonal indexing', for which
                indices that are 1-d arrays or lists subspace along
                each dimension independently. This behaviour is
                similar to Fortran but different to, for instance,
                `numpy` or `dask`.

        :Returns:

            `None`

        **Examples**

        Note that ``a`` is redefined for each example, as it is
        changed in-place.

        >>> a = np.arange(40).reshape(5, 8)
        >>> {{package}}.Data._set_subspace(a, [[1, 4 ,3], [7, 6, 1]],
        ...                    np.array([[-1, -2, -3]]))
        >>> print(a)
        [[ 0  1  2  3  4  5  6  7]
         [ 8 -3 10 11 12 13 -2 -1]
         [16 17 18 19 20 21 22 23]
         [24 -3 26 27 28 29 -2 -1]
         [32 -3 34 35 36 37 -2 -1]]

        >>> a = np.arange(40).reshape(5, 8)
        >>> {{package}}.Data._set_subspace(a, [[1, 4 ,3], [7, 6, 1]],
        ...                    np.array([[-1, -2, -3]]),
        ...                    orthogonal_indexing=False)
        >>> print(a)
        [[ 0  1  2  3  4  5  6  7]
         [ 8  9 10 11 12 13 14 -1]
         [16 17 18 19 20 21 22 23]
         [24 -3 26 27 28 29 30 31]
         [32 33 34 35 36 37 -2 39]]

        >>> a = np.arange(40).reshape(5, 8)
        >>> value = np.linspace(-1, -9, 9).reshape(3, 3)
        >>> print(value)
        [[-1. -2. -3.]
         [-4. -5. -6.]
         [-7. -8. -9.]]
        >>> {{package}}.Data._set_subspace(a, [[4, 4 ,1], [7, 6, 1]], value)
        >>> print(a)
        [[ 0  1  2  3  4  5  6  7]
         [ 8 -9 10 11 12 13 -8 -7]
         [16 17 18 19 20 21 22 23]
         [24 25 26 27 28 29 30 31]
         [32 -6 34 35 36 37 -5 -4]]

        """
        if not orthogonal_indexing:
            # --------------------------------------------------------
            # Apply non-orthogonal indexing
            # --------------------------------------------------------
            array[tuple(indices)] = value
            return

        # ------------------------------------------------------------
        # Still here? Then apply orthogonal indexing
        # ------------------------------------------------------------
        axes_with_list_indices = [
            i
            for i, x in enumerate(indices)
            if isinstance(x, list) or getattr(x, "shape", False)
        ]

        if len(axes_with_list_indices) < 2:
            # At most one axis has a list-of-integers index so we can
            # do a normal assignment
            array[tuple(indices)] = value
        else:
            # At least two axes have list-of-integers indices so we
            # can't do a normal assignment.
            #
            # The brute-force approach would be to do a separate
            # assignment to each set of elements of 'array' that are
            # defined by every possible combination of the integers
            # defined by the two index lists.
            #
            # For example, if the input 'indices' are ([1, 2, 4, 5],
            # slice(0:10), [8, 9]) then the brute-force approach would
            # be to do 4*2=8 separate assignments of 10 elements each.
            #
            # This can be reduced by a factor of ~2 per axis that has
            # list indices if we convert it to a sequence of "size 2"
            # slices (with a "size 1" slice at the end if there are an
            # odd number of list elements).
            #
            # In the above example, the input list index [1, 2, 4, 5]
            # can be mapped to two slices: slice(1,3,1), slice(4,6,1);
            # the input list index [8, 9] is mapped to slice(8,10,1)
            # and only 2 separate assignments of 40 elements each are
            # needed.
            indices1 = indices[:]
            for i, (x, size) in enumerate(zip(indices, array.shape)):
                if i in axes_with_list_indices:
                    # This index is a list (or similar) of integers
                    if not isinstance(x, list):
                        x = np.asanyarray(x).tolist()

                    y = []
                    args = [iter(x)] * 2
                    for start, stop in itertools.zip_longest(*args):
                        if start < 0:
                            start += size

                        if stop is None:
                            y.append(slice(start, start + 1))
                            break

                        if stop < 0:
                            stop += size

                        step = stop - start
                        if not step:
                            # (*) There is a repeated index in
                            #     positions 2N and 2N+1 (N>=0). Store
                            #     this as a single-element list
                            #     instead of a "size 2" slice, mainly
                            #     as an indicator that a special index
                            #     to 'value' might need to be
                            #     created. See below, where this
                            #     comment is referenced.
                            #
                            #     For example, the input list index
                            #     [1, 4, 4, 4, 6, 2, 7] will be mapped
                            #     to slice(1,5,3), [4], slice(6,1,-4),
                            #     slice(7,8,1)
                            y.append([start])
                        else:
                            if step > 0:
                                stop += 1
                            else:
                                stop -= 1

                            y.append(slice(start, stop, step))

                    indices1[i] = y
                else:
                    indices1[i] = (x,)

            if value.size == 1:
                # 'value' is logically scalar => simply assign it to
                # all index combinations.
                for i in itertools.product(*indices1):
                    array[i] = value
            else:
                # 'value' has two or more elements => for each index
                # combination for 'array' assign the corresponding
                # part of 'value'.
                indices2 = []
                ndim_difference = array.ndim - value.ndim
                for i2, size in enumerate(value.shape):
                    i1 = i2 + ndim_difference
                    if i1 not in axes_with_list_indices:
                        # The input 'indices[i1]' is a slice
                        indices2.append((slice(None),))
                        continue

                    index1 = indices1[i1]
                    if size == 1:
                        indices2.append((slice(None),) * len(index1))
                    else:
                        y = []
                        start = 0
                        for index in index1:
                            stop = start + 2
                            if isinstance(index, list):
                                # Two consecutive elements of 'value'
                                # are assigned to the same integer
                                # index of 'array'.
                                #
                                # See the (*) comment above.
                                start += 1

                            y.append(slice(start, stop))
                            start = stop

                        indices2.append(y)

                for i, j in zip(
                    itertools.product(*indices1), itertools.product(*indices2)
                ):
                    array[i] = value[j]

    @property
    def array(self):
        """A numpy array copy of the data.

        In-place changes to the returned numpy array do not affect the
        underlying dask array.

        The returned numpy array has the same mask hardness and fill
        values as the data.

        Compare with `compute`.

        **Performance**

        `array` causes all delayed operations to be computed. The
        returned `numpy` array is a deep copy of that returned by
        created `compute`.

        .. seealso:: `datetime_array`, `compute`, `persist`

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3.0], 'km')
        >>> a = d.array
        >>> isinstance(a, np.ndarray)
        True
        >>> print(a)
        [ 1.  2.  3.]
        >>> d[0] = -99
        >>> print(a[0])
        1.0
        >>> a[0] = 88
        >>> print(d[0])
        -99.0 km

        >>> d = {{package}}.{{class}}('2000-12-1', units='days since 1999-12-1')
        >>> print(d.array)
        366
        >>> print(d.datetime_array)
        2000-12-01 00:00:00

        """
        array = self.compute().copy()
        if issparse(array):
            array = array.toarray()
        elif not isinstance(array, np.ndarray):
            array = np.asanyarray(array)

        return array

    @property
    def compressed_array(self):
        """Returns an independent numpy array of the compressed data.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `get_compressed_axes`, `get_compressed_dimension`,
                     `get_compression_type`

        :Returns:

            `numpy.ndarray`
                An independent numpy array of the compressed data.

        **Examples**

        >>> a = d.compressed_array

        """
        ca = self._get_Array(None)
        if ca is None or not ca.get_compression_type():
            raise ValueError("not compressed: can't get compressed array")

        return ca.compressed_array

    @property
    def data(self):
        """The data as an object identity.

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2], 'm')
        >>> d.data is d
        True

        """
        return self

    @property
    def datetime_array(self):
        """An independent numpy array of date-time objects.

        Only applicable to data arrays with reference time units.

        If the calendar has not been set then the CF default calendar will
        be used and the units will be updated accordingly.

        The data-type of the data array is unchanged.

        .. seealso:: `array`, `compute`, `persist`

        **Performance**

        `datetime_array` causes all delayed operations to be computed.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `array`, `datetime_as_string`

        :Returns:

            `numpy.ndarray`
                An independent numpy array of the date-time objects.

        **Examples**

        >>> d = {{package}}.{{class}}([31, 62, 90], units='days since 2018-12-01')
        >>> a = d.datetime_array
        >>> print(a)
        [cftime.DatetimeGregorian(2019, 1, 1, 0, 0, 0, 0)
         cftime.DatetimeGregorian(2019, 2, 1, 0, 0, 0, 0)
         cftime.DatetimeGregorian(2019, 3, 1, 0, 0, 0, 0)]
        >>> print(a[1])
        2019-02-01 00:00:00

        >>> d = {{package}}.{{class}}(
        ...     [31, 62, 90], units='days since 2018-12-01', calendar='360_day')
        >>> a = d.datetime_array
        >>> print(a)
        [cftime.Datetime360Day(2019, 1, 2, 0, 0, 0, 0)
         cftime.Datetime360Day(2019, 2, 3, 0, 0, 0, 0)
         cftime.Datetime360Day(2019, 3, 1, 0, 0, 0, 0)]
        >>> print(a[1])
        2019-02-03 00:00:00

        """
        units = self.get_units(None)
        calendar = self.get_calendar("standard")

        if units is None or " since " not in units:
            raise ValueError(
                f"Can't create date-time array from units {units!r}"
            )

        if calendar == "none":
            raise ValueError(
                f"Can't create date-time array from units {units!r} "
                "because calendar is 'none'"
            )

        dx = self.to_dask_array()
        dx = convert_to_datetime(dx, units, calendar)

        a = dx.compute()

        if np.ma.isMA(a):
            if self.hardmask:
                a.harden_mask()
            else:
                a.soften_mask()

            a.set_fill_value(self.fill_value)

        return a

    @property
    def datetime_as_string(self):
        """Returns an independent numpy array with datetimes as strings.

        Specifically, returns an independent numpy array containing
        string representations of times since a reference date.

        Only applicable for reference time units.

        If the calendar has not been set then the CF default calendar of
        "standard" (i.e. the mixed Gregorian/Julian calendar as defined by
        Udunits) will be used.

        Conversions are carried out with the `netCDF4.num2date` function.

        .. versionadded:: (cfdm) 1.8.0

        .. seealso:: `array`, `datetime_array`

        :Returns:

            `numpy.ndarray`
                An independent numpy array of the date-time strings.

        **Examples**

        >>> d = {{package}}.{{class}}([31, 62, 90], units='days since 2018-12-01')
        >>> print(d.datetime_as_string)
        ['2019-01-01 00:00:00' '2019-02-01 00:00:00' '2019-03-01 00:00:00']

        >>> d = {{package}}.{{class}}(
        ...     [31, 62, 90], units='days since 2018-12-01', calendar='360_day')
        >>> print(d.datetime_as_string)
        ['2019-01-02 00:00:00' '2019-02-03 00:00:00' '2019-03-01 00:00:00']

        """
        return self.datetime_array.astype(str)

    @property
    def dtype(self):
        """The `numpy` data-type of the data.

        Always returned as a `numpy` data-type instance, but may be
        set as any object that converts to a `numpy` data-type.

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2.5, 3.9])
        >>> d.dtype
        dtype('float64')
        >>> print(d.array)
        [1.  2.5 3.9]
        >>> d.dtype = int
        >>> d.dtype
        dtype('int64')
        >>> print(d.array)
        [1 2 3]
        >>> d.dtype = 'float32'
        >>> print(d.array)
        [1. 2. 3.]
        >>> import numpy as np
        >>> d.dtype = np.dtype('int32')
        >>> d.dtype
        dtype('int32')
        >>> print(d.array)
        [1 2 3]

        """
        dx = self.to_dask_array()
        return dx.dtype

    @dtype.setter
    def dtype(self, value):
        dx = self.to_dask_array()

        # Only change the datatype if it's different to that of the
        # dask array
        if dx.dtype != value:
            dx = dx.astype(value)
            self._set_dask(dx)

    @property
    def hardmask(self):
        """Hardness of the mask.

        If the `hardmask` attribute is `True`, i.e. there is a hard
        mask, then unmasking an entry will silently not occur. This is
        the default, and prevents overwriting the mask.

        If the `hardmask` attribute is `False`, i.e. there is a soft
        mask, then masked entries may be overwritten with non-missing
        values.

        .. note:: Setting the `hardmask` attribute does not
                  immediately change the mask hardness, rather its
                  value indicates to other methods (such as `where`,
                  `transpose`, etc.) whether or not the mask needs
                  hardening or softening prior to an operation being
                  defined, and those methods will reset the mask
                  hardness if required.

                  By contrast, the `harden_mask` and `soften_mask`
                  methods immediately reset the mask hardness of the
                  underlying `dask` array, and also set the value of
                  the `hardmask` attribute.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `harden_mask`, `soften_mask`, `to_dask_array`,
                     `__setitem__`

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3])
        >>> d.hardmask
        True
        >>> d[0] = {{package}}.masked
        >>> print(d.array)
        [-- 2 3]
        >>> d[...] = 999
        >>> print(d.array)
        [-- 999 999]
        >>> d.hardmask = False
        >>> d.hardmask
        False
        >>> d[...] = -1
        >>> print(d.array)
        [-1 -1 -1]

        """
        return self._get_component("hardmask", _DEFAULT_HARDMASK)

    @hardmask.setter
    def hardmask(self, value):
        self._set_component("hardmask", bool(value))

    @property
    def mask(self):
        """The Boolean missing data mask of the data array.

        The Boolean mask has True where the data array has missing data
        and False otherwise.

        .. seealso:: `masked_values`

        :Returns:

            `Data`

        **Examples**

        >>> d = {{package}}.{{class}}(np.ma.array(
        ...     [[280.0,   -99,   -99,   -99],
        ...      [281.0, 279.0, 278.0, 279.5]],
        ...     mask=[[0, 1, 1, 1], [0, 0, 0, 0]]
        ... ))
        >>> d
        <{{repr}}Data(2, 4): [[280.0, ..., 279.5]]>
        >>> print(d.array)
        [[280.0    --    --    --]
         [281.0 279.0 278.0 279.5]]
        >>> d.mask
        <{{repr}}Data(2, 4): [[False, ..., False]]>
        >>> print(d.mask.array)
        [[False  True  True  True]
         [False False False False]]

        """
        d = self.copy(array=False)
        dx = self.to_dask_array()
        dx = da.ma.getmaskarray(dx)
        d._set_dask(dx)
        d.override_units(None, inplace=True)
        d.hardmask = _DEFAULT_HARDMASK
        return d

    @property
    def sparse_array(self):
        """Return an independent `scipy` sparse array of the data.

        In-place changes to the returned sparse array do not affect
        the underlying dask array.

        An `AttributeError` is raised if a sparse array representation
        is not available.

        **Performance**

        `sparse_array` causes all delayed operations to be
        computed. The returned sparse array is a deep copy of that
        returned by created `compute`.

        .. versionadded:: (cfdm) 1.11.0.0

        .. seealso:: `array`

        :Returns:

                An independent `scipy` sparse array of the data.

        **Examples**

        >>> from scipy.sparse import issparse
        >>> issparse(d.sparse_array)
        True

        """
        array = self.compute()
        if issparse(array):
            return array.copy()

        raise AttributeError(
            "A sparse array representation of the data is not available"
        )

    def any(self, axis=None, keepdims=True, split_every=None):
        """Test whether any data array elements evaluate to True.

        .. seealso:: `all`

        :Parameters:

            axis: (sequence of) `int`, optional
                Axis or axes along which a logical OR reduction is
                performed. The default (`None`) is to perform a
                logical OR over all the dimensions of the input
                array. *axis* may be negative, in which case it counts
                from the last to the first axis.

            {{collapse keepdims: `bool`, optional}}

            {{split_every: `int` or `dict`, optional}}

        :Returns:

            `{{class}}`
                Whether or any data array elements evaluate to True.

        **Examples**

        >>> d = {{package}}.{[class}}([[0, 2], [0, 4]])
        >>> d.any()
        <{{repr}}Data(1, 1): [[True]]>
        >>> d.any(keepdims=False)
        <{{repr}}Data(1, 1): True>
        >>> d.any(axis=0)
        <{{repr}}Data(1, 2): [[False, True]]>
        >>> d.any(axis=1)
        <{{repr}}Data(2, 1): [[True, True]]>
        >>> d.any(axis=())
        <{{repr}}Data(2, 2): [[False, ..., True]]>

        >>> d[0] = {{package}}.masked
        >>> print(d.array)
        [[-- --]
         [0 4]]
        >>> d.any(axis=0)
        <{{repr}}Data(1, 2): [[False, True]]>
        >>> d.any(axis=1)
        <{{repr}}Data(2, 1): [[--, True]]>

        >>> d[...] = {{package}}.masked
        >>> d.any()
        <{{repr}}Data(1, 1): [[--]]>
        >>> bool(d.any())
        False
        >>> bool(d.any(keepdims=False))
        False

        """
        d = self.copy(array=False)
        dx = self.to_dask_array()
        dx = da.any(dx, axis=axis, keepdims=keepdims, split_every=split_every)
        d._set_dask(dx)
        d.override_units(None, inplace=True)
        d.hardmask = _DEFAULT_HARDMASK
        return d

    @_inplace_enabled(default=False)
    def apply_masking(
        self,
        fill_values=None,
        valid_min=None,
        valid_max=None,
        valid_range=None,
        inplace=False,
    ):
        """Apply masking.

        Masking is applied according to the values of the keyword
        parameters.

        Elements that are already masked remain so.

        .. versionadded:: (cfdm) 1.8.2

        .. seealso:: `get_fill_value`, `hardmask`, `mask`

        :Parameters:

            fill_values: `bool` or sequence of scalars, optional
                Specify values that will be set to missing data. Data
                elements exactly equal to any of the values are set to
                missing data.

                If True then the value returned by the `get_fill_value`
                method, if such a value exists, is used.

                Zero or more values may be provided in a sequence of
                scalars.

                *Parameter example:*
                  Specify a fill value of 999: ``fill_values=[999]``

                *Parameter example:*
                  Specify fill values of 999 and -1.0e30:
                  ``fill_values=[999, -1.0e30]``

                *Parameter example:*
                  Use the fill value already set for the data:
                  ``fill_values=True``

                *Parameter example:*
                  Use no fill values: ``fill_values=False`` or
                  ``fill_value=[]``

            valid_min: number, optional
                A scalar specifying the minimum valid value. Data elements
                strictly less than this number will be set to missing
                data.

            valid_max: number, optional
                A scalar specifying the maximum valid value. Data elements
                strictly greater than this number will be set to missing
                data.

            valid_range: (number, number), optional
                A vector of two numbers specifying the minimum and maximum
                valid values, equivalent to specifying values for both
                *valid_min* and *valid_max* parameters. The *valid_range*
                parameter must not be set if either *valid_min* or
                *valid_max* is defined.

                *Parameter example:*
                  ``valid_range=[-999, 10000]`` is equivalent to setting
                  ``valid_min=-999, valid_max=10000``

            inplace: `bool`, optional
                If True then do the operation in-place and return `None`.

        :Returns:

            `{{class}}` or `None`
                The data with masked values. If the operation was in-place
                then `None` is returned.

        **Examples**


        >>> d = {{package}}.{{class}}(np.arange(12).reshape(3, 4), 'm')
        >>> d[1, 1] = {{package}}.masked
        >>> print(d.array)
        [[0  1  2  3]
         [4 --  6  7]
         [8  9 10 11]]

        >>> print(d.apply_masking().array)
        [[0  1  2  3]
         [4 --  6  7]
         [8  9 10 11]]
        >>> print(d.apply_masking(fill_values=[0]).array)
        [[-- 1 2 3]
         [4 -- 6 7]
         [8 9 10 11]]
        >>> print(d.apply_masking(fill_values=[0, 11]).array)
        [[-- 1 2 3]
         [4 -- 6 7]
         [8 9 10 --]]

        >>> print(d.apply_masking(valid_min=3).array)
        [[-- -- -- 3]
         [4 -- 6 7]
         [8 9 10 11]]
        >>> print(d.apply_masking(valid_max=6).array)
        [[0 1 2 3]
         [4 -- 6 --]
         [-- -- -- --]]
        >>> print(d.apply_masking(valid_range=[2, 8]).array)
        [[-- -- 2 3]
         [4 -- 6 7]
         [8 -- -- --]]

        >>> d.set_fill_value(7)
        >>> print(d.apply_masking(fill_values=True).array)
        [[0  1  2  3]
         [4 --  6 --]
         [8  9 10 11]]
        >>> print(d.apply_masking(fill_values=True,
        ...                       valid_range=[2, 8]).array)
        [[-- -- 2 3]
         [4 -- 6 --]
         [8 -- -- --]]

        """

        if valid_range is not None:
            if valid_min is not None or valid_max is not None:
                raise ValueError(
                    "Can't set 'valid_range' parameter with either the "
                    "'valid_min' nor 'valid_max' parameters"
                )

            try:
                if len(valid_range) != 2:
                    raise ValueError(
                        "'valid_range' parameter must be a vector of "
                        "two elements"
                    )
            except TypeError:
                raise ValueError(
                    "'valid_range' parameter must be a vector of "
                    "two elements"
                )

            valid_min, valid_max = valid_range

        d = _inplace_enabled_define_and_cleanup(self)

        if fill_values is None:
            fill_values = False

        if isinstance(fill_values, bool):
            if fill_values:
                fill_value = self.get_fill_value(None)
                if fill_value is not None:
                    fill_values = (fill_value,)
                else:
                    fill_values = ()
            else:
                fill_values = ()
        else:
            try:
                _ = iter(fill_values)
            except TypeError:
                raise TypeError(
                    "'fill_values' parameter must be a sequence or "
                    f"of type bool. Got type {type(fill_values)}"
                )
            else:
                if isinstance(fill_values, str):
                    raise TypeError(
                        "'fill_values' parameter must be a sequence or "
                        f"of type bool. Got type {type(fill_values)}"
                    )

        attributes = {"missing_value": fill_values}
        for attr, value in zip(
            ("valid_min", "valid_max", "valid_range"),
            (valid_min, valid_max, valid_range),
        ):
            if value is not None:
                attributes[attr] = value

        dx = self.to_dask_array()
        dx = NetCDFIndexer(dx, mask=True, scale=False, attributes=attributes)[
            ...
        ]
        d._set_dask(dx)
        return d

    @classmethod
    def asdata(cls, d, dtype=None, copy=False):
        """Convert the input to a `Data` object.

        If the input *d* has the Data interface (i.e. it has a
        `__data__` method), then the output of this method is used as
        the returned `Data` object. Otherwise, `Data(d)` is returned.

        .. versionadded (cfdm) NEXTVERSION

        :Parameters:

            d: data-like
                Input data in any form that can be converted to a
                `{{class}}` object. This includes `{{class}}` and
                `Field` objects, and objects with the `{{class}}`
                interface, `numpy` arrays and any object which may be
                converted to a `numpy` array.

           dtype: data-type, optional
                By default, the data-type is inferred from the input data.

           copy: `bool`, optional
                If True and *d* has the `{{class}}` interface, then a
                copy of `d.__data__()` is returned.

        :Returns:

            `{{class}}`
                `{{class}}` interpretation of *d*. No copy is
                performed on the input if it is already a `{{class}}`
                object with matching data type and *copy* is False.

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2])
        >>> {{package}}.{{class}}.asdata(d) is d
        True
        >>> d.asdata(d) is d
        True

        >>> {{package}}.{{class}}.asdata([1, 2])
        <{{repr}}Data: [1, 2]>

        >>> {{package}}.{{class}}.asdata(np.array([1, 2]))
        <{{repr}}Data: [1, 2]>

        """
        data = getattr(d, "__data__", None)
        if data is None:
            # d does not have a Data interface
            data = cls(d)
            if dtype is not None:
                data.dtype = dtype

            return data

        # d does have a Data interface
        data = data()
        if copy:
            data = data.copy()
            if dtype is not None and np.dtype(dtype) != data.dtype:
                data.dtype = dtype
        elif dtype is not None and np.dtype(dtype) != data.dtype:
            data = data.copy()
            data.dtype = dtype

        return data

    def atol(self, *value):
        """TODODASK Return the current value of the `cfdm.atol` function.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `rtol`

        """
        if value:
            value = value[0]
            if value is not None:
                return float(value)

        return _atol().value

    def compute(self):  # noqa: F811
        """A view of the computed data.

        In-place changes to the returned array *might* affect the
        underlying dask array, depending on how the dask array has
        been defined, including any delayed operations.

        The returned array has the same mask hardness and fill values
        as the data.

        Compare with `array`.

        **Performance**

        `compute` causes all delayed operations to be computed.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `persist`, `array`, `datetime_array`

        :Returns:

                An in-memory view of the data

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3.0], 'km')
        >>> d.compute()
        array([1., 2., 3.])

        >>> from scipy.sparse import csr_array
        >>> d = {{package}}.{{class}}(csr_array((2, 3)))
        >>> d.compute()
        <2x3 sparse array of type '<class 'numpy.float64'>'
                with 0 stored elements in Compressed Sparse Row format>
        >>>: d.array
        array([[0., 0., 0.],
               [0., 0., 0.]])
        >>> d.compute().toarray()
        array([[0., 0., 0.],
               [0., 0., 0.]])

        """
        a = self.to_dask_array().compute()

        if np.ma.isMA(a):
            if self.hardmask:
                a.harden_mask()
            else:
                a.soften_mask()

            a.set_fill_value(self.fill_value)

        return a

    def copy(self, array=True):
        """Return a deep copy.

        ``d.copy()`` is equivalent to ``copy.deepcopy(d)``.

        :Parameters:

            array: `bool`, optional
                If True (the default) then copy the array, else it
                is not copied.

        :Returns:

            `{{class}}`
                The deep copy.

        **Examples**

        >>> e = d.copy()
        >>> e = d.copy(array=False)

        """
        return super().copy(array=array)

    def creation_commands(
        self, name="data", namespace=None, indent=0, string=True
    ):
        """Return the commands that would create the data object.

        .. versionadded:: (cfdm) 1.8.7.0

        :Parameters:

            name: `str` or `None`, optional
                Set the variable name of `Data` object that the commands
                create.

            {{namespace: `str`, optional}}

            {{indent: `int`, optional}}

            {{string: `bool`, optional}}

        :Returns:

            {{returns creation_commands}}

        **Examples**

        >>> d = {{package}}.{{class}}([[0.0, 45.0], [45.0, 90.0]],
        ...                           units='degrees_east')
        >>> print(d.creation_commands())
        data = {{package}}.{{class}}([[0.0, 45.0], [45.0, 90.0]], units='degrees_east', dtype='f8')

        >>> d = {{package}}.{{class}}(['alpha', 'beta', 'gamma', 'delta'],
        ...                           mask = [1, 0, 0, 0])
        >>> d.creation_commands(name='d', namespace='', string=False)
        ["d = Data(['', 'beta', 'gamma', 'delta'], dtype='U5', mask=Data([True, False, False, False], dtype='b1'))"]

        """
        namespace0 = namespace
        if namespace is None:
            namespace = self._package() + "."
        elif namespace and not namespace.endswith("."):
            namespace += "."

        mask = self.mask
        if mask.any():
            if name == "mask":
                raise ValueError(
                    "When the data is masked, the 'name' parameter "
                    "can not have the value 'mask'"
                )
            masked = True
            array = self.filled().array.tolist()
        else:
            masked = False
            array = self.array.tolist()

        units = self.get_units(None)
        if units is None:
            units = ""
        else:
            units = f", units={units!r}"

        calendar = self.get_calendar(None)
        if calendar is None:
            calendar = ""
        else:
            calendar = f", calendar={calendar!r}"

        fill_value = self.get_fill_value(None)
        if fill_value is None:
            fill_value = ""
        else:
            fill_value = f", fill_value={fill_value}"

        dtype = self.dtype.descr[0][1][1:]

        if masked:
            mask = mask.creation_commands(
                name="mask", namespace=namespace0, indent=0, string=True
            )
            mask = mask.replace("mask = ", "mask=", 1)
            mask = f", {mask}"
        else:
            mask = ""

        if name is None:
            name = ""
        else:
            name = name + " = "

        out = []
        out.append(
            f"{name}{namespace}{self.__class__.__name__}({array}{units}"
            f"{calendar}, dtype={dtype!r}{mask}{fill_value})"
        )

        if string:
            indent = " " * indent
            out[0] = indent + out[0]
            out = ("\n" + indent).join(out)

        return out

    @_inplace_enabled(default=False)
    def filled(self, fill_value=None, inplace=False):
        """Replace masked elements with a fill value.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            fill_value: scalar, optional
                The fill value. By default the fill returned by
                `get_fill_value` is used, or if this is not set then the
                netCDF default fill value for the data type is used (as
                defined by `netCDF.fillvals`).

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The filled data, or `None` if the operation was
                in-place.

        **Examples**

        >>> d = {{package}}.{{class}}([[1, 2, 3]])
        >>> print(d.filled().array)
        [[1 2 3]]
        >>> d[0, 0] = {{package}}.masked
        >>> print(d.filled().array)
        [-9223372036854775806                    2                    3]
        >>> d.set_fill_value(-99)
        >>> print(d.filled().array)
        [[-99   2   3]]

        """
        d = _inplace_enabled_define_and_cleanup(self)

        if fill_value is None:
            fill_value = d.get_fill_value(None)
            if fill_value is None:  # still...
                fill_value = default_netCDF_fillvals().get(d.dtype.str[1:])
                if fill_value is None and d.dtype.kind in ("SU"):
                    fill_value = default_netCDF_fillvals().get("S1", None)

                if fill_value is None:
                    raise ValueError(
                        "Can't determine fill value for "
                        f"data type {d.dtype.str!r}"
                    )

        dx = d.to_dask_array()
        dx = dx.map_blocks(np.ma.filled, fill_value=fill_value, dtype=d.dtype)
        d._set_dask(dx)

        return d

    @_inplace_enabled(default=False)
    def insert_dimension(self, position=0, inplace=False):
        """Expand the shape of the data array.

        Inserts a new size 1 axis, corresponding to a given position in
        the data array shape.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `flatten`, `squeeze`, `transpose`

        :Parameters:

            position: `int`, optional
                Specify the position that the new axis will have in the
                data array. By default the new axis has position 0, the
                slowest varying position. Negative integers counting from
                the last position are allowed.

                *Parameter example:*
                  ``position=2``

                *Parameter example:*
                  ``position=-1``

            inplace: `bool`, optional
                If True then do the operation in-place and return `None`.

        :Returns:

            `{{class}}` or `None`
                The data with expanded axes. If the operation was in-place
                then `None` is returned.

        **Examples**

        >>> d.shape
        (19, 73, 96)
        >>> d.insert_dimension('domainaxis3').shape
        (1, 96, 73, 19)
        >>> d.insert_dimension('domainaxis3', position=3).shape
        (19, 73, 96, 1)
        >>> d.insert_dimension('domainaxis3', position=-1, inplace=True)
        >>> d.shape
        (19, 73, 1, 96)

        """
        d = _inplace_enabled_define_and_cleanup(self)

        # Parse position
        ndim = d.ndim
        if -ndim - 1 <= position < 0:
            position += ndim + 1
        elif not 0 <= position <= ndim:
            raise ValueError(
                f"Can't insert dimension: Invalid position: {position!r}"
            )

        shape = list(d.shape)
        shape.insert(position, 1)

        dx = d.to_dask_array()
        dx = dx.reshape(shape)

        # Inserting a dimension doesn't affect the cached elements nor
        # the CFA write status
        d._set_dask(dx, clear=_ALL ^ _CACHE ^ _CFA)

        # Delete hdf5 chunksizes
        d.nc_clear_hdf5_chunksizes()

        return d

    def get_count(self, default=ValueError()):
        """Return the count variable for a compressed array.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `get_index`, `get_list`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if a count
                variable has not been set. If set to an `Exception`
                instance then it will be raised instead.

        :Returns:

                The count variable.

        **Examples**

        >>> c = d.get_count()

        """
        try:
            return self._get_Array().get_count()
        except (AttributeError, ValueError):
            return self._default(
                default, f"{self.__class__.__name__!r} has no count variable"
            )

    def get_data(self, default=ValueError(), _units=None, _fill_value=None):
        """Returns the data.

        .. versionadded:: (cfdm) 1.10.1.0

        :Parameters:

            default: optional
                Ignored.

        :Returns:

            `Data`

        """
        return self

    def get_dependent_tie_points(self, default=ValueError()):
        """Return the list variable for a compressed array.

        .. versionadded:: (cfdm) 1.10.0.1

        .. seealso:: `get_tie_point_indices`,
                     `get_interpolation_parameters`, `get_index`,
                     `get_list`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if no
                dependent tie point index variables have been set. If
                set to an `Exception` instance then it will be raised
                instead.

        :Returns:

            `dict`
                The dependent tie point arrays needed by the
                interpolation method, keyed by the dependent tie point
                identities. Each key is a dependent tie point
                identity, whose value is a `Data` variable.

        **Examples**

        >>> l = d.get_dependent_tie_points()

        """
        try:
            return self._get_Array().get_dependent_tie_points()
        except (AttributeError, ValueError):
            return self._default(
                default,
                f"{self.__class__.__name__!r} has no dependent "
                "tie point index variables",
            )

    def get_index(self, default=ValueError()):
        """Return the index variable for a compressed array.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `get_count`, `get_list`

        :Parameters:

            default: optional
                Return *default* if index variable has not been set.

            default: optional
                Return the value of the *default* parameter if an index
                variable has not been set. If set to an `Exception`
                instance then it will be raised instead.

        :Returns:

                The index variable.

        **Examples**

        >>> i = d.get_index()

        """
        try:
            return self._get_Array().get_index()
        except (AttributeError, ValueError):
            return self._default(
                default, f"{self.__class__.__name__!r} has no index variable"
            )

    def get_interpolation_parameters(self, default=ValueError()):
        """Return the list variable for a compressed array.

        .. versionadded:: (cfdm) 1.10.0.1

        .. seealso:: `get_dependent_tie_points`,
                     `get_tie_point_indices`, `get_index`,
                     `get_list`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if no
                interpolation parameters have been set. If set to an
                `Exception` instance then it will be raised instead.

        :Returns:

            `dict`
                Interpolation parameters required by the subsampling
                interpolation method. Each key is an interpolation
                parameter term name, whose value is an
                `InterpolationParameter` variable.

                Interpolation parameter term names for the
                standardised interpolation methods are defined in CF
                Appendix J "Coordinate Interpolation Methods".

        **Examples**

        >>> l = d.get_interpolation_parameters()

        """
        try:
            return self._get_Array().get_interpolation_parameters()
        except (AttributeError, ValueError):
            return self._default(
                default,
                f"{self.__class__.__name__!r} has no subsampling "
                "interpolation parameters",
            )

    def get_list(self, default=ValueError()):
        """Return the list variable for a compressed array.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `get_count`, `get_index`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if an index
                variable has not been set. If set to an `Exception`
                instance then it will be raised instead.

        :Returns:

                The list variable.

        **Examples**

        >>> l = d.get_list()

        """
        try:
            return self._get_Array().get_list()
        except (AttributeError, ValueError):
            return self._default(
                default, f"{self.__class__.__name__!r} has no list variable"
            )

    def get_tie_point_indices(self, default=ValueError()):
        """Return the list variable for a compressed array.

        .. versionadded:: (cfdm) 1.10.0.1

        .. seealso:: `get_dependent_tie_points`,
                     `get_interpolation_parameters`,
                     `get_index`, `get_list`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if no tie
                point index variables have been set. If set to an
                `Exception` instance then it will be raised instead.

        :Returns:

            `dict`
                The tie point index variable for each subsampled
                dimension. A key identifies a subsampled dimension by
                its integer position in the compressed array, and its
                value is a `TiePointIndex` variable.

        **Examples**

        >>> l = d.get_tie_point_indices()

        """
        try:
            return self._get_Array().get_tie_point_indices()
        except (AttributeError, ValueError):
            return self._default(
                default,
                f"{self.__class__.__name__!r} has no "
                "tie point index variables",
            )

    def get_compressed_dimension(self, default=ValueError()):
        """Returns the compressed dimension's array position.

        That is, returns the position of the compressed dimension
        in the compressed array.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `compressed_array`, `get_compressed_axes`,
                     `get_compression_type`

        :Parameters:

            default: optional
                Return the value of the *default* parameter there is no
                compressed dimension. If set to an `Exception` instance
                then it will be raised instead.

        :Returns:

            `int`
                The position of the compressed dimension in the compressed
                array.

        **Examples**

        >>> d.get_compressed_dimension()
        2

        """
        try:
            return self._get_Array().get_compressed_dimension()
        except (AttributeError, ValueError):
            return self._default(
                default,
                f"{self.__class__.__name__!r} has no compressed dimension",
            )

    def harden_mask(self):
        """Force the mask to hard.

        Whether the mask of a masked array is hard or soft is
        determined by its `hardmask` property. `harden_mask` sets
        `hardmask` to `True`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `hardmask`, `soften_mask`

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3], hardmask=False)
        >>> d.hardmask
        False
        >>> d.harden_mask()
        >>> d.hardmask
        True

        >>> d = {{package}}.{{class}([1, 2, 3], mask=[False, True, False])
        >>> d.hardmask
        True
        >>> d[1] = 999
        >>> print(d.array)
        [1 -- 3]

        """
        dx = self.to_dask_array()
        dx = dx.map_blocks(cf_harden_mask, dtype=self.dtype)
        self._set_dask(dx, clear=_NONE)
        self.hardmask = True

    def _parse_indices(self, indices):
        """Parse indices of the data and return valid indices in a list.

        :Parameters:

            indices: `tuple` (not a `list`!)

        :Returns:

            `list`

        **Examples**


        >>> d = {{package}}.{{class}}(np.arange(100, 190).reshape(1, 10, 9))
        >>> d._parse_indices((slice(None, None, None), 1, 2))
        [slice(None, None, None), slice(1, 2, 1), slice(2, 3, 1)]
        >>> d._parse_indices((1,))
        [slice(1, 2, 1), slice(None, None, None), slice(None, None, None)]

        """
        shape = self.shape

        parsed_indices = []

        if not isinstance(indices, tuple):
            indices = (indices,)

        # Initialise the list of parsed indices as the input indices
        # with any Ellipsis objects expanded
        length = len(indices)
        n = len(shape)
        ndim = n
        for index in indices:
            if index is Ellipsis:
                m = n - length + 1
                parsed_indices.extend([slice(None)] * m)
                n -= m
            else:
                parsed_indices.append(index)
                n -= 1

            length -= 1

        len_parsed_indices = len(parsed_indices)

        if ndim and len_parsed_indices > ndim:
            raise IndexError(
                f"Invalid indices for data with shape {shape}: "
                f"{parsed_indices}"
            )

        if len_parsed_indices < ndim:
            parsed_indices.extend([slice(None)] * (ndim - len_parsed_indices))

        if not ndim and parsed_indices:
            raise IndexError(
                "Scalar data can only be indexed with () or Ellipsis"
            )

        for i, (index, size) in enumerate(zip(parsed_indices, shape)):
            if isinstance(index, slice):
                continue

            if isinstance(index, int):
                # E.g. 43 -> slice(43, 44, 1)
                if index < 0:
                    index += size

                index = slice(index, index + 1, 1)
            else:
                if getattr(getattr(index, "dtype", None), "kind", None) == "b":
                    # E.g. index is [True, False, True] -> [0, 2]
                    #
                    # Convert Booleans to non-negative integers. We're
                    # assuming that anything with a dtype attribute also
                    # has a size attribute.
                    if index.size != size:
                        raise IndexError(
                            "Invalid indices for data "
                            f"with shape {shape}: {parsed_indices}"
                        )

                    index = np.where(index)[0]

                if not np.ndim(index):
                    if index < 0:
                        index += size

                    index = slice(index, index + 1, 1)
                else:
                    len_index = len(index)
                    if len_index == 1:
                        # E.g. [3] -> slice(3, 4, 1)
                        index = index[0]
                        if index < 0:
                            index += size

                        index = slice(index, index + 1, 1)
                    else:
                        # E.g. [1, 3, 4] -> [1, 3, 4]
                        pass

            parsed_indices[i] = index

        return parsed_indices

    def maximum(self, axes=None, squeeze=False,
        split_every=None):
        """Return the maximum of an array or the maximum along axes.

        Missing data array elements are omitted from the calculation.

        .. versionadded:: (cfdm) 1.8.0

        .. seealso:: `minimum`

        :Parameters:

            axes: (sequence of) `int`, optional
                The axes over which to take the maximum. By default the
                maximum over all axes is returned.

                {{axes int examples}}

            squeeze: `bool`, optional
                If this is set to False, the default, the axes which
                are reduced are left in the result as dimensions with
                size one. With this option, the result will broadcast
                correctly against the original data.

                .. versionadded:: (cfdm) NEXTVERSION

            {{split_every: `int` or `dict`, optional}}

                .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `{{class}}`
                Maximum of the data along the specified axes.

        **Examples**


        >>> d = {{package}}.{{class}}(np.arange(24).reshape(1, 2, 3, 4))
        >>> d
        <{{repr}}Data(1, 2, 3, 4): [[[[0, ..., 23]]]]>
        >>> print(d.array)
        [[[[ 0  1  2  3]
           [ 4  5  6  7]
           [ 8  9 10 11]]
          [[12 13 14 15]
           [16 17 18 19]
           [20 21 22 23]]]]
        >>> e = d.max()
        >>> e
        <{{repr}}Data(1, 1, 1, 1): [[[[23]]]]>
        >>> print(e.array)
        [[[[23]]]]
        >>> e = d.max(2)
        >>> e
        <{{repr}}Data(1, 2, 1, 4): [[[[8, ..., 23]]]]>
        >>> print(e.array)
        [[[[ 8  9 10 11]]
          [[20 21 22 23]]]]
        >>> e = d.max([-2, -1])
        >>> e
        <{{repr}}Data(1, 2, 1, 1): [[[[11, 23]]]]>
        >>> print(e.array)
        [[[[11]]
          [[23]]]]

        """
        # Parse the axes. By default flattened input is used.
        try:
            axes = self._parse_axes(axes)
        except ValueError as error:
            raise ValueError(f"Can't find maximum of data: {error}")

        d = self.copy(array=False)
        dx = self.to_dask_array()
        dx = da.max(dx, axis=axes, keepdims=not squeeze,
                    split_every=split_every)
        d._set_dask(dx)

        # TODODASK
        if d.shape != self.shape:
            # Delete hdf5 chunksizes
            d.nc_clear_hdf5_chunksizes()

        return d

    def minimum(self, axes=None, split_every=None):
        """Return the minimum of an array or minimum along axes.

        Missing data array elements are omitted from the calculation.

        .. versionadded:: (cfdm) 1.8.0

        .. seealso:: `maximum`

        :Parameters:

            axes: (sequence of) `int`, optional
                The axes over which to take the minimum. By default the
                minimum over all axes is returned.

                {{axes int examples}}

            {{split_every: `int` or `dict`, optional}}

                .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `{{class}}`
                Minimum of the data along the specified axes.

        **Examples**


        >>> d = {{package}}.{{class}}(np.arange(24).reshape(1, 2, 3, 4))
        >>> d
        <{{repr}}Data(1, 2, 3, 4): [[[[0, ..., 23]]]]>
        >>> print(d.array)
        [[[[ 0  1  2  3]
           [ 4  5  6  7]
           [ 8  9 10 11]]
          [[12 13 14 15]
           [16 17 18 19]
           [20 21 22 23]]]]
        >>> e = d.min()
        >>> e
        <{{repr}}Data(1, 1, 1, 1): [[[[0]]]]>
        >>> print(e.array)
        [[[[0]]]]
        >>> e = d.min(2)
        >>> e
        <{{repr}}Data(1, 2, 1, 4): [[[[0, ..., 15]]]]>
        >>> print(e.array)
        [[[[ 0  1  2  3]]
          [[12 13 14 15]]]]
        >>> e = d.min([-2, -1])
        >>> e
        <{{repr}}Data(1, 2, 1, 1): [[[[0, 12]]]]>
        >>> print(e.array)
        [[[[ 0]]
          [[12]]]]

        """
        # Parse the axes. By default flattened input is used.
        try:
            axes = self._parse_axes(axes)
        except ValueError as error:
            raise ValueError(f"Can't find minimum of data: {error}")

        d = self.copy(array=False)

        dx = self.to_dask_array()
        dx = da.min(dx, axis=axes, keepdims=True, split_every=split_every)
        d._set_dask(dx)

        # TODODASK
        if d.shape != self.shape:
            # Delete hdf5 chunksizes
            d.nc_clear_hdf5_chunksizes()

        return d

    def rtol(self, *value):
        """TODODASK Return the current value of the `cfdm.atol` function.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `atol`

        """
        if value:
            value = value[0]
            if value is not None:
                return float(value)

        return _rtol().value

    def soften_mask(self):
        """Force the mask to soft.

        Whether the mask of a masked array is hard or soft is
        determined by its `hardmask` property. `soften_mask` sets
        `hardmask` to `False`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `hardmask`, `harden_mask`

        **Examples**

        >>> d = {{package}}.{{class}([1, 2, 3])
        >>> d.hardmask
        True
        >>> d.soften_mask()
        >>> d.hardmask
        False

        >>> d = {{package}}.{{class}}([1, 2, 3], mask=[False, True, False], hardmask=False)
        >>> d.hardmask
        False
        >>> d[1] = 999
        >>> print(d.array)
        [  1 999   3]

        """
        dx = self.to_dask_array()
        dx = dx.map_blocks(cf_soften_mask, dtype=self.dtype)
        self._set_dask(dx, clear=_NONE)
        self.hardmask = False

    @_inplace_enabled(default=False)
    def squeeze(self, axes=None, inplace=False):
        """Remove size 1 axes from the data.

        By default all size 1 axes are removed, but particular axes may be
        selected with the keyword arguments.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `flatten`, `insert_dimension`, `transpose`

        :Parameters:

            axes: (sequence of) `int`, optional
                The positions of the size one axes to be removed. By
                default all size one axes are removed.

                {{axes int examples}}

            inplace: `bool`, optional
                If True then do the operation in-place and return `None`.

        :Returns:

            `Data` or `None`
                The data with removed data axes. If the operation was
                in-place then `None` is returned.

        **Examples**

        >>> d.shape
        (1, 73, 1, 96)
        >>> f.squeeze().shape
        (73, 96)
        >>> d.squeeze(0).shape
        (73, 1, 96)
        >>> d.squeeze([-3, 2]).shape
        (73, 96)
        >>> d.squeeze(2, inplace=True)
        >>> d.shape
        (1, 73, 96)

        """
        d = _inplace_enabled_define_and_cleanup(self)

        try:
            axes = d._parse_axes(axes)
        except ValueError as error:
            raise ValueError(f"Can't squeeze data: {error}")

        shape = d.shape

        if axes is None:
            axes = tuple([i for i, n in enumerate(shape) if n == 1])
        else:
            # Check the squeeze axes
            for i in axes:
                if shape[i] > 1:
                    raise ValueError(
                        "Can't squeeze data: "
                        f"Can't remove axis of size {shape[i]}"
                    )

        if not axes:
            return d

        # Still here? Then the data array is not scalar and at least
        # one size 1 axis needs squeezing.
        dx = d.to_dask_array()
        dx = dx.squeeze(axis=iaxes)

        # Squeezing a dimension doesn't affect the cached elements
        d._set_dask(dx, clear=_ALL ^ _CACHE)

        # Delete hdf5 chunksizes
        d.nc_clear_hdf5_chunksizes()

        return d

    def sum(self, axes=None, squeeze=False):
        """Return the sum of an array or the sum along axes.

        Missing data array elements are omitted from the calculation.

        .. seealso:: `max`, `min`

        :Parameters:

            axes: (sequence of) `int`, optional
                The axes over which to calculate the sum. By default the
                sum over all axes is returned.

                {{axes int examples}}

            squeeze: `bool`, optional
                If this is set to False, the default, the axes which
                are reduced are left in the result as dimensions with
                size one. With this option, the result will broadcast
                correctly against the original data.

                .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `{{class}}`
                The sum of the data along the specified axes.

        **Examples**

        >>> a = np.ma.arange(12).reshape(4, 3)
        >>> d = {{package}}.{{class}}(a, 'K')
        >>> d[1, 1] = {{package}}.masked
        >>> print(d.array)
        [[0 1 2]
         [3 -- 5]
         [6 7 8]
         [9 10 11]]
        >>> d.sum()
        <{{repr}}Data(1, 1): [[62]] K>

        """
        # Parse the axes. By default flattened input is used.
        try:
            axes = self._parse_axes(axes)
        except ValueError as error:
            raise ValueError(f"Can't sum data: {error}")

        d = self.copy(array=False)

        dx = self.to_dask_array()
        dx = da.sum(dx, axis=axes, keepdims=not squeeze, split_every=split_every)
        d._set_dask(dx)

        # TODODASK
        if d.shape != self.shape:
            # Delete hdf5 chunksizes
            d.nc_clear_hdf5_chunksizes()

        return d

    @_inplace_enabled(default=False)
    def transpose(self, axes=None, inplace=False):
        """Permute the axes of the data array.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `flatten`, `insert_dimension`, `squeeze`

        :Parameters:

            axes: (sequence of) `int`
                The new axis order. By default the order is reversed.

                {{axes int examples}}

            inplace: `bool`, optional
                If True then do the operation in-place and return `None`.

        :Returns:

            `{{class}}` or `None`
                The data with permuted data axes. If the operation was
                in-place then `None` is returned.

        **Examples**

        >>> d.shape
        (19, 73, 96)
        >>> d.transpose().shape
        (96, 73, 19)
        >>> d.transpose([1, 0, 2]).shape
        (73, 19, 96)
        >>> d.transpose([-1, 0, 1], inplace=True)
        >>> d.shape
        (96, 19, 73)

        """
        d = _inplace_enabled_define_and_cleanup(self)

        ndim = d.ndim
        if axes is None:
            iaxes = tuple(range(ndim - 1, -1, -1))
        else:
            iaxes = d._parse_axes(axes)

        if iaxes == tuple(range(ndim)):
            # Short circuit if the transpose is a null operation
            return d

        dx = d.to_dask_array()
        try:
            dx = da.transpose(dx, axes=axes)
        except ValueError:
            raise ValueError(
                f"Can't transpose: Axes don't match array: {axes}"
            )

        d._set_dask(dx)

        # Delete hdf5 chunksizes
        d.nc_clear_hdf5_chunksizes()

        return d

    def get_compressed_axes(self):
        """Returns the dimensions that are compressed in the array.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `compressed_array`, `get_compressed_dimension`,
                     `get_compression_type`

        :Returns:

            `list`
                The dimensions of the data that are compressed to a single
                dimension in the underlying array. If the data are not
                compressed then an empty list is returned.

        **Examples**

        >>> d.shape
        (2, 3, 4, 5, 6)
        >>> d.compressed_array.shape
        (2, 14, 6)
        >>> d.get_compressed_axes()
        [1, 2, 3]

        >>> d.get_compression_type()
        ''
        >>> d.get_compressed_axes()
        []

        """
        ca = self._get_Array(None)

        if ca is None:
            return []

        return ca.get_compressed_axes()

    def get_compression_type(self):
        """Returns the type of compression applied to the array.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `compressed_array`, `compression_axes`,
                     `get_compressed_dimension`

        :Returns:

            `str`
                The compression type. An empty string means that no
                compression has been applied.

        **Examples**

        >>> d.get_compression_type()
        ''

        >>> d.get_compression_type()
        'gathered'

        >>> d.get_compression_type()
        'ragged contiguous'

        """
        ma = self._get_Array(None)
        if ma is None:
            return ""

        return ma.get_compression_type()

    @classmethod
    def empty(
        cls,
        shape,
        dtype=None,
        units=None,
        calendar=None,
        chunks=_DEFAULT_CHUNKS,
    ):
        """Return a new array of given shape and type, without
        initialising entries.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            shape: `int` or `tuple` of `int`
                The shape of the new array. e.g. ``(2, 3)`` or ``2``.

            dtype: data-type
                The desired output data-type for the array, e.g.
                `numpy.int8`. The default is `numpy.float64`.

            units: `str` or `Units`
                The units for the new data array.

            calendar: `str`, optional
                The calendar for reference time units.

            {{chunks: `int`, `tuple`, `dict` or `str`, optional}}

        :Returns:

            `Data`
                Array of uninitialised (arbitrary) data of the given
                shape and dtype.

        **Examples**

        >>> d = {{package}}.{{class}}.empty((2, 2))
        >>> print(d.array)
        [[ -9.74499359e+001  6.69583040e-309],
         [  2.13182611e-314  3.06959433e-309]]         #uninitialised

        >>> d = {{package}}.{{class}}.empty((2,), dtype=bool)
        >>> print(d.array)
        [ False  True]                                 #uninitialised

        """
        dx = da.empty(shape, dtype=dtype, chunks=chunks)
        return cls(dx, units=units, calendar=calendar)

    @_manage_log_level_via_verbosity
    def equals(
        self,
        other,
        rtol=None,
        atol=None,
        verbose=None,
        ignore_data_type=False,
        ignore_fill_value=False,
        ignore_compression=True,
        ignore_type=False,
        _check_values=True,
    ):
        """Whether two data arrays are the same.

        Equality is strict by default. This means that for data arrays to
        be considered equal:

        * the units and calendar must be the same,

        ..

        * the fill value must be the same (see the *ignore_fill_value*
          parameter), and

        ..

        * the arrays must have same shape and data type, the same missing
          data mask, and be element-wise equal (see the *ignore_data_type*
          parameter).

        {{equals tolerance}}

        Any compression is ignored by default, with only the arrays in
        their uncompressed forms being compared. See the
        *ignore_compression* parameter.

        Any type of object may be tested but, in general, equality is only
        possible with another cell measure construct, or a subclass of
        one. See the *ignore_type* parameter.

        .. versionadded:: (cfdm) 1.7.0

        :Parameters:

            other:
                The object to compare for equality.

            {{atol: number, optional}}

            {{rtol: number, optional}}

            ignore_fill_value: `bool`, optional
                If True then the fill value is omitted from the
                comparison.

            {{ignore_data_type: `bool`, optional}}

            {{ignore_compression: `bool`, optional}}

            {{ignore_type: `bool`, optional}}

            {{verbose: `int` or `str` or `None`, optional}}

        :Returns:

            `bool`
                Whether the two data arrays are equal.

        **Examples**

        >>> d.equals(d)
        True
        >>> d.equals(d.copy())
        True
        >>> d.equals('not a data array')
        False

        """
        # CF-PYTHON: needs to check non-data items before calling
        #            super().equals with _check_values=True, and to
        #            not check its own values
        
        pp = super()._equals_preprocess(
            other, verbose=verbose, ignore_type=ignore_type
        )

        if pp is True or pp is False:
            return pp

        other = pp

        # Check that each instance has the same shape
        if self.shape != other.shape:
            logger.info(
                f"{self.__class__.__name__}: Different shapes: "
                f"{self.shape} != {other.shape}"
            )  # pragma: no cover
            return False

        # Check that each instance has the same fill value
        if not ignore_fill_value and self.get_fill_value(
            None
        ) != other.get_fill_value(None):
            logger.info(
                f"{self.__class__.__name__}: Different fill value: "
                f"{self.get_fill_value(None)} != {other.get_fill_value(None)}"
            )  # pragma: no cover
            return False

        # Check that each instance has the same data type
        if not ignore_data_type and self.dtype != other.dtype:
            logger.info(
                f"{self.__class__.__name__}: Different data types: "
                f"{self.dtype} != {other.dtype}"
            )  # pragma: no cover
            return False

        # Return now if we have been asked to not check the array
        # values
        if not _check_values:
            return True

        # Check that each instance has the same units
        for attr in ("units", "calendar"):
            x = getattr(self, "get_" + attr)(None)
            y = getattr(other, "get_" + attr)(None)
            if x != y:
                logger.info(
                    f"{self.__class__.__name__}: Different {attr}: "
                    f"{x!r} != {y!r}"
                )  # pragma: no cover
                return False

        if not ignore_compression:
            # --------------------------------------------------------
            # Check for equal compression types
            # --------------------------------------------------------
            compression_type = self.get_compression_type()
            if compression_type != other.get_compression_type():
                logger.info(
                    f"{self.__class__.__name__}: Different compression types: "
                    f"{compression_type} != {other.get_compression_type()}"
                )  # pragma: no cover

                return False

            ## --------------------------------------------------------
            ## Check for equal compressed array values
            ## --------------------------------------------------------
            #if compression_type:
            #    if not self._equals(
            #        self.compressed_array,
            #        other.compressed_array,
            #        rtol=rtol,
            #        atol=atol,
            #    ):
            #        logger.info(
            #            f"{self.__class__.__name__}: Different compressed "
            #            "array values"
            #        )  # pragma: no cover
            #        return False

        # ------------------------------------------------------------
        # Check for equal (uncompressed) array values
        # ------------------------------------------------------------
        #if not self._equals(
        #    self.array,
        #    other.array,
        #    ignore_data_type=ignore_data_type,
        #    rtol=rtol,
        #    atol=atol,
        #):
        #    logger.info(
        #        f"{self.__class__.__name__}: Different array values "
        #        f"(atol={atol}, rtol={rtol})"
        #    )  # pragma: no cover
        #
        #    return False

        # ------------------------------------------------------------
        # Check for equal (uncompressed) array values
        # ------------------------------------------------------------

        # Check that corresponding elements are equal within a
        # tolerance. We assume that all inputs are masked arrays. Note
        # we compare the data first as this may return False due to
        # different dtype without having to wait until the compute
        # call.
        rtol = self.rtol(rtol)
        atol = self.atol(atol)

        self_dx = self.to_dask_array()
        other_dx = other.to_dask_array()

        self_is_numeric = is_numeric_dtype(self_dx)
        other_is_numeric = is_numeric_dtype(other_dx)
        if self_is_numeric and other_is_numeric:
            data_comparison = _da_ma_allclose(
                self_dx,
                other_dx,
                masked_equal=True,
                rtol=rtol,
                atol=atol,
            )
        elif not self_is_numeric and not other_is_numeric:
            # If the array (say d) is fully masked, then the output of
            # np.all(d == d) and therefore da.all(d == d) will be a
            # np.ma.masked object which has dtype('float64'), and not
            # a Boolean, causing issues later. To ensure data_comparison
            # is Boolean, we must do an early compute to check if it is
            # a masked object and if so, force the desired result (True).
            #
            # This early compute won't degrade performance because it
            # would be performed towards result.compute() below anyway.
            data_comparison = da.all(self_dx == other_dx).compute()
            if data_comparison is np.ma.masked:
                data_comparison = True

        else:  # one is numeric and other isn't => not equal (incompat. dtype)
            if is_log_level_info(logger):
                logger.info(
                    f"{self.__class__.__name__}: Different data types:"
                    f"{self_dx.dtype} != {other_dx.dtype}"
                )

            return False

        mask_comparison = da.all(
            da.equal(da.ma.getmaskarray(self_dx), da.ma.getmaskarray(other_dx))
        )

        # Apply a logical AND to confirm if both the mask and the data
        # are equal for the pair of masked arrays:
        result = da.logical_and(data_comparison, mask_comparison)

        if not result.compute():
            if is_log_level_info(logger):
                logger.info(
                    f"{self.__class__.__name__}: Different array values ("
                    f"atol={atol}, rtol={rtol})"
                )

            return False
     
        # ------------------------------------------------------------
        # Still here? Then the two data arrays are equal.
        # ------------------------------------------------------------
        return True

    def get_filenames(self):
        """Return the names of any files containing the data array.

        .. seealso:: `original_filenames`

        :Returns:

            `set`
                The file names in normalised, absolute form. If the
                data are all in memory then an empty `set` is
                returned.

        **Examples**

        >>> f = {{package}}.example_field(0)
        >>> {{package}}.write(f, 'temp_file.nc')
        >>> g = {{package}}.read('temp_file.nc')[0]
        >>> d = g.data
        >>> d.get_filenames()
        {'/data/user/temp_file.nc'}
        >>> d[...] = -99
        >>> d.get_filenames()
        set()

        """
        source = self.source(None)
        if source is None:
            return set()

        try:
            return set(source.get_filenames())
        except AttributeError:
            return set()

    def first_element(self):
        """Return the first element of the data as a scalar.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `last_element`, `second_element`

        :Returns:

                The first element of the data.

        **Examples**

        >>> d = {{package}}.{{class}}(9.0)
        >>> x = d.first_element()
        >>> print(x, type(x))
        9.0 <class 'float'>

        >>> d = {{package}}.{{class}}([[1, 2], [3, 4]])
        >>> x = d.first_element()
        >>> print(x, type(x))
        1 <class 'int'>
        >>> d[0, 0] = {{package}}.masked
        >>> y = d.first_element()
        >>> print(y, type(y))
        -- <class 'numpy.ma.core.MaskedConstant'>

        >>> d = {{package}}.{{class}}(['foo', 'bar'])
        >>> x = d.first_element()
        >>> print(x, type(x))
        foo <class 'str'>

        """
        # TODODASK: cached
        return self._item((slice(0, 1, 1),) * self.ndim)

    @_inplace_enabled(default=False)
    def flatten(self, axes=None, inplace=False):
        """Flatten axes of the data.

        Any subset of the axes may be flattened.

        The shape of the data may change, but the size will not.

        The flattening is executed in row-major (C-style) order. For
        example, the array ``[[1, 2], [3, 4]]`` would be flattened across
        both dimensions to ``[1 2 3 4]``.

        .. versionadded:: (cfdm) 1.7.11

        .. seealso:: `insert_dimension`, `squeeze`, `transpose`

        :Parameters:

            axes: (sequence of) `int`, optional
                Select the axes. By default all axes are flattened. No
                axes are flattened if *axes* is an empty sequence.

                {{axes int examples}}

            inplace: `bool`, optional
                If True then do the operation in-place and return `None`.

        :Returns:

            `{{class}}` or `None`
                The flattened data, or `None` if the operation was
                in-place.

        **Examples**

        >>> d = {{package}}.{{class}}(np.arange(24).reshape(1, 2, 3, 4))
        >>> d
        <{{repr}}Data(1, 2, 3, 4): [[[[0, ..., 23]]]]>
        >>> print(d.array)
        [[[[ 0  1  2  3]
           [ 4  5  6  7]
           [ 8  9 10 11]]
          [[12 13 14 15]
           [16 17 18 19]
           [20 21 22 23]]]]

        >>> e = d.flatten()
        >>> e
        <{{repr}}Data(24): [0, ..., 23]>
        >>> print(e.array)
        [ 0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18 19 20 21 22 23]

        >>> e = d.flatten([])
        >>> e
        <{{repr}}Data(1, 2, 3, 4): [[[[0, ..., 23]]]]>

        >>> e = d.flatten([1, 3])
        >>> e
        <{{repr}}Data(1, 8, 3): [[[0, ..., 23]]]>
        >>> print(e.array)
        [[[ 0  4  8]
          [ 1  5  9]
          [ 2  6 10]
          [ 3  7 11]
          [12 16 20]
          [13 17 21]
          [14 18 22]
          [15 19 23]]]

        >>> d.flatten([0, -1], inplace=True)
        >>> d
        <{{repr}}Data(4, 2, 3): [[[0, ..., 23]]]>
        >>> print(d.array)
        [[[ 0  4  8]
          [12 16 20]]
         [[ 1  5  9]
          [13 17 21]]
         [[ 2  6 10]
          [14 18 22]]
         [[ 3  7 11]
          [15 19 23]]]

        """
        d = _inplace_enabled_define_and_cleanup(self)

        ndim = d.ndim
        if not ndim:
            if axes or axes == 0:
                raise ValueError(
                    "Can't flatten: Can't remove axes from "
                    f"scalar {self.__class__.__name__}"
                )

            return d

        if axes is None:
            axes = list(range(ndim))
        else:
            axes = sorted(d._parse_axes(axes))

        n_axes = len(axes)
        if n_axes <= 1:
            return d

        dx = d.to_dask_array()

        # It is important that the first axis in the list is the
        # left-most flattened axis.
        #
        # E.g. if the shape is (10, 20, 30, 40, 50, 60) and the axes
        #      to be flattened are [2, 4], then the data must be
        #      transposed with order [0, 1, 2, 4, 3, 5]
        order = [i for i in range(ndim) if i not in axes]
        order[axes[0] : axes[0]] = axes
        dx = dx.transpose(order)

        # Find the flattened shape.
        #
        # E.g. if the *transposed* shape is (10, 20, 30, 50, 40, 60)
        #      and *transposed* axes [2, 3] are to be flattened then
        #      the new shape will be (10, 20, 1500, 40, 60)
        shape = d.shape
        new_shape = [n for i, n in enumerate(shape) if i not in axes]
        new_shape.insert(axes[0], reduce(mul, [shape[i] for i in axes], 1))

        dx = dx.reshape(new_shape)
        d._set_dask(dx)

        return d

    def last_element(self):
        """Return the last element of the data as a scalar.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `first_element`, `second_element`

        :Returns:

                The last element of the data.

        **Examples**

        >>> d = {{package}}.{{class}}(9.0)
        >>> x = d.last_element()
        >>> print(x, type(x))
        9.0 <class 'float'>

        >>> d = {{package}}.{{class}}([[1, 2], [3, 4]])
        >>> x = d.last_element()
        >>> print(x, type(x))
        4 <class 'int'>
        >>> d[-1, -1] = {{package}}.masked
        >>> y = d.last_element()
        >>> print(y, type(y))
        -- <class 'numpy.ma.core.MaskedConstant'>

        >>> d = {{package}}.{{class}}(['foo', 'bar'])
        >>> x = d.last_element()
        >>> print(x, type(x))
        bar <class 'str'>

        """
        # TODODASK: cached
        return self._item((slice(-1, None, 1),) * self.ndim)

    @_inplace_enabled(default=False)
    def masked_values(self, value, rtol=None, atol=None, inplace=False):
        """Mask using floating point equality.

        Masks the data where elements are approximately equal to the
        given value. For integer types, exact equality is used.

        .. versionadded:: (cfdm) 1.11.0.0

        .. seealso:: `mask`

        :Parameters:

            value: number
                Masking value.

            {{rtol: number, optional}}

            {{atol: number, optional}}

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The result of masking the data where approximately
                equal to *value*, or `None` if the operation was
                in-place.

        **Examples**

        >>> d = {{package}}.{{class}}([1, 1.1, 2, 1.1, 3])
        >>> e = d.masked_values(1.1)
        >>> print(e.array)
        [1.0 -- 2.0 -- 3.0]

        """
        d = _inplace_enabled_define_and_cleanup(self)

        dx = d.to_dask_array()
        dx = da.ma.masked_values(dx, value, rtol=self.rtol(rtol), atol=self.atol(atol))
        d._set_dask(dx)
        return d

    @_inplace_enabled(default=False)
    def override_units(self, units, inplace=False):
        """Override the data array units.

        Not to be confused with setting `set_units` TODODASK
        which are equivalent to the original units. This is different
        because in this case the new units need not be equivalent to the
        original ones and the data array elements will not be changed to
        reflect the new units.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `set_calendar`, `set_units`

        :Parameters:

            units: `str` or `None`
                The new units for the data array.

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The new data, or `None` if the operation was in-place.

        **Examples**

        >>> d = {{package}}.{{class}}(1012.0, 'hPa')
        >>> d.get_units()
        'hPa'
        >>> e = d.override_units('km')
        >>> e.get_units()
        'km'

        """
        d = _inplace_enabled_define_and_cleanup(self)
        if units is None:
            d.del_units(None)
        else:
            d.set_units(units)

        d.del_calendar(None)
        return d

    @_inplace_enabled(default=False)
    def persist(self, inplace=False):
        """Persist the underlying dask array into memory.

        This turns an underlying lazy dask array into a equivalent
        chunked dask array, but now with the results fully computed.

        `persist` is particularly useful when using distributed
        systems, because the results will be kept in distributed
        memory, rather than returned to the local process.

        Compare with `compute` and `array`.

        **Performance**

        `persist` causes all delayed operations to be computed.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `compute`, `array`, `datetime_array`,
                     `dask.array.Array.persist`

        :Parameters:

            {{inplace: `bool`, optional}}

        :Returns:

            `Data` or `None`
                The persisted data. If the operation was in-place then
                `None` is returned.

        **Examples**

        >>> e = d.persist()

        """
        d = _inplace_enabled_define_and_cleanup(self)

        dx = self.to_dask_array()
        dx = dx.persist()
        # TODODASK: cached
        d._set_dask(dx, clear=_ALL ^ _ARRAY ^ _CACHE)

        return d

    def second_element(self):
        """Return the second element of the data as a scalar.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `first_element`, `last_element`

        :Returns:

                The second element of the data.

        **Examples**

        >>> d = {{package}}.{{class}}([[1, 2], [3, 4]])
        >>> x = d.second_element()
        >>> print(x, type(x))
        2 <class 'int'>
        >>> d[0, 1] = {{package}}.masked
        >>> y = d.second_element()
        >>> print(y, type(y))
        -- <class 'numpy.ma.core.MaskedConstant'>

        >>> d = {{package}}.{{class}}(['foo', 'bar'])
        >>> x = d.second_element()
        >>> print(x, type(x))
        bar <class 'str'>

        """
        # TODODASK: cached
        return self._item(np.unravel_index(1, self.shape))

    def to_dask_array(self, apply_mask_hardness=False):
        """Convert the data to a `dask` array.

        .. warning:: By default, the mask hardness of the returned
                     dask array might not be the same as that
                     specified by the `hardmask` attribute.

                     This could cause problems if a subsequent
                     operation on the returned dask array involves the
                     un-masking of masked values (such as by indexed
                     assignment).

                     To guarantee that the mask hardness of the
                     returned dask array is correct, set the
                     *apply_mask_hardness* parameter to True.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            apply_mask_hardness: `bool`, optional
                If True then force the mask hardness of the returned
                array to be that given by the `hardmask` attribute.

        :Returns:

            `dask.array.Array`
                The dask array contained within the `Data` instance.

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3, 4], 'm')
        >>> dx = d.to_dask_array()
        >>> dx
        >>> dask.array<array, shape=(4,), dtype=int64, chunksize=(4,), chunktype=numpy.ndarray>
        >>> dask.array.asanyarray(d) is dx
        True

        >>> d.to_dask_array(apply_mask_hardness=True)
        dask.array<cf_harden_mask, shape=(4,), dtype=int64, chunksize=(4,), chunktype=numpy.ndarray>

        >>> d = {{package}}.{{class}}([1, 2, 3, 4], 'm', hardmask=False)
        >>> d.to_dask_array(apply_mask_hardness=True)
        dask.array<cf_soften_mask, shape=(4,), dtype=int64, chunksize=(4,), chunktype=numpy.ndarray>

        """
        out = self._get_component("dask", None)
        if out is None:
            raise ValueError(f"{self.__class__.__name__} object has no data")

        if apply_mask_hardness:
            if self.hardmask:
                self.harden_mask()
            else:
                self.soften_mask()

            out = self._get_component("dask")

        return out
    
    def to_dask_array(self, apply_mask_hardness=False, asanyarray=None):
        """Convert the data to a `dask` array.

        .. warning:: By default, the mask hardness of the returned
                     dask array might not be the same as that
                     specified by the `hardmask` attribute.

                     This could cause problems if a subsequent
                     operation on the returned dask array involves the
                     un-masking of masked values (such as by indexed
                     assignment).

                     To guarantee that the mask hardness of the
                     returned dask array is correct, set the
                     *apply_mask_hardness* parameter to True.

        .. versionadded:: 3.14.0

        :Parameters:

            apply_mask_hardness: `bool`, optional
                If True then force the mask hardness of the returned
                array to be that given by the `hardmask` attribute.

            {{asanyarray: `bool` or `None`, optional}}

                .. versionadded:: NEXTVERSION

        :Returns:

            `dask.array.Array`
                The dask array contained within the `Data` instance.

        **Examples**

        >>> d = cf.Data([1, 2, 3, 4], 'm')
        >>> dx = d.to_dask_array()
        >>> dx
        >>> dask.array<array, shape=(4,), dtype=int64, chunksize=(4,), chunktype=numpy.ndarray>
        >>> dask.array.asanyarray(d) is dx
        True

        >>> d.to_dask_array(apply_mask_hardness=True)
        dask.array<cf_harden_mask, shape=(4,), dtype=int64, chunksize=(4,), chunktype=numpy.ndarray>

        >>> d = cf.Data([1, 2, 3, 4], 'm', hardmask=False)
        >>> d.to_dask_array(apply_mask_hardness=True)
        dask.array<cf_soften_mask, shape=(4,), dtype=int64, chunksize=(4,), chunktype=numpy.ndarray>

        """
        dx = self._get_component("dask", None)
        if dx is None:
            raise ValueError(f"{self.__class__.__name__} object has no data")

        if apply_mask_hardness:
            if self.hardmask:
                self.harden_mask()
            else:
                self.soften_mask()

            dx = self._get_component("dask")
            # Note: The mask hardness functions have their own calls
            #       to 'cf_asanyarray', so we can don't need worry about
            #       setting another one.
        else:
            if asanyarray is None:
                asanyarray = self.__asanyarray__

            if asanyarray:
                # Add a new cf_asanyarray layer to the output graph
                dx = dx.map_blocks(cf_asanyarray, dtype=dx.dtype)

        return dx

    @_inplace_enabled(default=False)
    def to_memory(self, inplace=False):
        """Bring data on disk into memory.

        TODODASK

        There is no change to data that is already in memory.

        :Parameters:

            inplace: `bool`, optional
                If True then do the operation in-place and return `None`.

        :Returns:

            `{{class}}` or `None`
                A copy of the data in memory, or `None` if the
                operation was in-place.

        **Examples**

        >>> f = {{package}}.example_field(4)
        >>> f.data
        <{{repr}}Data(3, 26, 4): [[[290.0, ..., --]]] K>
        >>> f.data.to_memory()

        """
        raise NotImplementedError(
            "'Data.to_memory' is not available. "
            "Consider using 'Data.persist' instead."
        )

    @_inplace_enabled(default=False)
    def uncompress(self, inplace=False):
        """Uncompress the data.

        Only affects data that is compressed by convention, i.e.

          * Ragged arrays for discrete sampling geometries (DSG) and
            simple geometry cell definitions.

          * Compression by gathering.

          * Compression by coordinate subsampling.

        Data that is already uncompressed is returned
        unchanged. Whether the data is compressed or not does not
        alter its functionality nor external appearance, but may
        affect how the data are written to a dataset on disk.

        .. versionadded:: (cfdm) 1.7.3

        .. seealso:: `array`, `compressed_array`, `source`

        :Parameters:

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The uncompressed data, or `None` if the operation was
                in-place.

        **Examples**

        >>> d.get_compression_type()
        'ragged contiguous'
        >>> d.uncompress()
        >>> d.get_compression_type()
        ''

        """
        # CF-PYTHON: can inherit
        d = _inplace_enabled_define_and_cleanup(self)
        if d.get_compression_type():
            d._del_Array(None)

        return d

    def unique(self):
        """The unique elements of the data.

        Returns the sorted unique elements of the array.

        TODODASK: not that missing value might now in the returned array

        .. versionadded:: (cfdm) 1.7.0

        :Returns:

            `{{class}}`
                The unique values in a 1-d array.

        **Examples**

        >>> d = {{package}}.{{class}}([[4, 2, 1], [1, 2, 3]], 'metre')
        >>> print(d.array)
        [[4 2 1]
         [1 2 3]]
        >>> e = d.unique()
        >>> e
        <{{repr}}Data(4): [1, ..., 4] metre>
        >>> print(e.array)
        [1 2 3 4]
        >>> d[0, 0] = {{package}}.masked
        >>> print(d.array)
        [[-- 2 1]
         [1 2 3]]
        >>> e = d.unique()
        >>> print(e.array)
        [1 2 3 --]

        """
        d = self.copy(array=False)

        array = np.unique(self)
        dx = da.from_array(array)
        d._set_dask(dx)

        if d.shape != self.shape:
            d.nc_clear_hdf5_chunksizes()

        return d

    # ----------------------------------------------------------------
    # Aliases
    # ----------------------------------------------------------------
    def max(self, axes=None):
        """Alias for `maximum`."""
        return self.maximum(axes=axes)

    def min(self, axes=None):
        """Alias for `minimum`."""
        return self.minimum(axes=axes)
