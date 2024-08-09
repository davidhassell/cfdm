import logging
import math
import operator
from functools import reduce
from itertools import product, zip_longest
from math import prod
from numbers import Integral
from operator import mul
from os import sep

import dask.array as da
import numpy as np
from dask import compute  # noqa: F401
from dask.base import collections_to_dsk, is_dask_collection
from dask.optimization import cull
from netCDF4 import default_fillvals
from scipy.sparse import issparse

from cfdm import is_log_level_info

from .. import core
from ..constants import masked
from ..decorators import (
    _inplace_enabled,
    _inplace_enabled_define_and_cleanup,
    _manage_log_level_via_verbosity,
)
from ..functions import _numpy_allclose, abspath, parse_indices
from ..mixin.container import Container
from ..mixin.files import Files
from ..mixin.netcdf import NetCDFHDF5

# REVIEW: getitem: `data.py`: import asanyarray, filled
from .abstract import Array
from .config import (
    _ALL,
    _ARRAY,
    _CACHE,
    _CFA,
    _DEFAULT_CHUNKS,
    _DEFAULT_HARDMASK,
    _NONE,
    Units,
)
from .creation import to_dask
from .dask_utils import (
    cfdm_asanyarray,
    cfdm_where,
    filled,
    harden_mask,
    soften_mask,
)
from .utils import (
    allclose,
    convert_to_datetime,
    convert_to_reftime,
    first_non_missing_value,
    generate_axis_identifiers,
    is_numeric_dtype,
    new_axis_identifier,
)

logger = logging.getLogger(__name__)


class Data(Container, NetCDFHDF5, Files, core.Data):
    """An N-dimensional data array with units and masked values.

    * Contains an N-dimensional, indexable and broadcastable array with
      many similarities to a `numpy` array.

    * Contains the units of the array elements.

    * Supports masked arrays, regardless of whether or not it was
      initialised with a masked array.

    * Stores and operates on data arrays which are larger than the
      available memory.

    **Indexing**

    A data array is indexable in a similar way to numpy array:

    >>> d.shape
    (12, 19, 73, 96)
    >>> d[...].shape
    (12, 19, 73, 96)
    >>> d[slice(0, 9), 10:0:-2, :, :].shape
    (9, 5, 73, 96)

    There are three extensions to the numpy indexing functionality:

    * Size 1 dimensions are never removed by indexing.

      An integer index i takes the i-th element but does not reduce the
      rank of the output array by one:

      >>> d.shape
      (12, 19, 73, 96)
      >>> d[0, ...].shape
      (1, 19, 73, 96)
      >>> d[:, 3, slice(10, 0, -2), 95].shape
      (12, 1, 5, 1)

      Size 1 dimensions may be removed with the `squeeze` method.

    * The indices for each axis work independently.

      When more than one dimension's slice is a 1-d boolean sequence or
      1-d sequence of integers, then these indices work independently
      along each dimension (similar to the way vector subscripts work in
      Fortran), rather than by their elements:

      >>> d.shape
      (12, 19, 73, 96)
      >>> d[0, :, [0, 1], [0, 13, 27]].shape
      (1, 19, 2, 3)

    * Boolean indices may be any object which exposes the numpy array
      interface.

      >>> d.shape
      (12, 19, 73, 96)
      >>> d[..., d[0, 0, 0]>d[0, 0, 0].min()]

    """

    def __new__(cls, *args, **kwargs):
        """Store component classes."""
        instance = super().__new__(cls)
        instance._Units_class = Units
        return instance

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
    ):
        """**Initialisation**

        :Parameters:

            array: optional
                The array of values. May be a scalar or array-like
                object, including another `{{class}}` instance, anything
                with a `!to_dask_array` method, `numpy` array, `dask`
                array, `xarray` array, `cf.Array` subclass, `list`,
                `tuple`, scalar.

                *Parameter example:*
                  ``array=34.6``

                *Parameter example:*
                  ``array=[[1, 2], [3, 4]]``

                *Parameter example:*
                  ``array=numpy.ma.arange(10).reshape(2, 1, 5)``

            units: `str` or `Units`, optional
                The physical units of the data. if a `Units` object is
                provided then this an also set the calendar.

                The units (without the calendar) may also be set after
                initialisation with the `set_units` method.

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
                `None`, the `numpy` fill value appropriate to the array's
                data-type will be used (see
                `numpy.ma.default_fill_value`).

                The fill value may also be set after initialisation with
                the `set_fill_value` method.

                *Parameter example:*
                  ``fill_value=-999.``

            dtype: data-type, optional
                The desired data-type for the data. By default the
                data-type will be inferred form the *array*
                parameter.

                The data-type may also be set after initialisation with
                the `dtype` attribute.

                *Parameter example:*
                    ``dtype=float``

                *Parameter example:*
                    ``dtype='float32'``

                *Parameter example:*
                    ``dtype=numpy.dtype('i2')``

                .. versionadded:: 3.0.4

            mask: optional
                Apply this mask to the data given by the *array*
                parameter. By default, or if *mask* is `None`, no mask
                is applied. May be any scalar or array-like object
                (such as a `list`, `numpy` array or `{{class}}` instance)
                that is broadcastable to the shape of *array*. Masking
                will be carried out where the mask elements evaluate
                to `True`.

                This mask will applied in addition to any mask already
                defined by the *array* parameter.

            mask_value: scalar array_like
                Mask *array* where it is equal to *mask_value*, using
                numerically tolerant floating point equality.

                .. versionadded:: (cfdm) 1.11.0.0

            hardmask: `bool`, optional
                If False then the mask is soft. By default the mask is
                hard.

            dt: `bool`, optional
                If True then strings (such as ``'1990-12-01 12:00'``)
                given by the *array* parameter are re-interpreted as
                date-time objects. By default they are not.

            {{init source: optional}}

            {{init copy: `bool`, optional}}

            {{chunks: `int`, `tuple`, `dict` or `str`, optional}}

                .. versionadded:: (cfdm) NEXTVERSION

            to_memory: `bool`, optional
                If True then ensure that the original data are in
                memory, rather than on disk.

                If the original data are on disk, then reading data
                into memory during initialisation will slow down the
                initialisation process, but can considerably improve
                downstream performance by avoiding the need for
                independent reads for every dask chunk, each time the
                data are computed.

                In general, setting *to_memory* to True is not the same
                as calling the `persist` of the newly created `{{class}}`
                object, which also decompresses data compressed by
                convention and computes any data type, mask and
                date-time modifications.

                If the input *array* is a `dask.array.Array` object
                then *to_memory* is ignored.

                .. versionadded:: (cfdm) NEXTVERSION

            init_options: `dict`, optional
                Provide optional keyword arguments to methods and
                functions called during the initialisation process. A
                dictionary key identifies a method or function. The
                corresponding value is another dictionary whose
                key/value pairs are the keyword parameter names and
                values to be applied.

                Supported keys are:

                * ``'from_array'``: Provide keyword arguments to
                  the `dask.array.from_array` function. This is used
                  when initialising data that is not already a dask
                  array and is not compressed by convention.

                * ``'first_non_missing_value'``: Provide keyword
                  arguments to the
                  `{{package}}.data.utils.first_non_missing_value`
                  function. This is used when the input array contains
                  date-time strings or objects, and may affect
                  performance.

                 *Parameter example:*
                   ``{'from_array': {'inline_array': True}}``

        **Examples**

        >>> d = {{package}}.{{class}}(5)
        >>> d = {{package}}.{{class}}([1,2,3], units='K')
        >>> import numpy
        >>> d = {{package}}.{{class}}(numpy.arange(10).reshape(2,5),
        ...             units='m/s', fill_value=-999)
        >>> d = {{package}}.{{class}}('fly')
        >>> d = {{package}}.{{class}}(tuple('fly'))

        """
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

            # Units
            try:
                self._Units = self.Units
            except (ValueError, AttributeError):
                self._Units = self._Units_class(None)

            # Axis identifiers
            try:
                self._axes = source._axes
            except (ValueError, AttributeError):
                try:
                    self._axes = generate_axis_identifiers(source.ndim)
                except AttributeError:
                    pass

            # Mask hardness
            self.hardmask = getattr(source, "hardmask", _DEFAULT_HARDMASK)

            # File components
            self._initialise_netcdf(source)
            self._initialise_original_filenames(source)

            return

        super().__init__(
            array=array,
            fill_value=fill_value,
            _use_array=False,
        )

        # Initialise file components
        self._initialise_netcdf(source)
        self._initialise_original_filenames(source)

        # Set the units
        units = self._Units_class(units, calendar=calendar)
        self._Units = units

        # Set the mask hardness
        self.hardmask = hardmask

        if array is None:
            # No data has been set
            return

        sparse_array = issparse(array)

        try:
            ndim = array.ndim
        except AttributeError:
            ndim = np.ndim(array)

        # Create the _axes attribute: an ordered sequence of unique
        # (within this `{{class}}` instance) names for each array axis.
        self._axes = generate_axis_identifiers(ndim)

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

        # Set whether or not we're sure that the Data instance has a
        # deterministic name
        is_dask = is_dask_collection(array)

        # REVIEW: getitem: `__init__`: Set whether or not to call `np.asanyarray` on chunks to convert them to numpy arrays.
        # Set whether or not to call `np.asanyarray` on chunks to
        # convert them to numpy arrays.
        if is_dask:
            # We don't know what's in the dask array, so we should
            # assume that it might need converting to a numpy array.
            self._set_component("__asanyarray__", True, copy=False)
        else:
            # Use the array's __asanyarray__ value, if it has one.
            self._set_component(
                "__asanyarray__",
                bool(getattr(array, "__asanyarray__", False)),
                copy=False,
            )

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

        # REVIEW: getitem: `__init__`: set 'asanyarray'
        # Store the dask array
        self._set_dask(dx, clear=_NONE, asanyarray=None)

        # Override the data type
        if dtype is not None:
            self.dtype = dtype

        # Apply a mask
        if mask is not None:
            if sparse_array:
                raise ValueError("Can't mask sparse array")

            try:
                self.where(mask, masked, inplace=True)
            except AttributeError:
                array = self.array
                array = cfdm_where(array, mask, masked, array, hardmask)
                dx = da.from_array(array, chunks=chunks)
                self._set_dask(dx)

        # Apply masked values
        if mask_value is not None:
            if sparse_array:
                raise ValueError("Can't mask sparse array")

            self.masked_values(mask_value, inplace=True)

    @property
    def dask_compressed_array(self):
        """Returns a dask array of the compressed data.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `dask.array.Array`
                The compressed data.

        **Examples**

        >>> a = d.dask_compressed_array

        """
        ca = self.source(None)

        if ca is None or not ca.get_compression_type():
            raise ValueError("not compressed: can't get compressed dask array")

        return ca.to_dask_array()

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

    def __float__(self):
        """Called to implement the built-in function `float`

        x.__float__() <==> float(x)

        **Performance**

        `__float__` causes all delayed operations to be executed,
        unless the dask array size is already known to be greater than
        1.

        """
        if self.size != 1:
            raise TypeError(
                "only length-1 arrays can be converted to "
                f"Python scalars. Got {self}"
            )

        return float(self.array[(0,) * self.ndim])

    def __int__(self):
        """Called to implement the built-in function `int`

        x.__int__() <==> int(x)

        **Performance**

        `__int__` causes all delayed operations to be executed, unless
        the dask array size is already known to be greater than 1.

        """
        if self.size != 1:
            raise TypeError(
                "only length-1 arrays can be converted to "
                f"Python scalars. Got {self}"
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
        <{{repr}}{{class}}(1): [1] metres>
        <{{repr}}{{class}}(1): [2] metres>
        <{{repr}}{{class}}(1): [3] metres>

        >>> d = {{package}}.{{class}}([[1, 2], [3, 4]], 'metres')
        >>> for e in d:
        ...     print(repr(e))
        ...
        <{{repr}}{{class}}: [1, 2] metres>
        <{{repr}}{{class}}: [3, 4] metres>

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

    def __len__(self):
        """Called to implement the built-in function `len`.

        x.__len__() <==> len(x)

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
        # REVIEW: getitem: `__len__`: set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        dx = self.to_dask_array(asanyarray=False)
        if math.isnan(dx.size):
            logger.debug("Computing data len: Performance may be degraded")
            dx.compute_chunk_sizes()

        return len(dx)

    def __bool__(self):
        """Truth value testing and the built-in operation `bool`

        x.__bool__() <==> bool(x)

        **Performance**

        `__bool__` causes all delayed operations to be computed.

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
          the same behaviour as indexing on a `netCDF4.Variable` object.

        **Performance**

        If the shape of the data is unknown then it is calculated
        immediately by executing all delayed operations.

        . seealso:: `__keepdims_indexing__`,
                    `__orthogonal_indexing__`, `__setitem__`

        :Returns:

            `{{class}}`
                The subspace of the data.

        **Examples**

        >>> import numpy
        >>> d = {{package}}.{{class}}(numpy.arange(100, 190).reshape(1, 10, 9))
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
        if indices is Ellipsis:
            return self.copy()

        shape = self.shape
        keepdims = self.__keepdims_indexing__

        indices = parse_indices(shape, indices, keepdims=keepdims)

        new = self.copy()
        dx = self.to_dask_array(asanyarray=False)

        # ------------------------------------------------------------
        # Subspace the dask array
        # ------------------------------------------------------------
        if self.__orthogonal_indexing__:
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
        else:
            raise NotImplementedError(
                "Non-orthogonal indexing has not yet been implemented"
            )

        # REVIEW: getitem: `__getitem__`: set 'asanyarray=True' because subspaced chunks might not be in memory
        # ------------------------------------------------------------
        # Set the subspaced dask array
        #
        # * A subpspaced chunk might not result in an array in memory,
        #   so we set asanyarray=True to ensure that, if required,
        #   they are converted at compute time.
        # ------------------------------------------------------------
        new._set_dask(dx, clear=_ALL, asanyarray=True)

        # ------------------------------------------------------------
        # Get the axis identifiers for the subspace
        # ------------------------------------------------------------
        if not keepdims:
            new_axes = [
                axis
                for axis, x in zip(self._axes, indices)
                if not isinstance(x, Integral) and getattr(x, "shape", True)
            ]
            new._axes = new_axes

        if new.shape != self.shape:
            # Delete hdf5 chunksizes when the shape has changed.
            new.nc_clear_hdf5_chunksizes()  # TODODASK

        return new

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

    def __setitem__(self, indices, value):
        """Implement indexed assignment.

        x.__setitem__(indices, y) <==> x[indices]=y

        Assignment to data array elements defined by indices.

        Elements of a data array may be changed by assigning values to
        a subspace. See `__getitem__` for details on how to define
        subspace of the data array.

        .. note:: Currently at most one dimension's assignment index
                  may be a 1-d array of integers or booleans. This is
                  is different to `__getitem__`, which by default
                  applies 'orthogonal indexing' when multiple indices
                  of 1-d array of integers or booleans are present.

        **Missing data**

        The treatment of missing data elements during assignment to a
        subspace depends on the value of the `hardmask` attribute. If
        it is True then masked elements will not be unmasked,
        otherwise masked elements may be set to any value.

        In either case, unmasked elements may be set, (including
        missing data).

        Unmasked elements may be set to missing data by assignment to
        the `{{package}}.masked` constant or by assignment to a value
        which contains masked elements.

        **Performance**

        If the shape of the data is unknown then it is calculated
        immediately by executing all delayed operations.

        If indices for two or more dimensions are lists or 1-d arrays
        of Booleans or integers, and any of these are dask
        collections, then these dask collections will be
        computed immediately.

        .. seealso:: `__getitem__`, `__keedims_indexing__`,
                     `__orthogonal_indexing__`, `{{package}}.masked`,
                     `hardmask`

        """
        shape = self.shape

        # ancillary_mask = ()
        # try:
        #     arg = indices[0]
        # except (IndexError, TypeError):
        #     pass
        # else:
        #     if isinstance(arg, str) and arg == "mask":
        #         # The indices include an ancillary mask that defines
        #         # elements which are protected from assignment
        #         original_self = self.copy()
        #         ancillary_mask = indices[1]
        #         indices = indices[2:]
        #
        # indices, roll = parse_indices(
        #     shape,
        #     indices,
        #     cyclic=True,
        #     keepdims=self.__keepdims_indexing__,
        # )

        indices = parse_indices(
            shape,
            indices,
            keepdims=self.__keepdims_indexing__,
        )

        axes_with_list_indices = [
            i
            for i, x in enumerate(indices)
            if isinstance(x, list) or getattr(x, "shape", False)
        ]

        # When there are two or more 1-d array indices of Booleans or
        # integers, convert them to slices, if possible.
        #
        # Note: If any of these 1-d arrays is a dask collection, then
        #       this will be computed.
        if len(axes_with_list_indices) > 1:
            for i, index in enumerate(indices):
                if not (
                    isinstance(index, list) or getattr(index, "shape", False)
                ):
                    # Not a 1-d array
                    continue

                index = np.array(index)

                size = shape[i]
                if index.dtype == bool:
                    # Convert True values to integers
                    index = np.arange(size)[index]
                else:
                    # Make sure all integer values are non-negative
                    index = np.where(index < 0, index + size, index)

                if size == 1:
                    start = index[0]
                    index = slice(start, start + 1)
                else:
                    steps = index[1:] - index[:-1]
                    step = steps[0]
                    if step and not (steps - step).any():
                        # Array has a regular step, and so can be
                        # converted to a slice.
                        if step > 0:
                            start, stop = index[0], index[-1] + 1
                        elif step < 0:
                            start, stop = index[0], index[-1] - 1

                        if stop < 0:
                            stop = None

                        index = slice(start, stop, step)

                indices[i] = index

        # DASK: needs to be in cf-python
        # Roll axes with cyclic slices
        # if roll:
        #     # For example, if assigning to slice(-2, 3) has been
        #     # requested on a cyclic axis (and we're not using numpy
        #     # indexing), then we roll that axis by two points and
        #     # assign to slice(0, 5) instead. The axis is then unrolled
        #     # by two points afer the assignment has been made.
        #     axes = self._axes
        #     if not self._cyclic.issuperset([axes[i] for i in roll]):
        #         raise IndexError(
        #             "Can't do a cyclic assignment to a non-cyclic axis"
        #         )
        #
        #     roll_axes = tuple(roll.keys())
        #     shifts = tuple(roll.values())
        #     self.roll(shift=shifts, axis=roll_axes, inplace=True)

        # DASK: nneds to be in cf-python
        # Make sure that the units of value are the same as self
        # value = conform_units(value, self.Units)

        # Missing values could be affected, so make sure that the mask
        # hardness has been applied.
        dx = self.to_dask_array(apply_mask_hardness=True)

        # Do the assignment
        self._set_subspace(dx, indices, value)
        self._set_dask(dx)

        # DASK: nneds to be in cf-python
        # Unroll any axes that were rolled to enable a cyclic
        # assignment
        # if roll:
        #     shifts = [-shift for shift in shifts]
        #     self.roll(shift=shifts, axis=roll_axes, inplace=True)

        # DASK: nneds to be in cf-python
        # Reset the original array values at locations that are
        # excluded from the assignment by True values in any ancillary
        # masks
        # if ancillary_mask:
        #     indices = tuple(indices)
        #     original_self = original_self[indices]
        #     reset = self[indices]
        #     for mask in ancillary_mask:
        #         reset.where(mask, original_self, inplace=True)
        #
        #     self[indices] = reset

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
            raise
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

    # REVIEW: getitem: `__asanyarray__`: new property `__asanyarray__`
    @property
    def __asanyarray__(self):
        """Whether the chunks need conversion to a `numpy` array.

        .. versionadded:: (cfdm) NEXTVERSION

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
    def __orthogonal_indexing__(self):
        """Flag to indicate that orthogonal indexing is supported.

        Always True, indicating that 'orthogonal indexing' is
        applied. This means that when indices are 1-d arrays or lists
        then they subspace along each dimension independently. This
        behaviour is similar to Fortran, but different to `numpy`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `__keepdims_indexing__`, `__getitem__`,
                     `__setitem__`,
                     `netCDF4.Variable.__orthogonal_indexing__`

        **Examples**

        >>> d = {{package}}.{{class}}([[1, 2, 3],
        ...              [4, 5, 6]])
        >>> e = d[[0], [0, 2]]
        >>> e.shape
        (1, 2)
        >>> print(e.array)
        [[1 3]]
        >>> e = d[[0, 1], [0, 2]]
        >>> e.shape
        (2, 2)
        >>> print(e.array)
        [[1 3]
         [4 6]]

        """
        return True

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
        >>> a = numpy.array(d)
        >>> print(type(a))
        <class 'numpy.ndarray'>
        >>> a[0] = -99
        >>> d
        <{{repr}}{{class}}(3): [1, 2, 3]>
        >>> b = numpy.array(d, float)
        >>> print(b)
        [1. 2. 3.]

        """
        array = self.array
        if not dtype:
            return array
        else:
            return array.astype(dtype[0], copy=False)

    @__keepdims_indexing__.setter
    def __keepdims_indexing__(self, value):
        self._set_component("__keepdims_indexing__", bool(value), copy=False)

    def _binary_operation(self, other, method):
        """Implement binary arithmetic and comparison operations with
        the numpy broadcasting rules.

        It is called by the binary arithmetic and comparison
        methods, such as `__sub__`, `__imul__`, `__rdiv__`, `__lt__`, etc.

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

        >>> d = cf.Data([0, 1, 2, 3])
        >>> e = cf.Data([1, 1, 3, 4])

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

        # ------------------------------------------------------------
        # Ensure other is an independent Data object, for example
        # so that combination with cf.Query objects works.
        # ------------------------------------------------------------
        if not isinstance(other, self.__class__):
            if other is None:
                # Can't sensibly initialise a Data object from a bare
                # `None` (issue #281)
                other = np.array(None, dtype=object)

            other = type(self)(other)

        # ------------------------------------------------------------
        # Prepare data0 (i.e. self copied) and data1 (i.e. other)
        # ------------------------------------------------------------
        data0 = self.copy()

        # Cast as dask arrays
        dx0 = data0.to_dask_array()
        dx1 = other.to_dask_array()

        if inplace:
            # Find non-in-place equivalent operator (remove 'i')
            equiv_method = method[:2] + method[3:]
            # Need to add check in here to ensure that the operation is not
            # trying to cast in a way which is invalid. For example, doing
            # [an int array] ** float value = [a float array] is fine, but
            # doing this in-place would try to chance an int array into a
            # float one, which isn't valid casting. Therefore we need to
            # catch cases where __i<op>__ isn't possible even if __<op>__
            # is due to datatype consistency rules.
            result = getattr(dx0, equiv_method)(dx1)
        else:
            result = getattr(dx0, method)(dx1)

        if result is NotImplemented:
            raise TypeError(
                f"Unsupported operands for {method}: {self!r} and {other!r}"
            )

        # Set axes when other has more dimensions than self
        axes = None
        ndim0 = dx0.ndim
        if not ndim0:
            axes = other._axes
        else:
            diff = dx1.ndim - ndim0
            if diff > 0:
                axes = list(self._axes)
                for _ in range(diff):
                    axes.insert(0, new_axis_identifier(tuple(axes)))

        if inplace:
            self._set_dask(result)
            if axes is not None:
                self._axes = axes

            return self

        else:  # not, so concerns a new Data object copied from self, data0
            data0._set_dask(result)
            if axes is not None:
                data0._axes = axes

            return data0

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

    def _cfa_del_write(self):
        """Set the CFA write status of the data to `False`.

        TODOCFA: Placeholder

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `cfa_get_write`, `_cfa_set_write`

        :Returns:

            `bool`
                The CFA status prior to deletion.

        """
        return self._del_component("cfa_write", False)

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

    # REVIEW: getitem: `_set_dask`: new keyword 'asanyarray'
    def _set_dask(self, dx, copy=False, clear=_ALL, asanyarray=False):
        """Set the dask array.

        .. versionadded:: (cfdm) NEXTVERSION

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

        :Returns:

            `None`

        """
        if dx is NotImplemented:
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
                "trapped it and returned NotImplemented (seeprint , for "
                "instance, dask.array.core.elemwise). Print "
                "statements in a local copy of dask are possibly the "
                "way to go if the cause of the error is not obvious."
            )

        if copy:
            dx = dx.copy()

        self._set_component("dask", dx, copy=False)
        # REVIEW: getitem: `_set_dask`: set '__asanyarray__'
        if asanyarray is not None:
            self._set_component("__asanyarray__", bool(asanyarray), copy=False)

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
        out = self._del_component("dask", None)
        if out is None:
            return self._default(
                default, f"{self.__class__.__name__!r} has no dask array"
            )

        self._clear_after_dask_update(clear)
        return out

    def _del_cached_elements(self):
        """Delete any cached element values.

        Updates *data* in-place to remove the cached element values.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `_get_cached_elements`, `_set_cached_elements`

        :Returns:

            `None`

        """
        self._del_component("cached_elements", None)

    def _get_cached_elements(self):
        """Return the cache of selected element values.

        .. warning:: Never change the returned dictionary in-place.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `_del_cached_elements`, `_set_cached_elements`

        :Returns:

            `dict`
                The cached element values, where the keys are the
                element positions within the dask array and the values
                are the cached values for each position.

        **Examples**

        >>> d._get_cached_elements()
        {}

        >>> d._get_cached_elements()
        {0: 273.15, 1: 274.56, -1: 269.95}

        """
        return self._get_component("cached_elements", {})

    def _is_abstract_Array_subclass(self, array):
        """Whether or not an array is a type of Array.

        :Parameters:

            array:

        :Returns:

            `bool`

        """
        return isinstance(array, Array)

    def _set_cached_elements(self, elements):
        """Cache selected element values.

        Updates *data* in-place to store the given element values
        within its ``custom`` dictionary. TODODASK

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `_del_cached_elements`, `_get_cached_elements`

        :Parameters:

            elements: `dict`
               Zero or more element values to be cached, each keyed by
               a unique identifier to allow unambiguous retrieval.
               Existing cached elements not specified by *elements*
               will not be removed.

        :Returns:

            `None`

        **Examples**

        >>> d._set_cached_elements({0: 273.15})

        """
        if not elements:
            return

        cache = self._get_component("cached_elements", None)
        if cache:
            cache = cache.copy()
            cache.update(elements)
        else:
            cache = elements.copy()

        self._set_component("cached_elements", cache, copy=False)

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
                    for start, stop in zip_longest(*args):
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

            if prod(np.shape(value)) == 1:
                # 'value' is logically scalar => simply assign it to
                # all index combinations.
                for i in product(*indices1):
                    try:
                        array[i] = value
                    except NotImplementedError:
                        # Assume that 'i' contains multiple size 1
                        # lists, which are not implemented by Dask:
                        # replace the size lists with integers.
                        i = [
                            x[0] if isinstance(x, list) and len(x) == 1 else x
                            for x in i
                        ]
                        array[tuple(i)] = value

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

                for i, j in zip(product(*indices1), product(*indices2)):
                    try:
                        array[i] = value[j]
                    except NotImplementedError:
                        # Assume that 'i' contains multiple size 1
                        # lists, which are not implemented by Dask:
                        # replace the size lists with integers.
                        i = [
                            x[0] if isinstance(x, list) and len(x) == 1 else x
                            for x in i
                        ]
                        array[tuple(i)] = value[j]

    @property
    def Units(self):
        """The `Units` object containing the units of the data array.

        .. versionadded:: (cfdm) NEXTVERSION

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3], units='m')
        >>> d.Units
        <Units: m>
        >>> d.Units = {{package}}.Units('kilometres')
        >>> d.Units
        <Units: kilometres>
        >>> d.Units = {{package}}.Units('km')
        >>> d.Units
        <Units: km>

        """
        return self._Units

    @Units.setter
    def Units(self, value):
        # DASK: needs to be in cf-python
        self._Units = value

    @Units.deleter
    def Units(self):
        # DASK: needs to be in cf-python
        del self._Units

    @_inplace_enabled(default=False)
    def pad_missing(self, axis, pad_width=None, to_size=None, inplace=False):
        """Pad an axis with missing data.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            axis: `int`
                Select the axis for which the padding is to be
                applied.

                *Parameter example:*
                  Pad second axis: ``axis=1``.

                *Parameter example:*
                  Pad the last axis: ``axis=-1``.

            {{pad_width: sequence of `int`, optional}}

            {{to_size: `int`, optional}}

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The padded data, or `None` if the operation was
                in-place.

        **Examples**

        >>> d = {{package}}.{{class}}(np.arange(6).reshape(2, 3))
        >>> print(d.array)
        [[0 1 2]
         [3 4 5]]
        >>> e = d.pad_missing(1, (1, 2))
        >>> print(e.array)
        [[-- 0 1 2 -- --]
         [-- 3 4 5 -- --]]
        >>> f = e.pad_missing(0, (0, 1))
        >>> print(f.array)
        [[--  0  1  2 -- --]
         [--  3  4  5 -- --]
         [-- -- -- -- -- --]]

        >>> g = d.pad_missing(1, to_size=5)
        >>> print(g.array)
        [[0 1 2 -- --]
         [3 4 5 -- --]]

        """
        if not 0 <= axis < self.ndim:
            raise ValueError(
                f"'axis' must be a valid dimension position. Got {axis}"
            )

        if to_size is not None:
            # Set pad_width from to_size
            if pad_width is not None:
                raise ValueError("Can't set both 'pad_width' and 'to_size'")

            pad_width = (0, to_size - self.shape[axis])
        elif pad_width is None:
            raise ValueError("Must set either 'pad_width' or 'to_size'")

        pad_width = np.asarray(pad_width)
        if pad_width.shape != (2,) or not pad_width.dtype.kind == "i":
            raise ValueError(
                "'pad_width' must be a sequence of two integers. "
                f"Got: {pad_width}"
            )

        pad_width = tuple(pad_width)
        if any(n < 0 for n in pad_width):
            if to_size is not None:
                raise ValueError(
                    f"'to_size' ({to_size}) must not be smaller than the "
                    f"original axis size ({self.shape[axis]})"
                )

            raise ValueError(
                f"Can't set a negative number of pad values. Got: {pad_width}"
            )

        d = _inplace_enabled_define_and_cleanup(self)

        dx = d.to_dask_array()
        mask0 = da.ma.getmaskarray(dx)

        pad = [(0, 0)] * dx.ndim
        pad[axis] = pad_width

        # Pad the data with zero. This will lose the original mask.
        dx = da.pad(dx, pad, mode="constant", constant_values=0)

        # Pad the mask with True
        mask = da.pad(mask0, pad, mode="constant", constant_values=True)

        # Set the mask
        dx = da.ma.masked_where(mask, dx)

        d._set_dask(dx)
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

            `{{class}}` or `None`
                The persisted data. If the operation was in-place then
                `None` is returned.

        **Examples**

        >>> e = d.persist()

        """
        d = _inplace_enabled_define_and_cleanup(self)
        dx = self.to_dask_array()
        dx = dx.persist()
        d._set_dask(dx, clear=_ALL ^ _ARRAY ^ _CACHE)
        return d

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

        .. seealso:: `persist`, `array`, `datetime_array`,
                     `sparse_array`

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
        dx = self.to_dask_array()
        a = dx.compute()

        if np.ma.isMA(a):
            if self.hardmask:
                a.harden_mask()
            else:
                a.soften_mask()

            a.set_fill_value(self.get_fill_value(None))

        return a

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
    def rechunk(
        self,
        chunks=_DEFAULT_CHUNKS,
        threshold=None,
        block_size_limit=None,
        balance=False,
        inplace=False,
    ):
        """Change the chunk structure of the data.

        **Performance**

        Rechunking can sometimes be expensive and incur a lot of
        communication overheads.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `chunks`, `dask.array.rechunk`

        :Parameters:

            {{chunks: `int`, `tuple`, `dict` or `str`, optional}}

            {{threshold: `int`, optional}}

            {{block_size_limit: `int`, optional}}

            {{balance: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The rechunked data, or `None` if the operation was
                in-place.

        **Examples**

        >>> x = {{package}}.{{class}}.empty((1000, 1000), chunks=(100, 100))

        Specify uniform chunk sizes with a tuple

        >>> y = x.rechunk((1000, 10))

        Or chunk only specific dimensions with a dictionary

        >>> y = x.rechunk({0: 1000})

        Use the value ``-1`` to specify that you want a single chunk
        along a dimension or the value ``"auto"`` to specify that dask
        can freely rechunk a dimension to attain blocks of a uniform
        block size.

        >>> y = x.rechunk({0: -1, 1: 'auto'}, block_size_limit=1e8)

        If a chunk size does not divide the dimension then rechunk
        will leave any unevenness to the last chunk.

        >>> x.rechunk(chunks=(400, -1)).chunks
        ((400, 400, 200), (1000,))

        However if you want more balanced chunks, and don't mind
        `dask` choosing a different chunksize for you then you can use
        the ``balance=True`` option.

        >>> x.rechunk(chunks=(400, -1), balance=True).chunks
        ((500, 500), (1000,))

        """
        d = _inplace_enabled_define_and_cleanup(self)

        # REVIEW: getitem: `rechunk`: set 'asanyarray'
        # Dask rechunking is essentially a wrapper for __getitem__
        # calls on the chunks, which means that we can use the same
        # 'asanyarray' and 'clear' keywords to `_set_dask` as are used
        # in `__gettem__`.

        dx = d.to_dask_array(asanyarray=False)
        dx = dx.rechunk(chunks, threshold, block_size_limit, balance)
        d._set_dask(dx, clear=_ALL ^ _ARRAY ^ _CACHE, asanyarray=True)

        return d

    def _unary_operation(self, operation):
        """Implement unary arithmetic operations.

        It is called by the unary arithmetic methods, such as
        __abs__().

        .. seealso:: `_binary_operation`

        :Parameters:

            operation: `str`
                The unary arithmetic method name (such as "__invert__").

        :Returns:

            `{{class}}`
                A new Data array.

        **Examples**

        >>> d = {{package}}.{{class}}([[1, 2, -3, -4, -5]])

        >>> e = d._unary_operation('__abs__')
        >>> print(e.array)
        [[1 2 3 4 5]]

        >>> e = d.__abs__()
        >>> print(e.array)
        [[1 2 3 4 5]]

        >>> e = abs(d)
        >>> print(e.array)
        [[1 2 3 4 5]]

        """
        out = self.copy(array=False)

        dx = self.to_dask_array()
        dx = getattr(operator, operation)(dx)

        out._set_dask(dx)

        return out

    def __add__(self, other):
        """The binary arithmetic operation ``+``

        x.__add__(y) <==> x+y

        """
        return self._binary_operation(other, "__add__")

    def __iadd__(self, other):
        """The augmented arithmetic assignment ``+=``

        x.__iadd__(y) <==> x+=y

        """
        return self._binary_operation(other, "__iadd__")

    def __radd__(self, other):
        """The binary arithmetic operation ``+`` with reflected
        operands.

        x.__radd__(y) <==> y+x

        """
        return self._binary_operation(other, "__radd__")

    def __sub__(self, other):
        """The binary arithmetic operation ``-``

        x.__sub__(y) <==> x-y

        """
        return self._binary_operation(other, "__sub__")

    def __isub__(self, other):
        """The augmented arithmetic assignment ``-=``

        x.__isub__(y) <==> x-=y

        """
        return self._binary_operation(other, "__isub__")

    def __rsub__(self, other):
        """The binary arithmetic operation ``-`` with reflected
        operands.

        x.__rsub__(y) <==> y-x

        """
        return self._binary_operation(other, "__rsub__")

    def __mul__(self, other):
        """The binary arithmetic operation ``*``

        x.__mul__(y) <==> x*y

        """
        return self._binary_operation(other, "__mul__")

    def __imul__(self, other):
        """The augmented arithmetic assignment ``*=``

        x.__imul__(y) <==> x*=y

        """
        return self._binary_operation(other, "__imul__")

    def __rmul__(self, other):
        """The binary arithmetic operation ``*`` with reflected
        operands.

        x.__rmul__(y) <==> y*x

        """
        return self._binary_operation(other, "__rmul__")

    def __div__(self, other):
        """The binary arithmetic operation ``/``

        x.__div__(y) <==> x/y

        """
        return self._binary_operation(other, "__div__")

    def __idiv__(self, other):
        """The augmented arithmetic assignment ``/=``

        x.__idiv__(y) <==> x/=y

        """
        return self._binary_operation(other, "__idiv__")

    def __rdiv__(self, other):
        """The binary arithmetic operation ``/`` with reflected
        operands.

        x.__rdiv__(y) <==> y/x

        """
        return self._binary_operation(other, "__rdiv__")

    def __floordiv__(self, other):
        """The binary arithmetic operation ``//``

        x.__floordiv__(y) <==> x//y

        """
        return self._binary_operation(other, "__floordiv__")

    def __ifloordiv__(self, other):
        """The ``//=``

        x.__ifloordiv__(y) <==> x//=y

        """
        return self._binary_operation(other, "__ifloordiv__")

    def __rfloordiv__(self, other):
        """The ``//`` operation with reflected operands.

        x.__rfloordiv__(y) <==> y//x

        """
        return self._binary_operation(other, "__rfloordiv__")

    def __truediv__(self, other):
        """The binary arithmetic operation ``/`` (true division)

        x.__truediv__(y) <==> x/y

        """
        return self._binary_operation(other, "__truediv__")

    def __itruediv__(self, other):
        """The augmented arithmetic assignment ``/=`` (true division)

        x.__itruediv__(y) <==> x/=y

        """
        return self._binary_operation(other, "__itruediv__")

    def __rtruediv__(self, other):
        """The binary arithmetic operation ``/`` (true division) with
        reflected operands.

        x.__rtruediv__(y) <==> y/x

        """
        return self._binary_operation(other, "__rtruediv__")

    def __pow__(self, other, modulo=None):
        """The binary arithmetic operations ``**`` and ``pow``

        x.__pow__(y) <==> x**y

        """
        if modulo is not None:
            raise NotImplementedError(
                "3-argument power not supported for {!r}".format(
                    self.__class__.__name__
                )
            )

        return self._binary_operation(other, "__pow__")

    def __ipow__(self, other, modulo=None):
        """The augmented arithmetic assignment ``**=``

        x.__ipow__(y) <==> x**=y

        """
        if modulo is not None:
            raise NotImplementedError(
                "3-argument power not supported for {!r}".format(
                    self.__class__.__name__
                )
            )

        return self._binary_operation(other, "__ipow__")

    def __rpow__(self, other, modulo=None):
        """The binary arithmetic operations ``**`` and ``pow`` with
        reflected operands.

        x.__rpow__(y) <==> y**x

        """
        if modulo is not None:
            raise NotImplementedError(
                "3-argument power not supported for {!r}".format(
                    self.__class__.__name__
                )
            )

        return self._binary_operation(other, "__rpow__")

    def __mod__(self, other):
        """The binary arithmetic operation ``%``

        x.__mod__(y) <==> x % y

        """
        return self._binary_operation(other, "__mod__")

    def __imod__(self, other):
        """The binary arithmetic operation ``%=``

        x.__imod__(y) <==> x %= y

        """
        return self._binary_operation(other, "__imod__")

    def __rmod__(self, other):
        """The binary arithmetic operation ``%`` with reflected
        operands.

        x.__rmod__(y) <==> y % x

        """
        return self._binary_operation(other, "__rmod__")

    def __eq__(self, other):
        """The rich comparison operator ``==``

        x.__eq__(y) <==> x==y

        """
        return self._binary_operation(other, "__eq__")

    def __ne__(self, other):
        """The rich comparison operator ``!=``

        x.__ne__(y) <==> x!=y

        """
        return self._binary_operation(other, "__ne__")

    def __ge__(self, other):
        """The rich comparison operator ``>=``

        x.__ge__(y) <==> x>=y

        """
        return self._binary_operation(other, "__ge__")

    def __gt__(self, other):
        """The rich comparison operator ``>``

        x.__gt__(y) <==> x>y

        """
        return self._binary_operation(other, "__gt__")

    def __le__(self, other):
        """The rich comparison operator ``<=``

        x.__le__(y) <==> x<=y

        """
        return self._binary_operation(other, "__le__")

    def __lt__(self, other):
        """The rich comparison operator ``<``

        x.__lt__(y) <==> x<y

        """
        return self._binary_operation(other, "__lt__")

    def __and__(self, other):
        """The binary bitwise operation ``&``

        x.__and__(y) <==> x&y

        """
        return self._binary_operation(other, "__and__")

    def __iand__(self, other):
        """The augmented bitwise assignment ``&=``

        x.__iand__(y) <==> x&=y

        """
        return self._binary_operation(other, "__iand__")

    def __rand__(self, other):
        """The binary bitwise operation ``&`` with reflected operands.

        x.__rand__(y) <==> y&x

        """
        return self._binary_operation(other, "__rand__")

    def __or__(self, other):
        """The binary bitwise operation ``|``

        x.__or__(y) <==> x|y

        """
        return self._binary_operation(other, "__or__")

    def __ior__(self, other):
        """The augmented bitwise assignment ``|=``

        x.__ior__(y) <==> x|=y

        """
        return self._binary_operation(other, "__ior__")

    def __ror__(self, other):
        """The binary bitwise operation ``|`` with reflected operands.

        x.__ror__(y) <==> y|x

        """
        return self._binary_operation(other, "__ror__")

    def __xor__(self, other):
        """The binary bitwise operation ``^``

        x.__xor__(y) <==> x^y

        """
        return self._binary_operation(other, "__xor__")

    def __ixor__(self, other):
        """The augmented bitwise assignment ``^=``

        x.__ixor__(y) <==> x^=y

        """
        return self._binary_operation(other, "__ixor__")

    def __rxor__(self, other):
        """The binary bitwise operation ``^`` with reflected operands.

        x.__rxor__(y) <==> y^x

        """
        return self._binary_operation(other, "__rxor__")

    def __lshift__(self, y):
        """The binary bitwise operation ``<<``

        x.__lshift__(y) <==> x<<y

        """
        return self._binary_operation(y, "__lshift__")

    def __ilshift__(self, y):
        """The augmented bitwise assignment ``<<=``

        x.__ilshift__(y) <==> x<<=y

        """
        return self._binary_operation(y, "__ilshift__")

    def __rlshift__(self, y):
        """The binary bitwise operation ``<<`` with reflected operands.

        x.__rlshift__(y) <==> y<<x

        """
        return self._binary_operation(y, "__rlshift__")

    def __rshift__(self, y):
        """The binary bitwise operation ``>>``

        x.__lshift__(y) <==> x>>y

        """
        return self._binary_operation(y, "__rshift__")

    def __irshift__(self, y):
        """The augmented bitwise assignment ``>>=``

        x.__irshift__(y) <==> x>>=y

        """
        return self._binary_operation(y, "__irshift__")

    def __rrshift__(self, y):
        """The binary bitwise operation ``>>`` with reflected operands.

        x.__rrshift__(y) <==> y>>x

        """
        return self._binary_operation(y, "__rrshift__")

    def __abs__(self):
        """The unary arithmetic operation ``abs``

        x.__abs__() <==> abs(x)

        """
        return self._unary_operation("__abs__")

    def __neg__(self):
        """The unary arithmetic operation ``-``

        x.__neg__() <==> -x

        """
        return self._unary_operation("__neg__")

    def __invert__(self):
        """The unary bitwise operation ``~``

        x.__invert__() <==> ~x

        """
        return self._unary_operation("__invert__")

    def __pos__(self):
        """The unary arithmetic operation ``+``

        x.__pos__() <==> +x

        """
        return self._unary_operation("__pos__")

    @property
    def _Units(self):
        """Storage for the units in a `{{package}}.Units` object.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `Units`

        """
        return self._get_component("Units")

    @_Units.setter
    def _Units(self, value):
        self._set_component("Units", value, copy=False)

    @_Units.deleter
    def _Units(self):
        self._set_component("Units", self._Units_class(None), copy=False)

    @property
    def _axes(self):
        """Storage for the axis identifiers.

        Contains a `tuple` of identifiers, one for each array axis.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        # TODODASK Note: override in cf-python
        return self._get_component("axes")  # , None)

    @_axes.setter
    def _axes(self, value):
        self._set_component("axes", tuple(value), copy=False)
        # TODODASK Note: override in cf-python

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

    @property
    def chunks(self):
        """The `dask` chunk sizes for each dimension.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `npartitions`, `numblocks`, `rechunk`

        **Examples**

        >>> d = {{package}}.{{class}}.empty((6, 5), chunks=(2, 4))
        >>> d.chunks
        ((2, 2, 2), (4, 1))
        >>> d.numblocks
        (3, 2)
        >>> d.npartitions
        6

        """
        # REVIEW: getitem: `chunks`: set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        return self.to_dask_array(asanyarray=False).chunks

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
    def dtype(self):
        """The `numpy` data-type of the data.

        Always returned as a `numpy` data-type instance, but may be set
        as any object that converts to a `numpy` data-type.

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
        # REVIEW: getitem: `dtype`: set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        dx = self.to_dask_array(asanyarray=False)
        return dx.dtype

    @dtype.setter
    def dtype(self, value):
        # Only change the datatype if it's different to that of the
        # dask array
        if self.dtype != value:
            dx = self.to_dask_array()
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
        self._set_component("hardmask", bool(value), copy=False)

    @property
    def nbytes(self):
        """Total number of bytes consumed by the elements of the array.

        Does not include bytes consumed by the array mask

        **Performance**

        If the number of bytes is unknown then it is calculated
        immediately by executing all delayed operations.

        **Examples**

        >>> d = {{package}}.{{class}}([[1, 1.5, 2]])
        >>> d.dtype
        dtype('float64')
        >>> d.size, d.dtype.itemsize
        (3, 8)
        >>> d.nbytes
        24
        >>> d[0] = {{package}}.masked
        >>> print(d.array)
        [[-- 1.5 2.0]]
        >>> d.nbytes
        24

        """
        # REVIEW: getitem: `nbytes`: set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        dx = self.to_dask_array(asanyarray=False)
        if math.isnan(dx.size):
            logger.debug("Computing data nbytes: Performance may be degraded")
            dx.compute_chunk_sizes()

        return dx.nbytes

    @property
    def ndim(self):
        """Number of dimensions in the data array.

        **Examples**

        >>> d = {{package}}.{{class}}([[1, 2, 3], [4, 5, 6]])
        >>> d.ndim
        2

        >>> d = {{package}}.{{class}}([[1, 2, 3]])
        >>> d.ndim
        2

        >>> d = {{package}}.{{class}}([[3]])
        >>> d.ndim
        2

        >>> d = {{package}}.{{class}}([3])
        >>> d.ndim
        1

        >>> d = {{package}}.{{class}}(3)
        >>> d.ndim
        0

        """
        # REVIEW: getitem: `ndim`: set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        dx = self.to_dask_array(asanyarray=False)
        return dx.ndim

    @property
    def npartitions(self):
        """The total number of chunks.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `chunks`, `numblocks`, `rechunk`

        **Examples**

        >>> d = {{package}}.{{class}}.empty((6, 5), chunks=(2, 4))
        >>> d.chunks
        ((2, 2, 2), (4, 1))
        >>> d.numblocks
        (3, 2)
        >>> d.npartitions
        6

        """
        # REVIEW: getitem: `npartitions`: set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        return self.to_dask_array(asanyarray=False).npartitions

    @property
    def numblocks(self):
        """The number of chunks along each dimension.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `chunks`, `npartitions`, `rechunk`

        **Examples**

        >>> d = {{package}}.{{class}}.empty((6, 5), chunks=(2, 4))
        >>> d.chunks
        ((2, 2, 2), (4, 1))
        >>> d.numblocks
        (3, 2)
        >>> d.npartitions
        6

        """
        # REVIEW: getitem: `numblocks` set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        return self.to_dask_array(asanyarray=False).numblocks

    @property
    def shape(self):
        """Tuple of the data array's dimension sizes.

        **Performance**

        If the shape of the data is unknown then it is calculated
        immediately by executing all delayed operations.

        **Examples**

        >>> d = {{package}}.{{class}}([[1, 2, 3], [4, 5, 6]])
        >>> d.shape
        (2, 3)

        >>> d = {{package}}.{{class}}([[1, 2, 3]])
        >>> d.shape
        (1, 3)

        >>> d = {{package}}.{{class}}([[3]])
        >>> d.shape
        (1, 1)

        >>> d = {{package}}.{{class}}(3)
        >>> d.shape
        ()

        """
        # REVIEW: getitem: `shape`: set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        dx = self.to_dask_array(asanyarray=False)
        if math.isnan(dx.size):
            logger.debug("Computing data shape: Performance may be degraded")
            dx.compute_chunk_sizes()

        return dx.shape

    @property
    def size(self):
        """Number of elements in the data array.

        **Performance**

        If the size of the data is unknown then it is calculated
        immediately by executing all delayed operations.

        **Examples**

        >>> d = {{package}}.{{class}}([[1, 2, 3], [4, 5, 6]])
        >>> d.size
        6

        >>> d = {{package}}.{{class}}([[1, 2, 3]])
        >>> d.size
        3

        >>> d = {{package}}.{{class}}([[3]])
        >>> d.size
        1

        >>> d = {{package}}.{{class}}([3])
        >>> d.size
        1

        >>> d = {{package}}.{{class}}(3)
        >>> d.size
        1

        """
        # REVIEW: getitem: `size` set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        dx = self.to_dask_array(asanyarray=False)
        size = dx.size
        if math.isnan(size):
            logger.debug("Computing data size: Performance may be degraded")
            dx.compute_chunk_sizes()
            size = dx.size

        return size

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
        >>> isinstance(a, numpy.ndarray)
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
        a = self.compute().copy()
        if issparse(a):
            a = a.toarray()
        elif not isinstance(a, np.ndarray):
            a = np.asanyarray(a)

        if not a.size:
            return a

        # Set cached elements
        items = [0, -1]
        if a.ndim == 2 and a.shape[-1] == 2:
            items.extend((1, -2))
        elif a.size == 3:
            items.append(1)

        self._set_cached_elements({i: a.item(i) for i in items})

        return a

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

        **Examples**

        """
        units = self.Units

        if not units.isreftime:
            raise ValueError(
                f"Can't create date-time array from units {self._Units!r}"
            )

        calendar = getattr(units, "calendar", None)
        if calendar == "none":
            raise ValueError(
                f"Can't create date-time array from units {self._Units!r} "
                "because calendar is 'none'"
            )

        units1, reftime = units.units.split(" since ")

        # Convert months and years to days, because cftime won't work
        # otherwise.
        year_length = 365.242198781  # Number of days in a Udunits year
        if units1 in ("month", "months"):
            d = self * (year_length / 12)
            units = self._Units_class(f"days since {reftime}", calendar)
        elif units1 in ("year", "years", "yr"):
            d = self * year_length
            units = self._Units_class(f"days since {reftime}", calendar)
        else:
            d = self

        dx = d.to_dask_array()
        dx = convert_to_datetime(dx, units)

        a = dx.compute()

        if np.ma.isMA(a):
            if self.hardmask:
                a.harden_mask()
            else:
                a.soften_mask()

            a.set_fill_value(self.get_fill_value(None))

        return a

    @property
    def mask(self):
        """The Boolean missing data mask of the data array.

        The Boolean mask has True where the data array has missing data
        and False otherwise.

        :Returns:

            `{{class}}`

        **Examples**

        >>> d.shape
        (12, 73, 96)
        >>> m = d.mask
        >>> m.dtype
        dtype('bool')
        >>> m.shape
        (12, 73, 96)

        """
        mask_data_obj = self.copy(array=False)

        dx = self.to_dask_array()
        mask = da.ma.getmaskarray(dx)

        mask_data_obj._set_dask(mask)
        mask_data_obj._Units = self._Units_class(None)
        mask_data_obj.hardmask = _DEFAULT_HARDMASK

        return mask_data_obj

    def any(self, axis=None, keepdims=True, split_every=None):
        """Test whether any data array elements evaluate to True.

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

        >>> d = {{package}}.{{class}}([[0, 2], [0, 4]])
        >>> d.any()
        <{{repr}}{{class}}(1, 1): [[True]]>
        >>> d.any(keepdims=False)
        <{{repr}}{{class}}(1, 1): True>
        >>> d.any(axis=0)
        <{{repr}}{{class}}(1, 2): [[False, True]]>
        >>> d.any(axis=1)
        <{{repr}}{{class}}(2, 1): [[True, True]]>
        >>> d.any(axis=())
        <{{repr}}{{class}}(2, 2): [[False, ..., True]]>

        >>> d[0] = {{package}}.masked
        >>> print(d.array)
        [[-- --]
         [0 4]]
        >>> d.any(axis=0)
        <{{repr}}{{class}}(1, 2): [[False, True]]>
        >>> d.any(axis=1)
        <{{repr}}{{class}}(2, 1): [[--, True]]>

        >>> d[...] = {{package}}.masked
        >>> d.any()
        <{{repr}}{{class}}(1, 1): [[--]]>
        >>> bool(d.any())
        False
        >>> bool(d.any(keepdims=False))
        False

        """
        d = self.copy(array=False)
        dx = self.to_dask_array()
        dx = da.any(dx, axis=axis, keepdims=keepdims, split_every=split_every)
        d._set_dask(dx)
        d.hardmask = _DEFAULT_HARDMASK
        d._Units = self._Units_class(None)
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

                If True then the value returned by the
                `get_fill_value` method, if such a value exists, is
                used.

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
                A scalar specifying the minimum valid value. Data
                elements strictly less than this number will be set to
                missing data.

            valid_max: number, optional
                A scalar specifying the maximum valid value. Data
                elements strictly greater than this number will be set
                to missing data.

            valid_range: (number, number), optional
                A vector of two numbers specifying the minimum and
                maximum valid values, equivalent to specifying values
                for both *valid_min* and *valid_max* parameters. The
                *valid_range* parameter must not be set if either
                *valid_min* or *valid_max* is defined.

                *Parameter example:*
                  ``valid_range=[-999, 10000]`` is equivalent to setting
                  ``valid_min=-999, valid_max=10000``

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The data with masked values. If the operation was in-place
                then `None` is returned.

        **Examples**

        >>> import numpy
        >>> d = {{package}}.{{class}}(numpy.arange(12).reshape(3, 4), 'm')
        >>> d[1, 1] = {{package}}.masked
        >>> print(d.array)
        [[0 1 2 3]
         [4 -- 6 7]
         [8 9 10 11]]
        >>> print(d.apply_masking().array)
        [[0 1 2 3]
         [4 -- 6 7]
         [8 9 10 11]]
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
        [[0 1 2 3]
         [4 -- 6 --]
         [8 9 10 11]]
        >>> print(d.apply_masking(fill_values=True,
        ...                       valid_range=[2, 8]).array)
        [[-- -- 2 3]
         [4 -- 6 --]
         [8 -- -- --]]

        """
        # Parse valid_range
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

        # Parse fill_values
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
                iter(fill_values)
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

        d = _inplace_enabled_define_and_cleanup(self)

        dx = self.to_dask_array()

        mask = None
        if fill_values:
            mask = dx == fill_values[0]

            for fill_value in fill_values[1:]:
                mask |= dx == fill_value

        if valid_min is not None:
            if mask is None:
                mask = dx < valid_min
            else:
                mask |= dx < valid_min

        if valid_max is not None:
            if mask is None:
                mask = dx > valid_max
            else:
                mask |= dx > valid_max

        if mask is not None:
            dx = da.ma.masked_where(mask, dx)

        d._set_dask(dx)

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

        .. versionadded:: 3.0.0

        :Returns:

            `{{class}}`

        """
        return self

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

    def get_filenames(self):
        """The names of files containing parts of the data array.

        Returns the names of any files that may be required to deliver
        the computed data array. This set may contain fewer names than
        the collection of file names that defined the data when it was
        first instantiated, as could be the case after the data has
        been subspaced.

        **Implementation**

        A `dask` chunk that contributes to the computed array is
        assumed to reference data within a file if that chunk's array
        object has a callable `get_filenames` method, the output of
        which is added to the returned `set`.

        :Returns:

            `set`
                The file names. If no files are required to compute
                the data then an empty `set` is returned.

        **Examples**

        >>> d = {{package}}.{{class}}.empty((5, 8), 1, chunks=4)
        >>> d.get_filenames()
        set()

        >>> f = {{package}}.example_field(0)
        >>> {{package}}.write(f, "file_A.nc")
        >>> {{package}}.write(f, "file_B.nc")

        >>> a = {{package}}.read("file_A.nc", chunks=4)[0].data
        >>> a += 999
        >>> b = {{package}}.read("file_B.nc", chunks=4)[0].data
        >>> c = {{package}}.{{class}}(b.array, units=b.Units, chunks=4)
        >>> print(a.shape, b.shape, c.shape)
        (5, 8) (5, 8) (5, 8)
        >>> d = cf.Data.concatenate([a, a.copy(), b, c], axis=1) TODODASK
        >>> print(d.shape)
        (5, 32)

        >>> d.get_filenames()
        {'file_A.nc', 'file_B.nc'}
        >>> d[:, 2:7].get_filenames()
        {'file_A.nc'}
        >>> d[:, 2:14].get_filenames()
        {'file_A.nc', 'file_B.nc'}
        >>> d[:, 2:20].get_filenames()
        {'file_A.nc', 'file_B.nc'}
        >>> d[:, 2:30].get_filenames()
        {'file_A.nc', 'file_B.nc'}
        >>> d[:, 29:30].get_filenames()
        set()
        >>> d[2, 3] = -99
        >>> d[2, 3].get_filenames()
        {'file_A.nc'}

        """
        out = set()

        # REVIEW: getitem: `get_filenames`: set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        for a in self.todict(asanyarray=False).values():
            try:
                out.update(a.get_filenames())
            except AttributeError:
                pass

        # TODODASK: update

        return out

    def get_units(self, default=ValueError()):
        """Return the units.

        .. seealso:: `del_units`, `set_units`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the units
                have not been set. If set to an `Exception` instance then
                it will be raised instead.

        :Returns:

                The units.

        **Examples**

        >>> d.set_units('metres')
        >>> d.get_units()
        'metres'
        >>> d.del_units()
        >>> d.get_units()
        ValueError: Can't get non-existent units
        >>> print(d.get_units(None))
        None

        """
        try:
            return self.Units.units
        except (ValueError, AttributeError):
            return super().get_units(default=default)

    def get_calendar(self, default=ValueError()):
        """Return the calendar.

        .. seealso:: `del_calendar`, `set_calendar`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                calendar has not been set. If set to an `Exception`
                instance then it will be raised instead.

        :Returns:

                The calendar.

        **Examples**

        >>> d.set_calendar('julian')
        >>> d.get_calendar
        'metres'
        >>> d.del_calendar()
        >>> d.get_calendar()
        ValueError: Can't get non-existent calendar
        >>> print(d.get_calendar(None))
        None

        """
        try:
            return self.Units.calendar
        except (ValueError, AttributeError):
            return super().get_calendar(default=default)

    def add_file_location(self, location):
        """Add a new file location in-place.

        All data definitions that reference files are additionally
        referenced from the given location.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_file_location`, `file_locations`

        :Parameters:

            location: `str`
                The new location.

        :Returns:

            `str`
                The new location as an absolute path with no trailing
                path name component separator.

        **Examples**

        >>> d.add_file_location('/data/model/')
        '/data/model'

        """
        location = abspath(location).rstrip(sep)

        updated = False

        # REVIEW: getitem: `add_file_location`: set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        dsk = self.todict(asanyarray=False)
        for key, a in dsk.items():
            try:
                dsk[key] = a.add_file_location(location)
            except AttributeError:
                # This chunk doesn't contain a file array
                continue

            # This chunk contains a file array and the dask graph has
            # been updated
            updated = True

        if updated:
            dx = self.to_dask_array(asanyarray=False)
            dx = da.Array(dsk, dx.name, dx.chunks, dx.dtype, dx._meta)
            self._set_dask(dx, clear=_NONE, asanyarray=None)

        return location

    @_inplace_enabled(default=False)
    def max(
        self,
        axes=None,
        squeeze=False,
        mtol=1,
        split_every=None,
        inplace=False,
    ):
        """Calculate maximum values.

        Calculates the maximum value or the maximum values along axes.

        See
        https://ncas-cms.github.io/cf-python/analysis.html#collapse-methods
        for mathematical definitions.

        .. versionadded:: (cfdm) 1.8.0

         ..seealso:: `min`, `sum`

        :Parameters:

            {{collapse axes: (sequence of) `int`, optional}}

            {{collapse squeeze: `bool`, optional}}

            {{mtol: number, optional}}

            {{split_every: `int` or `dict`, optional}}

                .. versionadded:: 3.14.0

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The collapsed data, or `None` if the operation was
                in-place.

        **Examples**

        >>> a = np.ma.arange(12).reshape(4, 3)
        >>> d = {{package}}.{{class}}(a, 'K')
        >>> d[1, 1] = {{package}}.masked
        >>> print(d.array)
        [[0 1 2]
         [3 -- 5]
         [6 7 8]
         [9 10 11]]
        >>> d.max()
        <{{repr}}{{class}}(1, 1): [[11]] K>

        """
        # Parse the axes. By default flattened input is used.
        try:
            axes = self._parse_axes(axes)
        except ValueError as error:
            raise ValueError(f"Can't max data: {error}")

        d = self.copy()
        dx = d.to_dask_array()
        dx = dx.max(axis=axes, keepdims=not squeeze)
        d._set_dask(dx)

        # TODODASK: _axes

        # TODODASK: HDF5 chunks
        return d

    def min(
        self,
        axes=None,
        squeeze=False,
    ):
        """Calculate minimum values.

        Calculates the minimum value or the minimum values along axes.

        See
        https://ncas-cms.github.io/cf-python/analysis.html#collapse-methods
        for mathematical definitions.

        .. versionadded:: (cfdm) 1.8.0

         ..seealso:: `max`, `sum`

        :Parameters:

            {{collapse axes: (sequence of) `int`, optional}}

            {{collapse squeeze: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The collapsed data, or `None` if the operation was
                in-place.

        **Examples**

        >>> a = np.ma.arange(12).reshape(4, 3)
        >>> d = {{package}}.{{class}}(a, 'K')
        >>> d[1, 1] = {{package}}.masked
        >>> print(d.array)
        [[0 1 2]
         [3 -- 5]
         [6 7 8]
         [9 10 11]]
        >>> d.min()
        <{{repr}}{{class}}(1, 1): [[0]] K>

        """
        # Parse the axes. By default flattened input is used.
        try:
            axes = self._parse_axes(axes)
        except ValueError as error:
            raise ValueError(f"Can't min data: {error}")

        d = self.copy()
        dx = d.to_dask_array()
        dx = dx.min(axis=axes, keepdims=not squeeze)
        d._set_dask(dx)

        # TODODASK: _axes

        # TODODASK: HDF5 chunks
        return d

    def set_calendar(self, calendar):
        """Set the calendar.

        .. seealso:: `override_calendar`, `override_units`,
                     `del_calendar`, `get_calendar`

        :Parameters:

            value: `str`
                The new calendar.

        :Returns:

            `None`

        **Examples**

        >>> d.set_calendar('none')
        >>> d.get_calendar
        'none'
        >>> d.del_calendar()
        >>> d.get_calendar()
        ValueError: Can't get non-existent calendar
        >>> print(d.get_calendar(None))
        None

        """
        self.Units = self._Units_class(self.get_units(default=None), calendar)

    def set_units(self, value):
        """Set the units.

        .. seealso:: `override_units`, `del_units`, `get_units`,
                     `has_units`, `Units`

        :Parameters:

            value: `str`
                The new units.

        :Returns:

            `None`

        **Examples**

        >>> d.set_units('watt')
        >>> d.get_units()
        'watt'
        >>> d.del_units()
        >>> d.get_units()
        ValueError: Can't get non-existent units
        >>> print(d.get_units(None))
        None

        """
        self.Units = self._Units_class(value, self.get_calendar(default=None))

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

        .. versionadded:: 3.0.6

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
        d = _inplace_enabled_define_and_cleanup(self)
        if d.get_compression_type():
            d._del_Array(None)

        return d

    @_manage_log_level_via_verbosity
    def equals(
        self,
        other,
        rtol=None,
        atol=None,
        ignore_fill_value=False,
        ignore_data_type=False,
        ignore_type=False,
        verbose=None,
        traceback=False,
        ignore_compression=False,
        _check_values=True,
    ):
        """True if two data arrays are logically equal, False otherwise.

        {{equals tolerance}}

        :Parameters:

            other:
                The object to compare for equality.

            {{rtol: number, optional}}

            {{atol: number, optional}}

            ignore_fill_value: `bool`, optional
                If True then data arrays with different fill values are
                considered equal. By default they are considered unequal.

            {{ignore_data_type: `bool`, optional}}

            {{ignore_type: `bool`, optional}}

            {{verbose: `int` or `str` or `None`, optional}}

            {{ignore_compression: `bool`, optional}}

        :Returns:

            `bool`
                Whether or not the two instances are equal.

        **Examples**

        >>> d.equals(d)
        True
        >>> d.equals(d + 1)
        False

        """
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

        # Check that each instance has the same units.
        self_Units = self.Units
        other_Units = other.Units
        if self_Units != other_Units:
            if is_log_level_info(logger):
                logger.info(
                    f"{self.__class__.__name__}: Different Units "
                    f"({self_Units!r}, {other_Units!r})"
                )

            return False

        # Return now if we have been asked to not check the array
        # values
        if not _check_values:
            return True

        # ------------------------------------------------------------
        # Check that each instance has equal array values
        # ------------------------------------------------------------
        if rtol is None:
            rtol = self._rtol
        else:
            rtol = float(rtol)

        if atol is None:
            atol = self._atol
        else:
            atol = float(atol)

        self_dx = self.to_dask_array()
        other_dx = other.to_dask_array()

        # Return False if there are different cached elements. This
        # provides a possible short circuit for that case that two
        # arrays are not equal (but not in the case that they are).
        cache0 = self._get_cached_elements()
        if cache0:
            cache1 = other._get_cached_elements()
            if cache1 and sorted(cache0) == sorted(cache1):
                a = []
                b = []
                for key, value0 in cache0.items():
                    value1 = cache1[key]
                    if value0 is np.ma.masked or value1 is np.ma.masked:
                        # Don't test on masked values - this logic is
                        # determined elsewhere.
                        continue

                    # Make sure strings are unicode
                    try:
                        value0 = value0.decode()
                        value1 = value1.decode()
                    except AttributeError:
                        pass

                    a.append(value0)
                    b.append(value1)

                if a and not _numpy_allclose(a, b, rtol=rtol, atol=atol):
                    if is_log_level_info(logger):
                        logger.info(
                            f"{self.__class__.__name__}: Different array "
                            f"values (atol={atol}, rtol={rtol})"
                        )

                    return False

        # Now check that corresponding elements are equal within a tolerance.
        # We assume that all inputs are masked arrays. Note we compare the
        # data first as this may return False due to different dtype without
        # having to wait until the compute call.
        self_is_numeric = is_numeric_dtype(self_dx)
        other_is_numeric = is_numeric_dtype(other_dx)
        if self_is_numeric and other_is_numeric:
            data_comparison = allclose(
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

        # Apply a (dask) logical 'and' to confirm if both the mask and the
        # data are equal for the pair of masked arrays:
        result = da.logical_and(data_comparison, mask_comparison)
        if not result.compute():
            if is_log_level_info(logger):
                logger.info(
                    f"{self.__class__.__name__}: Different array values ("
                    f"atol={atol}, rtol={rtol})"
                )

            return False
        else:
            return True

    @_inplace_enabled(default=False)
    def insert_dimension(self, position=0, inplace=False):
        """Expand the shape of the data array in place.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `flatten`, `squeeze`, `transpose`

        :Parameters:

            position: `int`, optional
                Specify the position that the new axis will have in the data
                array axes. By default the new axis has position 0, the
                slowest varying position.

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`

        **Examples**

        """
        # TODODASKAPI bring back expand_dims alias (or rather alias this to
        # that)

        d = _inplace_enabled_define_and_cleanup(self)

        # Parse position
        if not isinstance(position, int):
            raise ValueError("Position parameter must be an integer")

        ndim = d.ndim
        if -ndim - 1 <= position < 0:
            position += ndim + 1
        elif not 0 <= position <= ndim:
            raise ValueError(
                f"Can't insert dimension: Invalid position {position!r}"
            )

        shape = list(d.shape)
        shape.insert(position, 1)

        dx = d.to_dask_array()
        dx = dx.reshape(shape)

        # Inserting a dimension doesn't affect the cached elements nor
        # the CFA write status
        d._set_dask(dx, clear=_ALL ^ _CACHE ^ _CFA)

        # Expand _axes
        axis = new_axis_identifier(d._axes)
        data_axes = list(d._axes)
        data_axes.insert(position, axis)
        d._axes = data_axes

        # TODODASK: HDF5 chunks

        return d

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

        >>> d = {{package}}.{{class}}([1, 2, 3], mask=[False, True, False])
        >>> d.hardmask
        True
        >>> d[1] = 999
        >>> print(d.array)
        [1 -- 3]

        """
        # REVIEW: getitem: `hardmask`: set 'asanyarray'
        # 'harden_mask' has its own call to 'asanyarray', so we
        # can set 'asanyarray=False'.
        dx = self.to_dask_array(asanyarray=False)
        dx = dx.map_blocks(harden_mask, dtype=self.dtype)
        self._set_dask(dx, clear=_NONE)
        self.hardmask = True

    def soften_mask(self):
        """Force the mask to soft.

        Whether the mask of a masked array is hard or soft is
        determined by its `hardmask` property. `soften_mask` sets
        `hardmask` to `False`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `hardmask`, `harden_mask`

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3])
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
        # REVIEW: getitem: `soften_mask`: set 'asanyarray'
        # 'soften_mask' has its own call to 'asanyarray', so we
        # can set 'asanyarray=False'.
        dx = self.to_dask_array(asanyarray=False)
        dx = dx.map_blocks(soften_mask, dtype=self.dtype)
        self._set_dask(dx, clear=_NONE)
        self.hardmask = False

    def file_locations(self):
        """The locations of files containing parts of the data.

        Returns the locations of any files that may be required to
        deliver the computed data array.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `add_file_location`, `del_file_location`

        :Returns:

            `set`
                The unique file locations as absolute paths with no
                trailing path name component separator.

        **Examples**

        >>> d.file_locations()
        {'/home/data1', 'file:///data2'}

        """
        out = set()

        # REVIEW: getitem: `file_locations`: set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        for key, a in self.todict(asanyarray=False).items():
            try:
                out.update(a.file_locations())
            except AttributeError:
                # This chunk doesn't contain a file array
                pass

        # TODODASK: check active-storage-new for updates to this method

        return out

    @_inplace_enabled(default=False)
    def filled(self, fill_value=None, inplace=False):
        """Replace masked elements with a fill value.

        .. versionadded:: (cfdm) 1.8.7.0

        :Parameters:

            fill_value: scalar, optional
                The fill value. By default the fill returned by
                `get_fill_value` is used, or if this is not set then the
                netCDF default fill value for the data type is used (as
                defined by `netCDF.fillvals`).

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The filled data, or `None` if the operation was in-place.

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
                fill_value = default_fillvals.get(d.dtype.str[1:])
                if fill_value is None and d.dtype.kind in ("SU"):
                    fill_value = default_fillvals.get("S1", None)

                if fill_value is None:
                    raise ValueError(
                        "Can't determine fill value for "
                        f"data type {d.dtype.str!r}"
                    )

        # REVIEW: getitem: `filled`: set 'asanyarray'
        # 'filled' has its own call to 'asanyarray', so we can
        # set 'asanyarray=False'.
        dx = d.to_dask_array(asanyarray=False)
        dx = dx.map_blocks(filled, fill_value=fill_value, dtype=d.dtype)
        d._set_dask(dx)

        return d

    def first_element(self):
        """Return the first element of the data as a scalar.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `last_element`, `second_element`

        **Performance**

        If possible, a cached value is returned. Otherwise the delayed
        operations needed to compute the element are executed, and
        cached for subsequent calls.

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
        try:
            return self._get_cached_elements()[0]
        except KeyError:
            item = self._item((slice(0, 1, 1),) * self.ndim)
            self._set_cached_elements({0: item})
            return item

    def second_element(self):
        """Return the second element of the data as a scalar.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `first_element`, `last_element`

        **Performance**

        If possible, a cached value is returned. Otherwise the delayed
        operations needed to compute the element are executed, and
        cached for subsequent calls.

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
        try:
            return self._get_cached_elements()[1]
        except KeyError:
            item = self._item(np.unravel_index(1, self.shape))
            self._set_cached_elements({1: item})
            return item

    def last_element(self):
        """Return the last element of the data as a scalar.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `first_element`, `second_element`

        **Performance**

        If possible, a cached value is returned. Otherwise the delayed
        operations needed to compute the element are executed, and
        cached for subsequent calls.

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
        try:
            return self._get_cached_elements()[-1]
        except KeyError:
            item = self._item((slice(-1, None, 1),) * self.ndim)
            self._set_cached_elements({-1: item})
            return item

    @_inplace_enabled(default=False)
    def flatten(self, axes=None, inplace=False):
        """Flatten specified axes of the data.

        Any subset of the axes may be flattened.

        The shape of the data may change, but the size will not.

        The flattening is executed in row-major (C-style) order. For
        example, the array ``[[1, 2], [3, 4]]`` would be flattened across
        both dimensions to ``[1 2 3 4]``.

        .. versionadded:: (cfdm) 1.7.11

        .. seealso:: `insert_dimension`, `squeeze`, `transpose`

        :Parameters:

            axes: (sequence of) `int`
                Select the axes to be flattened. By default all axes
                are flattened. Each axis is identified by its integer
                position. No axes are flattened if *axes* is an empty
                sequence.

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The flattened data, or `None` if the operation was
                in-place.

        **Examples**

        >>> import numpy as np
        >>> d = {{package}}.{{class}}(np.arange(24).reshape(1, 2, 3, 4))
        >>> d
        <{{repr}}{{class}}(1, 2, 3, 4): [[[[0, ..., 23]]]]>
        >>> print(d.array)
        [[[[ 0  1  2  3]
           [ 4  5  6  7]
           [ 8  9 10 11]]
          [[12 13 14 15]
           [16 17 18 19]
           [20 21 22 23]]]]

        >>> e = d.flatten()
        >>> e
        <{{repr}}{{class}}(24): [0, ..., 23]>
        >>> print(e.array)
        [ 0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18 19 20 21 22 23]

        >>> e = d.flatten([])
        >>> e
        <{{repr}}{{class}}(1, 2, 3, 4): [[[[0, ..., 23]]]]>

        >>> e = d.flatten([1, 3])
        >>> e
        <{{repr}}{{class}}(1, 8, 3): [[[0, ..., 23]]]>
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
        <{{repr}}{{class}}(4, 2, 3): [[[0, ..., 23]]]>
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

        # TODODASK: HDF5 chunks

        return d

    def chunk_indices(self):
        """Return indices that define each dask compute chunk.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `chunks`

        :Returns:

            `itertools.product`
                An iterator over tuples of indices of the data array.

        **Examples**

        >>> d = {{package}}.{{class}}(np.arange(405).reshape(3, 9, 15),
        ...     chunks=((1, 2), (9,), (4, 5, 6)))
        >>> d.npartitions
        6
        >>> for index in d.chunk_indices():
        ...     print(index)
        ...
        (slice(0, 1, None), slice(0, 9, None), slice(0, 4, None))
        (slice(0, 1, None), slice(0, 9, None), slice(4, 9, None))
        (slice(0, 1, None), slice(0, 9, None), slice(9, 15, None))
        (slice(1, 3, None), slice(0, 9, None), slice(0, 4, None))
        (slice(1, 3, None), slice(0, 9, None), slice(4, 9, None))
        (slice(1, 3, None), slice(0, 9, None), slice(9, 15, None))

        """
        from dask.utils import cached_cumsum

        chunks = self.chunks

        cumdims = [cached_cumsum(bds, initial_zero=True) for bds in chunks]
        indices = [
            [slice(s, s + dim) for s, dim in zip(starts, shapes)]
            for starts, shapes in zip(cumdims, chunks)
        ]
        return product(*indices)

    # REVIEW: getitem: `to_dask_array`: new keyword 'asanyarray'
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

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            apply_mask_hardness: `bool`, optional
                If True then force the mask hardness of the returned
                array to be that given by the `hardmask` attribute.

            {{asanyarray: `bool` or `None`, optional}}

        :Returns:

            `dask.array.Array`
                The dask array contained within the `{{class}}` instance.

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3, 4], 'm')
        >>> dx = d.to_dask_array()
        >>> dx
        >>> dask.array<array, shape=(4,), dtype=int64, chunksize=(4,), chunktype=numpy.ndarray>
        >>> dask.array.asanyarray(d) is dx
        True

        >>> d.to_dask_array(apply_mask_hardness=True)
        dask.array<harden_mask, shape=(4,), dtype=int64, chunksize=(4,), chunktype=numpy.ndarray>

        >>> d = {{package}}.{{class}}([1, 2, 3, 4], 'm', hardmask=False)
        >>> d.to_dask_array(apply_mask_hardness=True)
        dask.array<soften_mask, shape=(4,), dtype=int64, chunksize=(4,), chunktype=numpy.ndarray>

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
            #       to 'asanyarray', so we can don't need worry about
            #       setting another one.
        else:
            if asanyarray is None:
                asanyarray = self.__asanyarray__

            if asanyarray:
                # Add a new asanyarray layer to the output graph
                dx = dx.map_blocks(cfdm_asanyarray, dtype=dx.dtype)

        return dx

    def del_file_location(self, location):
        """Remove a file location in-place.

        All data definitions that reference files will have references
        to files in the given location removed from them.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `add_file_location`, `file_locations`

        :Parameters:

            location: `str`
                 The file location to remove.

        :Returns:

            `str`
                The removed location as an absolute path with no
                trailing path name component separator.

        **Examples**

        >>> d.del_file_location('/data/model/')
        '/data/model'

        """
        location = abspath(location).rstrip(sep)

        updated = False

        # REVIEW: getitem: `del_file_location`: set 'asanyarray'
        # The dask graph is never going to be computed, so we can set
        # 'asanyarray=False'.
        dsk = self.todict(asanyarray=False)
        for key, a in dsk.items():
            try:
                dsk[key] = a.del_file_location(location)
            except AttributeError:
                # This chunk doesn't contain a file array
                continue

            # This chunk contains a file array and the dask graph has
            # been updated
            updated = True

        if updated:
            dx = self.to_dask_array(asanyarray=False)
            dx = da.Array(dsk, dx.name, dx.chunks, dx.dtype, dx._meta)
            self._set_dask(dx, clear=_NONE, asanyarray=None)

        return location

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

        if rtol is None:
            rtol = self._rtol
        else:
            rtol = float(rtol)

        if atol is None:
            atol = self._atol
        else:
            atol = float(atol)

        dx = d.to_dask_array()
        dx = da.ma.masked_values(dx, value, rtol=rtol, atol=atol)
        d._set_dask(dx)
        return d

    def cull_graph(self):
        """Remove unnecessary tasks from the dask graph in-place.

        **Performance**

        An unnecessary task is one which does not contribute to the
        computed result. Such tasks are always automatically removed
        (culled) at compute time, but removing them beforehand might
        improve performance by reducing the amount of work done in
        later steps.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `dask.optimization.cull`

        :Returns:

            `None`

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3, 4, 5], chunks=3)
        >>> d = d[:2]
        >>> dict(d.to_dask_array().dask)
        {('array-21ea057f160746a3d3f0943bba945460', 0): array([1, 2, 3]),
         ('array-21ea057f160746a3d3f0943bba945460', 1): array([4, 5]),
         ('getitem-3e4edac0a632402f6b45923a6b9d215f',
          0): (<function dask.array.chunk.getitem(obj, index)>, ('array-21ea057f160746a3d3f0943bba945460',
           0), (slice(0, 2, 1),))}
        >>> d.cull_graph()
        >>> dict(d.to_dask_array().dask)
        {('getitem-3e4edac0a632402f6b45923a6b9d215f',
          0): (<function dask.array.chunk.getitem(obj, index)>, ('array-21ea057f160746a3d3f0943bba945460',
           0), (slice(0, 2, 1),)),
         ('array-21ea057f160746a3d3f0943bba945460', 0): array([1, 2, 3])}

        """
        # REVIEW: getitem: `cull_graph`: set 'asanyarray'
        dx = self.to_dask_array(asanyarray=False)
        dsk, _ = cull(dx.dask, dx.__dask_keys__())
        dx = da.Array(dsk, name=dx.name, chunks=dx.chunks, dtype=dx.dtype)
        self._set_dask(dx, clear=_NONE, asanyarray=None)

    @_inplace_enabled(default=False)
    def squeeze(self, axes=None, inplace=False):
        """Remove size 1 axes from the data array.

        By default all size 1 axes are removed, but particular axes
        may be selected with the keyword arguments.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `flatten`, `insert_dimension`, `transpose`

        :Parameters:

            axes: (sequence of) int, optional
                Select the axes. By default all size 1 axes are
                removed. The *axes* argument may be one, or a
                sequence, of integers that select the axis
                corresponding to the given position in the list of
                axes of the data array.

                No axes are removed if *axes* is an empty sequence.

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The squeezed data array.

        **Examples**

        >>> v.shape
        (1,)
        >>> v.squeeze()
        >>> v.shape
        ()

        >>> v.shape
        (1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1)
        >>> v.squeeze((0,))
        >>> v.shape
        (2, 1, 3, 1, 4, 1, 5, 1, 6, 1)
        >>> v.squeeze(1)
        >>> v.shape
        (2, 3, 1, 4, 1, 5, 1, 6, 1)
        >>> v.squeeze([2, 4])
        >>> v.shape
        (2, 3, 4, 5, 1, 6, 1)
        >>> v.squeeze([])
        >>> v.shape
        (2, 3, 4, 5, 1, 6, 1)
        >>> v.squeeze()
        >>> v.shape
        (2, 3, 4, 5, 6)

        """
        d = _inplace_enabled_define_and_cleanup(self)

        if not d.ndim:
            if axes or axes == 0:
                raise ValueError(
                    "Can't squeeze: Can't remove an axis from "
                    f"scalar {d.__class__.__name__}"
                )

            if inplace:
                d = None

            return d

        shape = d.shape

        if axes is None:
            iaxes = tuple([i for i, n in enumerate(shape) if n == 1])
        else:
            iaxes = d._parse_axes(axes)

            # Check the squeeze axes
            for i in iaxes:
                if shape[i] > 1:
                    raise ValueError(
                        f"Can't squeeze {d.__class__.__name__}: "
                        f"Can't remove axis of size {shape[i]}"
                    )

        if not iaxes:
            # Short circuit if the squeeze is a null operation
            return d

        # Still here? Then the data array is not scalar and at least
        # one size 1 axis needs squeezing.
        dx = d.to_dask_array()
        dx = dx.squeeze(axis=iaxes)

        # Squeezing a dimension doesn't affect the cached elements
        d._set_dask(dx, clear=_ALL ^ _CACHE)

        # Remove the squeezed axes names
        d._axes = [axis for i, axis in enumerate(d._axes) if i not in iaxes]

        # TODODASK: HDF5 chunks
        return d

    # REVIEW: getitem: `todict`: new keywords 'apply_mask_hardness', 'asanyarray'
    def todict(
        self, optimize_graph=True, apply_mask_hardness=False, asanyarray=None
    ):
        """Return a dictionary of the dask graph key/value pairs.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `tolist`, `to_dask_array`

        :Parameters:

            `optimize_graph`: `bool`
                If True, the default, then prior to being converted to
                a dictionary, the graph is optimised to remove unused
                chunks. Note that optimising the graph can add a
                considerable performance overhead.

            apply_mask_hardness: `bool`, optional
                If True then force the mask hardness of the returned
                array to be that given by the `hardmask` attribute.

            asanyarray: `bool` or `None`, optional
                If True then add a final operation to the Dask graph
                that converts chunks to `numpy` arrays, but only if
                chunk's array object has an `__asanyarray__` attribute
                that is also `True`. If False then do not do this. If
                `None`, the default, then the final operation is added
                if the `{{class}}` object's `__asanyarray__` attribute is
                `True`.

        :Returns:

            `dict`
                The dictionary of the dask graph key/value pairs.

        **Examples**

        >>> d = {{package}}.{{class}}([1, 2, 3, 4], chunks=2)
        >>> d.todict()
        {('array-2f41b21b4cd29f757a7bfa932bf67832', 0): array([1, 2]),
         ('array-2f41b21b4cd29f757a7bfa932bf67832', 1): array([3, 4])}
        >>> e = d[0]
        >>> e.todict()
        {('getitem-153fd24082bc067cf438a0e213b41ce6',
          0): (<function dask.array.chunk.getitem(obj, index)>, ('array-2f41b21b4cd29f757a7bfa932bf67832',
           0), (slice(0, 1, 1),)),
         ('array-2f41b21b4cd29f757a7bfa932bf67832', 0): array([1, 2])}
        >>> e.todict(optimize_graph=False)
        {('array-2f41b21b4cd29f757a7bfa932bf67832', 0): array([1, 2]),
         ('array-2f41b21b4cd29f757a7bfa932bf67832', 1): array([3, 4]),
         ('getitem-153fd24082bc067cf438a0e213b41ce6',
          0): (<function dask.array.chunk.getitem(obj, index)>, ('array-2f41b21b4cd29f757a7bfa932bf67832',
           0), (slice(0, 1, 1),))}

        """
        dx = self.to_dask_array(
            apply_mask_hardness=apply_mask_hardness, asanyarray=asanyarray
        )

        if optimize_graph:
            return collections_to_dsk((dx,), optimize_graph=True)

        return dict(collections_to_dsk((dx,), optimize_graph=False))

    def tolist(self):
        """Return the data as a scalar or (nested) list.

        Returns the data as an ``N``-levels deep nested list of Python
        scalars, where ``N`` is the number of data dimensions.

        If ``N`` is 0 then, since the depth of the nested list is 0,
        it will not be a list at all, but a simple Python scalar.

        .. versionadded:: (cfdm) NEXTVERSION

        .. sealso:: `todict`, `to_dask_array`

        :Returns:

            `list` or scalar
                The (nested) list of array elements, or a scalar if
                the data has 0 dimensions.

        **Examples**

        >>> d = {{package}}.{{class}}(9)
        >>> d.tolist()
        9

        >>> d = {{package}}.{{class}}([1, 2])
        >>> d.tolist()
        [1, 2]

        >>> d = {{package}}.{{class}}(([[1, 2], [3, 4]]))
        >>> d.tolist()
        [[1, 2], [3, 4]]

        >>> d.equals({{package}}.{{class}}(d.tolist()))
        True

        """
        return self.array.tolist()

    @_inplace_enabled(default=False)
    def transpose(self, axes=None, inplace=False):
        """Permute the axes of the data array.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `flatten', `insert_dimension`, `squeeze`

        :Parameters:

            axes: (sequence of) `int`
                The new axis order of the data array. By default the order
                is reversed. Each axis of the new order is identified by
                its original integer position.

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`

        **Examples**

        >>> d.shape
        (19, 73, 96)
        >>> d.transpose()
        >>> d.shape
        (96, 73, 19)
        >>> d.transpose([1, 0, 2])
        >>> d.shape
        (73, 96, 19)
        >>> d.transpose((-1, 0, 1))
        >>> d.shape
        (19, 73, 96)

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

        data_axes = d._axes
        d._axes = [data_axes[i] for i in iaxes]

        dx = d.to_dask_array()
        try:
            dx = da.transpose(dx, axes=axes)
        except ValueError:
            raise ValueError(
                f"Can't transpose: Axes don't match array: {axes}"
            )

        d._set_dask(dx)

        # TODODASk: HDF5 chunks
        return d

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

                .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `{{class}}`
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

    @_inplace_enabled(default=False)
    def reshape(self, *shape, merge_chunks=True, limit=None, inplace=False):
        """Change the shape of the data without changing its values.

        It assumes that the array is stored in row-major order, and
        only allows for reshapings that collapse or merge dimensions
        like ``(1, 2, 3, 4) -> (1, 6, 4)`` or ``(64,) -> (4, 4, 4)``.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            shape: `tuple` of `int`, or any number of `int`
                The new shape for the data, which should be compatible
                with the original shape. If an integer, then the
                result will be a 1-d array of that length. One shape
                dimension can be -1, in which case the value is
                inferred from the length of the array and remaining
                dimensions.

            merge_chunks: `bool`
                When True (the default) merge chunks using the logic
                in `dask.array.rechunk` when communication is
                necessary given the input array chunking and the
                output shape. When False, the input array will be
                rechunked to a chunksize of 1, which can create very
                many tasks. See `dask.array.reshape` for details.

            limit: int, optional
                The maximum block size to target in bytes. If no limit
                is provided, it defaults to a size in bytes defined by
                the `cf.chunksize` function.

            {{inplace: `bool`, optional}}

        :Returns:

            `Data` or `None`
                 The reshaped data, or `None` if the operation was
                 in-place.

        **Examples**

        >>> d = cf.Data(np.arange(12))
        >>> print(d.array)
        [ 0  1  2  3  4  5  6  7  8  9 10 11]
        >>> print(d.reshape(3, 4).array)
        [[ 0  1  2  3]
         [ 4  5  6  7]
         [ 8  9 10 11]]
        >>> print(d.reshape((4, 3)).array)
        [[ 0  1  2]
         [ 3  4  5]
         [ 6  7  8]
         [ 9 10 11]]
        >>> print(d.reshape(-1, 6).array)
        [[ 0  1  2  3  4  5]
         [ 6  7  8  9 10 11]]
        >>>  print(d.reshape(1, 1, 2, 6).array)
        [[[[ 0  1  2  3  4  5]
           [ 6  7  8  9 10 11]]]]
        >>> print(d.reshape(1, 1, -1).array)
        [[[[ 0  1  2  3  4  5  6  7  8  9 10 11]]]]

        """
        d = _inplace_enabled_define_and_cleanup(self)
        dx = d.to_dask_array()
        dx = dx.reshape(*shape, merge_chunks=merge_chunks, limit=limit)

        # Set axes when the new array has more dimensions than self
        axes = None
        ndim0 = self.ndim
        if not ndim0:
            axes = generate_axis_identifiers(dx.ndim)
        else:
            diff = dx.ndim - ndim0
            if diff > 0:
                axes = list(self._axes)
                for _ in range(diff):
                    axes.insert(0, new_axis_identifier(tuple(axes)))

        if axes is not None:
            d._axes = axes

        d._set_dask(dx)

        # cf-python: cyclic ?

        return d

    @_inplace_enabled(default=False)
    def sum(
        self,
        axes=None,
        squeeze=False,
    ):
        """Calculate sum values.

        Calculates the sum value or the sum values along axes.

        See
        https://ncas-cms.github.io/cf-python/analysis.html#collapse-methods
        for mathematical definitions.

        .. versionadded:: (cfdm) TODODASK

         ..seealso:: `max`, `min`

        :Parameters:

            {{collapse axes: (sequence of) `int`, optional}}

        :Returns:

            `{{class}}` or `None`
                The collapsed data, or `None` if the operation was
                in-place.

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
        <{{repr}}{{class}}(1, 1): [[62]] K>

        >>> w = np.linspace(1, 2, 3)
        >>> print(w)
        [1.  1.5 2. ]
        >>> d.sum(weights={{package}}.{{class}}(w, 'm'))
        <{{repr}}{{class}}(1, 1): [[97.0]] K>

        """
        # Parse the axes. By default flattened input is used.
        try:
            axes = self._parse_axes(axes)
        except ValueError as error:
            raise ValueError(f"Can't sum data: {error}")

        d = self.copy()
        dx = d.to_dask_array()
        dx = dx.sum(axis=axes, keepdims=not squeeze)
        d._set_dask(dx)

        # TODODASK: _axes

        # TODODASK: HDF5 chunks
        return d

    # ----------------------------------------------------------------
    # Aliases
    # ----------------------------------------------------------------
    @property
    def dtarray(self):
        """Alias for `datetime_array`"""
        return self.datetime_array