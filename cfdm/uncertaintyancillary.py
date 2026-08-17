import logging

from . import Quantization, UncertaintyAncillaryParameterisation, core, mixin
from .decorators import (
    _inplace_enabled,
    _inplace_enabled_define_and_cleanup,
    _manage_log_level_via_verbosity,
)
from .functions import parse_indices

logger = logging.getLogger(__name__)


class UncertaintyAncillary(
    mixin.QuantizationMixin,
    mixin.NetCDFDataInAttribute,
    mixin.NetCDFVariable,
    mixin.NetCDFDimension,
    mixin.PropertiesData,
    mixin.Files,
    core.UncertaintyAncillary,
):
    """An uncertainty ancillary construct of the CF data model.

    TODOU (copy from core/uncertaintyancillary.py)

    :NetCDF interface:

    {{netCDF variable}}

    The name of a trailing netCDF dimension spanned by an
    two-dimensionsal matrix of error-correlations (which does not
    correspond to directly to a domain axis construct) may be accessed
    with the `nc_set_dimension`, `nc_get_dimension`,
    `nc_del_dimension`, and `nc_has_dimension` methods.

    If the leading dimension of the matix corresponds to more than one
    domain axis construct, then its name will be that of the trailing
    dimension name but, less any digits at the end; or if trailing
    dimension name has no digits at the end then digits will be
    added. For instance, if the trailing dimension name is
    ``'latlon1'``, then the leading dimension name could be
    ``'latlon'``; and vice versa.

    {{netCDF variable group}}

    {{netCDF dataset chunks}}

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __new__(cls, *args, **kwargs):
        """Store component classes."""
        instance = super().__new__(cls)
        instance._UncertaintyAncillaryParameterisation = (
            UncertaintyAncillaryParameterisation
        )
        instance._Quantization = Quantization
        return instance

    def __getitem__(self, indices):
        """Return a subspace defined by indices.

        f.__getitem__(indices) <==> f[indices]

        For an error-correlation uncertainty ancillary, only indices
        for the leading half of the data array dimensions may be
        provided, and these are automatically propagated to the
        trailing dimensions in such a way as as to guarantee that the
        symmetrical structure of the data array is preserved in the
        subspaced construct. For instance, if the construct shape is
        ``(20, 30)`` and the data array shape is ``(20, 30, 20, 30)``,
        then *indices* of ``(slice(2:5), [1, 3])`` will result in a
        data array shape of ``(3, 2, 3, 2)``; and *indices* of ``0``
        will result in a data array shape of ``(1, 30, 1, 30)`.

        .. versionadded:: (cfdm) NEXTVERSION

        """
        # For an error-correlation uncertainty ancillary, the indices
        # need to be propagated to the trailing dimensions of the data
        # array.
        data = self.get_data(None, _units=False, _fill_value=False)
        if data is not None and self.has_trailing_dimensions():
            indices = parse_indices(
                self.shape, indices, keepdims=data.__keepdims_indexing__
            )
            indices = tuple(indices)
            indices *= 2

        return super().__getitem__(indices)

    def creation_commands(
        self,
        representative_data=False,
        namespace=None,
        indent=0,
        string=True,
        name="c",
        data_name="d",
        parameterisation_name="p",
        header=True,
    ):
        """Returns the commands to create the cell measure construct.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `{{package}}.Data.creation_commands`,
                     `{{package}}.Field.creation_commands`

        :Parameters:

            {{representative_data: `bool`, optional}}

            {{namespace: `str`, optional}}

            {{indent: `int`, optional}}

            {{string: `bool`, optional}}

            {{name: `str`, optional}}

            {{data_name: `str`, optional}}

            {{header: `bool`, optional}}

        :Returns:

            {{returns creation_commands}}

        **Examples**

        >>> x = {{package}}.TODOUCellMeasure(
        ...     measure='area',
        ...     properties={'units': 'm2'}
        ... )
        >>> x.set_data([100345.5, 123432.3, 101556.8])
        >>> print(x.creation_commands(header=False))
        c = {{package}}.CellMeasure()
        c.set_properties({'units': 'm2'})
        data = {{package}}.Data([100345.5, 123432.3, 101556.8], units='m2', dtype='f8')
        c.set_data(data)
        c.set_measure('area')

        """
        namespace0 = namespace
        if namespace is None:
            namespace = self._package() + "."
        elif namespace and not namespace.endswith("."):
            namespace += "."

        out = super().creation_commands(
            representative_data=representative_data,
            indent=indent,
            namespace=namespace,
            string=False,
            name=name,
            data_name=data_name,
            header=header,
        )

        try:
            out.append(
                f"{name}.set_trailing_dimensions"
                f"({self.has_trailing_dimensions()})"
            )
        except AttributeError:
            pass

        parameterisation = self.parameterisation
        if parameterisation:
            out.extend(
                parameterisation.creation_commands(
                    string=False,
                    indent=indent,
                    namespace=namespace0,
                    name=parameterisation_name,
                    header=False,
                )
            )
            out.append(f"{name}.set_parameterisation({parameterisation_name})")

        if string:
            indent = " " * indent
            out[0] = indent + out[0]
            out = ("\n" + indent).join(out)

        return out

    def dump(
        self,
        data=None,
        display=True,
        _omit_properties=None,
        _key=None,
        _level=0,
        _title=None,
        _axes=None,
        _axis_names=None,
        _construct_names=None,
    ):
        """A full description of the uncertainty ancillary construct.

        Returns a description of all properties, including those of
        components, and provides selected values of all data arrays.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            {{data: `bool` or `None`, optional}}

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            {{returns dump}}

        """
        if _title is None:
            _title = "Uncertainty Ancillary: "
            if _construct_names and _key in _construct_names:
                _title += _construct_names[_key]
            else:
                _title += self.identity(default="")

        string = super().dump(
            data=data,
            display=False,
            _key=_key,
            _omit_properties=_omit_properties,
            _level=_level,
            _title=_title,
            _axes=_axes,
            _axis_names=_axis_names,
        )

        string = [string]

        # Data parameterisation
        parameterisation = self.parameterisation
        if parameterisation:
            p = parameterisation.dump(
                display=False,
                _level=_level + 1,
                _construct_names=_construct_names,
            )
            string.append(p)

        return "\n".join(string)

    @_manage_log_level_via_verbosity
    def equals(
        self,
        other,
        rtol=None,
        atol=None,
        verbose=None,
        ignore_data_type=False,
        ignore_fill_value=False,
        ignore_properties=None,
        ignore_compression=True,
        ignore_type=False,
    ):
        """Whether two instances are the same.

        Equality is strict by default. This means that:

        * the same descriptive properties must be present, with the
          same values and data types, and vector-valued properties
          must also have same the size and be element-wise equal (see
          the *ignore_properties* and *ignore_data_type* parameters),
          and

        ..

        * if there are data arrays then they must have same shape and
          data type, the same missing data mask, and be element-wise
          equal (see the *ignore_data_type* parameter).

        {{equals tolerance}}

        Any type of object may be tested but, in general, equality is
        only possible with another object of the same type, or a
        subclass of one. See the *ignore_type* parameter.

        {{equals compression}}

        {{equals netCDF}}

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            other:
                The object to compare for equality.

            {{atol: number, optional}}

            {{rtol: number, optional}}

            {{ignore_fill_value: `bool`, optional}}

            {{verbose: `int` or `str` or `None`, optional}}

            {{ignore_properties: (sequence of) `str`, optional}}

            {{ignore_data_type: `bool`, optional}}

            {{ignore_compression: `bool`, optional}}

            {{ignore_type: `bool`, optional}}

        :Returns:

            `bool`
                Whether the two instances are equal.

        **Examples**

        >>> c.equals(c)
        True
        >>> c.equals(c.copy())
        True
        >>> c.equals(None)
        False

        """
        # Check the parameterisation (in the absence of domains)
        parameterisation0 = self.parameterisation
        parameterisation1 = other.parameterisation
        if not parameterisation0.equals(
            parameterisation1,
            rtol=rtol,
            atol=atol,
            verbose=verbose,
            ignore_type=ignore_type,
        ):
            logger.info(
                f"{self.__class__.__name__}: Different data parameterisations "
                f"({parameterisation0!r} != {parameterisation1!r})"
            )  # pragma: no cover
            return False

        if not super().equals(
            other,
            rtol=rtol,
            atol=atol,
            verbose=verbose,
            ignore_fill_value=ignore_fill_value,
            ignore_data_type=ignore_data_type,
            ignore_properties=ignore_properties,
            ignore_compression=ignore_compression,
            ignore_type=ignore_type,
        ):
            return False

        # Still here? Then the two instances are as equal as can be
        # ascertained in the absence of domains.
        return True

    def identity(self, default=""):
        """Return the canonical identity.

        TODOU By default the identity is the first found of the following:

        * The ``standard_name`` property.
        * The ``coverage_interval`` property, preceded by
          'ccoverage_interval='.
        * The ``cf_role`` property, preceded by 'cf_role='.
        * The ``long_name`` property, preceded by 'long_name='.
        * The netCDF variable name, preceded by 'ncvar%'.
        * The value of the default parameter.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `identities`

        :Parameters:

            default: optional
                If no identity can be found then return the value of the
                default parameter.

        :Returns:

                The identity.

        **Examples**

        TODOU
        >>> f = {{package}}.example_field(1)
        >>> c = f.get_construct('cellmeasure0')
        >>> c.get_measure()
        'area'

        >>> c.properties()
        {'units': 'km2'}
        >>> c.nc_get_variable()
        'cell_measure'
        >>> c.identity(default='no identity')
        'measure:area'

        >>> c.del_measure()
        'area'
        >>> c.identity()
        'ncvar%cell_measure'
        >>> c.nc_del_variable()
        'cell_measure'
        >>> c.identity()
        ''
        >>> c.identity(default='no identity')
        'no identity'

        """
        n = self.parameterisation.get_parameter(
            "error_correlation_structure", None
        )
        if n is not None:
            return f"error_correlation_structure:{n}"

        n = self.get_property("comment", None)
        if n is not None:
            return f"comment:{n}"

        return super().identity(default=default)

    def identities(self, generator=False, **kwargs):
        """Return all possible identities.

        TODOU The identities comprise:

        * The ``standard_name`` property.
        * All properties, preceded by the property name and a equals
          sign, e.g. ``'long_name=Air temperature'``.
        * The netCDF variable name, preceded by ``'ncvar%'``.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `identity`

        :Parameters:

            generator: `bool`, optional
                If True then return a generator for the identities,
                rather than a list.

            kwargs: optional
                Additional configuration parameters that may be used
                by subclasses.

        :Returns:

            `list` or generator
                The identities.

        **Examples**

        >>> TODOU c.identities()
        ['coverage_interval=standard_deviation',
         'coverage_probability=0.6827',
         'probability_distribution=gaussian',
         'units=1',
         'ncvar%uncertainty0']

        >>> for i in c.identities(generator=True):
        ...     print(i)
        ...
        coverage_interval=standard_deviation
        coverage_probability=0.6827
        probability_distribution=gaussian
        units=1
        ncvar%uncertainty

        """
        n = self.parameterisation.get_parameter(
            "error_correlation_structure", None
        )
        if n is not None:
            pre = ((f"error_correlation_structure:{n}",),)
            pre0 = kwargs.pop("pre", None)
            if pre0:
                pre = tuple(pre0) + pre

            kwargs["pre"] = pre

        g = self._iter(
            body=self._identities_iter(
                top_properties=("comment", "long_name")
            ),
            **kwargs,
        )
        if generator:
            return g

        return list(g)

    @_inplace_enabled(default=False)
    def insert_dimension(self, position=0, inplace=False):
        """Expand the shape of the data array.

        Inserts a new size 1 axis into the data array.

        For an error-correlation uncertainty ancillary, only a
        poisiton in the leading half of the data array dimensions may
        be provided, and this is automatically propagated to the
        trailing dimensions in such a way as as to guarantee that the
        symmetrical structure of the data array is preserved in the
        construct. For instance, if the construct shape is ``(20,
        30)`` and the data array shape is ``(20, 30, 20, 30)``, then
        *position* of ``1`` will result in a data array shape of
        ``(20, 1, 30, 20, 1, 30)``.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `squeeze`, `transpose`

        :Parameters:

            position: `int`, optional
                Specify the position that the new axis will have in
                the data array. By default the new axis has position
                0, the slowest varying position. Negative integers
                counting from the last position are allowed.

                *Example:*
                  ``position=2``

                *Example:*
                  ``position=-1``

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                A new instance with expanded data axes. If the
                operation was in-place then `None` is returned.

        **Examples**

        >>> TODOU f.shape
        (19, 73, 96)
        >>> f.insert_dimension(position=3).shape
        (19, 73, 96, 1)
        >>> f.insert_dimension(position=-1).shape
        (19, 73, 1, 96)

        """
        c = _inplace_enabled_define_and_cleanup(self)

        if self.has_data() and self.has_trailing_dimensions():
            # An axis in the trailing dimensions also needs to be
            # inserted
            try:
                ndim = c.ndim
            except AttributeError:
                return c

            original_ndim = c.ndim
            if -ndim - 1 <= position < 0:
                position += original_ndim + 1
            elif not 0 <= position <= ndim:
                raise ValueError(
                    f"Can't insert dimension: Invalid position {position!r}"
                )

            positions = (position, position + ndim + 1)
        else:
            positions = (position,)

        for p in positions:
            super(UncertaintyAncillary, c).insert_dimension(p, inplace=True)

        return c

    @_inplace_enabled(default=False)
    def squeeze(self, axes=None, inplace=False):
        """Remove size one axes from the data array.

        TODOU By default all size one axes are removed, but particular size
        one axes may be selected for removal.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `insert_dimension`, `transpose`

        :Parameters:

            axes: (sequence of) `int`, optional
                The positions of the size one axes to be removed. By
                default all size one axes are removed.

                {{axes int examples}}

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                A new instance with removed size 1 one data axes. If
                the operation was in-place then `None` is returned.

        **Examples**


        >>> TODOU f = {{package}}.{{class}}()
        >>> d = {{package}}.Data(numpy.arange(7008).reshape((1, 73, 1, 96)))
        >>> f.set_data(d)
        >>> f.shape
        (1, 73, 1, 96)
        >>> f.squeeze().shape
        (73, 96)
        >>> f.squeeze(0).shape
        (73, 1, 96)
        >>> f.squeeze([-3, 2]).shape
        (73, 96)

        """
        c = _inplace_enabled_define_and_cleanup(self)

        if (
            axes is not None
            and self.has_data()
            and self.has_trailing_dimensions()
        ):
            # Axes in the trailing dimensions also need to be
            # squeezed
            try:
                ndim = c.ndim
            except AttributeError:
                return c

            if isinstance(axes, int):
                axes = (axes,)

            axes0 = []
            for axis in axes:
                if 0 <= axis < ndim:
                    axes0.append(axis)
                elif -ndim <= axis < 0:
                    axes0.append(axis + ndim)
                else:
                    raise ValueError(
                        f"Can't squeeze: Axes don't match construct: {axes}"
                    )

            axes = axes0 + [i + ndim for i in axes0]

        super(UncertaintyAncillary, c).squeeze(axes, inplace=True)
        return c

    @_inplace_enabled(default=False)
    def transpose(self, axes=None, inplace=False):
        """Permute the axes of the data array.

        For an error-correlation uncertainty ancillary, only axes for
        the leading half of the data array dimensions may be provided,
        and these are automatically propagated to the trailing
        dimensions in such a way as as to guarantee that the
        symmetrical structure of the data array is preserved in the
        tranposed construct. For instance, if the construct shape is
        ``(20, 30)`` and the data array shape is ``(20, 30, 20, 30)``,
        then *axes* of ``[1, 0]`` will result in a data array shape of
        ``(30, 20, 30, 20)``.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `insert_dimension`, `squeeze`

        :Parameters:

            axes: (sequence of) `int`, optional
                The new axis order. By default the order is reversed.

                {{axes int examples}}

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The new construct with permuted data axes. If the
                operation was in-place then `None` is returned.

        """
        c = _inplace_enabled_define_and_cleanup(self)

        if self.has_data() and self.has_trailing_dimensions():
            # Axes in the trailing dimensions also need to be
            # transposed
            try:
                ndim = c.ndim
            except AttributeError:
                return c

            if axes is None:
                axes0 = list(range(ndim - 1, -1, -1))
            else:
                axes0 = c._parse_axes(axes)
                if len(axes0) != ndim:
                    raise ValueError(
                        f"Can't transpose: Axes don't match construct: {axes}"
                    )

            axes = axes0 + [i + ndim for i in axes0]

        super(UncertaintyAncillary, c).transpose(axes, inplace=True)
        return c
