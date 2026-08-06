from . import ErrorCorrelationModel, Quantization, core, mixin
from .decorators import _inplace_enabled, _inplace_enabled_define_and_cleanup


class UncertaintyAncillary(
    mixin.QuantizationMixin,
    mixin.NetCDFVariable,
    mixin.PropertiesData,
    mixin.Files,
    core.UncertaintyAncillary,
):
    """An uncertainty ancillary construct of the CF data model.

    TODOU (copy from core/uncertaintyancillary.py)

    :NetCDF interface:

    {{netCDF variable}}

    {{netCDF dataset chunks}}

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __new__(cls, *args, **kwargs):
        """Store component classes."""
        instance = super().__new__(cls)
        instance._ErrorCorrelationModel = ErrorCorrelationModel
        instance._Quantization = Quantization
        return instance

    def creation_commands(
        self,
        representative_data=False,
        namespace=None,
        indent=0,
        string=True,
        name="c",
        data_name="d",
        correlation_model_name="p",
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

        >>> x = {{package}}.CellMeasure(
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

        correlation_model = self.error_correlation_model
        if correlation_model:
            out.extend(
                correlation_model.creation_commands(
                    string=False,
                    indent=indent,
                    namespace=namespace0,
                    name=correlation_model_name,
                    header=False,
                )
            )
            out.append(
                f"{name}.set_correlation_model({correlation_model_name})"
            )

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
            _title = "Uncertainty Ancillary: " + self.identity(default="")

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

        # Error-correlation model
        error_correlation_model = self.error_correlation_model
        if error_correlation_model:
            ecm = error_correlation_model.dump(
                display=False, _level=_level + 1
            )
            string.append(ecm)

        return "\n".join(string)

    def identity(self, default=""):
        """Return the canonical identity.

        By default the identity is the first found of the following:

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
        error_correlation_model = self.error_correlation_model

        n = error_correlation_model.get_structure(None)
        if n is not None:
            return f"structure:{n}"

        n = error_correlation_model.get_comment(None)
        if n is not None:
            return f"comment:{n}"

        n = self.get_distribution_parameter(None)
        if n is not None:
            return f"distribution_parameter:{n}"

        return super().identity(default=default)

    def identities(self, generator=False, **kwargs):
        """Return all possible identities.

        The identities comprise:

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

        >>> c.identities()
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
        error_correlation_model = self.error_correlation_model

        n = error_correlation_model.get_structure(None)
        if n is not None:
            pre = ((f"structure:{n}",),)
            pre0 = kwargs.pop("pre", None)
            if pre0:
                pre = tuple(pre0) + pre

            kwargs["pre"] = pre

        n = error_correlation_model.get_comment(None)
        if n is not None:
            pre = ((f"comment:{n}",),)
            pre0 = kwargs.pop("pre", None)
            if pre0:
                pre = tuple(pre0) + pre

            kwargs["pre"] = pre

        n = self.get_distribution_parameter(None)
        if n is not None:
            pre = ((f"distribution_parameter:{n}",),)
            pre0 = kwargs.pop("pre", None)
            if pre0:
                pre = tuple(pre0) + pre

            kwargs["pre"] = pre

        if n is not None:
            return f"distribution_parameter:{n}"

        return super().identities(generator=generator, **kwargs)

    @_inplace_enabled(default=False)
    def transpose(self, axes=None, inplace=False):
        """Permute the axes of the data array.

        TODOU If axes have only been provided for the first half of the data
        array dimensions, then it is assumed that the data are error
        correlations, and the transpose axes for the second half of
        the data array dimensions are automatically added, e.g. ``[1,
        0]`` would become ``[1, 0, 3, 2]``.

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
        ndim = self.ndim
        if axes is None:
            iaxes = list(range(ndim - 1, -1, -1))
        else:
            iaxes = self._parse_axes(axes)
            if len(iaxes) != ndim:
                raise ValueError("TODOU")

        # For an error-correlation uncertainty ancillary, the ranspose
        # axes need to be propageted to the trailing dimensions of the
        # data array
        if self.get_distribution_parameter() == "error_correlation":
            iaxes = iaxes + [i + ndim for i in iaxes]

        c = _inplace_enabled_define_and_cleanup(self)
        super().transpose(iaxes, inplace=True)
        return c
