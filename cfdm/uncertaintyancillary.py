from . import Quantization, core, mixin


class UncertaintyAncillary(
    mixin.QuantizationMixin,
    mixin.NetCDFVariable,
    mixin.PropertiesData,
    mixin.Files,
    core.UncertaintyAncillary,
):
    """An uncertainty ancillary construct of the CF data model.

    TODOU (copy from appendix I when merged)

    **NetCDF interface**

    {{netCDF variable}}

    {{netCDF dataset chunks}}

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __new__(cls, *args, **kwargs):
        """Store component classes."""
        instance = super().__new__(cls)
        instance._Quantization = Quantization
        return instance

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

        return super().dump(
            data=data,
            display=display,
            _key=_key,
            _omit_properties=_omit_properties,
            _level=_level,
            _title=_title,
            _axes=_axes,
            _axis_names=_axis_names,
        )

    
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
        n = self.get_property("standard_name", None)
        if n is not None:
            return n

        for prop in ("coverage_interval", "cf_role", "long_name"):
            n = self.get_property(prop, None)
            if n is not None:
                return f"{prop}={n}"

        n = self.nc_get_variable(None)
        if n is not None:
            return f"ncvar%{n}"

        return default

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
        g = self._iter(
            body=self._identities_iter(
                top_properties=("coverage_interval", "cf_role", "long_name")
            ),
            **kwargs
        )
        if generator:
            return g

        return list(g)
