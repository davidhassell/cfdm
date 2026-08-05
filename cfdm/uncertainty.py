from . import Quantization, core, mixin
from .decorators import _display_or_return
from .probabilitydistribution import ProbabilityDistribution


class Uncertainty(
    mixin.QuantizationMixin,
    mixin.NetCDFVariable,
    mixin.PropertiesData,
    mixin.Files,
    core.Uncertainty,
):
    """An uncertainty construct of the CF data model.

    TODOU (copy from appendix I when merged)

    **NetCDF interface**

    {{netCDF variable}}

    {{netCDF dataset chunks}}

    .. versionadded:: (cfdm) NEXTVERSION

    """

    __ProbabilityDistribution = ProbabilityDistribution

    def __new__(cls, *args, **kwargs):
        """Store component classes."""
        instance = super().__new__(cls)
        instance._Quantization = Quantization
        instance._ProbabilityDistribution = ProbabilityDistribution
        return instance

    def __str__(self):
        """Called by the `str` built-in function.

        x.__str__() <==> str(x)

        .. versionadded:: (cfdm) 1.7.0

        """
        return self.identity(default=self.nc_get_variable(""))

    def creation_commands(
        self,
        representative_data=False,
        namespace=None,
        indent=0,
        string=True,
        name="c",
        probability_distribution_name="p",
        data_name="data",
        header=True,
    ):
        """Returns the commands to create the cell measure construct.

        .. versionadded:: (cfdm) 1.8.7.0

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

        probability_distribution = self.probability_distribution
        if probability_distribution:
            out.extend(
                probability_distribution.creation_commands(
                    string=False,
                    indent=indent,
                    namespace=namespace0,
                    name=probability_distribution_name,
                    header=False,
                )
            )
            out.append(
                f"{name}.set_probability_distribution({probability_distribution_name})"
            )

        if string:
            indent = " " * indent
            out[0] = indent + out[0]
            out = ("\n" + indent).join(out)

        return out

    @_display_or_return
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
        """A full description of the uncertainty construct.

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
        string = [
            super().dump(
                data=data,
                display=False,
                _key=_key,
                _omit_properties=_omit_properties,
                _level=_level,
                _title=_title,
                _axes=_axes,
                _axis_names=_axis_names,
            )
        ]

        if self.probability_distribution:
            pd = self.probability_distribution.dump(
                display=False,
                _level=_level + 1,
                _construct_names=_construct_names,
            )
            string.append(pd)

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
        n = self.get_property("uncertainty_component", None)
        if n is not None:
            return f"uncertainty_component={n}"

        n = self.get_property("coverage_interval", None)
        if n is not None:
            return f"coverage_interval={n}"

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
        g = self._iter(
            body=self._identities_iter(
                top_properties=(
                    "uncertainty_component",
                    "coverage_interval",
                    "cf_role",
                    "long_name",
                )
            ),
            **kwargs,
        )
        if generator:
            return g

        return list(g)
