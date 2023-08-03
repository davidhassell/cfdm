from . import PropertiesData


class Topology(PropertiesData):
    """Mixin class for TODOUGRID dimension or auxiliary coordinate
    constructs.

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def creation_commands(
        self,
        representative_data=False,
        namespace=None,
        indent=0,
        string=True,
        name="c",
        data_name="data",
        header=True,
    ):
        """Returns the commands to create the construct.

        .. versionadded:: (cfdm) TODOUGRIDVER

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

        TODOUGRID

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
        out = super().creation_commands(
            representative_data=representative_data,
            indent=0,
            namespace=namespace,
            string=False,
            name=name,
            data_name=data_name,
            header=header,
        )

        cell_type = self.get_cell_type(None)
        if cell_type is not None:
            out.append(f"{name}.set_cell_type({cell_type!r})")

        if string:
            indent = " " * indent
            out[0] = indent + out[0]
            out = ("\n" + indent).join(out)

        return out

    def identity(self, default=""):
        """Return the canonical identity.

        By default the identity is the first found of the following:

        * The cell type, preceded by ``'cell_type:'``.
        * The ``standard_name`` property.
        * The ``cf_role`` property, preceded by 'cf_role='.
        * The ``long_name`` property, preceded by 'long_name='.
        * The netCDF variable name, preceded by 'ncvar%'.
        * The value of the default parameter.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `identities`

        :Parameters:

            default: optional
                If no identity can be found then return the value of the
                default parameter.

        :Returns:

                The identity.

        **Examples**

        TODOUGRID

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
        n = self.get_cell_type(None)
        if n is not None:
            return f"cell_type:{n}"

        n = self.get_property("standard_name", None)
        if n is not None:
            return n

        for prop in ("cf_role", "long_name"):
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

        * The cell type, preceded by ``'cell_type:'``.
        * The ``standard_name`` property.
        * All properties, preceded by the property name and a colon,
          e.g. ``'long_name:Air temperature'``.
        * The netCDF variable name, preceded by ``'ncvar%'``.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `identity`

        :Parameters:

            generator: `bool`, optional
                If True then return a generator for the identities,
                rather than a list.

            kwargs: optional
                Additional configuration parameters. Currently
                none. Unrecognised parameters are ignored.

        :Returns:

            `list` or generator
                The identities.

        **Examples**

        TODOUGRID

        >>> f = {{package}}.example_field(1)
        >>> c = f.get_construct('cellmeasure0')
        >>> c.get_measure()
        'area'

        >>> c.properties()
        {'units': 'km2'}
        >>> c.nc_get_variable()
        'cell_measure'
        >>> c.identities()
        ['measure:area', 'units=km2', 'ncvar%cell_measure']
        >>> for i in c.identities(generator=True):
        ...     print(i)
        ...
        measure:area
        units=km2
        ncvar%cell_measure

        """
        cell_type = self.get_cell_type(None)
        if cell_type is not None:
            pre = ((f"cell_type:{cell_type}",),)
            pre0 = kwargs.pop("pre", None)
            if pre0:
                pre = tuple(pre0) + pre

            kwargs["pre"] = pre

        return super().identities(generator=generator, **kwargs)
