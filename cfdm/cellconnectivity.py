from . import core, mixin
from .decorators import _inplace_enabled, _inplace_enabled_define_and_cleanup


class CellConnectivity(
    mixin.NetCDFVariable,
    mixin.PropertiesData,
    mixin.Files,
    core.CellConnectivity,
):
    """A cell connectivity construct of the CF data model.

    TODOUGRID

    See CF Appendix I "The CF Data Model".

    **NetCDF interface**

    {{netCDF variable}}

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self,
            connectivity=None,
        properties=None,
        data=None,
        source=None,
        copy=True,
        _use_data=True,
    ):
        """**Initialisation**

        :Parameters:

            {{init connectivity: `str`, optional}}

            {{init properties: `dict`, optional}}

            {{init data: data_like, optional}}

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            connectivity=connectivity,
            properties=properties,
            data=data,
            source=source,
            copy=copy,
            _use_data=_use_data,
        )

        self._initialise_netcdf(source)
        self._initialise_original_filenames(source)

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
        """Returns the commands to create the cell connectivity construct.

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

        connectivity = self.get_connectivity(None)
        if connectivity  is not None:
            out.append(f"{name}.set_connectivity({connectivity!r})")

        if string:
            indent = " " * indent
            out[0] = indent + out[0]
            out = ("\n" + indent).join(out)

        return out

    def dump(
        self,
        display=True,
        _omit_properties=None,
        _key=None,
        _level=0,
        _title=None,
        _axes=None,
        _axis_names=None,
    ):
        """A full description of the cell connectivity construct.

        Returns a description of all properties, including those of
        components, and provides selected values of all data arrays.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            {{returns dump}}

        """
        if _title is None:
            _title = "Cell Connectivity: " + self.identity(default="")

        return super().dump(
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

        * The connectivity, preceded by ``'connectivity:'``.
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

        """
        n = self.get_connectivity(None)
        if n is not None:
            return f"connectivity:{n}"

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

        * The connectivity type, preceded by ``'connectivity:'``.
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

        """
        n = self.get_connectivity(None)
        if n is not None:
            pre = ((f"connectivity:{n}",),)
            pre0 = kwargs.pop("pre", None)
            if pre0:
                pre = tuple(pre0) + pre

            kwargs["pre"] = pre

        return super().identities(generator=generator, **kwargs)
