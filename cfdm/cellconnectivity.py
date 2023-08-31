from . import core, mixin


class CellConnectivity(
    mixin.NetCDFVariable,
    mixin.PropertiesData,
    mixin.Files,
    core.CellConnectivity,
):
    """A cell connectivity construct of the CF data model.

    A cell connectivity construct defines explicitly how cells
    arranged in two or three dimensions in real space but indexed by a
    single domain (discrete) axis are connected. Connectivity can only
    be provided when the domain axis construct also has a domain
    topology construct, and two cells can only be connected if they
    also have a topological relationship. For instance, the
    connectivity of two-dimensional face cells could be characterised
    by whether or not they have shared edges, where the edges are
    defined by connected nodes of the domain topology construct.

    The cell connectivity construct consists of an array recording the
    connectivity, and properties to describe the data. There must be a
    property indicating the condition by which the connectivity is
    derived from the domain topology. The array spans the domain axis
    construct with the addition of a ragged dimension that indexes a
    unique identity for the cell as well as the identities of all
    other cells to which it is connected. The cell identity must be
    recorded in the first element of the ragged dimension, otherwise
    the order is arbitrary. Note that the connectivity array for point
    cells is, by definition, equivalent to the array of the domain
    topology construct.

    When cell connectivity constructs are present they are considered
    to be definitive and must be used in preference to the
    connectivities implied by inspection of any other constructs,
    apart from the domain topology construct, which are not guaranteed
    to be the same.

    In CF-netCDF a cell topology construct can only be provided by a
    UGRID mesh topology variable. The construct array is supplied
    either indirectly by any of the UGRID variables that are used to
    define a domain topology construct, or directly by the UGRID
    "edge_edge_connectivity" variable (for edge cells) or
    "face_face_connectivity" variable (for face cells). In the direct
    case, the integer indices contained in the UGRID variable may be
    used as the cell identities, although the CF data model attaches
    no significance to the values other than the fact that some values
    are the same as others.

    Restricting the types of connectivity to those implied by the
    geospatial topology of the cells precludes connectivity derived
    from any other sources, but is consistent with UGRID encoding
    within CF-netCDF.

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

            {{init connectivty: `str`, optional}}

            {{init properties: `dict`, optional}}

                *Parameter example:*
                  ``properties={'long_name': 'face-face connectivity'}``

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
        if connectivity is not None:
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
