from . import core, mixin
from .decorators import _inplace_enabled, _inplace_enabled_define_and_cleanup


class DomainTopology(
    mixin.NetCDFVariable,
    mixin.PropertiesData,
    mixin.Files,
    core.DomainTopology,
):
    """A domain topology construct of the CF data model.

    TODOUGRID

    See CF Appendix I "The CF Data Model".

    **NetCDF interface**

    {{netCDF variable}}

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self,
        cell_type=None,
        properties=None,
        data=None,
        source=None,
        copy=True,
        _use_data=True,
    ):
        """**Initialisation**

        :Parameters:

            {{init cell_type: `str`, optional}}

            {{init properties: `dict`, optional}}

            {{init data: data_like, optional}}

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            cell_type=cell_type,
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
        """Returns the commands to create the domain topology construct.

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

        cell_type = self.get_cell_type(None)
        if cell_type  is not None:
            out.append(f"{name}.set_cell_type({cell_type!r})")

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
        """A full description of the domain topology construct.

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
            _title = "Domain Topology: " + self.identity(default="")

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

        * The cell type type, preceded by ``'cell_type:'``.
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
        n = self.get_cell_type(None)
        if n is not None:
            pre = ((f"cell_type:{n}",),)
            pre0 = kwargs.pop("pre", None)
            if pre0:
                pre = tuple(pre0) + pre

            kwargs["pre"] = pre

        return super().identities(generator=generator, **kwargs)

    @_inplace_enabled(default=False)
    def normalise(self, start_index=0, inplace=False):
        """Normalise the data values.

        Normalisation does not change the logical content of the
        data. It converts the data so that the set of unique values
        comprises an unbroken sequence from ``0`` to ``N-1`` (if the
        *start_index* parameter is ``0``), or ``1`` to ``N`` (if
        *start_index* is ``1``).

        Normalised data is in a form that may be suitable for creating
        a netCDF UGRID connectivity variable.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            start_index: `int`, optional
                The start index for bounds identification in the
                normalised data. Must be ``0`` (the default) or
                ``1``.

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The normailised domain topology construct, or `None`
                if the operation was in-place.

        **Examples*

        >>> data = {{package}}.Data(
        ...   [[1, 4, 5, 2], [4, 10, 1, -99], [122, 123, 106, 105]]
        ... )
        >>> data[1, 3] = {{package}}.masked
        >>> d = {{package}}.{{class}}(data=data)
        >>> print(d.array)
        [[1 4 5 2]
         [4 10 1 --]
         [122 123 106 105]]
        >>> d0 = d.normalise()
        >>> print(c.array)
        [[0 2 3 1]
         [2 4 0 --]
         [7 8 6 5]]
        >>> (d0.array == d.normalise().array).all()
        True

        >>> d1 = d.normalise(start_index=1)
        >>> print(d1.array)
        [[1 3 4 2]
         [3 5 1 --]
         [8 9 7 6]]
        >>> (d1.array == d0.array + 1).all()
        True

        """
        import numpy as np

        if start_index not in (0, 1):
            raise ValueError(
                "The 'start_index' parameter must be 0 or 1"
            )
        
        d = _inplace_enabled_define_and_cleanup(self)        

        data = d.array
        mask = np.ma.getmaskarray(data)

        if self.get_cell_type() == 'point':
            x = data[:, 0]
            xmin = x.min()
            if xmin < 0:
                x -= xmin

            for i, j in zip(x.tolist(), range(-1, -x.size-1, -1)):
                data = np.where(data == i, j, data)
            
            data *= -1
            if not start_index:
                data -= 1 

            data = np.ma.array(data, mask=mask)

            # Remove redundant cell indices (TODOUGRID - this should
            # be done in getitem)
            largest_index = data[0, -1]
            if data.max() > largest_index:
                data = np.ma.where(data > largest_index, np.ma.masked, data)
                
            # Move missing values to the end of the rows (TODOUGRID -
            # this should be done in getitem)
            data[:, 1:].sort(axis=1, endwith=True)
        else:
            n, b = np.where(~mask)
            data[n, b] = np.unique(data[n, b], return_inverse=True)[1]
            
            if start_index:
                data += 1
                
        d.set_data(data, copy=False)
        return d
