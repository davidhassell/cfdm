from . import core, mixin
from .decorators import _display_or_return


class ErrorCorrelationModel(
    mixin.Parameters,
    core.ErrorCorrelationModel,
):
    """TODO A cell bounds component.

    Specifically, a cell bounds component of a coordinate or domain
    ancillary construct of the CF data model.

    An array of cell bounds spans the same domain axes as its
    coordinate array, with the addition of an extra dimension whose
    size is that of the number of vertices of each cell. This extra
    dimension does not correspond to a domain axis construct since it
    does not relate to an independent axis of the domain. Note that,
    for climatological time axes, the bounds are interpreted in a
    special way indicated by the cell method constructs.

    In the CF data model, a bounds component does not have its own
    properties because they can not logically be different to those of
    the coordinate construct itself. However, it is sometimes desired
    to store attributes on a CF-netCDF bounds variable, so it is also
    allowed to provide properties to a bounds component.

    **NetCDF interface**

    {{netCDF variable}}

    {{netCDF variable group}}

    {{netCDF dataset chunks}}

    .. versionadded:: (cfdm) 1.7.0

    """

    def __bool__(self):
        """TODOU"""
        return (
            super().__bool__()
            or self.get_structure(None) is not None
            or self.get_comment(None) is not None
        )

    def creation_commands(
        self,
        namespace=None,
        indent=0,
        string=True,
        name="p",
        error_correlation_parameter_name="e",
        header=True,
    ):
        """Return the commands that would create the component.

        .. versionadded:: (cfdm) 1.12.2.0

        .. seealso:: `{{package}}.Field.creation_commands`

        :Parameters:

            {{namespace: `str`, optional}}

            {{indent: `int`, optional}}

            {{string: `bool`, optional}}

            {{name: `str`, optional}}

            {{header: `bool`, optional}}

        :Returns:

            {{returns creation_commands}}

        **Examples**

        >>> x = {{package}}.{{class}}({'algorithm': 'granular_bitround'})
        >>> x.nc_set_variable('var')
        >>> print(x.creation_commands(header=False))
        p = {{package}}.{{class}}()
        p.set_parameters({'algorithm': 'granular_bitround'})
        p.nc_set_variable('var')

        """
        if namespace is None:
            namespace = self._package() + "."
        elif namespace and not namespace.endswith("."):
            namespace += "."

        out = []

        if header:
            out.append("#")
            out.append("#")
            out[-1] += f" {self.__class__.__name__.lower()} component"

        out.append(f"{name} = {namespace}{self.__class__.__name__}()")

        structure = self.get_structure(None)
        if structure is not None:
            out.append(f"{name}.set_structure({structure!r})")

        for parameter, value in self.parameters().items():
            try:
                out.extend(
                    value.creation_commands(
                        indent=indent,
                        namespace=namespace,
                        string=False,
                        name=error_correlation_parameter_name,
                        header=False,
                    )
                )
            except (ValueError, TypeError):
                pass
            else:
                value = error_correlation_parameter_name

            out.append(f"{name}.set_parameter({parameter!r}, {value})")

        comment = self.get_comment(None)
        if comment is not None:
            out.append(f"{name}.set_comment({comment!r})")

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
        _title=None,
        _prefix="",
        _level=0,
    ):
        """TODOU A full description of the bounds component.

        Returns a description of all properties and provides selected
        values of all data arrays.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            {{data: `bool` or `None`, optional}}

            display: `bool`, optional
                If False then return the description as a string. By
                default the description is printed.

        :Returns:

            {{returns dump}}

        """
        indent = "    "
        indent0 = indent * _level
        indent1 = indent0 + indent
        indent2 = indent1 + indent

        if _title is None:
            string = [f"{indent0}Error-correlation model:"]
        else:
            string = [indent0 + _title]

        # Structure
        structure = self.get_structure(None)
        if structure is not None:
            string.append(f"{indent1}structure = {structure}")

        # Error-correlation parameters
        for parameter, value in sorted(self.parameters().items()):
            try:
                x = value.dump(
                    data=data,
                    display=False,
                    _level=_level + 1,
                    _create_title=False,
                )
            except (ValueError, TypeError):
                string.append(f"{indent2}{parameter} = {value!r}")
            else:
                string.append(f"{indent2}{parameter}:")
                string.append(x)

        # Comment
        comment = self.get_comment(None)
        if comment is not None:
            string.append(f"{indent1}comment = {comment}")

        return "\n".join(string)
