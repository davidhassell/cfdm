from . import core, mixin
from .decorators import _display_or_return


class ProbabilityDistribution(mixin.Parameters, core.ProbabilityDistribution):
    """Mixin to collect named parameters andTODOU domain ancillaries.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __bool__(self):
        """Called by the `bool` built-in function.

        x.__bool__() <==> bool(x)

        .. versionadded:: (cfdm) 1.7.0

        """
        return (
            super().__bool__()
            or bool(self.error_correlations())
            or self.get_distribution(None) is not None
        )

    def creation_commands(
        self, namespace=None, indent=0, string=True, name="p", header=True
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

        """
        out = super().creation_commands(
            indent=indent,
            namespace=namespace,
            string=False,
            name=name,
            header=header,
        )

        distribution = self.get_distribution(None)
        if distribution is not None:
            out.append(f"{name}.set_distribution({distribution!r})")

        error_correlations = self.error_correlations()
        if error_correlations:
            out.append(f"{name}.set_error_correlations({error_correlations})")

        if string:
            indent = " " * indent
            out[0] = indent + out[0]
            out = ("\n" + indent).join(out)

        return out

    @_display_or_return
    def dump(
        self,
        display=True,
        _level=0,
        _title=None,
        _construct_names=None,
    ):
        """A full description of the probability distribution.

        TOOU Returns a description of all properties, including those of
        components, and provides selected values of all data arrays.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

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
            string = [f"{indent0}Probability distribution:"]
        else:
            string = [f"{indent0}{_title}"]

        # Distribution
        distribution = self.get_distribution(None)
        if distribution is not None:
            string.append(f"{indent1}distribution = {distribution}")

        # Distribution parameters
        distribution_parameters = sorted(self.parameters().items())
        if distribution_parameters:
            string.append(f"{indent1}Distribution parameters:")
            if _construct_names:
                for term, key in distribution_parameters:
                    if key in _construct_names:
                        name = _construct_names.get(key, f"key:{key}")
                        name = f"Uncertainty Ancillary: {name}"
                    else:
                        name = ""

                    string.append(f"{indent2}{term} = {name}")
            else:
                for term, value in distribution_parameters:
                    string.append(f"{indent2}{term} = {value}")

        # Error correlations
        error_correlations = sorted(self.error_correlations())
        if error_correlations:
            string.append(f"{indent1}Error correlations:")
            if _construct_names:
                for key in error_correlations:
                    if key in _construct_names:
                        name = _construct_names.get(key, f"key:{key}")
                    else:
                        name = ""

                    string.append(f"{indent2}Uncertainty Ancillary: {name}")
            else:
                for value in error_correlations:
                    string.append(f"{indent2}Uncertainty Ancillary: {value}")

        return "\n".join(string)
