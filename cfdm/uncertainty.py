from . import Quantization, core, mixin
from .decorators import _display_or_return


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

    def __new__(cls, *args, **kwargs):
        """Store component classes."""
        instance = super().__new__(cls)
        instance._Quantization = Quantization
        return instance

    def __str__(self):
        """Called by the `str` built-in function.

        x.__str__() <==> str(x)

        .. versionadded:: (cfdm) 1.7.0

        """
        return self.identity(default=self.nc_get_variable(""))

    @_display_or_return
    def dump(
        self,
        data=None,
        display=True,
        _omit_properties=None,
        _key=None,
        _level=0,
        _title=None,
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
        indent1 = "    " * (_level + 1)

        string = super().dump(
            data=data,
            display=False,
            _key=_key,
            _omit_properties=_omit_properties,
            _level=_level,
            _title=_title,
        )

        #  Probability distribution
        probability_distribution = self.get_probability_distribution()
        name = probability_distribution.get_distribution(None)
        if name is not None:
            string.append(f"{indent1}Distribution:{name}")

        # Probability distribution parameters
        distribution_parameters = sorted(
            probability_distribution.distribution_parameters()
        )
        if _construct_names:
            for term, key in distribution_parameters().items():
                if key in _construct_names:
                    construct_name = (
                        "Distribution Parameter: "
                        + _construct_names.get(key, f"key:{key}")
                    )
                else:
                    construct_name = ""

                string.append(f"{indent1}{term} = {construct_name}")
        else:
            for term, value in distribution_parameters().items():
                string.append(f"{indent1}{term} = {value}")

        # Error correlations
        error_correlations = sorted(self.error_correlations())
        if _construct_names:
            for key in error_correlations:
                name = _construct_names.get(key, f"key:{key}")
                string.append(f"{indent1}Error Correlation: {name}")
        else:
            for identifier in error_correlations:
                string.append(f"{indent1}Error Correlation: {identifier}")

        return "\n".join(string)
