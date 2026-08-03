from . import Quantization, core, mixin


class Uncertainty(
    mixin.QuantizationMixin,
    mixin.NetCDFVariable,
    mixin.PropertiesData,
    mixin.Files,
    core.Uncertainty
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
        if _title is None:
            _title = "Uncertainty: " + self.identity(default="")

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
