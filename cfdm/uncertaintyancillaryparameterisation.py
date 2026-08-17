from . import core, mixin
from .decorators import _display_or_return


class UncertaintyAncillaryParameterisation(
    mixin.ParametersAncillaries, core.UncertaintyAncillaryParameterisation
):
    """TODOU o collect named parameters andTODOU domain ancillaries.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    @_display_or_return
    def dump(self, display=True, _level=0, _prefix="", _construct_names=None):
        """A full description of the uncertainty ancillary parameterisation.

        Returns a description of all properties, including those of
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

        out = [
            super().dump(
                display=False,
                _title="Data parameterisation:",
                _level=_level,
                _prefix=_prefix,
            )
        ]

        # Uncertainty ancillaries
        if _construct_names is None:
            _construct_names = {}

        for term, key in sorted(self.ancillaries().items()):
            construct_name = _construct_names.get(key, f"key:{key}")
            out.append(
                f"{indent1}{_prefix}{term} = "
                f"Uncertainty ancillary: {construct_name}"
            )

        return "\n".join(out)
