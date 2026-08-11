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
