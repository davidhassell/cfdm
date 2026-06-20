from ..zarrarray import ZarrArray
from .mixin import FragmentFileArrayMixin


class FragmentZarrArray(FragmentFileArrayMixin, ZarrArray):
    """A fragment of aggregated data in a file accessed with `zarr`.

    Deprecated at version NEXTVERSION and is no longer available. Use
    `{{package}}.FragmentXnetcdfArray` instead.

    .. versionadded:: (cfdm) 1.13.0.0

    """

    def __init__(self, *args, **kwargs):
        class DeprecationError(Exception):
            """Deprecation error."""

        raise DeprecationError(
            f"{self.__class__.__name__} was deprecated at version NEXTVERSION "
            "and is no longer available. Use FragmentXnetcdfArray instead."
        )
