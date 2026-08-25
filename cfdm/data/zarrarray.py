from . import abstract
from .mixin import IndexMixin


class ZarrArray(IndexMixin, abstract.FileArray):
    """A Zarr array accessed with `zarr`.

    Deprecated at version NEXTVERSION and is no longer available. Use
    `{{package}}.XnetcdfArray` instead.

    .. versionadded:: (cfdm) 1.12.2.0

    """

    def __init__(self, *args, **kwargs):
        class DeprecationError(Exception):
            """Deprecation error."""

        raise DeprecationError(
            f"{self.__class__.__name__} was deprecated at version NEXTVERSION "
            "and is no longer available. Use XnetcdfArray instead."
        )
