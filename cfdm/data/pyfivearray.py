from .abstract import FileArray
from .mixin import IndexMixin


class PyfiveArray(IndexMixin, FileArray):
    """A netCDF array accessed with `pyfive`.

    Deprecated at version NEXTVERSION and is no longer available. Use
    `{{package}}.XnetcdfArray` instead.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __init__(self, *args, **kwargs):
        class DeprecationError(Exception):
            """Deprecation error."""

        raise DeprecationError(
            f"{self.__class__.__name__} was deprecated at version NEXTVERSION "
            "and is no longer available. Use XnetcdfArray instead."
        )
