from . import abstract
from .mixin import IndexMixin


class H5netcdfArray(IndexMixin, abstract.FileArray):
    """A netCDF array accessed with `h5netcdf` using the `h5py` backend.

    Deprecated at version NEXTVERSION and is no longer available. Use
    `{{package}}.XnetcdfArray` instead.

    .. versionadded:: (cfdm) 1.11.2.0

    """

    def __init__(self, *args, **kwargs):
        class DeprecationError(Exception):
            """Deprecation error."""

        raise DeprecationError(
            f"{self.__class__.__name__} was deprecated at version NEXTVERSION "
            "and is no longer available. Use XnetcdfArray instead."
        )
