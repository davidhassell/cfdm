from ..p5netcdfarray import P5netcdfArray
from .mixin import FragmentFileArrayMixin

class FragmentP5netcdfArray(FragmentFileArrayMixin, P5netcdfArray):
    """Fragment of aggregated data in a file.

    .. versionadded:: (cfdm) NEXTVERSION

    """
1
