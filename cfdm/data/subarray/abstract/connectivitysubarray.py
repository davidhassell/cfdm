from ....core.utils import cached_property
from .subarray import Subarray


class ConnectivitySubarray(Subarray):
    """A subarray TODOUGRIDVER of an array compressed by subsampling.

    See CF section 8.3 "Lossy Compression by Coordinate Subsampling"
    and appendix J "Coordinate Interpolation Methods".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    @cached_property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return np.dtype(bool)
