from ..h5pyarray import H5pyArray
from .mixin import FragmentFileArrayMixin


class FragmentH5pyArray(FragmentFileArrayMixin, H5pyArray):
    """A fragment of aggregated data in a file accessed with `h5py`.

    .. versionadded:: (cfdm) NEXTVERSION

    """
