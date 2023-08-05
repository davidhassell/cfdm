from .numpyarray import NumpyArray


class SparseArray(NumpyArray):
    """TODOUGRID An underlying array stored in a netCDF file.

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    @property
    def array(self):
        return self._get_component("array").toarray()

    @property
    def sparse_array(self):
        return self._get_component("array").copy()
