from .abstract import MeshArray
from .subarray import CellConnectivitySubarray


class CellConnectivityArray(MeshArray):
    """An underlying UGRID connectivity array.

    For edge-edge, face-face and volume-volume connectivty onlu.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __new__(cls, *args, **kwargs):
        """Store subarray classes.

        If a child class requires different subarray classes than the
        ones defined here, then they must be defined in the __new__
        method of the child class.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        instance = super().__new__(cls)
        instance._Subarray = {"cell connectivity": CellConnectivitySubarray}
        return instance

    def __init__(
        self,
        cell_connectivity=None,
        start_index=None,
        source=None,
        copy=True,
    ):
        """**Initialisation**

        :Parameters:

            connectivity: array_like
                TODOUGRID

            start_index: `int`, optional
                TODOUGRID

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        if cell_connectivity is None:
            shape = None
        else:
            shape = cell_connectivity.shape
            shape = (shape[0], shape[1] + 1)

        super().__init__(
            compressed_array=cell_connectivity,
            shape=shape,
            start_index=start_index,
            compression_type="cell connectivity",
            source=source,
            copy=copy,
        )
