from .abstract import MeshArray
from .subarray import CellConnectivitySubarray


class CellConnectivityArray(MeshArray):
    """A connectivity array derived from a UGRID connectivity variable.

    A UGRID connectivity variable contains indices which map each cell
    to its neighbours, as found in a UGRID "edge_edge_connectivty",
    "face_face_connectivty", or "volume_volume_connectivty" variable.

    The connectivity array has one more column than the corresponding
    UGRID variable. The extra column, in the first position, contains
    the identifier for each cell.

    .. versionadded:: (cfdm) TODOUGRIDVER

    .. seealso:: `CellConnectivitySubarray`

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

            cell_connectivity: array_like
                A 2-d integer array that contains indices which map
                each cell to its neighbours, as found in a UGRID
                "edge_edge_connectivty" or "face_face_connectivty"
                variable.

            {{start_index: `int`}}

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        if cell_connectivity is None:
            shape = None
        else:
            shape = cell_connectivity.shape
            shape = (shape[0], shape[1] + 1)

        super().__init__(
            connectivity=cell_connectivity,
            shape=shape,
            compressed_dimensions={1: (1,)},
            start_index=start_index,
            compression_type="cell connectivity",
            source=source,
            copy=copy,
        )
