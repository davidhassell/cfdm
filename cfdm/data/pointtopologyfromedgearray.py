from math import nan

from ..subarray import PointTopologyFromEdgesSubarray
from .abstract import MeshArray


class PointTopologyFromEdgesArray(MeshArray):
    """An underlying UGRID connectivity array.

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
        instance._Subarray = {
            "point topology from edges": PointTopologyFromEdgeSubarray
        }
        return instance
    
    def __init__(
            edge_node_connectivity=None,
            shape=None,
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
        if shape is None:
            shap = (nan, nan)
            
        super().__init__(
            compressed_array=edge_node_connectivity,
            shape=shape,
            start_index=start_index,            
            compression_type="point topology from edges"
            source=source,
            copy=copy,
        )
