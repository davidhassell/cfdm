from math import nan

from ..subarray import (
    PointTopologyFromEdgesSubarray,
    PointTopologyFromFacesSubarray,
)
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
            "point topology from edges": PointTopologyFromEdgesSubarray,
            "point topology from faces": PointTopologyFromFacesSubarray,
        }
        return instance

    def __init__(
        edge_node_connectivity=None,
        face_node_connectivity=None,
        shape=None,
        start_index=None,
        source=None,
        copy=True,
    ):
        """**Initialisation**

        :Parameters:

            edge_node_connectivity: array_like, optional
                TODOUGRID

            face_node_connectivity: array_like, optional
                TODOUGRID

            shape
                The shape of the point cell domain topology array. If
                the shape is unknown then set to `None`, which will
                result in a shape of ``(math.nan, math.nan)``.

            {{start_index: `int`, optional}}

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        if edge_node_connectivity is None:
            node_connectivity = edge_node_connectivity
            compression_type = ("point topology from edges",)
        elif edge_node_connectivity is None:
            node_connectivity = face_node_connectivity
            compression_type = ("point topology from faces",)
        else:
            node_connectivity = None
            compression_type = None

        if shape is None:
            shape = (nan, nan)

        super().__init__(
            compressed_array=node_connectivity,
            shape=shape,
            compressed_dimensions={0: (0,), 1: (1,)},
            compression_type=compression_type,
            start_index=start_index,
            source=source,
            copy=copy,
        )
