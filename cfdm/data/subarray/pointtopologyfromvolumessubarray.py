import numpy as np

from .abstract import MeshSubarray
from .mixin import PointTopology


class PointTopoologyFromVolumesSubarray(MeshSubarray, PointTopology):
    """A subarray of a compressed 

    A subarray describes a unique part of the uncompressed array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """
    
    def _dddddd(self, node, node_connectivity):
        """ Find all nodes that are joined to *node* by an edge of a volume
        
        .. versionadded:: (cfdm) TODOUGRIDVER
        
        """       
        pass # TODOUGRID
