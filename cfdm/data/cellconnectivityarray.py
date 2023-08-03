from itertools import accumulate, product
from numbers import Number

import numpy as np

from ..core.utils import cached_property
from .abstract import ConnectivityArray #CompressedArray
from .subarray import CellConnectivitySubarray


class CellConnectivityArray(ConnectivityArray):
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
        instance._Subarray = {"cell connectivity": CellConnectivitySubarray}
        return instance
